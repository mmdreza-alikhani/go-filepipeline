package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// ErrHardShutdownTimeout means the workers were still alive after the forced
// cancellation deadline. The process returns the error and main exits non zero;
// every job that was mid flight is still described by its journal entry.
var ErrHardShutdownTimeout = errors.New("pipeline: hard shutdown deadline exceeded")

// Shutdown is deliberately two phase, because closing the channels in the wrong
// order either loses jobs or stops the draining workers.
//
// Phase 1 - stop the source, then let the chain drain:
//
//	cancel the source context      -> the watcher returns, closes queue[0]
//	queue[0] closed                -> scan workers finish the buffered jobs, exit
//	scan's closer closes queue[1]  -> convert drains, closes queue[2]
//	...                            -> compress closes queue[n]
//	sink drains queue[n]           -> every published artefact is complete
//
// Because a stage's workers are the only senders of its output channel, the
// close travels strictly upstream to downstream and no send can hit a closed
// channel.
//
// Phase 2 - force: if the grace deadline (or a second signal) arrives first, the
// root context is cancelled. Every stage aborts, the workers return, the closers
// still close their channels, and run() returns.
//
// What may be left half done, and what the next start does:
//
//   - A job whose stage was interrupted stays "running" in jobs/<id>.json. Its
//     scratch directory may exist under <work_dir>/tmp/<id>, and any staging
//     file it was writing lives in out/.tmp (never in out/).
//   - On restart, recovery.mode=requeue (the default) resets the job and replays
//     the whole chain. That is safe because every stage is a pure function of the
//     input and publish is content addressed: if the artefact already exists,
//     scan's probe skips the job and it is marked done without redoing work.
//     recovery.mode=resume first tries to publish a job whose compressed artefact
//     survived, then requeues the rest.
//   - Nothing is ever lost silently: a job is either done, failed (dead letter),
//     or still running in the journal.
func (p *Pipeline) Supervise(ctx context.Context, sigCh <-chan os.Signal) error {
	runErr := make(chan error, 1)
	go func() { runErr <- p.run(ctx) }()

	select {
	case err := <-runErr:
		return err
	case <-ctx.Done():
		p.o.Log.Warn("shutdown: phase 1, stopping the source and draining",
			"grace", p.o.Config.Shutdown.Grace.D().String())
	case <-sigCh:
		p.o.Log.Warn("shutdown: phase 1, stopping the source and draining",
			"grace", p.o.Config.Shutdown.Grace.D().String())
	}

	p.StopSource()
	return p.drain(runErr, sigCh)
}

// Run supervises the pipeline until it finishes, the context is cancelled, or
// SIGINT/SIGTERM arrives.
func (p *Pipeline) Run(ctx context.Context) error {
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	return p.Supervise(ctx, sigCh)
}

func (p *Pipeline) drain(runErr <-chan error, sigCh <-chan os.Signal) error {
	grace := p.o.Config.Shutdown.Grace.D()
	if grace <= 0 {
		grace = 30 * time.Second
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()

	for {
		select {
		case err := <-runErr:
			p.o.Log.Info("shutdown: drained cleanly")
			return err
		case <-sigCh:
			p.o.Log.Warn("shutdown: second signal, forcing cancellation")
			p.ForceCancel()
			return p.awaitHard(runErr, "second signal")
		case <-timer.C:
			p.o.Log.Warn("shutdown: grace deadline exceeded, forcing cancellation",
				"grace", grace.String())
			p.ForceCancel()
			return p.awaitHard(runErr, "grace deadline")
		}
	}
}

func (p *Pipeline) awaitHard(runErr <-chan error, cause string) error {
	hard := p.o.Config.Shutdown.Hard.D()
	if hard <= 0 {
		hard = 5 * time.Second
	}
	timer := time.NewTimer(hard)
	defer timer.Stop()

	select {
	case err := <-runErr:
		p.o.Log.Info("shutdown: forced cancel completed")
		return err
	case <-timer.C:
		p.o.Log.Error("shutdown: workers ignored cancellation, exiting anyway",
			"hard", hard.String(), "cause", cause)
		return fmt.Errorf("%w (cause: %s)", ErrHardShutdownTimeout, cause)
	}
}

// StopSource starts phase 1. It is idempotent and safe to call before run() has
// installed the cancel function.
func (p *Pipeline) StopSource() {
	p.mu.Lock()
	p.stopRequested = true
	cancel := p.cancelSource
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// ForceCancel starts phase 2. It is idempotent.
func (p *Pipeline) ForceCancel() {
	p.mu.Lock()
	if p.forceDone {
		p.mu.Unlock()
		return
	}
	p.forceDone = true
	cancel := p.cancelAll
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}
