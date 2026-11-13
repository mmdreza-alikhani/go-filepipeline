// Command pipeline runs the file processing service: inbox -> scan -> convert
// -> compress -> atomic publish, with a durable journal and a dead letter queue.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"example.com/filepipeline/internal/config"
	"example.com/filepipeline/internal/job"
	"example.com/filepipeline/internal/obs"
	"example.com/filepipeline/internal/pipeline"
	"example.com/filepipeline/internal/sink"
	"example.com/filepipeline/internal/source"
	"example.com/filepipeline/internal/stage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "pipeline: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath := flag.String("config", filepath.Join("configs", "config.example.json"), "path to the JSON configuration file")
	metricsAddr := flag.String("metrics-addr", "", "override the configured metrics listen address")
	printCfg := flag.Bool("print-config", false, "print the resolved configuration as JSON and exit")
	limit := flag.Int("limit", 0, "stop after emitting N jobs (0 = run until interrupted); useful for batch runs and demos")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	if *printCfg {
		data, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}

	level, err := obs.ParseLevel(cfg.LogLevel)
	if err != nil {
		return err
	}
	logger := obs.NewLogger(os.Stdout, level, strings.EqualFold(cfg.LogFormat, "json"))
	metrics := obs.NewMetrics()
	clock := job.RealClock{}

	store, err := job.NewStore(cfg.JournalDir(), logger)
	if err != nil {
		return err
	}
	publisher, err := sink.NewPublisher(sink.PublisherOptions{
		OutDir: cfg.OutDir, Log: logger, Metrics: metrics, Clock: clock,
	})
	if err != nil {
		return err
	}
	deadLetters, err := sink.NewDeadLetter(sink.DeadLetterOptions{
		Dir: cfg.FailedDir, Log: logger, Metrics: metrics, Clock: clock,
	})
	if err != nil {
		return err
	}

	stopMetrics, err := startMetrics(firstNonEmpty(*metricsAddr, cfg.MetricsAddr), metrics, logger)
	if err != nil {
		return err
	}
	defer stopMetrics()

	replay, err := recoverJournal(context.Background(), cfg, store, publisher, logger, clock)
	if err != nil {
		return err
	}

	stages, err := stage.Build(stage.Deps{
		Config: *cfg, Log: logger, Metrics: metrics, Clock: clock, Probe: publisher,
	})
	if err != nil {
		return err
	}

	watcher, err := source.NewPolling(source.Options{
		InboxDir:  cfg.InboxDir,
		Interval:  cfg.PollInterval.D(),
		StableAge: cfg.StableAge.D(),
		Log:       logger,
		Clock:     clock,
		Metrics:   metrics,
	})
	if err != nil {
		return err
	}
	var src source.Source = watcher
	if len(replay) > 0 {
		src = source.NewReplay(src, replay, logger)
	}
	if *limit > 0 {
		src = source.NewLimit(src, *limit, logger)
	}

	p, err := pipeline.New(pipeline.Options{
		Config:  *cfg,
		Log:     logger,
		Metrics: metrics,
		Store:   store,
		Clock:   clock,
		Rand:    job.NewRand(time.Now().UnixNano()),
		Source:  src,
		Stages:  stages,
		Sink:    &fanout{publish: publisher, fail: deadLetters},
	})
	if err != nil {
		return err
	}

	logger.Info("pipeline starting",
		"stages", cfg.Describe(),
		"inbox", cfg.InboxDir,
		"out", cfg.OutDir,
		"journal", cfg.JournalDir(),
		"replayed", len(replay),
		"recovery_mode", cfg.Recovery.Mode,
		"queue_size", cfg.QueueSize,
		"max_file_size", cfg.MaxFileSize,
		"max_inflight_bytes", cfg.MaxInflightBytes)

	err = p.Run(context.Background())

	completed := metrics.CounterValue("pipeline_jobs_completed_total", map[string]string{"stage": string(job.StagePublish)})
	failed := metrics.CounterValue("pipeline_jobs_failed_total", map[string]string{"stage": string(job.StageScan)}) +
		metrics.CounterValue("pipeline_jobs_failed_total", map[string]string{"stage": string(job.StageConvert)}) +
		metrics.CounterValue("pipeline_jobs_failed_total", map[string]string{"stage": string(job.StageCompress)}) +
		metrics.CounterValue("pipeline_jobs_failed_total", map[string]string{"stage": string(job.StagePublish)})
	skipped := metrics.CounterValue("pipeline_jobs_skipped_total", map[string]string{"stage": string(job.StageScan)})
	retries := metrics.CounterValue("pipeline_retries_total", map[string]string{"stage": string(job.StageScan)}) +
		metrics.CounterValue("pipeline_retries_total", map[string]string{"stage": string(job.StageConvert)}) +
		metrics.CounterValue("pipeline_retries_total", map[string]string{"stage": string(job.StageCompress)})
	logger.Info("pipeline stopped", "published", completed, "dead_lettered", failed, "skipped", skipped, "retries", retries)
	return err
}

// fanout lets the pipeline see one Sink while main keeps the two exits separate.
type fanout struct {
	publish *sink.Publisher
	fail    *sink.DeadLetter
}

func (f *fanout) Publish(ctx context.Context, j *job.Job) error { return f.publish.Publish(ctx, j) }

func (f *fanout) Fail(ctx context.Context, j *job.Job, st job.Stage, cause error) error {
	return f.fail.Fail(ctx, j, st, cause)
}

// recoverJournal decides what happens to jobs found on disk at startup.
//
// requeue (default): drop the scratch directory, reset the job and replay the
// whole chain. Cheap and always correct, because a finished artefact is detected
// by the content addressed probe in scan.
//
// resume: if the compressed artefact survived, publish it directly and skip the
// pipeline entirely; everything else is requeued.
func recoverJournal(ctx context.Context, cfg *config.Config, store *job.Store, pub *sink.Publisher, log *slog.Logger, clk job.Clock) ([]*job.Job, error) {
	all, err := store.List(ctx)
	if err != nil {
		return nil, err
	}
	var replay []*job.Job
	for _, j := range all {
		if j.Terminal() {
			dropScratch(cfg, j, log)
			continue
		}
		if strings.EqualFold(cfg.Recovery.Mode, "resume") && j.ArtifactReady() {
			if _, err := os.Stat(j.Artifact); err == nil {
				log.Info("recovery: publishing an artefact that survived the crash", "job", j.ID)
				if err := pub.Publish(ctx, j); err != nil {
					log.Warn("recovery: publish failed, requeueing", "job", j.ID, "err", err)
				} else {
					j.MarkDone(clk.Now())
					if err := store.Save(ctx, j.Clone()); err != nil {
						return nil, err
					}
					dropScratch(cfg, j, log)
					continue
				}
			}
		}
		dropScratch(cfg, j, log)
		j.Reset(clk.Now())
		if err := store.Save(ctx, j.Clone()); err != nil {
			return nil, err
		}
		replay = append(replay, j)
	}
	if len(replay) > 0 {
		log.Info("recovery: requeueing interrupted jobs", "count", len(replay), "mode", cfg.Recovery.Mode)
	}
	return replay, nil
}

// dropScratch removes a job's scratch directory, but only if it really is below
// the configured temp root: a corrupt journal must not turn into an rm -rf.
func dropScratch(cfg *config.Config, j *job.Job, log *slog.Logger) {
	if j.TempDir == "" {
		return
	}
	root := filepath.Clean(cfg.TempRoot())
	dir := filepath.Clean(j.TempDir)
	rel, err := filepath.Rel(root, dir)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		log.Warn("recovery: refusing to remove a scratch directory outside the temp root",
			"job", j.ID, "dir", j.TempDir)
		return
	}
	if err := os.RemoveAll(dir); err != nil {
		log.Warn("recovery: could not remove scratch directory", "job", j.ID, "dir", j.TempDir, "err", err)
	}
}

func startMetrics(addr string, metrics *obs.Metrics, log *slog.Logger) (func(), error) {
	if addr == "" {
		log.Info("metrics endpoint disabled")
		return func() {}, nil
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics: listen on %s: %w", addr, err)
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server stopped", "err", err)
		}
	}()
	log.Info("metrics endpoint listening", "addr", ln.Addr().String(), "path", "/metrics")
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
