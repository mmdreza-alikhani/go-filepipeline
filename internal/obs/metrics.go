package obs

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// LatencyBuckets are the default histogram buckets for stage latency, in seconds.
var LatencyBuckets = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
}

type metricKind uint8

const (
	kindCounter metricKind = iota
	kindGauge
	kindHistogram
)

func (k metricKind) String() string {
	switch k {
	case kindCounter:
		return "counter"
	case kindGauge:
		return "gauge"
	case kindHistogram:
		return "histogram"
	default:
		return "untyped"
	}
}

type series struct {
	labels  string
	value   float64
	live    func() float64
	buckets []uint64
	sum     float64
	count   uint64
}

type family struct {
	name   string
	help   string
	kind   metricKind
	bounds []float64

	mu     sync.Mutex
	order  []string
	series map[string]*series
}

// snapshot is a lock free copy of one series, taken while the family lock is held.
type snapshot struct {
	labels  string
	value   float64
	buckets []uint64
	sum     float64
	count   uint64
}

// Metrics is a minimal, dependency free Prometheus text-format registry.
//
// It deliberately implements only what the pipeline needs: counters, gauges,
// gauges backed by a callback (used for live queue depth), and cumulative
// histograms. All updates are serialised per metric family, which is cheap
// compared to the file IO the pipeline performs.
type Metrics struct {
	mu       sync.RWMutex
	families map[string]*family
	order    []string
}

// NewMetrics returns an empty registry.
func NewMetrics() *Metrics {
	return &Metrics{families: make(map[string]*family)}
}

func (m *Metrics) family(name, help string, kind metricKind, bounds []float64) *family {
	m.mu.Lock()
	defer m.mu.Unlock()
	if f, ok := m.families[name]; ok {
		return f
	}
	f := &family{
		name:   name,
		help:   help,
		kind:   kind,
		bounds: append([]float64(nil), bounds...),
		series: make(map[string]*series),
	}
	m.families[name] = f
	m.order = append(m.order, name)
	return f
}

func (f *family) get(key string, labels map[string]string) *series {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.series[key]
	if !ok {
		s = &series{labels: key}
		if f.kind == kindHistogram {
			s.buckets = make([]uint64, len(f.bounds))
		}
		f.series[key] = s
		f.order = append(f.order, key)
	}
	return s
}

// IncCounter adds v to the counter identified by name and labels.
func (m *Metrics) IncCounter(name, help string, labels map[string]string, v float64) {
	f := m.family(name, help, kindCounter, nil)
	s := f.get(encodeLabels(labels), labels)
	f.mu.Lock()
	s.value += v
	f.mu.Unlock()
}

// SetGauge sets the gauge identified by name and labels to v.
func (m *Metrics) SetGauge(name, help string, labels map[string]string, v float64) {
	f := m.family(name, help, kindGauge, nil)
	s := f.get(encodeLabels(labels), labels)
	f.mu.Lock()
	s.value = v
	f.mu.Unlock()
}

// RegisterGaugeFunc exposes a live value: fn is called on every scrape, so
// queue depth and in-flight bytes never need a periodic sampler goroutine.
func (m *Metrics) RegisterGaugeFunc(name, help string, labels map[string]string, fn func() float64) {
	f := m.family(name, help, kindGauge, nil)
	s := f.get(encodeLabels(labels), labels)
	f.mu.Lock()
	s.live = fn
	f.mu.Unlock()
}

// ObserveHistogram records one observation in seconds.
func (m *Metrics) ObserveHistogram(name, help string, bounds []float64, labels map[string]string, v float64) {
	f := m.family(name, help, kindHistogram, bounds)
	s := f.get(encodeLabels(labels), labels)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.bounds == nil || len(f.bounds) != len(s.buckets) {
		// A second registration with different buckets would corrupt the
		// cumulative counts; keep the first bucket layout and ignore the rest.
		return
	}
	for i, b := range f.bounds {
		if v <= b {
			s.buckets[i]++
		}
	}
	s.count++
	s.sum += v
}

// CounterValue reads a counter back. It exists for assertions in tests and for
// the operator summary logged at shutdown.
func (m *Metrics) CounterValue(name string, labels map[string]string) float64 {
	m.mu.RLock()
	f := m.families[name]
	m.mu.RUnlock()
	if f == nil {
		return 0
	}
	key := encodeLabels(labels)
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.series[key]; ok {
		return s.value
	}
	return 0
}

// Handler serves the registry in Prometheus text exposition format.
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := m.WriteProm(w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

// WriteProm renders every family, sorted by name, into w.
func (m *Metrics) WriteProm(w io.Writer) error {
	m.mu.RLock()
	fams := make([]*family, 0, len(m.order))
	for _, name := range m.order {
		fams = append(fams, m.families[name])
	}
	m.mu.RUnlock()
	sort.Slice(fams, func(i, j int) bool { return fams[i].name < fams[j].name })

	var b strings.Builder
	for _, f := range fams {
		f.mu.Lock()
		snaps := make([]snapshot, 0, len(f.order))
		for _, key := range f.order {
			s := f.series[key]
			v := s.value
			if s.live != nil {
				v = s.live()
			}
			snaps = append(snaps, snapshot{
				labels:  s.labels,
				value:   v,
				buckets: append([]uint64(nil), s.buckets...),
				sum:     s.sum,
				count:   s.count,
			})
		}
		f.mu.Unlock()
		if len(snaps) == 0 {
			continue
		}

		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", f.name, escapeHelp(f.help), f.name, f.kind)
		for _, s := range snaps {
			switch f.kind {
			case kindHistogram:
				cumulative := uint64(0)
				for i, bound := range f.bounds {
					cumulative = s.buckets[i]
					fmt.Fprintf(&b, "%s_bucket%s %d\n", f.name, withLE(s.labels, bound), cumulative)
				}
				fmt.Fprintf(&b, "%s_bucket%s %d\n", f.name, withLE(s.labels, math.Inf(1)), s.count)
				fmt.Fprintf(&b, "%s_sum%s %s\n", f.name, s.labels, formatFloat(s.sum))
				fmt.Fprintf(&b, "%s_count%s %d\n", f.name, s.labels, s.count)
			default:
				fmt.Fprintf(&b, "%s%s %s\n", f.name, s.labels, formatFloat(s.value))
			}
		}
	}
	if b.Len() == 0 {
		_, err := io.WriteString(w, "# no metrics recorded yet\n")
		return err
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func formatFloat(v float64) string {
	if v != v {
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// withLE appends the histogram le label to an encoded label set.
func withLE(labels string, bound float64) string {
	le := `le="` + formatFloat(bound) + `"`
	if labels == "" {
		return "{" + le + "}"
	}
	return labels[:len(labels)-1] + "," + le + "}"
}

func encodeLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(labels[k]))
		b.WriteString(`"`)
	}
	b.WriteByte('}')
	return b.String()
}

func escapeLabelValue(v string) string {
	if !strings.ContainsAny(v, "\\\"\n") {
		return v
	}
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(v)
}

func escapeHelp(v string) string {
	r := strings.NewReplacer(`\`, `\\`, "\n", `\n`)
	return r.Replace(v)
}
