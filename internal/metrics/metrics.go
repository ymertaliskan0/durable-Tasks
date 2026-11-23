// Package metrics exposes worker and queue metrics in the Prometheus format.
//
// Worker metrics are collected by wrapping the worker's store (Instrument),
// so the worker itself has no metrics code. Queue depth is read from the
// database whenever /metrics is scraped.
package metrics

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ymertaliskan0/durable-tasks/internal/storage"
	"github.com/ymertaliskan0/durable-tasks/internal/tasks"
)

// Metrics owns a private Prometheus registry with the durable-tasks metrics.
type Metrics struct {
	registry          *prometheus.Registry
	claimed           *prometheus.CounterVec
	outcomes          *prometheus.CounterVec
	duration          *prometheus.HistogramVec
	reclaimed         prometheus.Counter
	heartbeatFailures prometheus.Counter
	lostOwnership     prometheus.Counter
}

// New creates the metrics. Go runtime and process metrics are included.
func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		claimed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "durable_tasks_claimed_total",
			Help: "Tasks claimed by this worker, by task type and claimed status.",
		}, []string{"type", "status"}),
		outcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "durable_tasks_outcomes_total",
			Help: "Outcomes written by this worker, by task type and outcome.",
		}, []string{"type", "outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "durable_tasks_execution_seconds",
			Help:    "Time from claim to outcome write, by task type and outcome.",
			Buckets: prometheus.ExponentialBuckets(0.005, 4, 10), // 5ms .. ~22min
		}, []string{"type", "outcome"}),
		reclaimed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "durable_tasks_reclaimed_total",
			Help: "Tasks with expired leases reclaimed by this worker's stalker.",
		}),
		heartbeatFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "durable_tasks_heartbeat_failures_total",
			Help: "Heartbeats that failed for a reason other than lost ownership.",
		}),
		lostOwnership: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "durable_tasks_lost_ownership_total",
			Help: "Fenced writes rejected because another worker took the task over.",
		}),
	}
	m.registry.MustRegister(
		m.claimed, m.outcomes, m.duration, m.reclaimed, m.heartbeatFailures, m.lostOwnership,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Handler serves the metrics in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// StatusCounter is the part of the store the queue-depth gauge needs.
type StatusCounter interface {
	CountByStatus(ctx context.Context) (map[storage.Status]int, error)
}

// WatchQueue adds a durable_tasks_queue_depth gauge, by status, that is read
// from store on every scrape.
func (m *Metrics) WatchQueue(store StatusCounter) {
	m.registry.MustRegister(&queueCollector{store: store})
}

// Instrument wraps store so that every call the worker makes updates the
// metrics.
func (m *Metrics) Instrument(store tasks.Store) tasks.Store {
	return &instrumented{Store: store, m: m, claims: make(map[execKey]claim)}
}

type execKey struct {
	id  uuid.UUID
	gen int64
}

type claim struct {
	taskType string
	at       time.Time
}

// instrumented embeds tasks.Store, so it has every Store method; the ones
// below are overridden to record metrics and then call the wrapped store.
type instrumented struct {
	tasks.Store
	m *Metrics

	mu     sync.Mutex
	claims map[execKey]claim // in-flight executions, for the duration histogram
}

// maxTracked bounds the claims map: executions that never write an outcome
// (abandoned after losing ownership) would otherwise stay in it forever.
const maxTracked = 10_000

func (s *instrumented) ClaimNext(ctx context.Context, owner string, lease time.Duration) (storage.TaskState, error) {
	ts, err := s.Store.ClaimNext(ctx, owner, lease)
	if err == nil {
		s.m.claimed.WithLabelValues(ts.Type, string(ts.Status)).Inc()
		s.mu.Lock()
		if len(s.claims) >= maxTracked {
			clear(s.claims)
		}
		s.claims[execKey{ts.ID, ts.Generation}] = claim{ts.Type, time.Now()}
		s.mu.Unlock()
	}
	return ts, err
}

func (s *instrumented) Heartbeat(ctx context.Context, id uuid.UUID, gen int64, lease time.Duration) error {
	err := s.Store.Heartbeat(ctx, id, gen, lease)
	s.observeError(err, true)
	return err
}

func (s *instrumented) SaveState(ctx context.Context, id uuid.UUID, gen int64, state []byte) error {
	err := s.Store.SaveState(ctx, id, gen, state)
	s.observeError(err, false)
	return err
}

func (s *instrumented) Finish(ctx context.Context, id uuid.UUID, gen int64, state []byte) error {
	return s.outcome("finish", id, gen, s.Store.Finish(ctx, id, gen, state))
}

func (s *instrumented) ScheduleRetry(ctx context.Context, id uuid.UUID, gen int64, state []byte, lastErr string, delay time.Duration) error {
	return s.outcome("retry", id, gen, s.Store.ScheduleRetry(ctx, id, gen, state, lastErr, delay))
}

func (s *instrumented) Fail(ctx context.Context, id uuid.UUID, gen int64, state []byte, lastErr string) error {
	return s.outcome("fail", id, gen, s.Store.Fail(ctx, id, gen, state, lastErr))
}

func (s *instrumented) MarkCancelled(ctx context.Context, id uuid.UUID, gen int64) error {
	return s.outcome("cancelled", id, gen, s.Store.MarkCancelled(ctx, id, gen))
}

func (s *instrumented) Release(ctx context.Context, id uuid.UUID, gen int64) error {
	return s.outcome("release", id, gen, s.Store.Release(ctx, id, gen))
}

func (s *instrumented) ReclaimExpired(ctx context.Context, limit int) (int, error) {
	n, err := s.Store.ReclaimExpired(ctx, limit)
	s.m.reclaimed.Add(float64(n))
	return n, err
}

// outcome records one outcome write and passes err through.
func (s *instrumented) outcome(name string, id uuid.UUID, gen int64, err error) error {
	s.mu.Lock()
	c, ok := s.claims[execKey{id, gen}]
	delete(s.claims, execKey{id, gen})
	s.mu.Unlock()

	if err != nil {
		s.observeError(err, false)
		return err
	}
	if ok {
		s.m.outcomes.WithLabelValues(c.taskType, name).Inc()
		s.m.duration.WithLabelValues(c.taskType, name).Observe(time.Since(c.at).Seconds())
	}
	return nil
}

func (s *instrumented) observeError(err error, heartbeat bool) {
	switch {
	case err == nil:
	case errors.Is(err, storage.ErrWrongGeneration):
		s.m.lostOwnership.Inc()
	case heartbeat && !errors.Is(err, context.Canceled):
		s.m.heartbeatFailures.Inc()
	}
}

// queueCollector reports the number of tasks per status, queried on scrape.
type queueCollector struct {
	store StatusCounter
}

var queueDepthDesc = prometheus.NewDesc(
	"durable_tasks_queue_depth", "Tasks in the database, by status.", []string{"status"}, nil)

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) { ch <- queueDepthDesc }

func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	counts, err := c.store.CountByStatus(ctx)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(queueDepthDesc, err)
		return
	}
	for _, s := range []storage.Status{
		storage.StatusReadyToRun, storage.StatusRunning, storage.StatusFinished, storage.StatusFailed,
		storage.StatusReadyToCancel, storage.StatusCancelling, storage.StatusCancelled,
	} {
		ch <- prometheus.MustNewConstMetric(queueDepthDesc, prometheus.GaugeValue, float64(counts[s]), string(s))
	}
}
