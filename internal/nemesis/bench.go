package nemesis

import (
	"context"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/ymertaliskan0/durable-tasks/internal/storage"
	"github.com/ymertaliskan0/durable-tasks/internal/tasks"
	"github.com/ymertaliskan0/durable-tasks/internal/tasks/sleeptask"
)

// BenchConfig describes one benchmark run.
type BenchConfig struct {
	DSN         string
	Timeout     time.Duration // give up after this long (0 means 10 minutes)
	Tasks       int           // zero-duration sleep tasks to execute
	Workers     int           // in-process workers, each with its own lease owner
	Concurrency int           // task slots per worker
}

// BenchReport is the outcome of a benchmark run.
type BenchReport struct {
	Config    BenchConfig
	Elapsed   time.Duration // from starting the workers until the last task finished
	PerSecond float64       // tasks finished per second
	P50, P99  time.Duration // per-task overhead: claim to finish, including checkpoint and finish writes
}

// Bench measures the scheduler's own overhead: how many trivial tasks per
// second the workers can claim, checkpoint and finish.
//
// Returns the report, or an error if the benchmark could not run.
func Bench(ctx context.Context, cfg BenchConfig) (BenchReport, error) {
	rep := BenchReport{Config: cfg}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	dsn, drop, err := scratchDB(ctx, cfg.DSN, "bench")
	if err != nil {
		return rep, err
	}
	defer drop()

	store, err := storage.Open(ctx, dsn)
	if err != nil {
		return rep, err
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		return rep, err
	}

	for range cfg.Tasks {
		if _, _, err := store.CreateTask(ctx, storage.NewTask{
			Type: sleeptask.Type, Request: []byte(`{"steps":1,"step_duration":0}`),
		}); err != nil {
			return rep, err
		}
	}

	timed := &timedStore{Store: store, claimed: make(map[uuid.UUID]time.Time), want: cfg.Tasks, done: make(chan struct{})}
	reg := tasks.NewRegistry()
	reg.Register(sleeptask.Type, sleeptask.New)

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	var wg sync.WaitGroup
	start := time.Now()
	for i := range cfg.Workers {
		wcfg := tasks.DefaultConfig()
		wcfg.WorkerID = fmt.Sprintf("bench-%d", i)
		wcfg.Concurrency = cfg.Concurrency
		wcfg.PollInterval = 5 * time.Millisecond
		w, err := tasks.NewWorker(timed, reg, wcfg, nil)
		if err != nil {
			return rep, err
		}
		wg.Go(func() { _ = w.Run(runCtx) })
	}

	select {
	case <-timed.done:
	case <-ctx.Done():
		stop()
		wg.Wait()
		return rep, fmt.Errorf("bench: %d of %d tasks finished before giving up: %w", timed.finished(), cfg.Tasks, ctx.Err())
	}
	rep.Elapsed = time.Since(start)
	stop()
	wg.Wait()

	slices.Sort(timed.latencies)
	rep.PerSecond = float64(cfg.Tasks) / rep.Elapsed.Seconds()
	rep.P50 = percentile(timed.latencies, 0.50)
	rep.P99 = percentile(timed.latencies, 0.99)
	return rep, nil
}

// WriteBenchReport prints a one-paragraph summary of rep to w.
func WriteBenchReport(w io.Writer, rep BenchReport) error {
	c := rep.Config
	_, err := fmt.Fprintf(w, "durable-tasks benchmark: %d tasks, %d workers x %d slots\n"+
		"  %.0f tasks/s (%s total); per-task overhead p50 %s, p99 %s\n",
		c.Tasks, c.Workers, c.Concurrency, rep.PerSecond, rep.Elapsed.Round(time.Millisecond),
		rep.P50.Round(10*time.Microsecond), rep.P99.Round(10*time.Microsecond))
	return err
}

// timedStore records claim-to-finish times and signals when want tasks have
// finished.
type timedStore struct {
	tasks.Store

	mu        sync.Mutex
	claimed   map[uuid.UUID]time.Time
	latencies []time.Duration
	want      int
	done      chan struct{}
}

func (s *timedStore) ClaimNext(ctx context.Context, owner string, lease time.Duration) (storage.TaskState, error) {
	ts, err := s.Store.ClaimNext(ctx, owner, lease)
	if err == nil {
		s.mu.Lock()
		s.claimed[ts.ID] = time.Now()
		s.mu.Unlock()
	}
	return ts, err
}

func (s *timedStore) Finish(ctx context.Context, id uuid.UUID, gen int64, state []byte) error {
	err := s.Store.Finish(ctx, id, gen, state)
	if err == nil {
		s.mu.Lock()
		s.latencies = append(s.latencies, time.Since(s.claimed[id]))
		if len(s.latencies) == s.want {
			close(s.done)
		}
		s.mu.Unlock()
	}
	return err
}

func (s *timedStore) finished() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.latencies)
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(p*float64(len(sorted)-1))]
}
