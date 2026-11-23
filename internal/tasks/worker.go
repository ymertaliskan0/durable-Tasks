package tasks

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/ymertaliskan0/durable-tasks/internal/storage"
)

// Store is the subset of *storage.Postgres the worker uses. Tests wrap it to
// inject failures.
type Store interface {
	ClaimNext(ctx context.Context, owner string, lease time.Duration) (storage.TaskState, error)
	Heartbeat(ctx context.Context, id uuid.UUID, gen int64, lease time.Duration) error
	SaveState(ctx context.Context, id uuid.UUID, gen int64, state []byte) error
	Finish(ctx context.Context, id uuid.UUID, gen int64, state []byte) error
	ScheduleRetry(ctx context.Context, id uuid.UUID, gen int64, state []byte, lastErr string, delay time.Duration) error
	Fail(ctx context.Context, id uuid.UUID, gen int64, state []byte, lastErr string) error
	MarkCancelled(ctx context.Context, id uuid.UUID, gen int64) error
	Release(ctx context.Context, id uuid.UUID, gen int64) error
	ReclaimExpired(ctx context.Context, limit int) (int, error)
}

// reclaimBatch is how many expired tasks one stalker pass reclaims at most.
const reclaimBatch = 100

// Worker claims tasks from the store and executes them.
type Worker struct {
	store  Store
	reg    *Registry
	cfg    Config
	log    *slog.Logger
	jitter func() float64
}

// NewWorker creates a worker.
//
// Parameters:
//   - store: where tasks are claimed from and written back to.
//   - reg: the task types this worker can execute.
//   - cfg: the worker's settings; see DefaultConfig.
//   - log: where to log; nil discards logs.
//
// Returns an error if cfg is invalid.
func NewWorker(store Store, reg *Registry, cfg Config, log *slog.Logger) (*Worker, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Worker{
		store:  store,
		reg:    reg,
		cfg:    cfg,
		log:    log.With("worker", cfg.WorkerID),
		jitter: rand.Float64,
	}, nil
}

// Run executes tasks until ctx is cancelled, then shuts down gracefully.
//
// It runs Config.Concurrency task loops and one stalker loop. When ctx is
// cancelled, the loops stop claiming, running tasks are interrupted and
// released back to the queue, and Run returns once every loop has stopped.
//
// Returns nil after a clean shutdown.
func (w *Worker) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < w.cfg.Concurrency; i++ {
		wg.Go(func() { w.runLoop(ctx) })
	}
	wg.Go(func() { w.stalkerLoop(ctx) })
	wg.Wait()
	return nil
}

// runLoop claims and executes one task at a time until ctx is cancelled.
// When the queue is empty or the claim fails, it waits PollInterval before
// trying again, so an outage does not turn into a busy loop.
func (w *Worker) runLoop(ctx context.Context) {
	for ctx.Err() == nil {
		ts, err := w.store.ClaimNext(ctx, w.cfg.WorkerID, w.cfg.Lease)
		if err == nil {
			w.execute(ctx, ts)
		} else if errors.Is(err, storage.ErrNoTask) {
			sleep(ctx, w.cfg.PollInterval)
		} else {
			if ctx.Err() == nil {
				w.log.Error("claim failed", "err", err)
			}
			sleep(ctx, w.cfg.PollInterval)
		}
	}

}

// execute runs one claimed task: Task.Run, or Task.Cancel if the task was
// claimed for cleanup.
//
// The task runs under its own context, which the heartbeat cancels if the
// worker loses the task. When the task returns, execute stops the heartbeat,
// decides the outcome and writes it to the store.
func (w *Worker) execute(ctx context.Context, ts storage.TaskState) {
	taskCtx, cancel := context.WithCancelCause(ctx)
	var heartbeats sync.WaitGroup
	heartbeats.Go(func() { w.heartbeat(taskCtx, ts, cancel) })

	cleanup := ts.Status == storage.StatusCancelling
	task, err := w.prepare(ts)
	if err == nil {
		exec := &executionContext{store: w.store, task: task, id: ts.ID, gen: ts.Generation}
		if cleanup {
			err = protect(func() error { return task.Cancel(taskCtx, exec) })
		} else {
			err = protect(func() error { return task.Run(taskCtx, exec) })
		}
	}

	e := execution{
		cleanup:      cleanup,
		err:          err,
		retries:      ts.RetriableErrorCount,
		lost:         errors.Is(context.Cause(taskCtx), storage.ErrWrongGeneration) || errors.Is(err, storage.ErrWrongGeneration),
		shuttingDown: ctx.Err() != nil,
	}

	cancel(nil)
	heartbeats.Wait()

	w.apply(ctx, ts, task, e, decide(e, w.cfg.MaxRetries))
}

// heartbeat extends the task's lease every Lease/3 until ctx is done, so two
// heartbeats can be lost before the lease expires.
//
// If the store reports that the worker no longer owns the task, heartbeat
// cancels the task's context with storage.ErrWrongGeneration as the cause and
// stops. Other errors are logged and the next tick tries again.
func (w *Worker) heartbeat(ctx context.Context, ts storage.TaskState, cancel context.CancelCauseFunc) {
	ticker := time.NewTicker(w.cfg.Lease / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := w.store.Heartbeat(ctx, ts.ID, ts.Generation, w.cfg.Lease)
			if errors.Is(err, storage.ErrWrongGeneration) {
				cancel(err)
				return
			}
			if err != nil && ctx.Err() == nil {
				w.log.Warn("heartbeat failed", "task", ts.ID, "err", err)
			}
		}
	}
}

// stalkerLoop reclaims tasks whose lease expired, every StalkerInterval,
// until ctx is cancelled.
func (w *Worker) stalkerLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.StalkerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := w.store.ReclaimExpired(ctx, reclaimBatch)
			if err != nil && ctx.Err() == nil {
				w.log.Error("reclaim failed", "err", err)
			}
			if n > 0 {
				w.log.Info("reclaimed expired tasks", "count", n)
			}
		}
	}
}

// prepare creates the task's implementation and loads its request and
// state. Errors are NonRetriable: neither an unknown type nor an undecodable
// request gets better with retries.
func (w *Worker) prepare(ts storage.TaskState) (Task, error) {
	task, err := w.reg.New(ts.Type)
	if err != nil {
		return nil, err
	}
	if err := task.Load(ts.Request, ts.State); err != nil {
		return nil, NonRetriable(err)
	}
	return task, nil
}

// apply writes the outcome of one execution to the store.
//
// The write uses a context that survives ctx's cancellation, bounded by
// ShutdownGrace, so a task can still be released while the worker shuts
// down.
func (w *Worker) apply(ctx context.Context, ts storage.TaskState, task Task, e execution, out outcome) {
	log := w.log.With("task", ts.ID, "type", ts.Type, "gen", ts.Generation, "outcome", out.String())
	if e.err != nil {
		log = log.With("err", e.err)
	}
	if out == outcomeAbandon {
		log.Info("lost ownership; leaving the task to its new owner")
		return
	}

	writeCtx, done := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.ShutdownGrace)
	defer done()

	var err error
	switch out {
	case outcomeFinish:
		err = w.store.Finish(writeCtx, ts.ID, ts.Generation, w.snapshot(task, ts))
	case outcomeCancelled:
		err = w.store.MarkCancelled(writeCtx, ts.ID, ts.Generation)
	case outcomeRetry:
		delay := Backoff(ts.RetriableErrorCount, w.cfg.BackoffBase, w.cfg.BackoffMax, w.jitter())
		log = log.With("delay", delay)
		err = w.store.ScheduleRetry(writeCtx, ts.ID, ts.Generation, w.snapshot(task, ts), e.err.Error(), delay)
	case outcomeFail:
		err = w.store.Fail(writeCtx, ts.ID, ts.Generation, w.snapshot(task, ts), e.err.Error())
	case outcomeRelease:
		err = w.store.Release(writeCtx, ts.ID, ts.Generation)
	}

	switch {
	case err == nil:
		log.Info("task executed")
	case errors.Is(err, storage.ErrWrongGeneration):
		log.Info("lost ownership before writing the outcome")
	default:
		// The lease will expire and the stalker will hand the task out again.
		log.Error("writing the outcome failed", "write_err", err)
	}
}

// snapshot returns the task's current state, or the state it was claimed
// with if the task could not be created or cannot serialize itself.
func (w *Worker) snapshot(task Task, ts storage.TaskState) []byte {
	if task == nil {
		return ts.State
	}
	state, err := task.Save()
	if err != nil {
		w.log.Error("task.Save failed; keeping the previous state", "task", ts.ID, "err", err)
		return ts.State
	}
	return state
}

// protect calls fn and turns a panic into a retriable error, so one buggy
// task cannot crash the whole worker.
func protect(fn func() error) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = recovered(v)
		}
	}()
	return fn()
}

// sleep waits for d or until ctx is done, whichever comes first.
func sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
