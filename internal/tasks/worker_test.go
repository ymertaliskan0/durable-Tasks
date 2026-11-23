package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ymertaliskan0/durable-tasks/internal/pgtest"
	"github.com/ymertaliskan0/durable-tasks/internal/storage"
)

// Short timings keep the tests fast: heartbeats every 200ms.
func testConfig(workerID string) Config {
	return Config{
		WorkerID:        workerID,
		Concurrency:     2,
		Lease:           600 * time.Millisecond,
		PollInterval:    10 * time.Millisecond,
		StalkerInterval: 50 * time.Millisecond,
		MaxRetries:      3,
		BackoffBase:     10 * time.Millisecond,
		BackoffMax:      40 * time.Millisecond,
		ShutdownGrace:   2 * time.Second,
	}
}

func newTestStore(t *testing.T) *storage.Postgres {
	t.Helper()
	store, err := storage.Open(t.Context(), pgtest.NewDB(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	return store
}

func scriptRegistry(rec *recorder) *Registry {
	reg := NewRegistry()
	reg.Register(scriptType, rec.factory())
	return reg
}

func submit(t *testing.T, store *storage.Postgres, taskType string, s script) uuid.UUID {
	t.Helper()
	req, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := store.CreateTask(t.Context(), storage.NewTask{Type: taskType, Request: req})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// startWorker runs a worker in the background. The returned stop function
// cancels it and waits for Run to return; it also runs at test cleanup.
func startWorker(t *testing.T, store Store, reg *Registry, cfg Config) (stop func()) {
	t.Helper()
	w, err := NewWorker(store, reg, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// A real worker keeps running until it is stopped.
	select {
	case err := <-done:
		cancel()
		t.Fatalf("worker %s: Run returned before it was stopped (err: %v)", cfg.WorkerID, err)
	case <-time.After(50 * time.Millisecond):
	}

	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("worker %s: Run returned %v", cfg.WorkerID, err)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("worker %s: Run did not return within 5s of cancellation", cfg.WorkerID)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// waitStatus polls until the task reaches want, failing after 10s.
func waitStatus(t *testing.T, store *storage.Postgres, id uuid.UUID, want storage.Status) storage.TaskState {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ts, err := store.GetTask(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if ts.Status == want {
			return ts
		}
		if time.Now().After(deadline) {
			t.Fatalf("task status = %s after 10s, want %s (last_error %q)", ts.Status, want, ts.LastError)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitStarted(t *testing.T, rec *recorder) uuid.UUID {
	t.Helper()
	select {
	case id := <-rec.started:
		return id
	case <-time.After(10 * time.Second):
		t.Fatal("task did not start within 10s")
		return uuid.Nil
	}
}

func TestWorker_RunsTaskToFinished(t *testing.T) {
	store, rec := newTestStore(t), newRecorder()
	id := submit(t, store, scriptType, script{Steps: 3})
	startWorker(t, store, scriptRegistry(rec), testConfig("w1"))

	ts := waitStatus(t, store, id, storage.StatusFinished)
	if got := rec.stepsOf(id); !slices.Equal(got, []int{0, 1, 2}) {
		t.Errorf("steps run = %v, want [0 1 2]", got)
	}
	if string(ts.State) != `{"next":3}` {
		t.Errorf("final state = %s, want {\"next\":3}", ts.State)
	}
	if ts.LeaseOwner != "" {
		t.Errorf("lease owner = %q after finishing, want none", ts.LeaseOwner)
	}
}

func TestWorker_ResumesFromCheckpoint(t *testing.T) {
	store, rec := newTestStore(t), newRecorder()
	ctx := t.Context()
	id := submit(t, store, scriptType, script{Steps: 5})

	// Another worker ran steps 0-2, checkpointed, and released the task.
	ts, err := store.ClaimNext(ctx, "earlier-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveState(ctx, id, ts.Generation, []byte(`{"next":3}`)); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(ctx, id, ts.Generation); err != nil {
		t.Fatal(err)
	}

	startWorker(t, store, scriptRegistry(rec), testConfig("w1"))
	waitStatus(t, store, id, storage.StatusFinished)
	if got := rec.stepsOf(id); !slices.Equal(got, []int{3, 4}) {
		t.Errorf("steps run = %v, want [3 4]: the worker must resume at the checkpoint", got)
	}
}

func TestWorker_RetriesThenFinishes(t *testing.T) {
	store, rec := newTestStore(t), newRecorder()
	id := submit(t, store, scriptType, script{Steps: 1, RetriableFailures: 2})
	startWorker(t, store, scriptRegistry(rec), testConfig("w1"))

	ts := waitStatus(t, store, id, storage.StatusFinished)
	if ts.RetriableErrorCount != 2 || rec.count(rec.runs, id) != 3 {
		t.Errorf("retriable_error_count = %d, runs = %d; want 2 and 3", ts.RetriableErrorCount, rec.count(rec.runs, id))
	}
}

func TestWorker_RetriesExhaustedEndsFailedAfterCleanup(t *testing.T) {
	store, rec := newTestStore(t), newRecorder()
	id := submit(t, store, scriptType, script{Steps: 1, RetriableFailures: 100})
	startWorker(t, store, scriptRegistry(rec), testConfig("w1")) // MaxRetries 3

	ts := waitStatus(t, store, id, storage.StatusFailed)
	if ts.RetriableErrorCount != 3 || rec.count(rec.runs, id) != 4 {
		t.Errorf("retriable_error_count = %d, runs = %d; want 3 and 4", ts.RetriableErrorCount, rec.count(rec.runs, id))
	}
	if rec.count(rec.cancels, id) != 1 {
		t.Errorf("Cancel called %d times, want 1 (cleanup before failing)", rec.count(rec.cancels, id))
	}
	if ts.CancelReason != storage.CancelFailed || !strings.Contains(ts.LastError, "flaky") {
		t.Errorf("cancel_reason = %q, last_error = %q; want failed and the task's error", ts.CancelReason, ts.LastError)
	}
}

func TestWorker_UnmarkedErrorFailsWithoutRetry(t *testing.T) {
	store, rec := newTestStore(t), newRecorder()
	id := submit(t, store, scriptType, script{PermanentFailure: true})
	startWorker(t, store, scriptRegistry(rec), testConfig("w1"))

	ts := waitStatus(t, store, id, storage.StatusFailed)
	if rec.count(rec.runs, id) != 1 || ts.RetriableErrorCount != 0 {
		t.Errorf("runs = %d, retriable_error_count = %d; want 1 and 0", rec.count(rec.runs, id), ts.RetriableErrorCount)
	}
	if rec.count(rec.cancels, id) != 1 {
		t.Errorf("Cancel called %d times, want 1", rec.count(rec.cancels, id))
	}
}

func TestWorker_PanicIsRecoveredAndRetried(t *testing.T) {
	store, rec := newTestStore(t), newRecorder()
	id := submit(t, store, scriptType, script{Steps: 1, Panics: 1})
	startWorker(t, store, scriptRegistry(rec), testConfig("w1"))

	ts := waitStatus(t, store, id, storage.StatusFinished)
	if ts.RetriableErrorCount != 1 || !strings.Contains(ts.LastError, "panic") {
		t.Errorf("retriable_error_count = %d, last_error = %q; want 1 and a panic message", ts.RetriableErrorCount, ts.LastError)
	}
}

func TestWorker_UnknownTaskTypeFails(t *testing.T) {
	store, rec := newTestStore(t), newRecorder()
	id := submit(t, store, "no-such-type", script{})
	startWorker(t, store, scriptRegistry(rec), testConfig("w1"))

	ts := waitStatus(t, store, id, storage.StatusFailed)
	if !strings.Contains(ts.LastError, "unknown task type") {
		t.Errorf("last_error = %q, want it to name the unknown type", ts.LastError)
	}
}

func TestWorker_CleanupIsRetriedUntilItSucceeds(t *testing.T) {
	store, rec := newTestStore(t), newRecorder()
	id := submit(t, store, scriptType, script{PermanentFailure: true, CancelFailures: 2})
	startWorker(t, store, scriptRegistry(rec), testConfig("w1"))

	waitStatus(t, store, id, storage.StatusFailed)
	if got := rec.count(rec.cancels, id); got != 3 {
		t.Errorf("Cancel called %d times, want 3 (two failures, then success)", got)
	}
}

func TestWorker_HeartbeatKeepsLongTaskAlive(t *testing.T) {
	store, rec := newTestStore(t), newRecorder()
	// 1.2s of work with a 600ms lease, while the stalker runs every 50ms.
	id := submit(t, store, scriptType, script{Steps: 3, StepDelay: 400 * time.Millisecond})
	startWorker(t, store, scriptRegistry(rec), testConfig("w1"))

	ts := waitStatus(t, store, id, storage.StatusFinished)
	if ts.Generation != 1 || rec.count(rec.runs, id) != 1 {
		t.Errorf("generation = %d, runs = %d; want 1 and 1: the task was reclaimed while its worker was alive", ts.Generation, rec.count(rec.runs, id))
	}
}

func TestWorker_CancelRequestStopsRunningTask(t *testing.T) {
	store, rec := newTestStore(t), newRecorder()
	id := submit(t, store, scriptType, script{Block: true})
	spy := &spyStore{Store: store}
	startWorker(t, spy, scriptRegistry(rec), testConfig("w1"))

	waitStarted(t, rec)
	if err := store.RequestCancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	// The running worker learns about the cancel from its next heartbeat and
	// stops Run. A worker then claims the task for cleanup.
	select {
	case <-rec.stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Run kept running after the cancel request: the heartbeat must cancel its context")
	}
	waitStatus(t, store, id, storage.StatusCancelled)
	if rec.count(rec.cancels, id) != 1 {
		t.Errorf("Cancel called %d times, want 1", rec.count(rec.cancels, id))
	}
	// Generation 1 is the interrupted run. Its worker knew it had lost the
	// task, so it must not have tried to record an outcome for it.
	if w := spy.writesAt(1); len(w) != 0 {
		t.Errorf("outcome writes for the cancelled run: %v, want none", w)
	}
}

func TestWorker_ShutdownReleasesRunningTask(t *testing.T) {
	store, rec := newTestStore(t), newRecorder()
	id := submit(t, store, scriptType, script{Block: true})
	stop := startWorker(t, store, scriptRegistry(rec), testConfig("w1"))

	waitStarted(t, rec)
	stop() // fails the test if Run does not return promptly

	ts, err := store.GetTask(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if ts.Status != storage.StatusReadyToRun || ts.LeaseOwner != "" || ts.Generation != 2 {
		t.Errorf("after shutdown: status %s, owner %q, generation %d; want ready_to_run, none, 2 (released)",
			ts.Status, ts.LeaseOwner, ts.Generation)
	}
}

func TestWorker_StalkerReclaimsAbandonedTask(t *testing.T) {
	store, rec := newTestStore(t), newRecorder()
	id := submit(t, store, scriptType, script{Steps: 1})
	// A worker claims the task and dies without ever heartbeating.
	if _, err := store.ClaimNext(t.Context(), "dead-worker", 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	startWorker(t, store, scriptRegistry(rec), testConfig("w1"))
	ts := waitStatus(t, store, id, storage.StatusFinished)
	if ts.Generation != 3 {
		t.Errorf("generation = %d, want 3 (claim, reclaim, claim)", ts.Generation)
	}
}

// spyStore wraps a Store and records every outcome write the worker makes.
// With frozen set, it also simulates a frozen worker whose heartbeats never
// reach the database. Embedding the Store interface "inherits" every method
// that is not overridden here.
type spyStore struct {
	Store
	frozen bool

	mu     sync.Mutex
	writes []outcomeWrite
}

type outcomeWrite struct {
	method string
	gen    int64
}

func (s *spyStore) record(method string, gen int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes = append(s.writes, outcomeWrite{method, gen})
}

// writesAt returns the outcome writes made with generation gen.
func (s *spyStore) writesAt(gen int64) []outcomeWrite {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []outcomeWrite
	for _, w := range s.writes {
		if w.gen == gen {
			out = append(out, w)
		}
	}
	return out
}

func (s *spyStore) Heartbeat(ctx context.Context, id uuid.UUID, gen int64, lease time.Duration) error {
	if s.frozen {
		return nil
	}
	return s.Store.Heartbeat(ctx, id, gen, lease)
}

func (s *spyStore) Finish(ctx context.Context, id uuid.UUID, gen int64, state []byte) error {
	s.record("Finish", gen)
	return s.Store.Finish(ctx, id, gen, state)
}

func (s *spyStore) ScheduleRetry(ctx context.Context, id uuid.UUID, gen int64, state []byte, lastErr string, delay time.Duration) error {
	s.record("ScheduleRetry", gen)
	return s.Store.ScheduleRetry(ctx, id, gen, state, lastErr, delay)
}

func (s *spyStore) Fail(ctx context.Context, id uuid.UUID, gen int64, state []byte, lastErr string) error {
	s.record("Fail", gen)
	return s.Store.Fail(ctx, id, gen, state, lastErr)
}

func (s *spyStore) MarkCancelled(ctx context.Context, id uuid.UUID, gen int64) error {
	s.record("MarkCancelled", gen)
	return s.Store.MarkCancelled(ctx, id, gen)
}

func (s *spyStore) Release(ctx context.Context, id uuid.UUID, gen int64) error {
	s.record("Release", gen)
	return s.Store.Release(ctx, id, gen)
}

// zombieTask blocks until released, then tries to save its state.
type zombieTask struct {
	started chan<- struct{}
	release <-chan struct{}
	saveErr chan<- error
}

func (*zombieTask) Load([]byte, []byte) error { return nil }
func (*zombieTask) Save() ([]byte, error)     { return []byte(`{"zombie":true}`), nil }
func (*zombieTask) Cancel(context.Context, ExecutionContext) error {
	return nil
}

func (z *zombieTask) Run(ctx context.Context, exec ExecutionContext) error {
	z.started <- struct{}{}
	<-z.release // frozen: it does not even notice ctx
	err := exec.SaveState(ctx)
	z.saveErr <- err
	return err
}

// The fencing story end to end: worker A freezes, its lease expires, worker
// B's stalker reclaims the task and B finishes it. When A wakes up, its write
// is rejected and it must not touch the task again.
func TestWorker_FencesOutZombie(t *testing.T) {
	store, rec := newTestStore(t), newRecorder()
	id := submit(t, store, scriptType, script{Steps: 1})

	started, release, saveErr := make(chan struct{}, 1), make(chan struct{}), make(chan error, 1)
	zombieReg := NewRegistry()
	zombieReg.Register(scriptType, func() Task { return &zombieTask{started, release, saveErr} })

	cfgA := testConfig("zombie-A")
	cfgA.Concurrency = 1
	cfgA.Lease = 300 * time.Millisecond
	cfgA.StalkerInterval = time.Hour // only B reclaims
	storeA := &spyStore{Store: store, frozen: true}
	stopA := startWorker(t, storeA, zombieReg, cfgA)

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("worker A did not start the task")
	}

	startWorker(t, store, scriptRegistry(rec), testConfig("healthy-B"))
	finished := waitStatus(t, store, id, storage.StatusFinished)

	close(release) // A wakes up
	select {
	case err := <-saveErr:
		if !errors.Is(err, storage.ErrWrongGeneration) {
			t.Errorf("zombie's SaveState err = %v, want ErrWrongGeneration", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("zombie never tried to save")
	}
	stopA() // waits until A has fully handled the outcome

	// The database would reject A's writes anyway; a correct worker does not
	// even try once it knows it lost the task.
	if w := storeA.writesAt(1); len(w) != 0 {
		t.Errorf("zombie attempted outcome writes %v, want none (it lost ownership)", w)
	}

	after, err := store.GetTask(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != storage.StatusFinished || after.Generation != finished.Generation || string(after.State) != string(finished.State) {
		t.Errorf("zombie changed the task: before %s/gen %d/%s, after %s/gen %d/%s",
			finished.Status, finished.Generation, finished.State, after.Status, after.Generation, after.State)
	}
}

func TestWorker_ManyWorkersRunEachTaskOnce(t *testing.T) {
	store, rec := newTestStore(t), newRecorder()
	const n = 40
	ids := make([]uuid.UUID, n)
	for i := range ids {
		ids[i] = submit(t, store, scriptType, script{Steps: 1})
	}
	for _, name := range []string{"w1", "w2", "w3"} {
		cfg := testConfig(name)
		cfg.Concurrency = 3
		startWorker(t, store, scriptRegistry(rec), cfg)
	}
	for _, id := range ids {
		waitStatus(t, store, id, storage.StatusFinished)
		if runs := rec.count(rec.runs, id); runs != 1 {
			t.Errorf("task %s ran %d times, want 1", id, runs)
		}
	}
}
