package storage

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// ClaimNext
// ---------------------------------------------------------------------------

func TestClaimNext_EmptyQueue(t *testing.T) {
	p := newStore(t)
	_, err := p.ClaimNext(t.Context(), "w1", testLease)
	wantErrIs(t, err, ErrNoTask)
}

func TestClaimNext_TakesLeaseAndBumpsGeneration(t *testing.T) {
	p := newStore(t)
	id := mustCreate(t, p, NewTask{})
	before := dbNow(t, p)

	got := mustClaim(t, p, "worker-1")

	if got.ID != id {
		t.Fatalf("claimed %s, want %s", got.ID, id)
	}
	wantStatus(t, got, StatusRunning)
	if got.Generation != 1 {
		t.Errorf("generation = %d, want 1", got.Generation)
	}
	if got.LeaseOwner != "worker-1" {
		t.Errorf("lease owner = %q, want worker-1", got.LeaseOwner)
	}
	// The expiry is computed from the DB clock: now() + lease.
	if exp := got.LeaseExpiresAt; exp.Before(before.Add(testLease-time.Second)) || exp.After(before.Add(testLease+10*time.Second)) {
		t.Errorf("lease expires at %v, want about db now (%v) + %v", exp, before, testLease)
	}
	// RETURNING must give back the row as it is after the update.
	if stored := mustGet(t, p, id); stored.Generation != got.Generation || stored.Status != got.Status {
		t.Errorf("returned (gen %d, %s) but stored (gen %d, %s)", got.Generation, got.Status, stored.Generation, stored.Status)
	}

	// The only task is taken now.
	_, err := p.ClaimNext(t.Context(), "worker-2", testLease)
	wantErrIs(t, err, ErrNoTask)
}

func TestClaimNext_SkipsTasksNotDueYet(t *testing.T) {
	p := newStore(t)
	mustCreate(t, p, NewTask{})
	task := mustClaim(t, p, "w")
	if err := p.ScheduleRetry(t.Context(), task.ID, task.Generation, nil, "boom", time.Hour); err != nil {
		t.Fatalf("ScheduleRetry: %v", err)
	}

	_, err := p.ClaimNext(t.Context(), "w", testLease)
	wantErrIs(t, err, ErrNoTask)
}

// THE core property of the scheduler: under concurrency, every task is
// claimed by exactly one worker.
func TestClaimNext_EachTaskClaimedExactlyOnce(t *testing.T) {
	_, dsn := openTestDB(t)
	const (
		tasks        = 50
		workers      = 4 // separate pools, like separate processes
		perWorker    = 5 // goroutines per worker
		totalClaimer = workers * perWorker
	)

	stores := make([]*Postgres, workers)
	for i := range stores {
		s, err := Open(t.Context(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		stores[i] = s
	}
	if err := stores[0].Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	for i := range tasks {
		mustCreate(t, stores[0], NewTask{Request: []byte(fmt.Sprint(i))})
	}

	var (
		mu      sync.Mutex
		claimed = map[uuid.UUID]string{} // task -> claimer
		dupes   []string
		errs    []error
	)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for c := range totalClaimer {
		store := stores[c%workers]
		owner := fmt.Sprintf("claimer-%d", c)
		wg.Go(func() {
			<-start
			for {
				task, err := store.ClaimNext(t.Context(), owner, testLease)
				if errors.Is(err, ErrNoTask) {
					return
				}
				mu.Lock()
				if err != nil {
					errs = append(errs, err)
					mu.Unlock()
					return
				}
				if prev, ok := claimed[task.ID]; ok {
					dupes = append(dupes, fmt.Sprintf("%s claimed by %s and %s", task.ID, prev, owner))
				}
				claimed[task.ID] = owner
				mu.Unlock()
			}
		})
	}
	close(start)
	wg.Wait()

	for _, err := range errs {
		t.Errorf("ClaimNext: %v", err)
	}
	for _, d := range dupes {
		t.Errorf("double claim: %s", d)
	}
	if len(claimed) != tasks {
		t.Fatalf("%d distinct tasks claimed, want %d", len(claimed), tasks)
	}
}

// ---------------------------------------------------------------------------
// Fenced writes
// ---------------------------------------------------------------------------

func TestFencedWrites_HappyPath(t *testing.T) {
	p := newStore(t)
	id := mustCreate(t, p, NewTask{})
	task := mustClaim(t, p, "w")
	ctx := t.Context()

	if err := p.Heartbeat(ctx, id, task.Generation, testLease); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if err := p.SaveState(ctx, id, task.Generation, []byte("step-1")); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if got := mustGet(t, p, id).State; !bytes.Equal(got, []byte("step-1")) {
		t.Fatalf("state = %q, want step-1", got)
	}
	if err := p.Finish(ctx, id, task.Generation, []byte("done")); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	got := mustGet(t, p, id)
	wantStatus(t, got, StatusFinished)
	wantNoLease(t, got)
	if !bytes.Equal(got.State, []byte("done")) {
		t.Fatalf("state = %q, want done", got.State)
	}
}

// Every fenced method must reject a stale generation and leave the row
// untouched.
func TestFencedWrites_RejectWrongGeneration(t *testing.T) {
	ops := map[string]func(p *Postgres, id uuid.UUID, gen int64) error{
		"Heartbeat": func(p *Postgres, id uuid.UUID, gen int64) error {
			return p.Heartbeat(t.Context(), id, gen, testLease)
		},
		"SaveState": func(p *Postgres, id uuid.UUID, gen int64) error {
			return p.SaveState(t.Context(), id, gen, []byte("stale"))
		},
		"Finish": func(p *Postgres, id uuid.UUID, gen int64) error {
			return p.Finish(t.Context(), id, gen, []byte("stale"))
		},
		"ScheduleRetry": func(p *Postgres, id uuid.UUID, gen int64) error {
			return p.ScheduleRetry(t.Context(), id, gen, nil, "stale", 0)
		},
		"Fail": func(p *Postgres, id uuid.UUID, gen int64) error {
			return p.Fail(t.Context(), id, gen, nil, "stale")
		},
		"Release": func(p *Postgres, id uuid.UUID, gen int64) error {
			return p.Release(t.Context(), id, gen)
		},
	}

	p := newStore(t)
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			id := mustCreate(t, p, NewTask{Request: []byte(name)})
			// Claim until we get this subtest's task (earlier subtests leave
			// theirs running, so they won't come back).
			task := mustClaim(t, p, "w")
			if task.ID != id {
				t.Fatalf("claimed %s, want %s", task.ID, id)
			}

			err := op(p, id, task.Generation-1)
			wantErrIs(t, err, ErrWrongGeneration)

			after := mustGet(t, p, id)
			if after.Status != StatusRunning || after.Generation != task.Generation || after.State != nil {
				t.Fatalf("row changed by a rejected write: status=%s gen=%d state=%q", after.Status, after.Generation, after.State)
			}
		})
	}
}

// A heartbeat that arrives after Finish (same generation!) must be rejected
// with ErrWrongGeneration, not crash into the lease_iff_inflight constraint.
// That's why fenced writes also check status.
func TestHeartbeat_AfterFinishIsRejected(t *testing.T) {
	p := newStore(t)
	mustCreate(t, p, NewTask{})
	task := mustClaim(t, p, "w")
	if err := p.Finish(t.Context(), task.ID, task.Generation, nil); err != nil {
		t.Fatal(err)
	}

	err := p.Heartbeat(t.Context(), task.ID, task.Generation, testLease)
	wantErrIs(t, err, ErrWrongGeneration)
}

// The scenario the whole design exists for: worker A stalls past its lease,
// the stalker reclaims the task, worker B claims it, and then A wakes up.
// Every write from A must bounce; B's work must survive.
func TestStaleWorker_RejectedAfterReclaim(t *testing.T) {
	p := newStore(t)
	ctx := t.Context()
	id := mustCreate(t, p, NewTask{})

	a := mustClaim(t, p, "worker-A")
	if err := p.SaveState(ctx, id, a.Generation, []byte("A: chunk 3")); err != nil {
		t.Fatalf("A's SaveState before stalling: %v", err)
	}

	expireLease(t, p, id) // A froze (SIGSTOP) past its lease
	if n, err := p.ReclaimExpired(ctx, 10); err != nil || n != 1 {
		t.Fatalf("ReclaimExpired = %d, %v; want 1, nil", n, err)
	}
	b := mustClaim(t, p, "worker-B")
	if b.ID != id || b.Generation <= a.Generation {
		t.Fatalf("B claimed %s gen %d; want %s with gen > %d", b.ID, b.Generation, id, a.Generation)
	}
	if !bytes.Equal(b.State, []byte("A: chunk 3")) {
		t.Fatalf("B resumed from state %q, want A's last checkpoint", b.State)
	}

	// A wakes up (SIGCONT) and carries on as if nothing happened.
	wantErrIs(t, p.Heartbeat(ctx, id, a.Generation, testLease), ErrWrongGeneration)
	wantErrIs(t, p.SaveState(ctx, id, a.Generation, []byte("A: chunk 4")), ErrWrongGeneration)
	wantErrIs(t, p.Finish(ctx, id, a.Generation, []byte("A: done")), ErrWrongGeneration)

	if err := p.Finish(ctx, id, b.Generation, []byte("B: done")); err != nil {
		t.Fatalf("B's Finish: %v", err)
	}
	got := mustGet(t, p, id)
	wantStatus(t, got, StatusFinished)
	if !bytes.Equal(got.State, []byte("B: done")) {
		t.Fatalf("final state = %q, want B's", got.State)
	}
}

func TestScheduleRetry(t *testing.T) {
	p := newStore(t)
	ctx := t.Context()
	id := mustCreate(t, p, NewTask{})
	task := mustClaim(t, p, "w")
	before := dbNow(t, p)

	if err := p.ScheduleRetry(ctx, id, task.Generation, []byte("progress"), "disk busy", time.Hour); err != nil {
		t.Fatalf("ScheduleRetry: %v", err)
	}

	got := mustGet(t, p, id)
	wantStatus(t, got, StatusReadyToRun)
	wantNoLease(t, got)
	if got.RetriableErrorCount != 1 || got.LastError != "disk busy" || !bytes.Equal(got.State, []byte("progress")) {
		t.Fatalf("count=%d last_error=%q state=%q", got.RetriableErrorCount, got.LastError, got.State)
	}
	if got.RunAfter.Before(before.Add(59 * time.Minute)) {
		t.Fatalf("run_after = %v, want about %v + 1h", got.RunAfter, before)
	}
}

func TestScheduleRetry_ZeroDelayIsImmediatelyClaimable(t *testing.T) {
	p := newStore(t)
	mustCreate(t, p, NewTask{})
	first := mustClaim(t, p, "w")
	if err := p.ScheduleRetry(t.Context(), first.ID, first.Generation, nil, "x", 0); err != nil {
		t.Fatal(err)
	}
	second := mustClaim(t, p, "w")
	if second.ID != first.ID || second.Generation != first.Generation+1 {
		t.Fatalf("reclaimed %s gen %d, want %s gen %d", second.ID, second.Generation, first.ID, first.Generation+1)
	}
}

// Decision D2: a failed task goes through Cancel() cleanup and ends as
// "failed", not "cancelled".
func TestFail_RunsCleanupThenEndsFailed(t *testing.T) {
	p := newStore(t)
	ctx := t.Context()
	id := mustCreate(t, p, NewTask{})
	run := mustClaim(t, p, "w")

	if err := p.Fail(ctx, id, run.Generation, []byte("half copied"), "checksum mismatch"); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	got := mustGet(t, p, id)
	wantStatus(t, got, StatusReadyToCancel)
	wantNoLease(t, got)
	if got.CancelReason != CancelFailed || got.LastError != "checksum mismatch" || !bytes.Equal(got.State, []byte("half copied")) {
		t.Fatalf("cancel_reason=%q last_error=%q state=%q", got.CancelReason, got.LastError, got.State)
	}

	cleanup := mustClaim(t, p, "w") // a cancel runner picks it up
	wantStatus(t, cleanup, StatusCancelling)
	if err := p.MarkCancelled(ctx, id, cleanup.Generation); err != nil {
		t.Fatalf("MarkCancelled: %v", err)
	}
	got = mustGet(t, p, id)
	wantStatus(t, got, StatusFailed)
	wantNoLease(t, got)
}

func TestMarkCancelled_RequiresCancelling(t *testing.T) {
	p := newStore(t)
	mustCreate(t, p, NewTask{})
	task := mustClaim(t, p, "w") // running, not cancelling

	err := p.MarkCancelled(t.Context(), task.ID, task.Generation)
	wantErrIs(t, err, ErrWrongGeneration)
	wantStatus(t, mustGet(t, p, task.ID), StatusRunning)
}

// Graceful shutdown hands the task back immediately instead of making
// everyone wait for the lease to expire, and fences the old owner.
func TestRelease(t *testing.T) {
	p := newStore(t)
	ctx := t.Context()
	id := mustCreate(t, p, NewTask{})
	task := mustClaim(t, p, "w")

	if err := p.Release(ctx, id, task.Generation); err != nil {
		t.Fatalf("Release: %v", err)
	}
	got := mustGet(t, p, id)
	wantStatus(t, got, StatusReadyToRun)
	wantNoLease(t, got)
	if got.Generation != task.Generation+1 {
		t.Fatalf("generation = %d, want %d (Release must fence the old owner)", got.Generation, task.Generation+1)
	}
	wantErrIs(t, p.SaveState(ctx, id, task.Generation, nil), ErrWrongGeneration)

	if again := mustClaim(t, p, "w2"); again.ID != id {
		t.Fatalf("released task not claimable again")
	}
}
