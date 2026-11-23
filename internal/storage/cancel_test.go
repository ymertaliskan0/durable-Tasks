package storage

import (
	"testing"

	"github.com/google/uuid"
)

func TestRequestCancel_ReadyTask_FullFlow(t *testing.T) {
	p := newStore(t)
	ctx := t.Context()
	id := mustCreate(t, p, NewTask{})

	if err := p.RequestCancel(ctx, id); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	got := mustGet(t, p, id)
	wantStatus(t, got, StatusReadyToCancel)
	if got.CancelReason != CancelRequested || got.Generation != 1 {
		t.Fatalf("cancel_reason=%q generation=%d, want requested and 1", got.CancelReason, got.Generation)
	}

	c := mustClaim(t, p, "w") // the cancel runner
	wantStatus(t, c, StatusCancelling)
	if err := p.MarkCancelled(ctx, id, c.Generation); err != nil {
		t.Fatalf("MarkCancelled: %v", err)
	}
	wantStatus(t, mustGet(t, p, id), StatusCancelled)
}

// Cancelling bumps the generation, so the worker running the
// task learns about it from its next fenced write; no extra channel needed.
func TestRequestCancel_RunningTask_FencesTheOwner(t *testing.T) {
	p := newStore(t)
	ctx := t.Context()
	id := mustCreate(t, p, NewTask{})
	owner := mustClaim(t, p, "w")

	if err := p.RequestCancel(ctx, id); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	got := mustGet(t, p, id)
	wantStatus(t, got, StatusReadyToCancel)
	wantNoLease(t, got)
	if got.Generation != owner.Generation+1 {
		t.Fatalf("generation = %d, want %d", got.Generation, owner.Generation+1)
	}

	wantErrIs(t, p.Heartbeat(ctx, id, owner.Generation, testLease), ErrWrongGeneration)
}

func TestRequestCancel_IsIdempotent(t *testing.T) {
	p := newStore(t)
	ctx := t.Context()
	id := mustCreate(t, p, NewTask{})

	for i := range 3 {
		if err := p.RequestCancel(ctx, id); err != nil {
			t.Fatalf("RequestCancel #%d: %v", i+1, err)
		}
	}
	if gen := mustGet(t, p, id).Generation; gen != 1 {
		t.Fatalf("generation = %d after 3 requests, want 1 (only the first changes anything)", gen)
	}

	// Also a no-op while cancelling and after cancelled.
	c := mustClaim(t, p, "w")
	if err := p.RequestCancel(ctx, id); err != nil {
		t.Fatalf("RequestCancel while cancelling: %v", err)
	}
	if err := p.MarkCancelled(ctx, id, c.Generation); err != nil {
		t.Fatalf("MarkCancelled (was the generation bumped by a no-op RequestCancel?): %v", err)
	}
	if err := p.RequestCancel(ctx, id); err != nil {
		t.Fatalf("RequestCancel after cancelled: %v", err)
	}
}

// A user cancel must not overwrite cancel_reason=failed, or a failed task
// would end up reported as "cancelled".
func TestRequestCancel_KeepsFailedReason(t *testing.T) {
	p := newStore(t)
	ctx := t.Context()
	id := mustCreate(t, p, NewTask{})
	run := mustClaim(t, p, "w")
	if err := p.Fail(ctx, id, run.Generation, nil, "boom"); err != nil {
		t.Fatal(err)
	}

	if err := p.RequestCancel(ctx, id); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	if got := mustGet(t, p, id).CancelReason; got != CancelFailed {
		t.Fatalf("cancel_reason = %q, want failed", got)
	}
}

func TestRequestCancel_EndedTask(t *testing.T) {
	p := newStore(t)
	ctx := t.Context()
	id := mustCreate(t, p, NewTask{})
	task := mustClaim(t, p, "w")
	if err := p.Finish(ctx, id, task.Generation, nil); err != nil {
		t.Fatal(err)
	}

	wantErrIs(t, p.RequestCancel(ctx, id), ErrAlreadyEnded)
	wantStatus(t, mustGet(t, p, id), StatusFinished)
}

func TestRequestCancel_NotFound(t *testing.T) {
	p := newStore(t)
	wantErrIs(t, p.RequestCancel(t.Context(), uuid.New()), ErrNotFound)
}
