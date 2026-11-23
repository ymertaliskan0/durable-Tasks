package storage

import (
	"fmt"
	"testing"
)

func TestReclaimExpired_OnlyTouchesExpiredLeases(t *testing.T) {
	p := newStore(t)
	ctx := t.Context()
	mustCreate(t, p, NewTask{Request: []byte("1")})
	mustCreate(t, p, NewTask{Request: []byte("2")})
	dead := mustClaim(t, p, "dead-worker")
	alive := mustClaim(t, p, "live-worker")
	expireLease(t, p, dead.ID)

	n, err := p.ReclaimExpired(ctx, 100)
	if err != nil || n != 1 {
		t.Fatalf("ReclaimExpired = %d, %v; want 1, nil", n, err)
	}

	got := mustGet(t, p, dead.ID)
	wantStatus(t, got, StatusReadyToRun)
	wantNoLease(t, got)
	if got.Generation != dead.Generation+1 {
		t.Fatalf("reclaimed generation = %d, want %d (reclaim must fence the old owner)", got.Generation, dead.Generation+1)
	}

	untouched := mustGet(t, p, alive.ID)
	if untouched.Status != StatusRunning || untouched.Generation != alive.Generation {
		t.Fatalf("live task changed: status=%s gen=%d", untouched.Status, untouched.Generation)
	}

	// Nothing left to do: a second pass is a no-op.
	if n, err := p.ReclaimExpired(ctx, 100); err != nil || n != 0 {
		t.Fatalf("second ReclaimExpired = %d, %v; want 0, nil", n, err)
	}
}

// A worker that dies during Cancel() cleanup: the task goes back to
// ready_to_cancel, not ready_to_run.
func TestReclaimExpired_CancellingGoesBackToReadyToCancel(t *testing.T) {
	p := newStore(t)
	id := mustCreate(t, p, NewTask{})
	if err := p.RequestCancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	c := mustClaim(t, p, "w")
	wantStatus(t, c, StatusCancelling)
	expireLease(t, p, id)

	if n, err := p.ReclaimExpired(t.Context(), 100); err != nil || n != 1 {
		t.Fatalf("ReclaimExpired = %d, %v; want 1, nil", n, err)
	}
	got := mustGet(t, p, id)
	wantStatus(t, got, StatusReadyToCancel)
	wantNoLease(t, got)
}

func TestReclaimExpired_RespectsLimit(t *testing.T) {
	p := newStore(t)
	for i := range 5 {
		mustCreate(t, p, NewTask{Request: []byte(fmt.Sprint(i))})
		expireLease(t, p, mustClaim(t, p, "w").ID)
	}

	n, err := p.ReclaimExpired(t.Context(), 3)
	if err != nil || n != 3 {
		t.Fatalf("ReclaimExpired(limit 3) = %d, %v; want 3, nil", n, err)
	}
	if n, _ := p.ReclaimExpired(t.Context(), 3); n != 2 {
		t.Fatalf("second ReclaimExpired = %d, want the remaining 2", n)
	}
}
