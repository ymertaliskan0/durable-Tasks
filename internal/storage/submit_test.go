package storage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestCreateTask_ThenGetTask(t *testing.T) {
	p := newStore(t)
	id := mustCreate(t, p, NewTask{Type: "copy", Request: []byte(`{"src":"a"}`)})

	if id.Version() != 7 {
		t.Errorf("id version = %d, want 7 (uuid.NewV7)", id.Version())
	}

	got := mustGet(t, p, id)
	if got.ID != id || got.Type != "copy" || !bytes.Equal(got.Request, []byte(`{"src":"a"}`)) {
		t.Fatalf("got id=%s type=%q request=%q", got.ID, got.Type, got.Request)
	}
	wantStatus(t, got, StatusReadyToRun)
	wantNoLease(t, got)
	if got.State != nil {
		t.Errorf("state = %q, want nil (never saved)", got.State)
	}
	if got.Generation != 0 || got.RetriableErrorCount != 0 {
		t.Errorf("generation=%d retriable_error_count=%d, want 0 and 0", got.Generation, got.RetriableErrorCount)
	}
	if got.LastError != "" || got.CancelReason != "" || got.IdempotencyKey != "" {
		t.Errorf("last_error=%q cancel_reason=%q idempotency_key=%q, want all empty",
			got.LastError, got.CancelReason, got.IdempotencyKey)
	}
	if got.CreatedAt.IsZero() || got.ModifiedAt.IsZero() || got.RunAfter.IsZero() {
		t.Errorf("timestamps not set: created=%v modified=%v run_after=%v", got.CreatedAt, got.ModifiedAt, got.RunAfter)
	}
}

func TestGetTask_NotFound(t *testing.T) {
	p := newStore(t)
	_, err := p.GetTask(t.Context(), uuid.New())
	wantErrIs(t, err, ErrNotFound)
}

func TestCreateTask_SameIdempotencyKeyReturnsExistingTask(t *testing.T) {
	p := newStore(t)
	nt := NewTask{Type: "copy", Request: []byte("r"), IdempotencyKey: "key-1"}

	first := mustCreate(t, p, nt)
	second, created, err := p.CreateTask(t.Context(), nt)
	if err != nil {
		t.Fatalf("second CreateTask: %v", err)
	}
	if created {
		t.Error("second CreateTask: created = true, want false")
	}
	if second != first {
		t.Errorf("second CreateTask returned %s, want the existing %s", second, first)
	}
	if got := mustGet(t, p, first).IdempotencyKey; got != "key-1" {
		t.Errorf("idempotency key = %q, want key-1", got)
	}
}

// Reusing a key for a *different* task is a client bug, and silently
// returning the old task would hide it.
func TestCreateTask_SameKeyDifferentTaskIsAConflict(t *testing.T) {
	p := newStore(t)
	mustCreate(t, p, NewTask{Type: "copy", Request: []byte("r"), IdempotencyKey: "key-1"})

	for _, nt := range []NewTask{
		{Type: "copy", Request: []byte("other request"), IdempotencyKey: "key-1"},
		{Type: "sleep", Request: []byte("r"), IdempotencyKey: "key-1"},
	} {
		_, _, err := p.CreateTask(t.Context(), nt)
		wantErrIs(t, err, ErrIdempotencyConflict)
	}
}

func TestCreateTask_WithoutKeyAlwaysCreates(t *testing.T) {
	p := newStore(t)
	a := mustCreate(t, p, NewTask{Request: []byte("same")})
	b := mustCreate(t, p, NewTask{Request: []byte("same")})
	if a == b {
		t.Fatal("two keyless submissions returned the same id")
	}
}

// Ten clients retry the same submission at once: exactly one creates the
// task, and all of them get its ID.
func TestCreateTask_ConcurrentSameKey(t *testing.T) {
	p := newStore(t)
	const clients = 10
	nt := NewTask{Type: "copy", Request: []byte("r"), IdempotencyKey: "race"}

	ids := make([]uuid.UUID, clients)
	created := make([]bool, clients)
	errs := make([]error, clients)
	var wg sync.WaitGroup
	for i := range clients {
		wg.Go(func() { ids[i], created[i], errs[i] = p.CreateTask(t.Context(), nt) })
	}
	wg.Wait()

	nCreated := 0
	for i := range clients {
		if errs[i] != nil {
			t.Fatalf("client %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("client %d got %s, client 0 got %s", i, ids[i], ids[0])
		}
		if created[i] {
			nCreated++
		}
	}
	if nCreated != 1 {
		t.Fatalf("%d clients saw created=true, want exactly 1", nCreated)
	}
}

func TestListTasks_FiltersByStatusAndLimits(t *testing.T) {
	p := newStore(t)
	var ids []uuid.UUID
	for i := range 5 {
		ids = append(ids, mustCreate(t, p, NewTask{Request: []byte(fmt.Sprint(i))}))
	}
	mustClaim(t, p, "w") // one task becomes running

	all, err := p.ListTasks(t.Context(), "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 5 {
		t.Fatalf("ListTasks(all) returned %d tasks, want 5", len(all))
	}
	for i, task := range all {
		if task.ID != ids[i] {
			t.Fatalf("ListTasks(all)[%d] = %s, want %s (oldest first)", i, task.ID, ids[i])
		}
	}

	ready, err := p.ListTasks(t.Context(), StatusReadyToRun, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 4 {
		t.Fatalf("ListTasks(ready_to_run) returned %d, want 4", len(ready))
	}

	limited, err := p.ListTasks(t.Context(), "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 2 {
		t.Fatalf("ListTasks(limit 2) returned %d, want 2", len(limited))
	}
}

func TestCountByStatus(t *testing.T) {
	p := newStore(t)
	for range 3 {
		mustCreate(t, p, NewTask{})
	}
	mustClaim(t, p, "w")

	counts, err := p.CountByStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if counts[StatusReadyToRun] != 2 || counts[StatusRunning] != 1 || len(counts) != 2 {
		t.Fatalf("counts = %v, want map[ready_to_run:2 running:1]", counts)
	}
}
