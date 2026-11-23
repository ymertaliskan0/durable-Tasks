package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Helpers shared by the Milestone 2 tests. Each one calls t.Fatalf on
// failure, so tests stay focused on the behaviour they check.

const testLease = time.Minute

// newStore opens a fresh database with all migrations applied.
func newStore(t *testing.T) *Postgres {
	t.Helper()
	p, _ := openTestDB(t)
	if err := p.Migrate(t.Context()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return p
}

func mustCreate(t *testing.T, p *Postgres, nt NewTask) uuid.UUID {
	t.Helper()
	if nt.Type == "" {
		nt.Type = "test"
	}
	id, created, err := p.CreateTask(t.Context(), nt)
	if err != nil {
		t.Fatalf("CreateTask(%+v): %v", nt, err)
	}
	if !created {
		t.Fatalf("CreateTask(%+v): created = false, want true", nt)
	}
	return id
}

func mustClaim(t *testing.T, p *Postgres, owner string) TaskState {
	t.Helper()
	task, err := p.ClaimNext(t.Context(), owner, testLease)
	if err != nil {
		t.Fatalf("ClaimNext(%q): %v", owner, err)
	}
	return task
}

func mustGet(t *testing.T, p *Postgres, id uuid.UUID) TaskState {
	t.Helper()
	task, err := p.GetTask(t.Context(), id)
	if err != nil {
		t.Fatalf("GetTask(%s): %v", id, err)
	}
	return task
}

// expireLease moves a task's lease into the past, simulating a worker that
// died (or froze) a while ago, so tests never have to sleep.
func expireLease(t *testing.T, p *Postgres, id uuid.UUID) {
	t.Helper()
	_, err := p.pool.Exec(t.Context(),
		`UPDATE tasks SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, id)
	if err != nil {
		t.Fatalf("expireLease: %v", err)
	}
}

// dbNow reads the database clock. Lease times are computed with the database
// clock, so tests compare against it, not against time.Now().
func dbNow(t *testing.T, p *Postgres) time.Time {
	t.Helper()
	var now time.Time
	if err := p.pool.QueryRow(t.Context(), "SELECT now()").Scan(&now); err != nil {
		t.Fatalf("SELECT now(): %v", err)
	}
	return now
}

func wantErrIs(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("err = %v, want errors.Is(err, %v)", err, target)
	}
}

func wantStatus(t *testing.T, task TaskState, want Status) {
	t.Helper()
	if task.Status != want {
		t.Fatalf("status = %s, want %s", task.Status, want)
	}
}

func wantNoLease(t *testing.T, task TaskState) {
	t.Helper()
	if task.LeaseOwner != "" || !task.LeaseExpiresAt.IsZero() {
		t.Fatalf("lease = (%q, %v), want none", task.LeaseOwner, task.LeaseExpiresAt)
	}
}
