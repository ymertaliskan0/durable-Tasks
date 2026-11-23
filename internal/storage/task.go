package storage

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Status is a task's position in its lifecycle. The normal path is
// ready_to_run → running → finished. The cancel path is ready_to_cancel →
// cancelling → cancelled, or failed when the task is cleaning up after a
// non-retriable error. See docs/design.md for the full state machine.
type Status string

const (
	StatusReadyToRun    Status = "ready_to_run"
	StatusRunning       Status = "running"
	StatusFinished      Status = "finished"
	StatusFailed        Status = "failed"
	StatusReadyToCancel Status = "ready_to_cancel"
	StatusCancelling    Status = "cancelling"
	StatusCancelled     Status = "cancelled"
)

// Ended reports whether s is a terminal status: finished, failed or
// cancelled. A task in a terminal status never changes again.
func (s Status) Ended() bool {
	return s == StatusFinished || s == StatusFailed || s == StatusCancelled
}

// CancelReason records why a task is on the cancel path, which decides its
// final status once cleanup is done. It is empty for tasks that were never on
// the cancel path.
type CancelReason string

const (
	CancelRequested CancelReason = "requested" // a user asked; ends as cancelled
	CancelFailed    CancelReason = "failed"    // Run failed for good; ends as failed
)

// TaskState is a snapshot of one stored task.
//
// Database NULLs are represented by zero values: "" for strings, nil for
// State and the zero time.Time for LeaseExpiresAt.
type TaskState struct {
	ID                  uuid.UUID
	Type                string
	Request             []byte       // the task's input; never changes after creation
	State               []byte       // the last saved checkpoint; nil until the first save
	Status              Status       // the task's position in its lifecycle
	Generation          int64        // fencing token; incremented on every claim, reclaim, cancel and release
	RetriableErrorCount int          // number of retries scheduled so far
	LastError           string       // the most recent error message, if any
	CancelReason        CancelReason // why the task is on the cancel path, if it is
	IdempotencyKey      string       // the submitter's key, if any
	LeaseOwner          string       // the worker holding the task while running or cancelling
	LeaseExpiresAt      time.Time    // when that worker's claim lapses without a heartbeat
	RunAfter            time.Time    // the task cannot be claimed before this time
	CreatedAt           time.Time
	ModifiedAt          time.Time
}

// Errors returned by the storage methods, usually wrapped with context. Check
// for them with errors.Is.
var (
	// ErrNotFound means no task has the given id.
	ErrNotFound = errors.New("storage: task not found")

	// ErrNoTask means ClaimNext found no task that is due.
	ErrNoTask = errors.New("storage: no claimable task")

	// ErrWrongGeneration means a fenced write was rejected because the
	// caller no longer owns the task: it was reclaimed, cancelled or
	// released, or is no longer in a status that allows the write. The
	// caller must stop working on the task.
	ErrWrongGeneration = errors.New("storage: wrong generation (task no longer owned)")

	// ErrIdempotencyConflict means the idempotency key is already used by a
	// task with a different type or request.
	ErrIdempotencyConflict = errors.New("storage: idempotency key reused with a different task")

	// ErrAlreadyEnded means RequestCancel was called on a task that already
	// finished or failed.
	ErrAlreadyEnded = errors.New("storage: task already ended")
)

// taskColumns is the column list, in order, that scanTask reads. Use it in
// SELECT and RETURNING clauses. COALESCE maps NULL text to "" because a Go
// string cannot hold NULL.
const taskColumns = `
	id, task_type, request, state, status, generation, retriable_error_count,
	COALESCE(last_error, ''), COALESCE(cancel_reason, ''),
	COALESCE(idempotency_key, ''), COALESCE(lease_owner, ''),
	lease_expires_at, run_after, created_at, modified_at`

// scanTask reads one task from a row whose columns are taskColumns.
//
// Parameters:
//   - row: the result of QueryRow, or the current row of a Rows iteration.
//
// Returns the task. The scan error is returned unwrapped, so callers can
// detect pgx.ErrNoRows.
func scanTask(row pgx.Row) (TaskState, error) {
	var t TaskState
	var expires *time.Time
	err := row.Scan(
		&t.ID, &t.Type, &t.Request, &t.State, &t.Status, &t.Generation,
		&t.RetriableErrorCount, &t.LastError, &t.CancelReason,
		&t.IdempotencyKey, &t.LeaseOwner, &expires,
		&t.RunAfter, &t.CreatedAt, &t.ModifiedAt,
	)
	if err != nil {
		return TaskState{}, err
	}
	if expires != nil {
		t.LeaseExpiresAt = *expires
	}
	return t, nil
}

// notFoundIfNoRows adds context to an error from a single-row query.
//
// Parameters:
//   - op: names the operation in the error message.
//   - err: the query error.
//
// Returns an error wrapping ErrNotFound if err is pgx.ErrNoRows, and err
// wrapped with op otherwise.
func notFoundIfNoRows(op string, err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("storage: %s: %w", op, ErrNotFound)
	}
	return fmt.Errorf("storage: %s: %w", op, err)
}
