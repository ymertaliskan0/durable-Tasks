package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// This file holds everything a worker does to a task it executes: claim it,
// then make fenced writes until the task leaves its hands.
//
// Every write after the claim is fenced:
//
//	WHERE id = $1 AND generation = $2 AND status IN (<expected statuses>)
//
// If it matches no row, the caller no longer owns the task and gets
// ErrWrongGeneration. The status check is needed as well as the generation,
// because Finish does not change the generation: without it, a heartbeat that
// arrives after Finish would match and hand a finished task a new lease.
//
// Every write that leaves an in-flight status (running, cancelling) clears
// both lease_owner and lease_expires_at, and every write sets modified_at.
// Times always come from the database clock, now(), never from the worker.

// claimSQL takes the next due task. The subquery picks one row and locks it;
// SKIP LOCKED makes concurrent claimers pass over rows another transaction
// has locked, so every caller gets a different task.
const claimSQL = `
	UPDATE tasks SET
		status = CASE status
			WHEN 'ready_to_run'    THEN 'running'
			WHEN 'ready_to_cancel' THEN 'cancelling'
		END,
		generation       = generation + 1,
		lease_owner      = $1,
		lease_expires_at = now() + $2::interval,
		modified_at      = now()
	WHERE id = (
		SELECT id FROM tasks
		WHERE status IN ('ready_to_run', 'ready_to_cancel')
		  AND run_after <= now()
		ORDER BY run_after
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	)
	RETURNING ` + taskColumns

// ClaimNext atomically claims the next due task for a worker.
//
// It picks the ready_to_run or ready_to_cancel task with the earliest
// run_after that is due, moves it to running or cancelling respectively,
// increments its generation and gives owner a lease. Concurrent callers never
// claim the same task.
//
// Parameters:
//   - owner: identifies the claiming worker; stored as the lease owner.
//   - lease: how long the claim stays valid without a heartbeat.
//
// Returns the task as it is after the claim, or ErrNoTask if no task is due.
func (p *Postgres) ClaimNext(ctx context.Context, owner string, lease time.Duration) (TaskState, error) {
	task, err := scanTask(p.pool.QueryRow(ctx, claimSQL, owner, lease))
	if errors.Is(err, pgx.ErrNoRows) {
		return TaskState{}, ErrNoTask
	}
	if err != nil {
		return TaskState{}, fmt.Errorf("storage: claim next: %w", err)
	}
	return task, nil
}

// fencedUpdate runs one fenced UPDATE.
//
// Parameters:
//   - op: names the operation in error messages.
//   - sql: an UPDATE whose WHERE clause checks id, generation and status.
//   - args: the values for the placeholders in sql.
//
// Returns an error wrapping ErrWrongGeneration if the UPDATE matched no row.
func (p *Postgres) fencedUpdate(ctx context.Context, op, sql string, args ...any) error {
	tag, err := p.pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("storage: %s: %w", op, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("storage: %s: %w", op, ErrWrongGeneration)
	}
	return nil
}

// Heartbeat extends the lease of a running or cancelling task to now()+lease.
//
// Parameters:
//   - id, gen: the task and the generation the caller claimed it at.
//   - lease: the new lease duration, counted from now.
//
// Returns an error wrapping ErrWrongGeneration if the caller no longer owns
// the task; the caller must then stop working on it.
func (p *Postgres) Heartbeat(ctx context.Context, id uuid.UUID, gen int64, lease time.Duration) error {
	return p.fencedUpdate(ctx, "heartbeat", `
		UPDATE tasks SET
			lease_expires_at = now() + $3::interval,
			modified_at      = now()
		WHERE id = $1 AND generation = $2 AND status IN ('running', 'cancelling')`,
		id, gen, lease)
}

// SaveState stores a checkpoint of a running or cancelling task.
//
// Parameters:
//   - id, gen: the task and the generation the caller claimed it at.
//   - state: the serialized task progress.
//
// Returns an error wrapping ErrWrongGeneration if the caller no longer owns
// the task.
func (p *Postgres) SaveState(ctx context.Context, id uuid.UUID, gen int64, state []byte) error {
	return p.fencedUpdate(ctx, "save state", `
		UPDATE tasks SET
			state       = $3,
			modified_at = now()
		WHERE id = $1 AND generation = $2 AND status IN ('running', 'cancelling')`,
		id, gen, state)
}

// Finish marks a running task finished and releases its lease.
//
// Parameters:
//   - id, gen: the task and the generation the caller claimed it at.
//   - state: the task's final serialized state.
//
// Returns an error wrapping ErrWrongGeneration if the caller no longer owns
// the task or the task is not running.
func (p *Postgres) Finish(ctx context.Context, id uuid.UUID, gen int64, state []byte) error {
	return p.fencedUpdate(ctx, "finish", `
		UPDATE tasks SET
			status           = 'finished',
			state            = $3,
			lease_owner      = NULL,
			lease_expires_at = NULL,
			modified_at      = now()
		WHERE id = $1 AND generation = $2 AND status = 'running'`,
		id, gen, state)
}

// ScheduleRetry hands a task back after a retriable error.
//
// A running task returns to ready_to_run and a cancelling task to
// ready_to_cancel. The retry counter is incremented, the error is recorded,
// the lease is released, and the task cannot be claimed again until delay has
// passed.
//
// Parameters:
//   - id, gen: the task and the generation the caller claimed it at.
//   - state: the latest serialized state.
//   - lastErr: the error message to record.
//   - delay: the backoff before the task becomes claimable again.
//
// Returns an error wrapping ErrWrongGeneration if the caller no longer owns
// the task.
func (p *Postgres) ScheduleRetry(ctx context.Context, id uuid.UUID, gen int64, state []byte, lastErr string, delay time.Duration) error {
	return p.fencedUpdate(ctx, "schedule retry", `
		UPDATE tasks SET
			status = CASE status
				WHEN 'running'    THEN 'ready_to_run'
				WHEN 'cancelling' THEN 'ready_to_cancel'
			END,
			state                 = $3,
			retriable_error_count = retriable_error_count + 1,
			last_error            = $4,
			run_after             = now() + $5::interval,
			lease_owner           = NULL,
			lease_expires_at      = NULL,
			modified_at           = now()
		WHERE id = $1 AND generation = $2 AND status IN ('running', 'cancelling')`,
		id, gen, state, lastErr, delay)
}

// Fail sends a running task down the cleanup path after a non-retriable
// error.
//
// The task moves to ready_to_cancel with cancel reason failed, so a worker
// runs Task.Cancel to undo partial work and the task then ends as failed. The
// lease is released.
//
// Parameters:
//   - id, gen: the task and the generation the caller claimed it at.
//   - state: the latest serialized state, which Cancel needs to know what to
//     undo.
//   - lastErr: the error message to record.
//
// Returns an error wrapping ErrWrongGeneration if the caller no longer owns
// the task or the task is not running.
func (p *Postgres) Fail(ctx context.Context, id uuid.UUID, gen int64, state []byte, lastErr string) error {
	return p.fencedUpdate(ctx, "fail", `
		UPDATE tasks SET
			status           = 'ready_to_cancel',
			cancel_reason    = 'failed',
			state            = $3,
			last_error       = $4,
			run_after        = now(),
			lease_owner      = NULL,
			lease_expires_at = NULL,
			modified_at      = now()
		WHERE id = $1 AND generation = $2 AND status = 'running'`,
		id, gen, state, lastErr)
}

// MarkCancelled ends a cancelling task after Task.Cancel succeeded.
//
// The final status is failed if the task was cancelled because it failed,
// and cancelled if a user requested it. The lease is released.
//
// Parameters:
//   - id, gen: the task and the generation the caller claimed it at.
//
// Returns an error wrapping ErrWrongGeneration if the caller no longer owns
// the task or the task is not cancelling.
func (p *Postgres) MarkCancelled(ctx context.Context, id uuid.UUID, gen int64) error {
	return p.fencedUpdate(ctx, "mark cancelled", `
		UPDATE tasks SET
			status = CASE cancel_reason
				WHEN 'failed'    THEN 'failed'
				WHEN 'requested' THEN 'cancelled'
			END,
			lease_owner      = NULL,
			lease_expires_at = NULL,
			modified_at      = now()
		WHERE id = $1 AND generation = $2 AND status = 'cancelling'`,
		id, gen)
}

// Release gives a task back during graceful shutdown.
//
// A running task returns to ready_to_run and a cancelling task to
// ready_to_cancel, immediately claimable. The generation is incremented, so
// any late write from the releasing worker is rejected.
//
// Parameters:
//   - id, gen: the task and the generation the caller claimed it at.
//
// Returns an error wrapping ErrWrongGeneration if the caller no longer owns
// the task.
func (p *Postgres) Release(ctx context.Context, id uuid.UUID, gen int64) error {
	return p.fencedUpdate(ctx, "release", `
		UPDATE tasks SET
			status = CASE status
				WHEN 'running'    THEN 'ready_to_run'
				WHEN 'cancelling' THEN 'ready_to_cancel'
			END,
			generation       = generation + 1,
			run_after        = now(),
			lease_owner      = NULL,
			lease_expires_at = NULL,
			modified_at      = now()
		WHERE id = $1 AND generation = $2 AND status IN ('running', 'cancelling')`,
		id, gen)
}
