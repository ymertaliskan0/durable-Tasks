package storage

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// RequestCancel asks for a task to be cancelled. Calling it again is safe.
//
// A ready_to_run or running task moves to ready_to_cancel with cancel reason
// requested, and its lease is released. Its generation is incremented, so a
// worker still running it gets ErrWrongGeneration on its next heartbeat or
// write and stops; a worker then claims it from ready_to_cancel and runs
// Task.Cancel. A task already on the cancel path is left unchanged, including
// its cancel reason, so a task cleaning up after a failure still ends as
// failed.
//
// Parameters:
//   - id: the task to cancel.
//
// Returns nil if the task is now on the cancel path or already was. Returns
// an error wrapping ErrAlreadyEnded if the task finished or failed, and one
// wrapping ErrNotFound if no task has this id.
func (p *Postgres) RequestCancel(ctx context.Context, id uuid.UUID) error {
	tag, err := p.pool.Exec(ctx, `
		UPDATE tasks SET
			status           = 'ready_to_cancel',
			cancel_reason    = 'requested',
			generation       = generation + 1,
			run_after        = now(),
			lease_owner      = NULL,
			lease_expires_at = NULL,
			modified_at      = now()
		WHERE id = $1 AND status IN ('ready_to_run', 'running')`,
		id)
	if err != nil {
		return fmt.Errorf("storage: request cancel: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	// Nothing changed: find out why. A task on the cancel path never leaves
	// it and an ended task never changes, so the answer cannot become wrong
	// between the two statements.
	var status Status
	err = p.pool.QueryRow(ctx, "SELECT status FROM tasks WHERE id = $1", id).Scan(&status)
	if err != nil {
		return notFoundIfNoRows("request cancel", err)
	}

	switch status {
	case StatusFinished, StatusFailed:
		return fmt.Errorf("storage: request cancel: %w", ErrAlreadyEnded)
	default:
		return nil
	}
}
