package storage

import (
	"context"
	"fmt"
)

// ReclaimExpired hands back tasks whose owner stopped heartbeating, for
// example because it crashed or was paused.
//
// A running task whose lease has expired returns to ready_to_run, and an
// expired cancelling task returns to ready_to_cancel, immediately claimable.
// Each reclaimed task's generation is incremented, so the old owner's writes
// are rejected if it wakes up, and its lease is released. The retry counter
// is not incremented: an expired lease says the worker failed, not the task.
// Concurrent callers never reclaim the same task.
//
// Parameters:
//   - limit: the maximum number of tasks to reclaim in this call, oldest
//     lease first.
//
// Returns the number of tasks reclaimed.
func (p *Postgres) ReclaimExpired(ctx context.Context, limit int) (int, error) {
	tag, err := p.pool.Exec(ctx, `
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
		WHERE id IN (
			SELECT id FROM tasks
			WHERE status IN ('running', 'cancelling')
			  AND lease_expires_at < now()
			ORDER BY lease_expires_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)`,
		limit)
	if err != nil {
		return 0, fmt.Errorf("storage: reclaim expired: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
