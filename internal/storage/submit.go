package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// NewTask describes a task a client submits. The stored task starts as
// ready_to_run at generation 0 and is claimable immediately.
type NewTask struct {
	Type           string // selects the Task implementation that runs it
	Request        []byte // the task's input, serialized by its type; stored as-is
	IdempotencyKey string // optional; "" means none
}

// CreateTask stores a new task.
//
// Submitting is idempotent per key: if a task with the same IdempotencyKey
// already exists, no task is created and the existing task's id is returned.
// This holds even when several clients submit the same key concurrently.
// Tasks without a key are always created.
//
// Parameters:
//   - nt: the task to create.
//
// Returns the task's id, and created = true if this call inserted it. Returns
// an error wrapping ErrIdempotencyConflict if the key is already used by a
// task with a different type or request.
func (p *Postgres) CreateTask(ctx context.Context, nt NewTask) (id uuid.UUID, created bool, err error) {
	id, err = uuid.NewV7()
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("storage: create task: new id: %w", err)
	}

	if nt.Request == nil {
		nt.Request = []byte{}
	}

	err = p.pool.QueryRow(ctx, `
		INSERT INTO tasks (id, task_type, request, status, idempotency_key)
		VALUES ($1, $2, $3, 'ready_to_run', NULLIF($4, ''))
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id`,
		id, nt.Type, nt.Request, nt.IdempotencyKey).Scan(&id)
	if err == nil {
		return id, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, fmt.Errorf("storage: create task: %w", err)
	}

	// No row returned: ON CONFLICT skipped the insert because a task with
	// this key already exists. Load it and check it is the same submission.
	existing, err := scanTask(p.pool.QueryRow(ctx,
		"SELECT "+taskColumns+" FROM tasks WHERE idempotency_key = $1",
		nt.IdempotencyKey))
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("storage: create task: load existing: %w", err)
	}
	if existing.Type != nt.Type || !bytes.Equal(existing.Request, nt.Request) {
		return uuid.Nil, false, fmt.Errorf("storage: create task %q: %w", nt.IdempotencyKey, ErrIdempotencyConflict)
	}
	return existing.ID, false, nil
}

// GetTask loads one task.
//
// Parameters:
//   - id: the task to load.
//
// Returns the task, or an error wrapping ErrNotFound if no task has this id.
func (p *Postgres) GetTask(ctx context.Context, id uuid.UUID) (TaskState, error) {
	t, err := scanTask(p.pool.QueryRow(ctx, "SELECT "+taskColumns+" FROM tasks WHERE id = $1", id))
	if err != nil {
		return TaskState{}, notFoundIfNoRows("get task", err)
	}
	return t, nil
}

// ListTasks returns tasks ordered by creation time, oldest first.
//
// Parameters:
//   - status: only return tasks in this status; "" returns every status.
//   - limit: the maximum number of tasks to return.
//
// Returns the matching tasks; the slice is empty if there are none.
func (p *Postgres) ListTasks(ctx context.Context, status Status, limit int) ([]TaskState, error) {
	rows, err := p.pool.Query(ctx, "SELECT "+taskColumns+`
		FROM tasks
		WHERE $1 = '' OR status = $1
		ORDER BY created_at, id
		LIMIT $2`,
		string(status), limit)
	if err != nil {
		return nil, fmt.Errorf("storage: list tasks: %w", err)
	}
	defer rows.Close()

	var tasks []TaskState
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("storage: list tasks: %w", err)
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: list tasks: %w", err)
	}
	return tasks, nil
}

// CountByStatus counts the tasks in each status.
//
// Returns a map from status to count. Statuses with no tasks are absent.
func (p *Postgres) CountByStatus(ctx context.Context) (map[Status]int, error) {
	rows, err := p.pool.Query(ctx, "SELECT status, count(*) FROM tasks GROUP BY status")
	if err != nil {
		return nil, fmt.Errorf("storage: count by status: %w", err)
	}
	defer rows.Close()

	counts := make(map[Status]int)
	for rows.Next() {
		var status Status
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("storage: count by status: %w", err)
		}
		counts[status] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: count by status: %w", err)
	}
	return counts, nil
}
