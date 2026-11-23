package tasks

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// executionContext is the ExecutionContext handed to one execution of one
// task. It is bound to the generation the task was claimed at, so every
// write it makes is fenced.
type executionContext struct {
	store Store
	task  Task
	id    uuid.UUID
	gen   int64
}

func (e *executionContext) TaskID() uuid.UUID { return e.id }

func (e *executionContext) Generation() int64 { return e.gen }

// SaveState persists the task's current state, as returned by Task.Save.
//
// Returns an error wrapping storage.ErrWrongGeneration if this execution no
// longer owns the task; the task must then stop.
func (e *executionContext) SaveState(ctx context.Context) error {
	state, err := e.task.Save()
	if err != nil {
		return fmt.Errorf("tasks: save state: %w", err)
	}
	return e.store.SaveState(ctx, e.id, e.gen, state)
}
