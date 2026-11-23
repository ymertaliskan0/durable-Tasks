// Package tasks is the execution framework. It holds the Task contract that
// concrete tasks implement and, from Milestone 3, the Worker that claims,
// runs, heartbeats and retries them.
//
// Modelled on cloud/tasks in Nebius NBS (github.com/ydb-platform/nbs).
package tasks

import (
	"context"

	"github.com/google/uuid"
)

// Task is one durable, resumable unit of work. The framework may stop a task
// at any point (crash, lease loss, cancellation), build a new instance on
// another worker, call Load with the last saved state, and call Run again.
// So Run must be safe to repeat from any saved state.
type Task interface {
	// Save serializes the task's progress. The framework calls it whenever
	// state is persisted (ExecutionContext.SaveState, finish, retry).
	Save() ([]byte, error)

	// Load restores a task from its immutable request and its last saved
	// state. state is nil when the task has never saved.
	Load(request, state []byte) error

	// Run does the work. It must return promptly once ctx is done. Returning
	// nil marks the task finished.
	Run(ctx context.Context, exec ExecutionContext) error

	// Cancel undoes partial work (for example, deletes a half-written file).
	// It may run after a crash, so it must also be idempotent.
	Cancel(ctx context.Context, exec ExecutionContext) error
}

// ExecutionContext is the framework's handle passed into Run and Cancel.
type ExecutionContext interface {
	TaskID() uuid.UUID

	// Generation is the fencing token of this execution attempt. Every
	// claim, reclaim or cancel request increments it in the database.
	Generation() int64

	// SaveState persists Task.Save() if this execution still owns the task.
	// It returns an error wrapping storage.ErrWrongGeneration otherwise, and
	// the task must then stop immediately.
	SaveState(ctx context.Context) error
}

// Step is one checkpointed unit of work inside Run. RunSteps (Milestone 3)
// saves state after each step, so on restart a task resumes at the first
// step that did not complete.
type Step func(ctx context.Context) error
