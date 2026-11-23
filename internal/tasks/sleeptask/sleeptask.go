// Package sleeptask is a demo task that sleeps for a number of steps,
// checkpointing after each one. It is used by tests and benchmarks.
package sleeptask

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ymertaliskan0/durable-tasks/internal/tasks"
)

// Type is the task type name to register and submit with.
const Type = "sleep"

// Request describes the work: Steps sleeps of StepDuration each.
type Request struct {
	Steps        int           `json:"steps"`
	StepDuration time.Duration `json:"step_duration"`
}

// State is the task's checkpoint: the index of the next step to run.
type State struct {
	Next int `json:"next"`
}

// Task implements tasks.Task.
type Task struct {
	req   Request
	state State
}

// New returns an empty Task; it is the tasks.Factory for Type.
func New() tasks.Task { return &Task{} }

// Load restores the task from its request and saved state.
func (t *Task) Load(request, state []byte) error {
	if err := json.Unmarshal(request, &t.req); err != nil {
		return fmt.Errorf("sleeptask: decode request: %w", err)
	}
	if t.req.Steps <= 0 || t.req.StepDuration < 0 {
		return errors.New("sleeptask: steps must be positive and step_duration non-negative")
	}
	if state != nil {
		if err := json.Unmarshal(state, &t.state); err != nil {
			return fmt.Errorf("sleeptask: decode state: %w", err)
		}
	}
	return nil
}

// Save returns the task's checkpoint.
func (t *Task) Save() ([]byte, error) { return json.Marshal(t.state) }

// Run sleeps through the remaining steps, checkpointing after each one.
func (t *Task) Run(ctx context.Context, exec tasks.ExecutionContext) error {
	steps := make([]tasks.Step, t.req.Steps)
	for i := range steps {
		steps[i] = func(ctx context.Context) error { return sleep(ctx, t.req.StepDuration) }
	}
	return tasks.RunSteps(ctx, t.state.Next, steps, func(ctx context.Context, next int) error {
		t.state.Next = next
		return exec.SaveState(ctx)
	})
}

// Cancel has nothing to undo.
func (t *Task) Cancel(context.Context, tasks.ExecutionContext) error { return nil }

// sleep waits for d or until ctx is done, whichever comes first.
func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
