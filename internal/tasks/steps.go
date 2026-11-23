package tasks

import (
	"context"
	"fmt"
)

// RunSteps runs steps[start:] in order and checkpoints after each one, so a
// task that is interrupted resumes at the first step that did not complete.
//
// It checks ctx before every step and stops as soon as ctx is done.
//
// Parameters:
//   - start: the index of the first step to run, usually loaded from the
//     task's saved state.
//   - steps: the task's steps; each must be safe to repeat, because a crash
//     after a step but before its checkpoint runs it again.
//   - saveState: called after step i with next = i+1; it records next in the
//     task's state and persists it (ExecutionContext.SaveState).
//
// Returns the first error from a step, from saveState or from ctx.
func RunSteps(ctx context.Context, start int, steps []Step, saveState func(ctx context.Context, next int) error) error {
	if start < 0 || start > len(steps) {
		return NonRetriable(fmt.Errorf("start step %d out of range [0, %d]", start, len(steps)))
	}
	for i := start; i < len(steps); i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := steps[i](ctx); err != nil {
			return err
		}
		if err := saveState(ctx, i+1); err != nil {
			return err
		}
	}
	return nil
}
