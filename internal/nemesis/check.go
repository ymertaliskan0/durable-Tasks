package nemesis

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/ymertaliskan0/durable-tasks/internal/storage"
	"github.com/ymertaliskan0/durable-tasks/internal/tasks/chunkcopy"
)

// job is one copy task the crash test submitted.
type job struct {
	ID  uuid.UUID
	Src string
	Dst string
}

// TaskReader is the part of the store the checker needs.
type TaskReader interface {
	GetTask(ctx context.Context, id uuid.UUID) (storage.TaskState, error)
}

// Result is the checker's verdict on one crash-test run.
type Result struct {
	Tasks         int      // tasks checked
	Finished      int      // tasks that ended as finished
	Recovered     int      // finished tasks that were interrupted at least once (generation > 1)
	MaxGeneration int64    // highest generation seen: how often one task changed hands
	Retries       int      // sum of retriable_error_count
	Problems      []string // one line per violated invariant; empty means the run passed
}

// Passed reports whether every invariant held.
func (r Result) Passed() bool { return len(r.Problems) == 0 }

// check verifies the crash test's invariants for every job: the task
// exists, it finished, it holds no lease, and its destination is
// byte-identical to its source (SHA-256).
//
// Parameters:
//   - store: reads the tasks' final states.
//   - jobs: the tasks the crash test submitted, with their file paths.
//
// Returns the verdict, with exactly one problem per broken task. Returns an
// error only if the check itself cannot run, for example when the database
// or a source file is unreadable.
func check(ctx context.Context, store TaskReader, jobs []job) (Result, error) {
	r := Result{Tasks: len(jobs)}
	for _, j := range jobs {
		state, err := store.GetTask(ctx, j.ID)
		if errors.Is(err, storage.ErrNotFound) {
			r.Problems = append(r.Problems, fmt.Sprintf("task %s: lost", j.ID))
			continue
		}
		if err != nil {
			return Result{}, fmt.Errorf("check: %w", err)
		}
		r.Retries += state.RetriableErrorCount
		r.MaxGeneration = max(r.MaxGeneration, state.Generation)
		if state.Status != storage.StatusFinished {
			r.Problems = append(r.Problems, fmt.Sprintf("task %s: expected finished, got %s (last error %q)", j.ID, state.Status, state.LastError))
			continue
		}
		r.Finished++
		if state.Generation > 1 {
			r.Recovered++
		}
		if state.LeaseOwner != "" {
			r.Problems = append(r.Problems, fmt.Sprintf("task %s: finished with active lease", j.ID))
		}
		srcHash, err := chunkcopy.HashFile(j.Src)
		if err != nil {
			return Result{}, fmt.Errorf("check: %w", err)
		}
		dstHash, err := chunkcopy.HashFile(j.Dst)
		if err != nil {
			r.Problems = append(r.Problems, fmt.Sprintf("task %s: unreadable destination: %v", j.ID, err))
			continue
		}
		if !bytes.Equal(srcHash, dstHash) {
			r.Problems = append(r.Problems, fmt.Sprintf("task %s: copy mismatch", j.ID))
		}
	}
	return r, nil
}
