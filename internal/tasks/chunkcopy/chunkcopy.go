// Package chunkcopy is a task that copies a file in fixed-size chunks,
// checkpointing after each chunk, and finally verifies the copy with SHA-256.
//
// Every chunk write is positional and deterministic: chunk i always writes
// the same bytes at offset i*ChunkSize. A chunk that is written twice (after
// a crash, or by a zombie worker that lost the task) therefore leaves the
// file unchanged, which is what makes the task safe to resume and repeat.
package chunkcopy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/ymertaliskan0/durable-tasks/internal/tasks"
)

// Type is the task type name to register and submit with.
const Type = "chunkcopy"

// DefaultChunkSize is used when a request does not set ChunkSize.
const DefaultChunkSize = 4 << 20 // 4 MiB

// Request describes one copy. Src and Dst must be absolute paths, because
// the worker's working directory is unrelated to the submitter's.
type Request struct {
	Src       string `json:"src"`
	Dst       string `json:"dst"`
	ChunkSize int64  `json:"chunk_size,omitempty"`
	// ChunkDelay pauses after each chunk. It slows copies down for demos and
	// the crash test, so tasks are still running when workers are killed.
	ChunkDelay time.Duration `json:"chunk_delay,omitempty"`
}

// State is the task's checkpoint.
type State struct {
	Size      int64 `json:"size"`       // size of Src, recorded when the copy starts
	Started   bool  `json:"started"`    // Dst has been created with the right size
	NextChunk int   `json:"next_chunk"` // first chunk not yet copied
}

// Task implements tasks.Task.
type Task struct {
	req   Request
	state State
}

// New returns an empty Task; it is the tasks.Factory for Type.
func New() tasks.Task { return &Task{} }

// Load restores the task from its request and saved state.
//
// Returns an error if the request is malformed; the worker treats it as
// non-retriable.
func (t *Task) Load(request, state []byte) error {
	if err := json.Unmarshal(request, &t.req); err != nil {
		return fmt.Errorf("chunkcopy: decode request: %w", err)
	}
	if !filepath.IsAbs(t.req.Src) || !filepath.IsAbs(t.req.Dst) {
		return errors.New("chunkcopy: src and dst must be absolute paths")
	}
	if t.req.ChunkSize == 0 {
		t.req.ChunkSize = DefaultChunkSize
	}
	if t.req.ChunkSize < 0 || t.req.ChunkDelay < 0 {
		return errors.New("chunkcopy: chunk_size and chunk_delay must not be negative")
	}
	if state != nil {
		if err := json.Unmarshal(state, &t.state); err != nil {
			return fmt.Errorf("chunkcopy: decode state: %w", err)
		}
	}
	return nil
}

// Save returns the task's checkpoint.
func (t *Task) Save() ([]byte, error) { return json.Marshal(t.state) }

// Run copies the remaining chunks and verifies the result.
//
// The first execution records the source size and creates Dst at that size.
// Each chunk is flushed to disk (fsync) before its checkpoint is saved, so a
// checkpoint never claims more than what is durably written.
//
// Returns a NonRetriable error if the source is missing or the final copy
// does not match the source; I/O errors while copying are retriable.
func (t *Task) Run(ctx context.Context, exec tasks.ExecutionContext) error {
	src, err := os.Open(t.req.Src)
	if errors.Is(err, fs.ErrNotExist) {
		return tasks.NonRetriable(fmt.Errorf("chunkcopy: %w", err))
	}
	if err != nil {
		return tasks.Retriable(fmt.Errorf("chunkcopy: %w", err))
	}
	defer func() { _ = src.Close() }()

	if !t.state.Started {
		info, err := src.Stat()
		if err != nil {
			return tasks.Retriable(fmt.Errorf("chunkcopy: %w", err))
		}
		t.state.Size = info.Size()
	}

	dst, err := os.OpenFile(t.req.Dst, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return tasks.Retriable(fmt.Errorf("chunkcopy: %w", err))
	}
	defer func() { _ = dst.Close() }()

	if !t.state.Started {
		// Truncate also cuts off anything a previous, larger file left behind.
		if err := dst.Truncate(t.state.Size); err != nil {
			return tasks.Retriable(fmt.Errorf("chunkcopy: %w", err))
		}
		t.state.Started = true
		if err := exec.SaveState(ctx); err != nil {
			return err
		}
	}

	chunks := int((t.state.Size + t.req.ChunkSize - 1) / t.req.ChunkSize)
	steps := make([]tasks.Step, chunks)
	for i := range steps {
		off := int64(i) * t.req.ChunkSize
		n := min(t.req.ChunkSize, t.state.Size-off)
		steps[i] = func(ctx context.Context) error {
			if err := copyChunk(src, dst, off, n); err != nil {
				return tasks.Retriable(err)
			}
			return pause(ctx, t.req.ChunkDelay)
		}
	}
	err = tasks.RunSteps(ctx, t.state.NextChunk, steps, func(ctx context.Context, next int) error {
		if err := dst.Sync(); err != nil {
			return tasks.Retriable(fmt.Errorf("chunkcopy: fsync: %w", err))
		}
		t.state.NextChunk = next
		return exec.SaveState(ctx)
	})
	if err != nil {
		return err
	}
	return verify(t.req.Src, t.req.Dst)
}

// Cancel removes Dst, undoing a partial copy. A missing Dst is not an error,
// so Cancel can be repeated.
func (t *Task) Cancel(context.Context, tasks.ExecutionContext) error {
	if err := os.Remove(t.req.Dst); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("chunkcopy: remove destination: %w", err)
	}
	return nil
}

// copyChunk copies n bytes at offset off from src to the same offset in dst.
//
// The copy is positional: chunk i always writes the same bytes to the same
// place, so repeating it (after a crash, or from a zombie worker) leaves
// the file unchanged.
//
// Returns an error if fewer than n bytes could be read or written.
func copyChunk(src io.ReaderAt, dst io.WriterAt, off, n int64) error {
	buffer := make([]byte, n)
	read, err := src.ReadAt(buffer, off)
	if err != nil && read < int(n) {
		return fmt.Errorf("chunkcopy: read at %d: %w", off, err)
	}

	written, err := dst.WriteAt(buffer[:read], off)
	if err != nil || written < read {
		return fmt.Errorf("chunkcopy: write at %d: %w", off, err)
	}

	return nil
}

// pause waits for d, or until ctx is done.
func pause(ctx context.Context, d time.Duration) error {
	if d == 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// verify compares the SHA-256 of src and dst.
//
// Returns a NonRetriable error on a mismatch: repeating the same chunks
// cannot fix it.
func verify(src, dst string) error {
	a, err := hashFile(src)
	if err != nil {
		return tasks.Retriable(err)
	}
	b, err := hashFile(dst)
	if err != nil {
		return tasks.Retriable(err)
	}
	if !bytes.Equal(a, b) {
		return tasks.NonRetriable(fmt.Errorf("chunkcopy: %s does not match %s after copying", dst, src))
	}
	return nil
}

// HashFile returns the SHA-256 of the file at path.
func HashFile(path string) ([]byte, error) { return hashFile(path) }

func hashFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("chunkcopy: %w", err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, fmt.Errorf("chunkcopy: hash %s: %w", path, err)
	}
	return h.Sum(nil), nil
}
