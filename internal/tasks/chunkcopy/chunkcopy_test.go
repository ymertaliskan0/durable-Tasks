package chunkcopy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/ymertaliskan0/durable-tasks/internal/tasks"
)

// fakeExec records how often the task checkpoints, and can fail the n-th
// SaveState to simulate a crash right after it.
type fakeExec struct {
	task   tasks.Task
	saves  int
	failAt int // SaveState number that fails; 0 = never
	states [][]byte
}

var errCrash = errors.New("simulated crash")

func (*fakeExec) TaskID() uuid.UUID { return uuid.Nil }
func (*fakeExec) Generation() int64 { return 1 }
func (e *fakeExec) SaveState(context.Context) error {
	e.saves++
	if e.saves == e.failAt {
		return errCrash
	}
	s, _ := e.task.Save()
	e.states = append(e.states, s)
	return nil
}

func randomFile(t *testing.T, size int) string {
	t.Helper()
	data := make([]byte, size)
	r := rand.NewChaCha8([32]byte{byte(size)})
	_, _ = r.Read(data)
	path := filepath.Join(t.TempDir(), "src")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func load(t *testing.T, req Request, state []byte) *Task {
	t.Helper()
	b, _ := json.Marshal(req)
	task := New().(*Task)
	if err := task.Load(b, state); err != nil {
		t.Fatal(err)
	}
	return task
}

func sameContent(t *testing.T, a, b string) {
	t.Helper()
	x, err := os.ReadFile(a)
	if err != nil {
		t.Fatal(err)
	}
	y, err := os.ReadFile(b)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(x, y) {
		t.Fatalf("%s and %s differ (sizes %d and %d)", a, b, len(x), len(y))
	}
}

func TestCopyChunk(t *testing.T) {
	src := bytes.NewReader([]byte("0123456789"))
	dst := make(writerAt, 10)
	if err := copyChunk(src, dst, 3, 4); err != nil {
		t.Fatal(err)
	}
	if got := string(dst); got != "\x00\x00\x003456\x00\x00\x00" {
		t.Errorf("dst = %q, want bytes 3-6 copied in place", got)
	}
	// Writing the same chunk again (a zombie or a retry) changes nothing.
	if err := copyChunk(src, dst, 3, 4); err != nil {
		t.Fatal(err)
	}
	if got := string(dst); got != "\x00\x00\x003456\x00\x00\x00" {
		t.Errorf("after a repeated write, dst = %q", got)
	}
	if err := copyChunk(src, dst, 8, 4); err == nil {
		t.Error("copying past the end of src should fail")
	}
}

// eofReader returns io.EOF together with a full read at the end of its
// data, which the io.ReaderAt contract allows.
type eofReader []byte

func (r eofReader) ReadAt(p []byte, off int64) (int, error) {
	n := copy(p, r[off:])
	if off+int64(n) == int64(len(r)) {
		return n, io.EOF
	}
	return n, nil
}

func TestCopyChunk_FullReadWithEOF(t *testing.T) {
	dst := make(writerAt, 4)
	if err := copyChunk(eofReader("abcd"), dst, 2, 2); err != nil {
		t.Fatalf("a full read ending in io.EOF is not an error: %v", err)
	}
	if string(dst) != "\x00\x00cd" {
		t.Errorf("dst = %q", string(dst))
	}
}

// writerAt is an in-memory io.WriterAt.
type writerAt []byte

func (w writerAt) WriteAt(p []byte, off int64) (int, error) {
	return copy(w[off:], p), nil
}

func TestRun_CopiesAndVerifies(t *testing.T) {
	for _, size := range []int{0, 1, 1000, 1024, 5000} {
		src := randomFile(t, size)
		dst := filepath.Join(t.TempDir(), "dst")
		task := load(t, Request{Src: src, Dst: dst, ChunkSize: 1024}, nil)
		exec := &fakeExec{task: task}
		if err := task.Run(t.Context(), exec); err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		sameContent(t, src, dst)
		wantSaves := 1 + (size+1023)/1024 // start + one per chunk
		if exec.saves != wantSaves {
			t.Errorf("size %d: %d checkpoints, want %d", size, exec.saves, wantSaves)
		}
	}
}

func TestRun_ResumesAfterCrash(t *testing.T) {
	src := randomFile(t, 10_000)
	dst := filepath.Join(t.TempDir(), "dst")
	req := Request{Src: src, Dst: dst, ChunkSize: 1000}

	// First execution "crashes" on its 5th checkpoint, the one after chunk 3.
	first := load(t, req, nil)
	exec := &fakeExec{task: first, failAt: 5}
	if err := first.Run(t.Context(), exec); !errors.Is(err, errCrash) {
		t.Fatalf("first run: err = %v, want the simulated crash", err)
	}
	last := exec.states[len(exec.states)-1]

	// A new instance resumes from the last saved checkpoint.
	second := load(t, req, last)
	exec2 := &fakeExec{task: second}
	if err := second.Run(t.Context(), exec2); err != nil {
		t.Fatal(err)
	}
	sameContent(t, src, dst)
	if exec2.saves != 7 { // chunks 3..9: chunk 3 is redone because its checkpoint was lost
		t.Errorf("resumed run made %d checkpoints, want 7", exec2.saves)
	}
}

func TestRun_DetectsCorruption(t *testing.T) {
	src := randomFile(t, 4000)
	dst := filepath.Join(t.TempDir(), "dst")
	task := load(t, Request{Src: src, Dst: dst, ChunkSize: 1000}, nil)
	if err := task.Run(t.Context(), &fakeExec{task: task}); err != nil {
		t.Fatal(err)
	}
	// Corrupt the copy and pretend every chunk is done: only verify runs.
	if err := os.WriteFile(dst, make([]byte, 4000), 0o644); err != nil {
		t.Fatal(err)
	}
	done := load(t, Request{Src: src, Dst: dst, ChunkSize: 1000}, []byte(`{"size":4000,"started":true,"next_chunk":4}`))
	if err := done.Run(t.Context(), &fakeExec{task: done}); !tasks.IsNonRetriable(err) {
		t.Errorf("err = %v, want a non-retriable mismatch", err)
	}
}

func TestRun_MissingSourceIsPermanent(t *testing.T) {
	dir := t.TempDir()
	task := load(t, Request{Src: filepath.Join(dir, "nope"), Dst: filepath.Join(dir, "dst")}, nil)
	if err := task.Run(t.Context(), &fakeExec{task: task}); !tasks.IsNonRetriable(err) {
		t.Errorf("err = %v, want non-retriable", err)
	}
}

func TestCancel_RemovesDestinationAndIsRepeatable(t *testing.T) {
	src := randomFile(t, 3000)
	dst := filepath.Join(t.TempDir(), "dst")
	task := load(t, Request{Src: src, Dst: dst, ChunkSize: 1000}, nil)
	if err := task.Run(t.Context(), &fakeExec{task: task}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := task.Cancel(t.Context(), &fakeExec{task: task}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(dst); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("dst still exists after Cancel (err %v)", err)
	}
}

func TestLoad_RejectsRelativePaths(t *testing.T) {
	if err := New().Load([]byte(`{"src":"a","dst":"/tmp/b"}`), nil); err == nil {
		t.Error("Load accepted a relative src")
	}
}
