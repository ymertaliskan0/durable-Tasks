package nemesis

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ymertaliskan0/durable-tasks/internal/storage"
)

// fakeReader serves task states from a map.
type fakeReader map[uuid.UUID]storage.TaskState

func (f fakeReader) GetTask(_ context.Context, id uuid.UUID) (storage.TaskState, error) {
	ts, ok := f[id]
	if !ok {
		return storage.TaskState{}, storage.ErrNotFound
	}
	return ts, nil
}

// newJob writes a source file and a destination file with the given
// contents ("" for dst means: do not create it).
func newJob(t *testing.T, src, dst string) job {
	t.Helper()
	dir := t.TempDir()
	j := job{ID: uuid.New(), Src: filepath.Join(dir, "src"), Dst: filepath.Join(dir, "dst")}
	if err := os.WriteFile(j.Src, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	if dst != "" {
		if err := os.WriteFile(j.Dst, []byte(dst), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return j
}

func finished(id uuid.UUID, gen int64) storage.TaskState {
	return storage.TaskState{ID: id, Status: storage.StatusFinished, Generation: gen}
}

func TestCheck_AllGood(t *testing.T) {
	a, b := newJob(t, "hello", "hello"), newJob(t, "world", "world")
	tsB := finished(b.ID, 5)
	tsB.RetriableErrorCount = 2
	reader := fakeReader{a.ID: finished(a.ID, 1), b.ID: tsB}

	r, err := check(t.Context(), reader, []job{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if !r.Passed() {
		t.Fatalf("problems on a clean run: %v", r.Problems)
	}
	want := Result{Tasks: 2, Finished: 2, Recovered: 1, MaxGeneration: 5, Retries: 2}
	if r.Tasks != want.Tasks || r.Finished != want.Finished || r.Recovered != want.Recovered ||
		r.MaxGeneration != want.MaxGeneration || r.Retries != want.Retries {
		t.Errorf("result = %+v, want %+v", r, want)
	}
}

func TestCheck_ReportsEveryViolation(t *testing.T) {
	ok := newJob(t, "fine", "fine")
	stuck := newJob(t, "x", "")
	failed := newJob(t, "x", "")
	corrupt := newJob(t, "original", "0riginal")
	missingDst := newJob(t, "x", "")
	leased := newJob(t, "y", "y")
	lost := newJob(t, "z", "z") // not in the database at all

	tsStuck := storage.TaskState{ID: stuck.ID, Status: storage.StatusRunning, Generation: 2, LeaseOwner: "w"}
	tsFailed := storage.TaskState{ID: failed.ID, Status: storage.StatusFailed, LastError: "boom"}
	tsLeased := finished(leased.ID, 1)
	tsLeased.LeaseOwner = "w"
	reader := fakeReader{
		ok.ID:         finished(ok.ID, 1),
		stuck.ID:      tsStuck,
		failed.ID:     tsFailed,
		corrupt.ID:    finished(corrupt.ID, 3),
		missingDst.ID: finished(missingDst.ID, 1),
		leased.ID:     tsLeased,
	}

	r, err := check(t.Context(), reader, []job{ok, stuck, failed, corrupt, missingDst, leased, lost})
	if err != nil {
		t.Fatal(err)
	}
	if r.Passed() {
		t.Fatal("check passed a run with violations")
	}
	for _, id := range []uuid.UUID{stuck.ID, failed.ID, corrupt.ID, missingDst.ID, leased.ID, lost.ID} {
		if !mentions(r.Problems, id.String()) {
			t.Errorf("no problem reported for task %s; problems: %v", id, r.Problems)
		}
	}
	if mentions(r.Problems, ok.ID.String()) {
		t.Errorf("a problem was reported for the healthy task: %v", r.Problems)
	}
	if len(r.Problems) != 6 {
		t.Errorf("%d problems, want exactly one per broken task (6): %v", len(r.Problems), r.Problems)
	}
	if r.Finished != 4 || r.Tasks != 7 {
		t.Errorf("finished = %d, tasks = %d; want 4 and 7", r.Finished, r.Tasks)
	}
}

func mentions(lines []string, s string) bool {
	for _, l := range lines {
		if strings.Contains(l, s) {
			return true
		}
	}
	return false
}
