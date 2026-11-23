package metrics

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ymertaliskan0/durable-tasks/internal/storage"
	"github.com/ymertaliskan0/durable-tasks/internal/tasks"
)

// fakeStore answers just enough for the instrumented wrapper.
type fakeStore struct {
	tasks.Store // nil: calling an unimplemented method panics
	gen         int64
}

func (f *fakeStore) ClaimNext(context.Context, string, time.Duration) (storage.TaskState, error) {
	f.gen++
	return storage.TaskState{ID: uuid.New(), Type: "sleep", Status: storage.StatusRunning, Generation: f.gen}, nil
}
func (*fakeStore) Finish(context.Context, uuid.UUID, int64, []byte) error { return nil }
func (*fakeStore) Heartbeat(context.Context, uuid.UUID, int64, time.Duration) error {
	return storage.ErrWrongGeneration
}
func (*fakeStore) ReclaimExpired(context.Context, int) (int, error) { return 3, nil }
func (*fakeStore) CountByStatus(context.Context) (map[storage.Status]int, error) {
	return map[storage.Status]int{storage.StatusFinished: 7}, nil
}

func TestInstrumentedStoreAndHandler(t *testing.T) {
	m := New()
	fake := &fakeStore{}
	m.WatchQueue(fake)
	store := m.Instrument(fake)
	ctx := t.Context()

	ts, _ := store.ClaimNext(ctx, "w", time.Second)
	_ = store.Finish(ctx, ts.ID, ts.Generation, nil)
	_ = store.Heartbeat(ctx, ts.ID, ts.Generation, time.Second)
	_, _ = store.ReclaimExpired(ctx, 10)

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, want := range []string{
		`durable_tasks_claimed_total{status="running",type="sleep"} 1`,
		`durable_tasks_outcomes_total{outcome="finish",type="sleep"} 1`,
		`durable_tasks_execution_seconds_count{outcome="finish",type="sleep"} 1`,
		`durable_tasks_lost_ownership_total 1`,
		`durable_tasks_reclaimed_total 3`,
		`durable_tasks_queue_depth{status="finished"} 7`,
		`durable_tasks_queue_depth{status="running"} 0`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics output is missing %q", want)
		}
	}
}
