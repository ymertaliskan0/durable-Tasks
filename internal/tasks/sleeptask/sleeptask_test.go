package sleeptask

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

type countingExec struct{ saves int }

func (*countingExec) TaskID() uuid.UUID                 { return uuid.Nil }
func (*countingExec) Generation() int64                 { return 1 }
func (e *countingExec) SaveState(context.Context) error { e.saves++; return nil }

func TestLoad_Validates(t *testing.T) {
	for _, req := range []string{`not json`, `{"steps":0}`, `{"steps":1,"step_duration":-1}`} {
		if err := New().Load([]byte(req), nil); err == nil {
			t.Errorf("Load(%s) accepted an invalid request", req)
		}
	}
}

func TestRun_ResumesFromSavedState(t *testing.T) {
	task := New()
	if err := task.Load([]byte(`{"steps":4,"step_duration":1000}`), []byte(`{"next":1}`)); err != nil {
		t.Fatal(err)
	}
	exec := &countingExec{}
	if err := task.Run(t.Context(), exec); err != nil {
		t.Fatal(err)
	}
	if exec.saves != 3 {
		t.Errorf("SaveState called %d times, want 3 (steps 1-3)", exec.saves)
	}
	state, _ := task.Save()
	if string(state) != `{"next":4}` {
		t.Errorf("state = %s, want {\"next\":4}", state)
	}
}

func TestRun_StopsWhenCancelled(t *testing.T) {
	task := New()
	if err := task.Load([]byte(`{"steps":3,"step_duration":60000000000}`), nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := task.Run(ctx, &countingExec{}); err == nil {
		t.Error("Run returned nil after its context expired")
	}
}
