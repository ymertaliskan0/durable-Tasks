package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

// scriptType is the task type of scriptTask.
const scriptType = "script"

// script is a scriptTask's request: it says how the task should behave.
// Counters such as Panics apply to the first executions of the task and are
// counted across workers by a shared recorder.
type script struct {
	Steps             int           `json:"steps"`
	StepDelay         time.Duration `json:"step_delay"`
	Panics            int           `json:"panics"`             // first N runs panic
	RetriableFailures int           `json:"retriable_failures"` // next N runs fail with a retriable error
	PermanentFailure  bool          `json:"permanent_failure"`  // later runs fail with an unmarked error
	Block             bool          `json:"block"`              // Run blocks until its context is done
	CancelFailures    int           `json:"cancel_failures"`    // first N Cancel calls fail
}

type scriptState struct {
	Next int `json:"next"`
}

// recorder records what scriptTasks did, keyed by task ID. A new Task value
// is created for every execution, so this is where history survives.
type recorder struct {
	mu      sync.Mutex
	runs    map[uuid.UUID]int
	cancels map[uuid.UUID]int
	steps   map[uuid.UUID][]int
	started chan uuid.UUID // receives the task ID when a blocking Run starts
	stopped chan uuid.UUID // receives the task ID when a blocking Run returns
}

func newRecorder() *recorder {
	return &recorder{
		runs:    make(map[uuid.UUID]int),
		cancels: make(map[uuid.UUID]int),
		steps:   make(map[uuid.UUID][]int),
		started: make(chan uuid.UUID, 100),
		stopped: make(chan uuid.UUID, 100),
	}
}

func (r *recorder) inc(m map[uuid.UUID]int, id uuid.UUID) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	m[id]++
	return m[id]
}

func (r *recorder) count(m map[uuid.UUID]int, id uuid.UUID) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return m[id]
}

func (r *recorder) stepsOf(id uuid.UUID) []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.steps[id]...)
}

// factory returns a Factory for scriptTasks that report to r.
func (r *recorder) factory() Factory {
	return func() Task { return &scriptTask{rec: r} }
}

type scriptTask struct {
	rec   *recorder
	req   script
	state scriptState
}

func (s *scriptTask) Load(request, state []byte) error {
	if err := json.Unmarshal(request, &s.req); err != nil {
		return err
	}
	if state != nil {
		return json.Unmarshal(state, &s.state)
	}
	return nil
}

func (s *scriptTask) Save() ([]byte, error) { return json.Marshal(s.state) }

func (s *scriptTask) Run(ctx context.Context, exec ExecutionContext) error {
	id := exec.TaskID()
	n := s.rec.inc(s.rec.runs, id)
	switch {
	case n <= s.req.Panics:
		panic("scripted panic")
	case n <= s.req.Panics+s.req.RetriableFailures:
		return Retriable(errors.New("scripted flaky error"))
	case s.req.PermanentFailure:
		return errors.New("scripted permanent error")
	case s.req.Block:
		s.rec.started <- id
		<-ctx.Done()
		s.rec.stopped <- id
		return ctx.Err()
	}

	steps := make([]Step, s.req.Steps)
	for i := range steps {
		steps[i] = func(ctx context.Context) error {
			s.rec.mu.Lock()
			s.rec.steps[id] = append(s.rec.steps[id], i)
			s.rec.mu.Unlock()
			sleep(ctx, s.req.StepDelay)
			return ctx.Err()
		}
	}
	return RunSteps(ctx, s.state.Next, steps, func(ctx context.Context, next int) error {
		s.state.Next = next
		return exec.SaveState(ctx)
	})
}

func (s *scriptTask) Cancel(_ context.Context, exec ExecutionContext) error {
	if n := s.rec.inc(s.rec.cancels, exec.TaskID()); n <= s.req.CancelFailures {
		return errors.New("scripted cleanup error")
	}
	return nil
}
