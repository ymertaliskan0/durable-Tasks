package tasks

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestIsRetriable(t *testing.T) {
	base := errors.New("x")
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unmarked", base, false},
		{"retriable", Retriable(base), true},
		{"retriable, wrapped again", fmt.Errorf("ctx: %w", Retriable(base)), true},
		{"non-retriable", NonRetriable(base), false},
		{"non-retriable wins", NonRetriable(Retriable(base)), false},
	} {
		if got := IsRetriable(tc.err); got != tc.want {
			t.Errorf("%s: IsRetriable = %v, want %v", tc.name, got, tc.want)
		}
	}
	if !errors.Is(Retriable(base), base) {
		t.Error("Retriable(err) should unwrap to err")
	}
	if Retriable(nil) != nil || NonRetriable(nil) != nil {
		t.Error("wrapping nil should return nil")
	}
}

func TestBackoff(t *testing.T) {
	const base, max = 100 * time.Millisecond, 2 * time.Second
	for _, tc := range []struct {
		retries int
		ceiling time.Duration // the d in [d/2, d)
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{5, max}, // 3.2s, capped
		{1000, max},
	} {
		lo := Backoff(tc.retries, base, max, 0)
		hi := Backoff(tc.retries, base, max, 0.999999)
		if lo != tc.ceiling/2 {
			t.Errorf("retries=%d jitter=0: got %v, want %v", tc.retries, lo, tc.ceiling/2)
		}
		if hi < lo || hi >= tc.ceiling {
			t.Errorf("retries=%d jitter~1: got %v, want in [%v, %v)", tc.retries, hi, lo, tc.ceiling)
		}
	}
}

func TestRunSteps(t *testing.T) {
	ctx := t.Context()
	var ran, saved []int
	steps := make([]Step, 4)
	for i := range steps {
		steps[i] = func(context.Context) error { ran = append(ran, i); return nil }
	}
	save := func(_ context.Context, next int) error { saved = append(saved, next); return nil }

	if err := RunSteps(ctx, 1, steps, save); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ran, []int{1, 2, 3}) || !slices.Equal(saved, []int{2, 3, 4}) {
		t.Errorf("ran %v saved %v, want ran [1 2 3] saved [2 3 4]", ran, saved)
	}

	t.Run("step error stops without saving that step", func(t *testing.T) {
		boom := errors.New("boom")
		saved = nil
		steps := []Step{
			func(context.Context) error { return nil },
			func(context.Context) error { return boom },
			func(context.Context) error { t.Error("step after a failure ran"); return nil },
		}
		if err := RunSteps(ctx, 0, steps, save); !errors.Is(err, boom) {
			t.Errorf("err = %v, want boom", err)
		}
		if !slices.Equal(saved, []int{1}) {
			t.Errorf("saved %v, want [1]", saved)
		}
	})

	t.Run("cancelled context stops before the next step", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		steps := []Step{
			func(context.Context) error { cancel(); return nil },
			func(context.Context) error { t.Error("step ran after cancel"); return nil },
		}
		if err := RunSteps(ctx, 0, steps, save); !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	})

	t.Run("start out of range", func(t *testing.T) {
		if err := RunSteps(ctx, 5, steps, save); !IsNonRetriable(err) {
			t.Errorf("err = %v, want a non-retriable error", err)
		}
	})
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	r.Register("a", func() Task { return &scriptTask{} })
	if _, err := r.New("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.New("missing"); !IsNonRetriable(err) || !strings.Contains(err.Error(), "unknown task type") {
		t.Errorf("New(missing) err = %v, want a non-retriable unknown-type error", err)
	}
	defer func() {
		if recover() == nil {
			t.Error("registering a type twice should panic")
		}
	}()
	r.Register("a", func() Task { return &scriptTask{} })
}

func TestProtect(t *testing.T) {
	err := protect(func() error { panic("kaboom") })
	if !IsRetriable(err) || !strings.Contains(err.Error(), "kaboom") {
		t.Errorf("err = %v, want a retriable error mentioning the panic", err)
	}
	if err := protect(func() error { return nil }); err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

func TestDecide(t *testing.T) {
	flaky := Retriable(errors.New("flaky"))
	broken := errors.New("broken")              // unmarked: not retriable
	permanent := NonRetriable(errors.New("no")) // explicitly permanent
	const maxRetries = 3

	for _, tc := range []struct {
		name string
		e    execution
		want outcome
	}{
		{"run succeeded", execution{}, outcomeFinish},
		{"cleanup succeeded", execution{cleanup: true}, outcomeCancelled},

		{"lost ownership beats everything", execution{lost: true, err: flaky, shuttingDown: true}, outcomeAbandon},
		{"lost ownership even if Run returned nil", execution{lost: true}, outcomeAbandon},

		{"success during shutdown is still recorded", execution{shuttingDown: true}, outcomeFinish},
		{"interrupted by shutdown", execution{shuttingDown: true, err: context.Canceled}, outcomeRelease},
		{"cleanup interrupted by shutdown", execution{cleanup: true, shuttingDown: true, err: context.Canceled}, outcomeRelease},

		{"retriable error with retries left", execution{err: flaky, retries: 2}, outcomeRetry},
		{"retriable error, retries used up", execution{err: flaky, retries: 3}, outcomeFail},
		{"unmarked error is not retried", execution{err: broken}, outcomeFail},
		{"non-retriable error", execution{err: permanent}, outcomeFail},

		{"cleanup error is retried", execution{cleanup: true, err: broken}, outcomeRetry},
		{"cleanup ignores the retry limit", execution{cleanup: true, err: flaky, retries: 99}, outcomeRetry},
		{"cleanup gives up on a non-retriable error", execution{cleanup: true, err: permanent}, outcomeCancelled},
	} {
		if got := decide(tc.e, maxRetries); got != tc.want {
			t.Errorf("%s: decide = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestNewWorker_RejectsInvalidConfig(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"empty worker id":    func(c *Config) { c.WorkerID = "" },
		"zero concurrency":   func(c *Config) { c.Concurrency = 0 },
		"zero lease":         func(c *Config) { c.Lease = 0 },
		"negative retries":   func(c *Config) { c.MaxRetries = -1 },
		"backoff max < base": func(c *Config) { c.BackoffMax = c.BackoffBase / 2 },
	} {
		cfg := DefaultConfig()
		mutate(&cfg)
		if _, err := NewWorker(nil, NewRegistry(), cfg, nil); err == nil {
			t.Errorf("%s: NewWorker accepted an invalid config", name)
		}
	}
	if _, err := NewWorker(nil, NewRegistry(), DefaultConfig(), nil); err != nil {
		t.Errorf("DefaultConfig rejected: %v", err)
	}
}
