package tasks

import (
	"errors"
	"fmt"
)

// RetriableError marks an error as temporary: the worker retries the task
// with backoff, up to Config.MaxRetries times. Create one with Retriable.
type RetriableError struct{ Err error }

func (e *RetriableError) Error() string { return "retriable: " + e.Err.Error() }

// Unwrap returns the underlying error, so errors.Is and errors.As see it.
func (e *RetriableError) Unwrap() error { return e.Err }

// NonRetriableError marks an error as permanent: the task is cleaned up with
// Task.Cancel and ends as failed. Create one with NonRetriable.
type NonRetriableError struct{ Err error }

func (e *NonRetriableError) Error() string { return "non-retriable: " + e.Err.Error() }

// Unwrap returns the underlying error, so errors.Is and errors.As see it.
func (e *NonRetriableError) Unwrap() error { return e.Err }

// Retriable wraps err as a RetriableError. It returns nil if err is nil.
func Retriable(err error) error {
	if err == nil {
		return nil
	}
	return &RetriableError{Err: err}
}

// NonRetriable wraps err as a NonRetriableError. It returns nil if err is nil.
func NonRetriable(err error) error {
	if err == nil {
		return nil
	}
	return &NonRetriableError{Err: err}
}

// IsRetriable reports whether err was marked with Retriable. Unmarked errors
// are not retriable: a task opts in to retries explicitly. If an error is
// marked both ways, NonRetriable wins.
func IsRetriable(err error) bool {
	var nr *NonRetriableError
	if errors.As(err, &nr) {
		return false
	}
	var r *RetriableError
	return errors.As(err, &r)
}

// IsNonRetriable reports whether err was explicitly marked with NonRetriable.
func IsNonRetriable(err error) bool {
	var nr *NonRetriableError
	return errors.As(err, &nr)
}

// recovered converts a panic value into a retriable error. A panic is treated
// as a bug that a retry might not hit again (for example, a race), and the
// retry limit stops it from looping forever.
func recovered(v any) error {
	return Retriable(fmt.Errorf("panic: %v", v))
}
