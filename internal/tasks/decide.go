package tasks

// outcome is what the worker writes to the store after one execution.
type outcome int

const (
	outcomeAbandon   outcome = iota // write nothing: another worker owns the task now
	outcomeFinish                   // Store.Finish: Run succeeded
	outcomeCancelled                // Store.MarkCancelled: cleanup is over
	outcomeRetry                    // Store.ScheduleRetry: try again after a backoff
	outcomeFail                     // Store.Fail: go down the cleanup path, then end as failed
	outcomeRelease                  // Store.Release: the worker is shutting down; hand the task back
)

func (o outcome) String() string {
	return [...]string{"abandon", "finish", "cancelled", "retry", "fail", "release"}[o]
}

// execution summarises one execution of a task, as input to decide.
type execution struct {
	cleanup      bool  // the task ran Task.Cancel (status cancelling), not Task.Run
	err          error // what Run or Cancel returned
	lost         bool  // the worker learned it no longer owns the task
	shuttingDown bool  // the worker's context was cancelled
	retries      int   // the task's retriable_error_count before this execution
}

// decide picks what to write to the store after one execution. It is a pure
// function, so every rule can be tested in a table.
//
// The rules are checked in order; the first that matches wins:
//  1. The worker lost the task: abandon it without writing.
//  2. The work succeeded: finish a run, or end a cleanup as cancelled. This
//     holds even during shutdown, so completed work is never thrown away.
//  3. The worker is shutting down: release the task for another worker.
//  4. A cleanup failed: retry it, without a limit, because cleanup must
//     eventually happen. A NonRetriable error means cleanup is impossible,
//     so the task ends instead.
//  5. A run failed with a retriable error and retries remain: retry.
//  6. Otherwise: fail, which sends the task down the cleanup path.
//
// Parameters:
//   - e: what happened during the execution.
//   - maxRetries: retriable errors allowed before a run fails.
//
// Returns the outcome to apply.
func decide(e execution, maxRetries int) outcome {

	switch {
	case e.lost:
		return outcomeAbandon
	case e.err == nil:
		if e.cleanup {
			return outcomeCancelled
		}
		return outcomeFinish
	case e.shuttingDown:
		return outcomeRelease
	case e.cleanup:
		if IsNonRetriable(e.err) {
			return outcomeCancelled
		}
		return outcomeRetry
	case IsRetriable(e.err) && e.retries < maxRetries:
		return outcomeRetry
	default:
		return outcomeFail
	}
}
