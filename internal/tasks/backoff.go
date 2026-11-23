package tasks

import "time"

// Backoff returns how long to wait before retry number retries+1.
//
// The delay doubles with every retry, starting at base and capped at max.
// Half of it is fixed and half is scaled by jitter, so many tasks failing at
// the same moment do not all retry at the same moment.
//
// Parameters:
//   - retries: how many retries the task has had so far (0 for the first).
//   - base: the delay before the first retry.
//   - max: the largest delay ever returned.
//   - jitter: a random number in [0, 1).
//
// Returns a delay in [d/2, d), where d = min(base * 2^retries, max).
func Backoff(retries int, base, max time.Duration, jitter float64) time.Duration {
	d := base
	// Double step by step instead of computing base << retries, which
	// overflows for large retry counts.
	for i := 0; i < retries && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	half := d / 2
	return half + time.Duration(jitter*float64(half))
}
