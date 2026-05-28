// Package retry implements the worker's exponential-backoff retry
// policy. Used by sweepers (transient sweep failures shouldn't crash
// the loop) and by the notifications fanout (a flaky webhook shouldn't
// drop the message on the first 5xx).
//
// Policy shape:
//
//	delay(n) = min(InitialDelay * Multiplier^n + jitter, MaxDelay)
//
// Jitter is a small uniform random factor that spreads thundering herds.
// MaxAttempts of 0 means "retry forever (until ctx is cancelled)".
package retry

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"time"
)

// Policy carries the knobs that shape backoff. Construct one per
// caller; safe for concurrent Run() calls because nothing is mutated
// at runtime.
type Policy struct {
	// InitialDelay is the wait before the first retry. Default 1s if
	// zero.
	InitialDelay time.Duration

	// MaxDelay caps the per-attempt wait. Default 1h if zero (matches
	// the worker issue's "capped at 1h").
	MaxDelay time.Duration

	// Multiplier is the exponential base. Default 2.0 if zero.
	Multiplier float64

	// JitterFraction adds [0, jitter*delay) noise to each wait.
	// Default 0.2 if zero.
	JitterFraction float64

	// MaxAttempts is the upper bound on tries (including the first).
	// 0 means "retry until ctx is cancelled". Default 0.
	MaxAttempts int
}

// DefaultPolicy returns a Policy with the worker's standard shape:
// 1s initial, 2x growth, 1h cap, 20% jitter, retry forever. Suitable
// for sweepers and notifications.
func DefaultPolicy() Policy {
	return Policy{
		InitialDelay:   1 * time.Second,
		MaxDelay:       1 * time.Hour,
		Multiplier:     2.0,
		JitterFraction: 0.2,
		MaxAttempts:    0,
	}
}

// ErrPermanent wraps an error that retry must NOT swallow — the loop
// returns immediately. Callers use this when they detect a permanent
// failure (e.g. 4xx on a webhook that's just misconfigured).
type ErrPermanent struct{ Err error }

func (e *ErrPermanent) Error() string { return "permanent: " + e.Err.Error() }
func (e *ErrPermanent) Unwrap() error { return e.Err }

// Permanent marks err as non-retryable.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &ErrPermanent{Err: err}
}

// Run executes fn until it returns nil OR returns an ErrPermanent OR
// MaxAttempts is reached OR ctx is cancelled. Between attempts it
// sleeps for an exponentially-increasing duration with jitter.
//
// Returns the LAST error fn produced (unwrapped from ErrPermanent if
// applicable) or ctx.Err() if cancellation won.
func (p Policy) Run(ctx context.Context, fn func(ctx context.Context) error) error {
	policy := p.withDefaults()
	var lastErr error
	for attempt := 0; policy.MaxAttempts == 0 || attempt < policy.MaxAttempts; attempt++ {
		if attempt > 0 {
			wait := policy.delay(attempt - 1)
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
		}
		err := fn(ctx)
		if err == nil {
			return nil
		}
		var perm *ErrPermanent
		if errors.As(err, &perm) {
			return perm.Err
		}
		lastErr = err
	}
	return lastErr
}

// delay computes the backoff for the (0-indexed) retry attempt.
func (p Policy) delay(attempt int) time.Duration {
	policy := p.withDefaults()
	base := float64(policy.InitialDelay) * math.Pow(policy.Multiplier, float64(attempt))
	if base > float64(policy.MaxDelay) {
		base = float64(policy.MaxDelay)
	}
	if policy.JitterFraction > 0 {
		base += rand.Float64() * policy.JitterFraction * base
	}
	return time.Duration(base)
}

// withDefaults returns a copy with zero fields filled in. Lets the
// public methods stay zero-value-friendly.
func (p Policy) withDefaults() Policy {
	if p.InitialDelay == 0 {
		p.InitialDelay = 1 * time.Second
	}
	if p.MaxDelay == 0 {
		p.MaxDelay = 1 * time.Hour
	}
	if p.Multiplier == 0 {
		p.Multiplier = 2.0
	}
	if p.JitterFraction == 0 {
		p.JitterFraction = 0.2
	}
	return p
}
