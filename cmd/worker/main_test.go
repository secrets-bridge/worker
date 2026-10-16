package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

// WRK-N-01: a failing reveal-sessions run gives up inside one tick
// instead of backing off for up to an hour while it holds the
// sweeper lock.
func TestRevealSessionsRetry_GivesUpWithinOneTick(t *testing.T) {
	policy := revealSessionsRetry()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	attempts := 0
	start := time.Now()
	err := policy.Run(ctx, func(context.Context) error {
		attempts++
		return errors.New("database unavailable")
	})
	elapsed := time.Since(start)

	if err == nil || ctx.Err() != nil {
		t.Fatalf("err = %v, ctx err = %v; want the run error back before the deadline", err, ctx.Err())
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d want 3", attempts)
	}
	// Default tick is 5s (config.go); the retry budget must fit in it.
	if elapsed >= 5*time.Second {
		t.Fatalf("retries took %v; want well under the 5s tick", elapsed)
	}
}
