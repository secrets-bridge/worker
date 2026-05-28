package retry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/secrets-bridge/worker/internal/retry"
)

func TestRun_SucceedsFirstCall(t *testing.T) {
	calls := 0
	err := retry.Policy{InitialDelay: 1 * time.Millisecond, MaxAttempts: 5}.Run(t.Context(), func(context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d want 1", calls)
	}
}

func TestRun_RetriesUntilSuccess(t *testing.T) {
	calls := 0
	err := retry.Policy{InitialDelay: 1 * time.Millisecond, MaxAttempts: 5}.Run(t.Context(), func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d want 3", calls)
	}
}

func TestRun_ReturnsLastErrorAtMaxAttempts(t *testing.T) {
	calls := 0
	wantErr := errors.New("transient")
	err := retry.Policy{InitialDelay: 1 * time.Millisecond, MaxAttempts: 3}.Run(t.Context(), func(context.Context) error {
		calls++
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v want transient", err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d want 3", calls)
	}
}

func TestRun_PermanentShortCircuits(t *testing.T) {
	calls := 0
	underlying := errors.New("hard failure")
	err := retry.Policy{InitialDelay: 1 * time.Millisecond, MaxAttempts: 10}.Run(t.Context(), func(context.Context) error {
		calls++
		return retry.Permanent(underlying)
	})
	if !errors.Is(err, underlying) {
		t.Fatalf("err = %v want underlying", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d want 1 (permanent should not retry)", calls)
	}
}

func TestRun_ContextCancellationStops(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	calls := 0
	err := retry.Policy{InitialDelay: 50 * time.Millisecond, MaxAttempts: 0}.Run(ctx, func(context.Context) error {
		calls++
		return errors.New("transient")
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v want DeadlineExceeded", err)
	}
}

func TestDefaultPolicy_ReasonableShape(t *testing.T) {
	p := retry.DefaultPolicy()
	if p.InitialDelay <= 0 || p.MaxDelay < p.InitialDelay {
		t.Fatalf("default policy bad: %+v", p)
	}
	if p.Multiplier < 1 {
		t.Fatalf("default multiplier = %v", p.Multiplier)
	}
}
