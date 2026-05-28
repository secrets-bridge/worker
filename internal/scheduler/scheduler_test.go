package scheduler_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/secrets-bridge/worker/internal/retry"
	"github.com/secrets-bridge/worker/internal/scheduler"
)

type countingTask struct {
	name string
	mu   sync.Mutex
	runs int
	err  error
}

func (c *countingTask) Name() string { return c.name }
func (c *countingTask) Run(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.runs++
	return c.err
}
func (c *countingTask) Runs() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.runs
}

// Most scheduler behavior is exercised end-to-end against a live Redis;
// these tests stay at the unit level by skipping lock acquisition via
// the package's internal knob — the goal here is to lock in the Run /
// retry / tick wiring, not Redis semantics (which the api repo already
// covers in its own tests).

func TestScheduler_TicksAtInterval(t *testing.T) {
	task := &countingTask{name: "tick-test"}
	s := scheduler.NewForTest()
	s.Register(scheduler.TaskRegistration{
		Task:     task,
		Interval: 15 * time.Millisecond,
		Retry:    retry.Policy{InitialDelay: 1 * time.Millisecond, MaxAttempts: 1},
	})

	ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
	defer cancel()
	<-s.Start(ctx)

	if task.Runs() < 3 {
		t.Fatalf("runs = %d want >= 3", task.Runs())
	}
}

func TestScheduler_FailingTaskRetried(t *testing.T) {
	var attempts atomic.Int32
	task := &funcTask{
		name: "fail-then-succeed",
		fn: func(_ context.Context) error {
			n := attempts.Add(1)
			if n < 3 {
				return errors.New("transient")
			}
			return nil
		},
	}
	s := scheduler.NewForTest()
	s.Register(scheduler.TaskRegistration{
		Task:     task,
		Interval: 1 * time.Hour, // only one tick — but retry will fire several times
		Retry:    retry.Policy{InitialDelay: 1 * time.Millisecond, MaxAttempts: 5},
	})
	ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
	defer cancel()

	// Trigger one immediate tick manually via TickForTest, then stop.
	if err := s.TickForTest(ctx, task.Name()); err != nil {
		t.Fatalf("TickForTest: %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d want 3", got)
	}
}

func TestScheduler_DrainsOnContextCancel(t *testing.T) {
	task := &countingTask{name: "drain-test"}
	s := scheduler.NewForTest()
	s.Register(scheduler.TaskRegistration{
		Task:     task,
		Interval: 20 * time.Millisecond,
		Retry:    retry.Policy{InitialDelay: 1 * time.Millisecond, MaxAttempts: 1},
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := s.Start(ctx)
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("scheduler did not drain after ctx cancel")
	}
}

type funcTask struct {
	name string
	fn   func(ctx context.Context) error
}

func (f *funcTask) Name() string                       { return f.name }
func (f *funcTask) Run(ctx context.Context) error      { return f.fn(ctx) }
