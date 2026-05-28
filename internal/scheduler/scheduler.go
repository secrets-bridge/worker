// Package scheduler runs periodic tasks (sweepers) under a
// distributed lock so multiple worker replicas can run concurrently
// without double-executing.
//
// Model:
//
//	Multiple worker replicas, one Redis. Each replica's scheduler
//	tries to acquire a per-task lock (e.g. "worker:sweeper:wraps-expired")
//	at every tick. Whichever replica wins runs the task; the others
//	skip the tick. The lock auto-renews while the task runs and is
//	released afterwards.
//
//	Failure modes covered:
//	 - Replica crashes mid-task → lease expires, next tick another
//	   replica picks up.
//	 - Redis flips (failover) → lock returns ErrLockLost via
//	   StartRenewal; the task's context is cancelled so it stops
//	   safely without doing double work.
//	 - Persistent task failure → retried with exponential backoff
//	   via the retry package; metric updates either way.
package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/secrets-bridge/api/pkg/runtime"
	"github.com/secrets-bridge/worker/internal/retry"
)

// Task is one unit of periodic worker work.
type Task interface {
	// Name identifies the task for logging / metrics / lock naming.
	// Must be unique across all registered tasks. Stable lock names
	// matter — a rename without coordination would let two replicas
	// run the old + new names simultaneously during a rolling deploy.
	Name() string

	// Run executes one cycle. Returning an error triggers retry per
	// the configured retry.Policy; the runner does NOT block the next
	// scheduled tick on a retrying run (the lock is released between
	// retries to let another replica try if this one's flaking).
	Run(ctx context.Context) error
}

// TaskRegistration binds a Task to its cadence + retry policy.
type TaskRegistration struct {
	Task     Task
	Interval time.Duration // tick cadence; first tick fires after Interval
	Retry    retry.Policy  // applied to Run failures
	// Lease bounds how long ONE task run can hold the lock without
	// renewal; the runner auto-renews so the actual hold time is
	// Lease-bounded only on full crash. Default 60s if zero.
	Lease time.Duration
}

// Scheduler runs registered tasks under Redis-backed mutual exclusion.
type Scheduler struct {
	rt       *runtime.Client
	logger   *slog.Logger
	tasks    []TaskRegistration
	skipLock bool // testing knob; not exposed via public API
	mu       sync.Mutex
	stopped  bool

	// Metrics
	runs   *prometheus.CounterVec // labels: task, outcome
	durs   *prometheus.HistogramVec
	missed *prometheus.CounterVec // labels: task — counted when lock was held by another replica
}

// NewForTest constructs a Scheduler that skips Redis lock acquisition.
// Used by unit tests to exercise the tick + retry wiring without
// booting a real Redis. NOT exported through any public API surface
// beyond this package.
func NewForTest() *Scheduler {
	s := New(nil, slog.Default(), nil)
	s.skipLock = true
	return s
}

// TickForTest invokes one tick of a registered task synchronously.
// Returns an error only if the task name isn't registered.
func (s *Scheduler) TickForTest(ctx context.Context, name string) error {
	for _, r := range s.tasks {
		if r.Task.Name() == name {
			s.tickOnce(ctx, r)
			return nil
		}
	}
	return errors.New("scheduler: no registered task named " + name)
}

// New constructs a Scheduler. registry is the Prometheus registry to
// register collectors on; pass nil to skip metric registration (the
// scheduler still records to internal counters but they won't be
// scraped).
func New(rt *runtime.Client, logger *slog.Logger, registry prometheus.Registerer) *Scheduler {
	s := &Scheduler{
		rt:     rt,
		logger: logger,
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "worker_scheduler_runs_total",
			Help: "Number of scheduled task runs, labelled by outcome (success|failure|skipped_lock).",
		}, []string{"task", "outcome"}),
		durs: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "worker_scheduler_run_duration_seconds",
			Help:    "Wall-clock duration of scheduled task runs.",
			Buckets: prometheus.DefBuckets,
		}, []string{"task"}),
		missed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "worker_scheduler_lock_skipped_total",
			Help: "Number of ticks where the lock was held by another replica.",
		}, []string{"task"}),
	}
	if registry != nil {
		registry.MustRegister(s.runs, s.durs, s.missed)
	}
	return s
}

// Register adds reg to the set the scheduler will run when Start is
// called. Safe to call before Start, NOT safe to call after.
func (s *Scheduler) Register(reg TaskRegistration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if reg.Lease == 0 {
		reg.Lease = 60 * time.Second
	}
	s.tasks = append(s.tasks, reg)
}

// Start launches a goroutine per registered task. Each goroutine
// loops until ctx is cancelled; the returned channel closes once all
// goroutines have drained.
func (s *Scheduler) Start(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	var wg sync.WaitGroup
	for _, reg := range s.tasks {
		wg.Add(1)
		go func(r TaskRegistration) {
			defer wg.Done()
			s.runLoop(ctx, r)
		}(reg)
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	return done
}

// runLoop is the per-task ticker.
func (s *Scheduler) runLoop(ctx context.Context, reg TaskRegistration) {
	t := time.NewTicker(reg.Interval)
	defer t.Stop()
	s.logger.Info("scheduler task registered",
		"task", reg.Task.Name(),
		"interval", reg.Interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.tickOnce(ctx, reg)
		}
	}
}

// tickOnce handles one task fire: acquire lock, run with retry,
// release. Lock contention is observed as a metric but never logged at
// warn level — it's the EXPECTED outcome for N-1 of N replicas.
func (s *Scheduler) tickOnce(parent context.Context, reg TaskRegistration) {
	name := reg.Task.Name()
	lockName := "worker:sweeper:" + name

	var lock *runtime.Lock
	if !s.skipLock {
		var err error
		lock, err = s.rt.AcquireLock(parent, lockName, reg.Lease)
		if errors.Is(err, runtime.ErrLockHeld) {
			s.missed.WithLabelValues(name).Inc()
			s.runs.WithLabelValues(name, "skipped_lock").Inc()
			return
		}
		if err != nil {
			s.logger.Warn("scheduler: lock acquire failed",
				"task", name, "error", err)
			s.runs.WithLabelValues(name, "failure").Inc()
			return
		}
		runCtx, cancel := context.WithCancel(parent)
		stopRenew := lock.StartRenewal(runCtx, func() {
			s.logger.Warn("scheduler: lock lease lost", "task", name)
			cancel()
		})
		defer func() {
			stopRenew()
			if err := lock.Release(parent); err != nil && !errors.Is(err, runtime.ErrLockLost) {
				s.logger.Warn("scheduler: lock release failed", "task", name, "error", err)
			}
			cancel()
		}()
		parent = runCtx
	}

	start := time.Now()
	err := reg.Retry.Run(parent, reg.Task.Run)
	s.durs.WithLabelValues(name).Observe(time.Since(start).Seconds())
	if err != nil {
		s.logger.Error("scheduler: task failed",
			"task", name, "error", err)
		s.runs.WithLabelValues(name, "failure").Inc()
		return
	}
	s.runs.WithLabelValues(name, "success").Inc()
}
