package sweepers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/runtime"
	"github.com/secrets-bridge/api/pkg/sanitize"
	"github.com/secrets-bridge/api/pkg/storage"

	"github.com/secrets-bridge/worker/internal/notifications"
)

// Slice P4 (EPIC P) — DB-backed discover scheduler.
//
// The previous shape (env-var-driven `SB_DISCOVER_TARGETS_JSON`) is
// preserved as a backwards-compat fallback so charts and operators
// can roll forward without breakage. The deprecation warning carries
// the calendar-removal date so anyone reading the logs sees the
// timeline.

// jobCreator is the slice of api/pkg/storage.SyncJobs used here.
type jobCreator interface {
	Create(ctx context.Context, j *storage.SyncJob) error
}

// providerConnectionScheduler is the slice of the api repo's
// ProviderConnectionRepository the scheduler needs. Defined as a
// narrow interface so unit tests can inject a fake without a real
// Postgres pool.
type providerConnectionScheduler interface {
	ListDueForDiscovery(ctx context.Context, now time.Time) ([]storage.DiscoverTarget, error)
	MarkDiscoverStarted(ctx context.Context, id uuid.UUID, now time.Time) error
	MarkDiscoverFinished(ctx context.Context, id uuid.UUID, status, sanitizedErr string, now time.Time) error
}

// redisLocker is the slice of runtime.Client used for per-target
// locking. The fake in tests implements both AcquireLock and the
// minimal Lock type's release semantics.
type redisLocker interface {
	AcquireLock(ctx context.Context, name string, lease time.Duration) (*runtime.Lock, error)
}

// DiscoverTarget is the env-var-driven shape. Preserved as the
// fallback path; new deployments should bind connections via the
// admin API (EPIC P) instead.
type DiscoverTarget struct {
	Name           string         `json:"name"`
	Cluster        string         `json:"cluster"`
	ProviderType   string         `json:"provider_type"`
	ProviderConfig map[string]any `json:"provider_config"`
	Scope          map[string]any `json:"scope,omitempty"`
}

// DiscoverScheduler periodically enqueues `discover` jobs.
//
// Source of truth precedence:
//
//  1. DB (provider_connections) — primary source. Each tick calls
//     ListDueForDiscovery, claims the per-target Redis lock, marks
//     the row 'running', and enqueues the job. The api's JobService
//     OnCompleted hook later flips the row to 'success'/'failure'.
//  2. Env var (SB_DISCOVER_TARGETS_JSON) — fallback. Only used when
//     the DB returns zero rows AND the env var is set. Emits a
//     deprecation WARN per tick so operators know to migrate.
type DiscoverScheduler struct {
	Jobs        jobCreator
	Connections providerConnectionScheduler
	Locks       redisLocker

	// EnvFallback is the parsed SB_DISCOVER_TARGETS_JSON list.
	// Empty when no env var is set — the fallback path no-ops.
	EnvFallback []DiscoverTarget

	Notifier notifications.Notifier
	Logger   *slog.Logger

	// Now is injected for tests; real wiring leaves it nil.
	Now func() time.Time
}

// Name returns the stable identifier for this sweeper.
func (s DiscoverScheduler) Name() string { return "discover-scheduler" }

// Run runs the DB-backed pass first; if it returned no targets AND
// the env-var fallback is non-empty, runs the fallback with a loud
// deprecation log.
func (s DiscoverScheduler) Run(ctx context.Context) error {
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}

	// 1. DB-backed pass. Even with zero rows + an env-var fallback
	// configured, we still call this — it's the canonical signal that
	// DB targets are wired (or not).
	if s.Connections != nil {
		targets, err := s.Connections.ListDueForDiscovery(ctx, now())
		if err != nil {
			notify(ctx, s.Notifier, notifications.Event{
				Severity:  notifications.SeverityError,
				Component: s.Name(),
				Title:     "discover scheduler: failed to list DB targets",
				Detail:    sanitize.DiscoverError(err.Error()),
			})
			return fmt.Errorf("sweepers: list due for discovery: %w", err)
		}
		if len(targets) > 0 {
			if len(s.EnvFallback) > 0 {
				s.logDeprecation("SB_DISCOVER_TARGETS_JSON is ignored because DB-backed discover targets exist")
			}
			return s.enqueueDBTargets(ctx, targets, now())
		}
	}

	// 2. Env-var fallback. Hard-deprecated; lasts 3 months past P4.
	if len(s.EnvFallback) == 0 {
		return nil
	}
	s.logDeprecation("SB_DISCOVER_TARGETS_JSON fallback is in use")
	return s.enqueueEnvTargets(ctx)
}

// enqueueDBTargets walks the DB rows. For each target the scheduler:
//
//  1. Tries to acquire `worker:discover:<id>` with a lease of
//     2 * discover_interval_seconds (capped at 2h to bound a
//     misconfigured interval). Lock contention skips the target.
//  2. MarkDiscoverStarted — flips the row to running.
//  3. Enqueues a discover job carrying connection_id in the payload
//     so the api's OnDiscoverJobCompleted hook can route status
//     updates.
//  4. On enqueue failure, rolls back via MarkDiscoverFinished
//     (status=failure, sanitized error). The Redis lock is left to
//     expire naturally — re-acquiring before expiry would let two
//     replicas race on the next tick.
func (s DiscoverScheduler) enqueueDBTargets(ctx context.Context, targets []storage.DiscoverTarget, now time.Time) error {
	var errs []error
	enqueued := 0
	skippedLocked := 0
	for _, target := range targets {
		lockName := "discover:" + target.ID.String()
		lease := time.Duration(target.DiscoverIntervalSeconds) * time.Second * 2
		if lease <= 0 || lease > 2*time.Hour {
			lease = 2 * time.Hour
		}
		lock, err := s.Locks.AcquireLock(ctx, lockName, lease)
		if err != nil {
			if errors.Is(err, runtime.ErrLockHeld) {
				skippedLocked++
				continue
			}
			errs = append(errs, fmt.Errorf("acquire lock %s: %w", lockName, err))
			continue
		}
		// Lock acquired — note the *Lock value but don't release; the
		// agent's lifecycle (claim + complete) is the natural end-of-
		// life, and the lease bounds the worst case.
		_ = lock

		if err := s.Connections.MarkDiscoverStarted(ctx, target.ID, now); err != nil {
			errs = append(errs, fmt.Errorf("mark started %s: %w", target.ID, err))
			continue
		}

		job := &storage.SyncJob{
			JobType:       storage.JobTypeDiscover,
			Status:        storage.JobStatusQueued,
			CorrelationID: uuid.New(),
			AgentScope: map[string]any{
				"cluster_name": target.ClusterName,
			},
			Payload: map[string]any{
				"connection_id":          target.ID.String(),
				"target_provider_type":   string(target.Type),
				"target_provider_config": target.Scope,
				"cluster_name":           target.ClusterName,
			},
		}
		if err := s.Jobs.Create(ctx, job); err != nil {
			// Roll back so admin UIs don't show "running" forever.
			// MarkDiscoverFinished sanitizes its argument again on the
			// api side; we pre-sanitize here as defense in depth so a
			// provider error string with token-shaped substrings is
			// already redacted when it lands in last_discover_error.
			clean := sanitize.DiscoverError("enqueue failed: " + err.Error())
			_ = s.Connections.MarkDiscoverFinished(ctx, target.ID,
				storage.DiscoverStatusFailure, clean, now)
			errs = append(errs, fmt.Errorf("enqueue %s: %w", target.ID, err))
			continue
		}
		enqueued++
	}

	if enqueued > 0 {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityInfo,
			Component: s.Name(),
			Title:     "discover jobs enqueued",
			Metadata: map[string]any{
				"enqueued":       enqueued,
				"skipped_locked": skippedLocked,
			},
			Time: now,
		})
	}
	if len(errs) > 0 {
		joined := errors.Join(errs...)
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityError,
			Component: s.Name(),
			Title:     "some discover targets failed",
			Detail:    sanitize.DiscoverError(joined.Error()),
		})
		return joined
	}
	return nil
}

// enqueueEnvTargets is the deprecated env-var path. Identical to the
// pre-P4 behaviour, kept compiling so existing deployments keep
// running while operators migrate.
func (s DiscoverScheduler) enqueueEnvTargets(ctx context.Context) error {
	var errs []error
	enqueued := 0
	for _, target := range s.EnvFallback {
		payload := map[string]any{
			"target_provider_type":   target.ProviderType,
			"target_provider_config": target.ProviderConfig,
			"cluster_name":           target.Cluster,
		}
		if target.Scope != nil {
			payload["scope"] = target.Scope
		}
		job := &storage.SyncJob{
			JobType:       storage.JobTypeDiscover,
			Status:        storage.JobStatusQueued,
			CorrelationID: uuid.New(),
			AgentScope:    map[string]any{"cluster_name": target.Cluster},
			Payload:       payload,
		}
		if err := s.Jobs.Create(ctx, job); err != nil {
			errs = append(errs, fmt.Errorf("target=%s: %w", target.Name, err))
			continue
		}
		enqueued++
	}
	if enqueued > 0 {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityInfo,
			Component: s.Name(),
			Title:     "discover jobs enqueued (env fallback)",
			Metadata:  map[string]any{"enqueued": enqueued},
		})
	}
	if len(errs) > 0 {
		joined := errors.Join(errs...)
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityError,
			Component: s.Name(),
			Title:     "some env-fallback targets failed",
			Detail:    sanitize.DiscoverError(joined.Error()),
		})
		return joined
	}
	return nil
}

// logDeprecation emits the standard SB_DISCOVER_TARGETS_JSON
// deprecation warning. The message carries the calendar deadline
// agreed at §6 sign-off so anyone reading the logs sees the timeline.
// The exact deadline is "3 months after DB-backed provider connection
// discovery ships" — operators count from the P4 merge date.
func (s DiscoverScheduler) logDeprecation(prefix string) {
	if s.Logger == nil {
		return
	}
	s.Logger.Warn(
		prefix+": SB_DISCOVER_TARGETS_JSON is deprecated and will be removed 3 months after DB-backed provider connection discovery ships. Please migrate discovery targets to provider_connections via the admin API.",
		"task", s.Name(),
	)
}

// ParseTargets decodes the SB_DISCOVER_TARGETS_JSON env var value.
// Empty input → empty list, not an error (the scheduler tolerates
// having no targets configured; it just becomes a no-op).
func ParseTargets(envValue string) ([]DiscoverTarget, error) {
	if envValue == "" {
		return nil, nil
	}
	var out []DiscoverTarget
	if err := json.Unmarshal([]byte(envValue), &out); err != nil {
		return nil, fmt.Errorf("sweepers: SB_DISCOVER_TARGETS_JSON: %w", err)
	}
	for i, t := range out {
		if t.Name == "" {
			return nil, fmt.Errorf("sweepers: target[%d].name required", i)
		}
		if t.Cluster == "" {
			return nil, fmt.Errorf("sweepers: target[%d].cluster required (matches agent SB_CLUSTER_NAME)", i)
		}
		if t.ProviderType == "" {
			return nil, fmt.Errorf("sweepers: target[%d].provider_type required", i)
		}
	}
	return out, nil
}
