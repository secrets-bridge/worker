package sweepers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/storage"
	"github.com/secrets-bridge/worker/internal/notifications"
)

// jobCreator is the slice of api/pkg/storage.SyncJobs used here.
type jobCreator interface {
	Create(ctx context.Context, j *storage.SyncJob) error
}

// DiscoverTarget is one configured target the scheduler periodically
// enqueues a discover job for. Today the targets come from a JSON
// env var (SB_DISCOVER_TARGETS_JSON); a future PR adds a
// `provider_connections` admin API and the scheduler will read from
// that instead.
type DiscoverTarget struct {
	// Name is a human label used in audit / logs.
	Name string `json:"name"`
	// Cluster is the cluster_name the discovered secrets get stamped
	// with on the agent side. Must match an agent's SB_CLUSTER_NAME
	// for the discover job to find a runner.
	Cluster string `json:"cluster"`
	// ProviderType matches a registered ResolverByType on the agent
	// side: "vault", "aws-sm", etc.
	ProviderType string `json:"provider_type"`
	// ProviderConfig is the provider-specific connection bag (Vault
	// address, AWS region, ...). Forwarded verbatim into the job
	// payload; the agent's resolver consumes it.
	ProviderConfig map[string]any `json:"provider_config"`
	// Scope is the optional ProviderScope the discover call walks
	// (project / environment / label selector).
	Scope map[string]any `json:"scope,omitempty"`
}

// DiscoverScheduler enqueues a `discover` job per configured target
// on every tick. Replicas don't enqueue duplicates because the
// scheduler's Redis lock means only one replica runs the tick.
type DiscoverScheduler struct {
	Jobs     jobCreator
	Targets  []DiscoverTarget
	Notifier notifications.Notifier
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

// Name returns the stable identifier for this sweeper.
func (s DiscoverScheduler) Name() string { return "discover-scheduler" }

// Run creates one discover job per configured target. Per-target
// failures are accumulated and reported in the notification so a
// single broken target doesn't drop the rest.
func (s DiscoverScheduler) Run(ctx context.Context) error {
	if len(s.Targets) == 0 {
		return nil
	}
	var errs []error
	enqueued := 0
	for _, target := range s.Targets {
		payload := map[string]any{
			"target_provider_type":   target.ProviderType,
			"target_provider_config": target.ProviderConfig,
			"cluster_name":           target.Cluster,
		}
		if target.Scope != nil {
			payload["scope"] = target.Scope
		}
		job := &storage.SyncJob{
			JobType:       storage.JobType("discover"),
			Status:        storage.JobStatusQueued,
			CorrelationID: uuid.New(),
			AgentScope: map[string]any{
				"cluster_name": target.Cluster,
			},
			Payload: payload,
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
			Title:     "discover jobs enqueued",
			Metadata:  map[string]any{"enqueued": enqueued},
			Time:      time.Now().UTC(),
		})
	}
	if len(errs) > 0 {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityError,
			Component: s.Name(),
			Title:     "some discover targets failed to enqueue",
			Detail:    errors.Join(errs...).Error(),
		})
		return errors.Join(errs...)
	}
	return nil
}
