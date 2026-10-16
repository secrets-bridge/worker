package sweepers

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/storage"

	"github.com/secrets-bridge/worker/internal/notifications"
)

// AgentsStale flips agents.status from 'active' → 'stale' when
// last_seen_at has been idle past the configured cutoff. Operators
// see this as the early-warning signal that a deployed agent's heart
// stopped beating (network partition, OOM, configuration mistake,
// node drain).
//
// The status transition is reversible: the next successful heartbeat
// will flip the row back to `active` (per the AgentService heartbeat
// flow on the api side). The sweeper is the observability mechanism,
// not a punitive one.
//
// Implementation: raw SQL on the pool rather than a repository
// method, because the api side doesn't need this query for any of
// its own flows. The worker owns the "stale" determination cadence
// + cutoff; the api owns the active/revoked transitions.
type AgentsStale struct {
	Pool       pgxRowQuerier
	Notifier   notifications.Notifier
	StaleAfter time.Duration
	// Audit receives one agent.stale event per flipped agent (WRK-N-02).
	Audit auditAppender
	Now   func() time.Time
}

// Name returns the stable identifier for this sweeper.
func (s AgentsStale) Name() string { return "agents-stale" }

// Run flips stale agents, audits each flip and notifies with the count.
func (s AgentsStale) Run(ctx context.Context) error {
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	cutoff := now().Add(-s.StaleAfter)
	const q = `
		UPDATE agents
		SET status = 'stale'
		WHERE status = 'active'
		  AND last_seen_at < $1
		RETURNING id`
	ids, err := s.flip(ctx, q, cutoff)
	if err != nil {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityError,
			Component: s.Name(),
			Title:     "flag stale agents failed",
			Detail:    err.Error(),
		})
		return fmt.Errorf("sweepers: flag stale agents: %w", err)
	}
	// Agents carry no correlation id of their own; one id per sweep
	// ties the batch together.
	runCorrelation := uuid.New()
	for _, id := range ids {
		recordAudit(ctx, s.Audit, s.Name(), storage.AuditEvent{
			Action:        "agent.stale",
			Resource:      "agent:" + id.String(),
			CorrelationID: runCorrelation,
			Metadata: map[string]any{
				"reason":              "heartbeat_missed",
				"stale_after_seconds": int64(s.StaleAfter / time.Second),
			},
		})
	}
	if flipped := len(ids); flipped > 0 {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityWarn,
			Component: s.Name(),
			Title:     "agents flagged stale (no heartbeat past cutoff)",
			Detail:    fmt.Sprintf("cutoff=%s", cutoff.UTC().Format(time.RFC3339)),
			Metadata:  map[string]any{"flipped": int64(flipped)},
		})
	}
	return nil
}

func (s AgentsStale) flip(ctx context.Context, q string, cutoff time.Time) ([]uuid.UUID, error) {
	rows, err := s.Pool.Query(ctx, q, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
