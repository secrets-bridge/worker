package sweepers

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/storage"

	"github.com/secrets-bridge/worker/internal/notifications"
)

// GatewaysStale flips gateways.status from active to stale when the
// Gateway has not sent a heartbeat for StaleAfter (design section 8,
// gateways-stale). The Gateway keeps its credential (the
// gateways_secret_matches_status CHECK allows a secret on active and
// stale alike) and can still authenticate, claim and report: stale is a
// signal for operators and for the timed_out reason
// (gateway_unavailable), not a revoke. The api flips the row back to
// active on the next accepted heartbeat (Gateways.RecordHeartbeat).
//
// A Gateway that enrolled and never sent a heartbeat is judged on its
// enrolled_at. pending, disabled and revoked rows are never touched.
//
// Lock order: the statement locks gateways rows only (FOR NO KEY UPDATE,
// SKIP LOCKED), never a task, so it cannot invert the api's order
// (gateway row first, then tasks). A Gateway whose row a heartbeat or a
// claim holds right now is skipped; it is alive anyway.
type GatewaysStale struct {
	Pool       pgxRowQuerier
	StaleAfter time.Duration
	// Audit receives one gateway.stale event per flipped Gateway.
	Audit    auditAppender
	Notifier notifications.Notifier
	// Limit bounds the rows flipped per tick; the rest go next tick.
	Limit int
}

// Name returns the stable identifier for this sweeper (the lock name).
func (s GatewaysStale) Name() string { return "gateways-stale" }

// Run flips the quiet Gateways, audits each and notifies with the count.
// The cutoff is computed by the database (now() - StaleAfter) because
// the api writes last_seen_at on its own clock and the database is the
// one clock both sides share.
func (s GatewaysStale) Run(ctx context.Context) error {
	limit := s.Limit
	if limit <= 0 {
		limit = 500
	}
	staleAfter := fmt.Sprintf("%d milliseconds", s.StaleAfter.Milliseconds())
	const q = `
		UPDATE gateways g SET status = 'stale'
		WHERE g.id IN (
			SELECT id FROM gateways
			WHERE status = 'active'
			  AND COALESCE(last_seen_at, enrolled_at) < now() - $1::interval
			ORDER BY COALESCE(last_seen_at, enrolled_at), id
			LIMIT $2
			FOR NO KEY UPDATE SKIP LOCKED)
		  AND g.status = 'active'
		RETURNING g.id`
	ids, err := queryIDs(ctx, s.Pool, q, staleAfter, limit)
	if err != nil {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityError,
			Component: s.Name(),
			Title:     "flag stale gateways failed",
			Detail:    "error class " + auditErrorClass(err),
		})
		return fmt.Errorf("sweepers: flag stale gateways: %w", err)
	}
	// Gateways carry no correlation id of their own; one id per sweep
	// ties the batch together.
	runCorrelation := uuid.New()
	for _, id := range ids {
		gatewaysFlaggedStale.Inc()
		recordAudit(ctx, s.Audit, s.Name(), storage.AuditEvent{
			Action:        "gateway.stale",
			Resource:      "gateway:" + id.String(),
			CorrelationID: runCorrelation,
			Metadata: map[string]any{
				"gateway_id":          id.String(),
				"reason":              "heartbeat_missed",
				"stale_after_seconds": int64(s.StaleAfter / time.Second),
			},
		})
	}
	if n := len(ids); n > 0 {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityWarn,
			Component: s.Name(),
			Title:     "gateways flagged stale (no heartbeat past cutoff); observation waits, deployments are not failed",
			Metadata:  map[string]any{"flipped": int64(n)},
		})
	}
	return nil
}

// GatewayReplicasPrune deletes gateway_replicas rows not seen for
// Retention (design section 3.3: "a sweeper prunes old rows"). The api
// counts replicas seen within its own window (10 minutes) for the
// replica limit, so Retention must be at least that window; config
// enforces the floor. No audit row: a replica row is a heartbeat cache,
// not a state the operator acted on (design section 8).
//
// Lock order: the Gateway row is locked first, FOR SHARE as the api's
// task paths do, then its replica rows are deleted. The heartbeat takes
// the Gateway row FOR UPDATE before it reads and upserts the replica, so
// a heartbeat and a prune of the same Gateway serialise: the heartbeat
// either refreshes last_seen_at first (the row is then not old) or sees
// the replica gone and runs the replica-limit check for it as a new
// one. Gateways whose row is busy are skipped until the next tick.
type GatewayReplicasPrune struct {
	Pool      pgxRowQuerier
	Retention time.Duration
	// Limit bounds the Gateways handled per tick.
	Limit int
}

// Name returns the stable identifier for this sweeper (the lock name).
func (s GatewayReplicasPrune) Name() string { return "gateway-replicas-prune" }

// Run deletes the old replica rows and counts them.
func (s GatewayReplicasPrune) Run(ctx context.Context) error {
	limit := s.Limit
	if limit <= 0 {
		limit = 500
	}
	retention := fmt.Sprintf("%d milliseconds", s.Retention.Milliseconds())
	const q = `
		WITH g AS (
			SELECT id FROM gateways
			WHERE id IN (SELECT DISTINCT gateway_id FROM gateway_replicas WHERE last_seen_at < now() - $1::interval)
			ORDER BY id
			LIMIT $2
			FOR SHARE SKIP LOCKED
		)
		DELETE FROM gateway_replicas r USING g
		WHERE r.gateway_id = g.id AND r.last_seen_at < now() - $1::interval
		RETURNING r.gateway_id`
	ids, err := queryIDs(ctx, s.Pool, q, retention, limit)
	if err != nil {
		return fmt.Errorf("sweepers: prune gateway replicas: %w", err)
	}
	if n := len(ids); n > 0 {
		gatewayReplicasPruned.Add(float64(n))
	}
	return nil
}

// queryIDs runs an UPDATE/DELETE ... RETURNING <uuid> and collects the
// ids.
func queryIDs(ctx context.Context, pool pgxRowQuerier, q string, args ...any) ([]uuid.UUID, error) {
	rows, err := pool.Query(ctx, q, args...)
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
