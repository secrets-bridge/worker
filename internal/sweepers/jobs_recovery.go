package sweepers

import (
	"context"
	"fmt"
	"time"

	"github.com/secrets-bridge/worker/internal/notifications"
)

// JobsRecovery flips sync_jobs whose claim_expires_at has passed
// back from `claimed` to `expired`. Two things drive this:
//
//  1. The api side's ClaimNext already RE-ENTERS expired claims
//     into the working set inline so the next claim picks them up
//     — that's correctness. The worker's job here is OBSERVABILITY:
//     mark the row terminal-ish (`expired`) so the audit log shows
//     the dead-claim event and the admin UI doesn't show a forever-
//     claimed row.
//  2. If a worker / agent crashed mid-execution, the row sits in
//     `claimed` indefinitely from a human-readable status standpoint
//     until something explicit flips it.
//
// Raw SQL on the pool — same reasoning as agents-stale; the api
// side's ClaimNext does the work for the claim path, the worker
// owns the explicit expiry transition.
type JobsRecovery struct {
	Pool      pgxQuerier
	Notifier  notifications.Notifier
	Now       func() time.Time
}

// Name returns the stable identifier for this sweeper.
func (s JobsRecovery) Name() string { return "jobs-recovery" }

// Run flips expired claimed jobs and notifies with the count.
func (s JobsRecovery) Run(ctx context.Context) error {
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	const q = `
		UPDATE sync_jobs
		SET status = 'expired'
		WHERE status = 'claimed'
		  AND claim_expires_at IS NOT NULL
		  AND claim_expires_at < $1`
	tag, err := s.Pool.Exec(ctx, q, now())
	if err != nil {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityError,
			Component: s.Name(),
			Title:     "recover stuck jobs failed",
			Detail:    err.Error(),
		})
		return fmt.Errorf("sweepers: recover stuck jobs: %w", err)
	}
	flipped := tag.RowsAffected()
	if flipped > 0 {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityWarn,
			Component: s.Name(),
			Title:     "stuck jobs marked expired",
			Detail:    "claim_expires_at past; agent likely crashed mid-execute",
			Metadata:  map[string]any{"flipped": flipped},
		})
	}
	return nil
}
