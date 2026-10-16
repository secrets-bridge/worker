package sweepers

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/storage"

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
	Pool     pgxRowQuerier
	Notifier notifications.Notifier
	// Audit receives one job.expired event per flipped job, under the
	// job's own correlation id (WRK-N-02).
	Audit auditAppender
	Now   func() time.Time
}

// Name returns the stable identifier for this sweeper.
func (s JobsRecovery) Name() string { return "jobs-recovery" }

// expiredJob is the value-free slice of a flipped sync_jobs row.
type expiredJob struct {
	id          uuid.UUID
	correlation uuid.UUID
	jobType     string
}

// Run flips expired claimed jobs, audits each flip and notifies with
// the count. The UPDATE is unchanged apart from RETURNING.
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
		  AND claim_expires_at < $1
		RETURNING id, correlation_id, job_type`
	jobs, err := s.flip(ctx, q, now())
	if err != nil {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityError,
			Component: s.Name(),
			Title:     "recover stuck jobs failed",
			Detail:    err.Error(),
		})
		return fmt.Errorf("sweepers: recover stuck jobs: %w", err)
	}
	for _, j := range jobs {
		recordAudit(ctx, s.Audit, s.Name(), storage.AuditEvent{
			Action:        "job.expired",
			Resource:      "job:" + j.id.String(),
			CorrelationID: j.correlation,
			Metadata: map[string]any{
				"job_type": j.jobType,
				"reason":   "claim_expired",
			},
		})
	}
	if flipped := len(jobs); flipped > 0 {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityWarn,
			Component: s.Name(),
			Title:     "stuck jobs marked expired",
			Detail:    "claim_expires_at past; agent likely crashed mid-execute",
			Metadata:  map[string]any{"flipped": int64(flipped)},
		})
	}
	return nil
}

func (s JobsRecovery) flip(ctx context.Context, q string, now time.Time) ([]expiredJob, error) {
	rows, err := s.Pool.Query(ctx, q, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []expiredJob
	for rows.Next() {
		var j expiredJob
		if err := rows.Scan(&j.id, &j.correlation, &j.jobType); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
