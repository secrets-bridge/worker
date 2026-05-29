package sweepers

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/storage"
	"github.com/secrets-bridge/worker/internal/notifications"
)

// gitopsTimeoutRepo is the slice the timeout sweeper needs. Defined
// here so unit tests can plug a fake; the real *storage.GitOpsObservations
// satisfies both methods.
type gitopsTimeoutRepo interface {
	FindTimedOut(ctx context.Context, now time.Time, limit int) ([]*storage.GitOpsObservation, error)
	Transition(ctx context.Context, id uuid.UUID, state storage.GitOpsObservationState, at time.Time) error
}

// GitOpsTimeoutSweeper flips gitops_observations rows whose
// timeout_at has passed (and are still in queued/active) to
// applied_unverified. This is the "queue doesn't fill with forever-
// pending requests" rule from BRD §26.4 / §26.8.
//
// Defended-against case: a rollout that's still progressing past the
// configured timeout. Operators can resolve manually with audit; the
// row's state is distinct from `applied` so the UI shows what
// happened.
type GitOpsTimeoutSweeper struct {
	Repo     gitopsTimeoutRepo
	Notifier notifications.Notifier
	Limit    int
	Now      func() time.Time
}

// Name returns the stable identifier for this sweeper.
func (s GitOpsTimeoutSweeper) Name() string { return "gitops-timeout" }

// Run finds timed-out observations and transitions them.
func (s GitOpsTimeoutSweeper) Run(ctx context.Context) error {
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	limit := s.Limit
	if limit <= 0 {
		limit = 50
	}
	t := now()
	rows, err := s.Repo.FindTimedOut(ctx, t, limit)
	if err != nil {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityError,
			Component: s.Name(),
			Title:     "find timed-out gitops observations failed",
			Detail:    err.Error(),
		})
		return fmt.Errorf("sweepers: find timed-out observations: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}
	flipped := 0
	for _, r := range rows {
		if err := s.Repo.Transition(ctx, r.ID, storage.GitOpsStateAppliedUnverified, t); err != nil {
			notify(ctx, s.Notifier, notifications.Event{
				Severity:  notifications.SeverityWarn,
				Component: s.Name(),
				Title:     "transition timed-out observation failed",
				Detail:    err.Error(),
				Metadata: map[string]any{
					"observation_id":   r.ID.String(),
					"application_name": r.ApplicationName,
				},
			})
			continue
		}
		flipped++
	}
	if flipped > 0 {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityWarn,
			Component: s.Name(),
			Title:     "gitops observations timed out",
			Detail:    fmt.Sprintf("flipped %d row(s) to applied_unverified", flipped),
			Metadata:  map[string]any{"flipped": flipped},
		})
	}
	return nil
}
