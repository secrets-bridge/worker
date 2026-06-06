package sweepers

import (
	"context"
	"fmt"
	"time"

	"github.com/secrets-bridge/api/pkg/storage"

	"github.com/secrets-bridge/worker/internal/notifications"
)

// crossTeamFillExpirer is the slice of api/pkg/storage.AccessRequests
// the sweeper needs. Defined here so unit tests can inject a fake;
// the real *storage.AccessRequests satisfies it.
type crossTeamFillExpirer interface {
	SweepFillExpired(ctx context.Context, now time.Time) ([]storage.SweptFillExpired, error)
}

// CrossTeamFillExpired closes the fill window on every cross_team
// request whose Team B failed to fill within fill_ttl_seconds. The
// storage method's atomic UPDATE ... RETURNING is the same
// distributed-safe pattern the M3 reveal-sessions-expired sweeper
// uses — multiple worker replicas can run this concurrently without
// double-processing.
//
// The service-layer (api) Fill check also rejects late writers via
// ErrFillWindowExpired, so this sweeper is defense in depth: it
// closes the observable state for everyone (dashboards, GET /requests,
// the inbox) within the configured cadence even when no fill attempt
// arrives to trigger the service-layer rejection.
type CrossTeamFillExpired struct {
	Repo     crossTeamFillExpirer
	Notifier notifications.Notifier
	// Now is injected for tests; real wiring leaves it nil.
	Now func() time.Time
}

// Name returns the stable identifier for this sweeper.
func (s CrossTeamFillExpired) Name() string { return "cross-team-fill-window-expired" }

// Run flips every past-TTL pending_values cross_team row to 'expired'
// in a single round trip and emits one info notification per non-empty
// batch with the swept count. Zero-rows: no notification (mirrors
// M3's posture).
func (s CrossTeamFillExpired) Run(ctx context.Context) error {
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	swept, err := s.Repo.SweepFillExpired(ctx, now())
	if err != nil {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityError,
			Component: s.Name(),
			Title:     "sweep cross_team fill expired failed",
			Detail:    err.Error(),
		})
		return fmt.Errorf("sweepers: sweep cross_team fill expired: %w", err)
	}
	if len(swept) == 0 {
		return nil
	}
	notify(ctx, s.Notifier, notifications.Event{
		Severity:  notifications.SeverityInfo,
		Component: s.Name(),
		Title:     "cross_team fill windows expired",
		Metadata: map[string]any{
			"requests_swept": len(swept),
		},
	})
	return nil
}
