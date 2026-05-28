package sweepers

import (
	"context"
	"fmt"
	"time"

	"github.com/secrets-bridge/worker/internal/notifications"
)

// staleMarker is the slice of api/pkg/storage.Secrets used here.
type staleMarker interface {
	MarkStaleAsMissing(ctx context.Context, cutoff time.Time) (int64, error)
}

// SecretsStale flips rows in the discovered-secrets table to
// `missing` when their last_seen_at is older than the configured
// cutoff. The row STAYS in the table — operators see "we used to see
// this; we don't anymore" rather than the row vanishing. The
// admin UI surfaces missing rows so drift is visible.
type SecretsStale struct {
	Repo           staleMarker
	Notifier       notifications.Notifier
	// StaleAfter controls how old a row's last_seen_at must be before
	// MarkStaleAsMissing flips it. Typically several discovery cycles
	// (e.g. 3 × discover_interval) so a brief failed discover doesn't
	// flap rows.
	StaleAfter time.Duration
	Now        func() time.Time
}

// Name returns the stable identifier for this sweeper.
func (s SecretsStale) Name() string { return "secrets-stale" }

// Run flips stale rows and notifies with the count.
func (s SecretsStale) Run(ctx context.Context) error {
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	cutoff := now().Add(-s.StaleAfter)
	flipped, err := s.Repo.MarkStaleAsMissing(ctx, cutoff)
	if err != nil {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityError,
			Component: s.Name(),
			Title:     "mark stale secrets failed",
			Detail:    err.Error(),
		})
		return fmt.Errorf("sweepers: mark stale secrets: %w", err)
	}
	if flipped > 0 {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityWarn,
			Component: s.Name(),
			Title:     "discovered secrets marked missing",
			Detail:    fmt.Sprintf("cutoff=%s", cutoff.UTC().Format(time.RFC3339)),
			Metadata:  map[string]any{"flipped": flipped},
		})
	}
	return nil
}
