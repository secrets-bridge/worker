package sweepers

import (
	"context"
	"fmt"
	"time"

	"github.com/secrets-bridge/worker/internal/notifications"
)

// wrapDeleter is the slice of api/pkg/storage.SecretWraps used here.
type wrapDeleter interface {
	DeleteExpired(ctx context.Context, now time.Time) (int64, error)
}

// WrapsExpired sweeps expired wraps from secret_wraps. The wraps
// table holds the envelope-encrypted blobs of in-flight secret
// values; once a wrap is past its expires_at it's by definition
// unrecoverable (single-shot semantics also kept it from being
// retrieved), so deleting frees disk + audit volume without changing
// any visible behavior.
type WrapsExpired struct {
	Repo     wrapDeleter
	Notifier notifications.Notifier
	// Now is injected for testing; the real wiring leaves it nil and
	// the sweeper uses time.Now().
	Now func() time.Time
}

// Name returns the stable identifier for this sweeper.
func (s WrapsExpired) Name() string { return "wraps-expired" }

// Run deletes every wrap whose expires_at is in the past, then
// notifies with the count.
func (s WrapsExpired) Run(ctx context.Context) error {
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	deleted, err := s.Repo.DeleteExpired(ctx, now())
	if err != nil {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityError,
			Component: s.Name(),
			Title:     "delete expired wraps failed",
			Detail:    err.Error(),
		})
		return fmt.Errorf("sweepers: delete expired wraps: %w", err)
	}
	if deleted > 0 {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityInfo,
			Component: s.Name(),
			Title:     "expired wraps purged",
			Metadata:  map[string]any{"deleted": deleted},
		})
	}
	return nil
}
