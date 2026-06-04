package sweepers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/storage"

	"github.com/secrets-bridge/worker/internal/notifications"
)

// revealSessionSweeper is the slice of api/pkg/storage.RevealSessions
// the sweeper needs. Defined here so the test harness can inject a
// fake; the real *storage.RevealSessions satisfies it.
type revealSessionSweeper interface {
	SweepExpired(ctx context.Context, now time.Time) ([]storage.SweptRevealSession, error)
}

// wrapExpirer is the slice of api/pkg/storage.SecretWraps the sweeper
// needs. SetExpiry is the only mutation we make against the wraps
// repo — every successfully swept session's wrap_ids get their TTL
// flipped to "now" so any post-expire retrieve attempt returns
// ErrExpired (clean 410 to the SPA, which may still hold the wrap_id
// in memory across a refresh).
type wrapExpirer interface {
	SetExpiry(ctx context.Context, id uuid.UUID, expiresAt time.Time) error
}

// RevealSessionsExpired guarantees the session-level invariant: when
// a reveal session's TTL elapses, EVERY wrap it issued becomes
// unreachable. The api itself enforces single-shot consume at the
// row level (Slice M1) and refuses retrieve once a wrap's
// expires_at is in the past, so by advancing each wrap_id's
// expires_at in lockstep with the session, this sweeper closes the
// window even if the SPA still holds the id.
//
// Idempotent: the storage layer's UPDATE … WHERE expired_at IS NULL
// matches no row on a re-sweep, so a second invocation is a no-op.
type RevealSessionsExpired struct {
	Sessions revealSessionSweeper
	Wraps    wrapExpirer
	Notifier notifications.Notifier
	// Now is injected for tests; real wiring leaves it nil.
	Now func() time.Time
}

// Name returns the stable identifier for this sweeper.
func (s RevealSessionsExpired) Name() string { return "reveal-sessions-expired" }

// Run sweeps every reveal_sessions row whose expires_at is in the
// past and flips its wraps' TTL to "now". Per-wrap SetExpiry failures
// are joined and surfaced — a single broken wrap_id must NOT drop
// the rest of the sweep, since each row in the result is independent
// and the session-level row has already been marked expired.
func (s RevealSessionsExpired) Run(ctx context.Context) error {
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	swept, err := s.Sessions.SweepExpired(ctx, now())
	if err != nil {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityError,
			Component: s.Name(),
			Title:     "sweep expired reveal sessions failed",
			Detail:    err.Error(),
		})
		return fmt.Errorf("sweepers: sweep expired reveal sessions: %w", err)
	}
	if len(swept) == 0 {
		return nil
	}

	var (
		wrapTotal      int
		wrapErrs       []error
		expiryDeadline = now()
	)
	for _, sess := range swept {
		for _, wid := range sess.WrapIDs {
			wrapTotal++
			if err := s.Wraps.SetExpiry(ctx, wid, expiryDeadline); err != nil {
				wrapErrs = append(wrapErrs, fmt.Errorf("session=%s wrap=%s: %w", sess.ID, wid, err))
			}
		}
	}

	severity := notifications.SeverityInfo
	title := "reveal sessions expired"
	if len(wrapErrs) > 0 {
		severity = notifications.SeverityWarn
		title = "reveal sessions expired with wrap-expiry errors"
	}
	notify(ctx, s.Notifier, notifications.Event{
		Severity:  severity,
		Component: s.Name(),
		Title:     title,
		Metadata: map[string]any{
			"sessions_swept": len(swept),
			"wraps_expired":  wrapTotal - len(wrapErrs),
			"wrap_errors":    len(wrapErrs),
		},
	})

	if len(wrapErrs) > 0 {
		return fmt.Errorf("sweepers: reveal session wrap-expiry: %w", errors.Join(wrapErrs...))
	}
	return nil
}
