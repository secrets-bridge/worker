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

// endedSessionWrapClamper is the worker-side self-heal query. The real
// implementation is RevealSessionStore.
type endedSessionWrapClamper interface {
	ClampEndedSessionWraps(ctx context.Context, now time.Time) ([]uuid.UUID, error)
}

// RevealSessionsExpired guarantees the session-level invariant: when
// a reveal session's TTL elapses, EVERY wrap it issued becomes
// unreachable. The api itself enforces single-shot consume at the
// row level (Slice M1) and refuses retrieve once a wrap's
// expires_at is in the past, so by advancing each wrap_id's
// expires_at in lockstep with the session, this sweeper closes the
// window even if the SPA still holds the id.
//
// SweepExpired commits expired_at before the wraps are touched, so a
// session is returned exactly once. If a per-wrap update then fails,
// no later tick sees that session again. Every tick therefore also
// runs a self-heal (Store.ClampEndedSessionWraps) that clamps any
// unconsumed wrap of any ended session, including rows left behind by
// older builds or by a failed api-side Hide Now. WRK-N-01.
//
// Idempotent: the storage layer's UPDATE … WHERE expired_at IS NULL
// matches no row on a re-sweep, and the clamp only moves expires_at
// down, so a second invocation is a no-op.
type RevealSessionsExpired struct {
	Sessions revealSessionSweeper
	Wraps    wrapExpirer
	// Store runs the self-heal. Required; Run reports an error every
	// tick when it is missing so a wiring mistake is loud.
	Store    endedSessionWrapClamper
	Notifier notifications.Notifier
	// Now is injected for tests; real wiring leaves it nil.
	Now func() time.Time
}

// Name returns the stable identifier for this sweeper.
func (s RevealSessionsExpired) Name() string { return "reveal-sessions-expired" }

// Run sweeps every reveal_sessions row whose expires_at is in the
// past, flips its wraps' TTL to "now", then runs the self-heal.
//
// A wrap that is already consumed (ErrAlreadyConsumed) or already gone
// (ErrNotFound, deleted by wraps-expired) is closed, not a failure.
// Other per-wrap failures don't stop the loop and are covered by the
// self-heal in the same tick; Run only returns an error when the sweep
// or the self-heal itself failed, which is when a retry can help.
func (s RevealSessionsExpired) Run(ctx context.Context) error {
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	t := now()

	var errs []error
	swept, err := s.Sessions.SweepExpired(ctx, t)
	if err != nil {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityError,
			Component: s.Name(),
			Title:     "sweep expired reveal sessions failed",
			Detail:    err.Error(),
		})
		errs = append(errs, fmt.Errorf("sweepers: sweep expired reveal sessions: %w", err))
	}

	var (
		wrapsExpired  int
		alreadyClosed int
		wrapErrs      []error
	)
	for _, sess := range swept {
		for _, wid := range sess.WrapIDs {
			err := s.Wraps.SetExpiry(ctx, wid, t)
			switch {
			case err == nil:
				wrapsExpired++
			case errors.Is(err, storage.ErrAlreadyConsumed), errors.Is(err, storage.ErrNotFound):
				alreadyClosed++
			default:
				wrapErrs = append(wrapErrs, fmt.Errorf("session=%s wrap=%s: %w", sess.ID, wid, err))
			}
		}
	}

	var (
		healed  []uuid.UUID
		healErr error
	)
	if s.Store == nil {
		healErr = errors.New("sweepers: reveal-sessions-expired has no Store; ended-session wraps are not self-healed")
	} else {
		healed, healErr = s.Store.ClampEndedSessionWraps(ctx, t)
	}
	if healErr != nil {
		// Per-wrap failures are only covered once the self-heal has run,
		// so surface them with it.
		errs = append(errs, wrapErrs...)
		errs = append(errs, fmt.Errorf("sweepers: reveal session wrap self-heal: %w", healErr))
	}

	if len(swept) > 0 || len(healed) > 0 || len(wrapErrs) > 0 || healErr != nil {
		severity := notifications.SeverityInfo
		title := "reveal sessions expired"
		if len(wrapErrs) > 0 || healErr != nil {
			severity = notifications.SeverityWarn
			title = "reveal sessions expired with wrap-expiry errors"
		} else if len(healed) > 0 {
			severity = notifications.SeverityWarn
			title = "reveal sessions expired; wraps of earlier sessions clamped"
		}
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  severity,
			Component: s.Name(),
			Title:     title,
			Metadata: map[string]any{
				"sessions_swept":       len(swept),
				"wraps_expired":        wrapsExpired,
				"wraps_already_closed": alreadyClosed,
				"wrap_errors":          len(wrapErrs),
				"wraps_self_healed":    len(healed),
				"self_heal_failed":     healErr != nil,
			},
		})
	}

	return errors.Join(errs...)
}
