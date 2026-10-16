package sweepers_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/storage"
	"github.com/secrets-bridge/worker/internal/notifications"
	"github.com/secrets-bridge/worker/internal/sweepers"
)

// --- fakes for the reveal sessions sweeper -------------------------

type fakeRevealSessionSweeper struct {
	swept []storage.SweptRevealSession
	err   error
	calls int
}

func (f *fakeRevealSessionSweeper) SweepExpired(_ context.Context, _ time.Time) ([]storage.SweptRevealSession, error) {
	f.calls++
	return f.swept, f.err
}

type fakeWrapExpirer struct {
	mu       sync.Mutex
	calls    []uuid.UUID
	errAtCal map[int]error // 1-indexed
	got      map[uuid.UUID]time.Time
}

func (f *fakeWrapExpirer) SetExpiry(_ context.Context, id uuid.UUID, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, id)
	if f.got == nil {
		f.got = map[uuid.UUID]time.Time{}
	}
	f.got[id] = at
	if e, ok := f.errAtCal[len(f.calls)]; ok {
		return e
	}
	return nil
}

type fakeClamper struct {
	healed []uuid.UUID
	err    error
	calls  int
}

func (f *fakeClamper) ClampEndedSessionWraps(_ context.Context, _ time.Time) ([]uuid.UUID, error) {
	f.calls++
	return f.healed, f.err
}

// --- tests ---------------------------------------------------------

func TestRevealSessionsExpired_HappyPath(t *testing.T) {
	now := time.Date(2026, 6, 4, 19, 0, 0, 0, time.UTC)
	sess1 := storage.SweptRevealSession{ID: uuid.New(), WrapIDs: []uuid.UUID{uuid.New(), uuid.New()}}
	sess2 := storage.SweptRevealSession{ID: uuid.New(), WrapIDs: []uuid.UUID{uuid.New()}}
	sessions := &fakeRevealSessionSweeper{swept: []storage.SweptRevealSession{sess1, sess2}}
	wraps := &fakeWrapExpirer{}
	notif := &recordingNotifier{}

	sw := sweepers.RevealSessionsExpired{
		Sessions: sessions,
		Wraps:    wraps,
		Store:    &fakeClamper{},
		Notifier: notif,
		Now:      func() time.Time { return now },
	}
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if sessions.calls != 1 {
		t.Fatalf("SweepExpired calls = %d want 1", sessions.calls)
	}
	if len(wraps.calls) != 3 {
		t.Fatalf("SetExpiry calls = %d want 3", len(wraps.calls))
	}
	for _, id := range append(append([]uuid.UUID{}, sess1.WrapIDs...), sess2.WrapIDs...) {
		gotAt, ok := wraps.got[id]
		if !ok {
			t.Errorf("wrap %s not expired", id)
		}
		if !gotAt.Equal(now) {
			t.Errorf("wrap %s expires_at = %v want %v", id, gotAt, now)
		}
	}
	titles := notif.titles()
	if len(titles) != 1 || !strings.Contains(titles[0], "reveal sessions expired") {
		t.Fatalf("titles = %v", titles)
	}
}

func TestRevealSessionsExpired_IdempotentZeroRows(t *testing.T) {
	sessions := &fakeRevealSessionSweeper{}
	wraps := &fakeWrapExpirer{}
	notif := &recordingNotifier{}

	sw := sweepers.RevealSessionsExpired{
		Sessions: sessions,
		Wraps:    wraps,
		Store:    &fakeClamper{},
		Notifier: notif,
	}
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(wraps.calls) != 0 {
		t.Errorf("SetExpiry called %d times on empty sweep", len(wraps.calls))
	}
	if len(notif.events) != 0 {
		t.Errorf("notifications on empty sweep: %d", len(notif.events))
	}
}

func TestRevealSessionsExpired_PartialWrapExpiryFails(t *testing.T) {
	// 2 sessions, 3 wraps total; the 2nd wrap fails to expire. The
	// sweeper must still attempt the remaining wraps and warn. With the
	// self-heal healthy the failed wrap is covered in the same tick, so
	// Run succeeds (a retry would have nothing left to do); when the
	// self-heal also fails, both errors come back so the tick retries.
	for _, tc := range []struct {
		name    string
		healErr error
		wantErr []string
	}{
		{name: "self-heal covers it"},
		{name: "self-heal fails too", healErr: errors.New("conn closed"), wantErr: []string{"wrap row gone", "conn closed"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess1 := storage.SweptRevealSession{ID: uuid.New(), WrapIDs: []uuid.UUID{uuid.New(), uuid.New()}}
			sess2 := storage.SweptRevealSession{ID: uuid.New(), WrapIDs: []uuid.UUID{uuid.New()}}
			sessions := &fakeRevealSessionSweeper{swept: []storage.SweptRevealSession{sess1, sess2}}
			wraps := &fakeWrapExpirer{errAtCal: map[int]error{2: errors.New("wrap row gone")}}
			store := &fakeClamper{err: tc.healErr}
			notif := &recordingNotifier{}

			sw := sweepers.RevealSessionsExpired{
				Sessions: sessions,
				Wraps:    wraps,
				Store:    store,
				Notifier: notif,
			}
			err := sw.Run(t.Context())
			if len(tc.wantErr) == 0 && err != nil {
				t.Fatalf("Run: %v want nil (self-heal ran)", err)
			}
			for _, want := range tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("err = %v want to contain %q", err, want)
				}
			}
			if len(wraps.calls) != 3 {
				t.Errorf("SetExpiry attempted on %d wraps; want 3 (no early-exit)", len(wraps.calls))
			}
			if store.calls != 1 {
				t.Errorf("self-heal ran %d times; want 1", store.calls)
			}
			titles := notif.titles()
			if len(titles) != 1 || !strings.Contains(titles[0], "wrap-expiry errors") {
				t.Fatalf("titles = %v want a warn notification mentioning wrap-expiry errors", titles)
			}
			if got := notif.events[0].Metadata["wrap_errors"]; got != 1 {
				t.Errorf("wrap_errors = %v want 1", got)
			}
		})
	}
}

// WRK-N-01: a consumed wrap (the user already revealed it) or one the
// wraps-expired sweeper already deleted is closed, not an error.
func TestRevealSessionsExpired_ConsumedOrDeletedWrapIsNotAnError(t *testing.T) {
	sess := storage.SweptRevealSession{ID: uuid.New(), WrapIDs: []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}}
	sessions := &fakeRevealSessionSweeper{swept: []storage.SweptRevealSession{sess}}
	wraps := &fakeWrapExpirer{errAtCal: map[int]error{
		1: storage.ErrAlreadyConsumed,
		2: fmt.Errorf("lookup: %w", storage.ErrNotFound),
	}}
	notif := &recordingNotifier{}
	sw := sweepers.RevealSessionsExpired{Sessions: sessions, Wraps: wraps, Store: &fakeClamper{}, Notifier: notif}
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v want nil", err)
	}
	if len(notif.events) != 1 || notif.events[0].Severity != notifications.SeverityInfo {
		t.Fatalf("events = %+v want one info notification", notif.events)
	}
	md := notif.events[0].Metadata
	if md["wraps_already_closed"] != 2 || md["wraps_expired"] != 1 || md["wrap_errors"] != 0 {
		t.Fatalf("metadata = %+v", md)
	}
}

// The self-heal runs on every tick, also when nothing new expired, and
// a clamp it makes is reported.
func TestRevealSessionsExpired_SelfHealRunsWithNoNewSessions(t *testing.T) {
	store := &fakeClamper{healed: []uuid.UUID{uuid.New()}}
	notif := &recordingNotifier{}
	sw := sweepers.RevealSessionsExpired{
		Sessions: &fakeRevealSessionSweeper{},
		Wraps:    &fakeWrapExpirer{},
		Store:    store,
		Notifier: notif,
	}
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if store.calls != 1 {
		t.Fatalf("self-heal calls = %d want 1", store.calls)
	}
	if len(notif.events) != 1 || notif.events[0].Metadata["wraps_self_healed"] != 1 {
		t.Fatalf("events = %+v want one notification with wraps_self_healed=1", notif.events)
	}
}

// A sweeper wired without its Store must not fail silently.
func TestRevealSessionsExpired_MissingStoreIsLoud(t *testing.T) {
	sw := sweepers.RevealSessionsExpired{
		Sessions: &fakeRevealSessionSweeper{},
		Wraps:    &fakeWrapExpirer{},
		Notifier: &recordingNotifier{},
	}
	if err := sw.Run(t.Context()); err == nil || !strings.Contains(err.Error(), "no Store") {
		t.Fatalf("err = %v want a missing-Store error", err)
	}
}

func TestRevealSessionsExpired_SweepFails(t *testing.T) {
	sessions := &fakeRevealSessionSweeper{err: errors.New("db down")}
	wraps := &fakeWrapExpirer{}
	notif := &recordingNotifier{}

	sw := sweepers.RevealSessionsExpired{
		Sessions: sessions,
		Wraps:    wraps,
		Store:    &fakeClamper{},
		Notifier: notif,
	}
	err := sw.Run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "db down") {
		t.Fatalf("err = %v", err)
	}
	if len(wraps.calls) != 0 {
		t.Errorf("SetExpiry called %d times despite sweep failure", len(wraps.calls))
	}
	titles := notif.titles()
	if len(titles) != 1 || !strings.Contains(titles[0], "failed") {
		t.Fatalf("titles = %v want failure notification", titles)
	}
}

func TestRevealSessionsExpired_Name(t *testing.T) {
	if got := (sweepers.RevealSessionsExpired{}).Name(); got != "reveal-sessions-expired" {
		t.Fatalf("Name() = %q", got)
	}
}
