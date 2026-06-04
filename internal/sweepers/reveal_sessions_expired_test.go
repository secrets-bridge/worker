package sweepers_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/storage"
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
	// 2 sessions, 3 wraps total; the 2nd wrap fails to expire — the
	// sweeper must still attempt the remaining wraps + surface a
	// non-nil error.
	sess1 := storage.SweptRevealSession{ID: uuid.New(), WrapIDs: []uuid.UUID{uuid.New(), uuid.New()}}
	sess2 := storage.SweptRevealSession{ID: uuid.New(), WrapIDs: []uuid.UUID{uuid.New()}}
	sessions := &fakeRevealSessionSweeper{swept: []storage.SweptRevealSession{sess1, sess2}}
	wraps := &fakeWrapExpirer{errAtCal: map[int]error{2: errors.New("wrap row gone")}}
	notif := &recordingNotifier{}

	sw := sweepers.RevealSessionsExpired{
		Sessions: sessions,
		Wraps:    wraps,
		Notifier: notif,
	}
	err := sw.Run(t.Context())
	if err == nil {
		t.Fatal("Run returned nil; expected aggregated wrap-expiry error")
	}
	if !strings.Contains(err.Error(), "wrap row gone") {
		t.Errorf("err = %v want to contain 'wrap row gone'", err)
	}
	if len(wraps.calls) != 3 {
		t.Errorf("SetExpiry attempted on %d wraps; want 3 (no early-exit)", len(wraps.calls))
	}
	titles := notif.titles()
	if len(titles) != 1 || !strings.Contains(titles[0], "wrap-expiry errors") {
		t.Fatalf("titles = %v want a warn notification mentioning wrap-expiry errors", titles)
	}
}

func TestRevealSessionsExpired_SweepFails(t *testing.T) {
	sessions := &fakeRevealSessionSweeper{err: errors.New("db down")}
	wraps := &fakeWrapExpirer{}
	notif := &recordingNotifier{}

	sw := sweepers.RevealSessionsExpired{
		Sessions: sessions,
		Wraps:    wraps,
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
