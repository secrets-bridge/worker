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

// --- fakes ----------------------------------------------------------

type fakeCrossTeamFillExpirer struct {
	mu     sync.Mutex
	swept  []storage.SweptFillExpired
	err    error
	calls  int
}

func (f *fakeCrossTeamFillExpirer) SweepFillExpired(_ context.Context, _ time.Time) ([]storage.SweptFillExpired, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.swept, f.err
}

// --- tests ----------------------------------------------------------

func TestCrossTeamFillExpired_HappyPath(t *testing.T) {
	now := time.Date(2026, 6, 5, 9, 0, 0, 0, time.UTC)
	swept := []storage.SweptFillExpired{
		{ID: uuid.New(), RequesterID: "alice@example.com"},
		{ID: uuid.New(), RequesterID: "carol@example.com"},
		{ID: uuid.New(), RequesterID: "dave@example.com"},
	}
	repo := &fakeCrossTeamFillExpirer{swept: swept}
	notif := &recordingNotifier{}

	sw := sweepers.CrossTeamFillExpired{
		Repo:     repo,
		Notifier: notif,
		Now:      func() time.Time { return now },
	}
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if repo.calls != 1 {
		t.Fatalf("SweepFillExpired calls = %d want 1", repo.calls)
	}
	titles := notif.titles()
	if len(titles) != 1 || !strings.Contains(titles[0], "cross_team fill") {
		t.Fatalf("titles = %v", titles)
	}
}

func TestCrossTeamFillExpired_ZeroRowsNoNotification(t *testing.T) {
	repo := &fakeCrossTeamFillExpirer{}
	notif := &recordingNotifier{}

	sw := sweepers.CrossTeamFillExpired{Repo: repo, Notifier: notif}
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(notif.events) != 0 {
		t.Errorf("zero-rows notify count = %d; want 0 (mirror M3 posture)", len(notif.events))
	}
}

func TestCrossTeamFillExpired_SweepFailureNotifiesAndReturnsError(t *testing.T) {
	repo := &fakeCrossTeamFillExpirer{err: errors.New("postgres unavailable")}
	notif := &recordingNotifier{}

	sw := sweepers.CrossTeamFillExpired{Repo: repo, Notifier: notif}
	err := sw.Run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "postgres unavailable") {
		t.Fatalf("err = %v want to contain 'postgres unavailable'", err)
	}
	titles := notif.titles()
	if len(titles) != 1 || !strings.Contains(titles[0], "failed") {
		t.Fatalf("titles = %v want failure notification", titles)
	}
}

func TestCrossTeamFillExpired_Name(t *testing.T) {
	got := sweepers.CrossTeamFillExpired{}.Name()
	if got != "cross-team-fill-window-expired" {
		t.Fatalf("Name() = %q", got)
	}
}
