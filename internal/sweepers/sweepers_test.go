package sweepers_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/secrets-bridge/api/pkg/storage"
	"github.com/secrets-bridge/worker/internal/notifications"
	"github.com/secrets-bridge/worker/internal/sweepers"
)

// --- fakes ----------------------------------------------------------

type fakeWrapDeleter struct {
	deleted int64
	err     error
	calls   int
}

func (f *fakeWrapDeleter) DeleteExpired(_ context.Context, _ time.Time) (int64, error) {
	f.calls++
	return f.deleted, f.err
}

type fakeStaleMarker struct {
	flipped int64
	err     error
	got     time.Time
}

func (f *fakeStaleMarker) MarkStaleAsMissing(_ context.Context, cutoff time.Time) (int64, error) {
	f.got = cutoff
	return f.flipped, f.err
}

type fakePool struct {
	mu          sync.Mutex
	lastSQL     string
	lastArgs    []any
	rowsTouched int64
	err         error
}

func (f *fakePool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastSQL = sql
	f.lastArgs = args
	if f.err != nil {
		return pgconn.CommandTag{}, f.err
	}
	// pgconn.CommandTag is opaque outside the package; pgconn doesn't
	// expose a setter for RowsAffected, so the test asserts on the
	// notification metadata rather than on the returned tag.
	_ = f.rowsTouched
	return pgconn.CommandTag{}, nil
}

type fakeJobCreator struct {
	created []*storage.SyncJob
	err     error
}

func (f *fakeJobCreator) Create(_ context.Context, j *storage.SyncJob) error {
	if f.err != nil {
		return f.err
	}
	f.created = append(f.created, j)
	return nil
}

type recordingNotifier struct {
	mu     sync.Mutex
	events []notifications.Event
}

func (r *recordingNotifier) Notify(_ context.Context, e notifications.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
	return nil
}
func (r *recordingNotifier) Name() string { return "recording" }

func (r *recordingNotifier) titles() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.events))
	for i, e := range r.events {
		out[i] = e.Title
	}
	return out
}

// --- WrapsExpired --------------------------------------------------

func TestWrapsExpired_NotifiesOnDelete(t *testing.T) {
	repo := &fakeWrapDeleter{deleted: 3}
	notif := &recordingNotifier{}
	sw := sweepers.WrapsExpired{Repo: repo, Notifier: notif}
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if repo.calls != 1 {
		t.Fatalf("calls = %d want 1", repo.calls)
	}
	titles := notif.titles()
	if len(titles) != 1 || !strings.Contains(titles[0], "expired wraps") {
		t.Fatalf("notifications = %v", titles)
	}
}

func TestWrapsExpired_NoNotifyOnZero(t *testing.T) {
	repo := &fakeWrapDeleter{deleted: 0}
	notif := &recordingNotifier{}
	sw := sweepers.WrapsExpired{Repo: repo, Notifier: notif}
	_ = sw.Run(t.Context())
	if len(notif.events) != 0 {
		t.Fatalf("expected no notification when zero rows; got %v", notif.events)
	}
}

func TestWrapsExpired_ErrorReturnedAndNotified(t *testing.T) {
	repo := &fakeWrapDeleter{err: errors.New("db down")}
	notif := &recordingNotifier{}
	sw := sweepers.WrapsExpired{Repo: repo, Notifier: notif}
	err := sw.Run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "db down") {
		t.Fatalf("err = %v", err)
	}
	titles := notif.titles()
	if len(titles) != 1 || !strings.Contains(titles[0], "failed") {
		t.Fatalf("notif titles = %v", titles)
	}
}

// --- SecretsStale --------------------------------------------------

func TestSecretsStale_CutoffComputedFromStaleAfter(t *testing.T) {
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	repo := &fakeStaleMarker{flipped: 2}
	notif := &recordingNotifier{}
	sw := sweepers.SecretsStale{
		Repo:       repo,
		Notifier:   notif,
		StaleAfter: 1 * time.Hour,
		Now:        func() time.Time { return now },
	}
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	wantCutoff := now.Add(-1 * time.Hour)
	if !repo.got.Equal(wantCutoff) {
		t.Fatalf("cutoff = %v want %v", repo.got, wantCutoff)
	}
	if len(notif.events) != 1 {
		t.Fatalf("notifications = %v", notif.events)
	}
}

// --- AgentsStale ---------------------------------------------------

func TestAgentsStale_RunsExpectedSQL(t *testing.T) {
	pool := &fakePool{}
	notif := &recordingNotifier{}
	sw := sweepers.AgentsStale{Pool: pool, Notifier: notif, StaleAfter: 30 * time.Minute}
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(pool.lastSQL, "UPDATE agents") || !strings.Contains(pool.lastSQL, "'stale'") {
		t.Fatalf("unexpected SQL: %s", pool.lastSQL)
	}
	if !strings.Contains(pool.lastSQL, "'active'") {
		t.Fatalf("SQL does not filter on active: %s", pool.lastSQL)
	}
}

func TestAgentsStale_PropagatesError(t *testing.T) {
	pool := &fakePool{err: errors.New("conn closed")}
	sw := sweepers.AgentsStale{Pool: pool, StaleAfter: time.Hour}
	err := sw.Run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "conn closed") {
		t.Fatalf("err = %v", err)
	}
}

// --- JobsRecovery --------------------------------------------------

func TestJobsRecovery_RunsExpectedSQL(t *testing.T) {
	pool := &fakePool{}
	sw := sweepers.JobsRecovery{Pool: pool}
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, frag := range []string{"UPDATE sync_jobs", "'expired'", "'claimed'", "claim_expires_at"} {
		if !strings.Contains(pool.lastSQL, frag) {
			t.Fatalf("SQL missing %q: %s", frag, pool.lastSQL)
		}
	}
}

// --- DiscoverScheduler ---------------------------------------------

func TestDiscoverScheduler_EnqueuesOnePerTarget(t *testing.T) {
	jc := &fakeJobCreator{}
	notif := &recordingNotifier{}
	sw := sweepers.DiscoverScheduler{
		Jobs:     jc,
		Notifier: notif,
		Targets: []sweepers.DiscoverTarget{
			{Name: "prod-vault", Cluster: "prod-eu", ProviderType: "vault"},
			{Name: "prod-aws", Cluster: "prod-us", ProviderType: "aws-sm",
				ProviderConfig: map[string]any{"region": "us-east-1"}},
		},
	}
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(jc.created) != 2 {
		t.Fatalf("created = %d want 2", len(jc.created))
	}
	if jc.created[0].JobType != "discover" {
		t.Fatalf("first job type = %v", jc.created[0].JobType)
	}
	if jc.created[0].AgentScope["cluster_name"] != "prod-eu" {
		t.Fatalf("first scope cluster = %v", jc.created[0].AgentScope["cluster_name"])
	}
	if jc.created[1].Payload["target_provider_type"] != "aws-sm" {
		t.Fatalf("second payload type = %v", jc.created[1].Payload["target_provider_type"])
	}
}

func TestDiscoverScheduler_ParseTargets(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		ts, err := sweepers.ParseTargets("")
		if err != nil || ts != nil {
			t.Fatalf("got (%v, %v) want (nil, nil)", ts, err)
		}
	})
	t.Run("missing required field", func(t *testing.T) {
		_, err := sweepers.ParseTargets(`[{"name":"x","cluster":"c"}]`)
		if err == nil || !strings.Contains(err.Error(), "provider_type") {
			t.Fatalf("err = %v want provider_type required", err)
		}
	})
	t.Run("valid", func(t *testing.T) {
		ts, err := sweepers.ParseTargets(`[{"name":"x","cluster":"c","provider_type":"vault","provider_config":{"a":"b"}}]`)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(ts) != 1 || ts[0].Name != "x" || ts[0].ProviderConfig["a"] != "b" {
			t.Fatalf("parsed = %+v", ts)
		}
	})
}

func TestDiscoverScheduler_PartialFailure(t *testing.T) {
	// First Create succeeds, second fails. The sweeper must return
	// the error but ALSO have enqueued the one that worked.
	failing := &flakyJobCreator{failOn: 2}
	notif := &recordingNotifier{}
	sw := sweepers.DiscoverScheduler{
		Jobs:     failing,
		Notifier: notif,
		Targets: []sweepers.DiscoverTarget{
			{Name: "good", Cluster: "c", ProviderType: "vault"},
			{Name: "bad", Cluster: "c", ProviderType: "vault"},
		},
	}
	err := sw.Run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "bad") {
		t.Fatalf("err = %v", err)
	}
	if failing.success != 1 {
		t.Fatalf("expected 1 enqueued, got %d", failing.success)
	}
}

type flakyJobCreator struct {
	calls   int
	failOn  int
	success int
}

func (f *flakyJobCreator) Create(_ context.Context, _ *storage.SyncJob) error {
	f.calls++
	if f.calls == f.failOn {
		return errors.New("boom")
	}
	f.success++
	return nil
}
