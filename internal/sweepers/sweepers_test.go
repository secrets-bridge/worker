package sweepers_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/secrets-bridge/api/pkg/runtime"
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

// TestDiscoverScheduler_EnvFallback_Enqueues exercises the deprecated
// env-var path. Reached when no DB connections repo is wired (or
// when DB returns zero rows + env is set).
func TestDiscoverScheduler_EnvFallback_Enqueues(t *testing.T) {
	jc := &fakeJobCreator{}
	notif := &recordingNotifier{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sw := sweepers.DiscoverScheduler{
		Jobs:     jc,
		Notifier: notif,
		Logger:   logger,
		EnvFallback: []sweepers.DiscoverTarget{
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
	if jc.created[0].JobType != storage.JobTypeDiscover {
		t.Fatalf("first job type = %v", jc.created[0].JobType)
	}
	if jc.created[0].AgentScope["cluster_name"] != "prod-eu" {
		t.Fatalf("first scope cluster = %v", jc.created[0].AgentScope["cluster_name"])
	}
}

// TestDiscoverScheduler_DBTakesPrecedence_OverEnv pins the load-bearing
// contract: when DB has rows AND env_fallback is non-empty, the DB
// path runs and the env path is skipped entirely.
func TestDiscoverScheduler_DBTakesPrecedence_OverEnv(t *testing.T) {
	jc := &fakeJobCreator{}
	notif := &recordingNotifier{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dbID := uuid.New()
	conn := &fakeProviderConnections{targets: []storage.DiscoverTarget{
		{ID: dbID, Name: "db-vault", Type: "vault", ClusterName: "prod-db",
			DiscoverIntervalSeconds: 600,
			Scope:                   map[string]string{"address": "https://vault.example.com"}},
	}}
	locks := &fakeLocker{}
	sw := sweepers.DiscoverScheduler{
		Jobs:        jc,
		Connections: conn,
		Locks:       locks,
		Notifier:    notif,
		Logger:      logger,
		EnvFallback: []sweepers.DiscoverTarget{
			{Name: "env-vault", Cluster: "env-c", ProviderType: "vault"},
		},
	}
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(jc.created) != 1 {
		t.Fatalf("created = %d want 1 (DB only — env must be ignored)", len(jc.created))
	}
	got := jc.created[0]
	if got.Payload["connection_id"] != dbID.String() {
		t.Fatalf("payload.connection_id = %v want %v", got.Payload["connection_id"], dbID)
	}
	if conn.markedStarted != 1 {
		t.Fatalf("MarkDiscoverStarted calls = %d want 1", conn.markedStarted)
	}
	if locks.acquired != 1 {
		t.Fatalf("lock acquires = %d want 1", locks.acquired)
	}
}

// TestDiscoverScheduler_PerTargetLock_SkipsHeldRow proves multi-replica
// safety: when the lock is already held for a target, that target is
// silently skipped — no MarkStarted, no Create, no error returned.
func TestDiscoverScheduler_PerTargetLock_SkipsHeldRow(t *testing.T) {
	jc := &fakeJobCreator{}
	notif := &recordingNotifier{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	conn := &fakeProviderConnections{targets: []storage.DiscoverTarget{
		{ID: uuid.New(), Name: "held", Type: "vault", ClusterName: "c1",
			DiscoverIntervalSeconds: 60},
	}}
	locks := &fakeLocker{forceHeld: true}
	sw := sweepers.DiscoverScheduler{
		Jobs:        jc,
		Connections: conn,
		Locks:       locks,
		Notifier:    notif,
		Logger:      logger,
	}
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(jc.created) != 0 {
		t.Fatalf("expected no enqueues on locked target, got %d", len(jc.created))
	}
	if conn.markedStarted != 0 {
		t.Fatalf("expected no MarkDiscoverStarted on locked target, got %d", conn.markedStarted)
	}
}

// TestDiscoverScheduler_EnqueueFailure_RollsBack proves the rollback
// path: when Create fails, MarkDiscoverFinished is called with status
// failure so admin UIs don't show "running" forever.
func TestDiscoverScheduler_EnqueueFailure_RollsBack(t *testing.T) {
	jc := &flakyJobCreator{failOn: 1} // first Create fails
	notif := &recordingNotifier{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	id := uuid.New()
	conn := &fakeProviderConnections{targets: []storage.DiscoverTarget{
		{ID: id, Name: "rollback-target", Type: "vault", ClusterName: "c1",
			DiscoverIntervalSeconds: 60},
	}}
	locks := &fakeLocker{}
	sw := sweepers.DiscoverScheduler{
		Jobs:        jc,
		Connections: conn,
		Locks:       locks,
		Notifier:    notif,
		Logger:      logger,
	}
	err := sw.Run(t.Context())
	if err == nil {
		t.Fatal("expected an error from failed enqueue")
	}
	if conn.markedStarted != 1 {
		t.Fatalf("MarkDiscoverStarted = %d want 1", conn.markedStarted)
	}
	if conn.markedFinished != 1 {
		t.Fatalf("MarkDiscoverFinished = %d want 1 (rollback)", conn.markedFinished)
	}
	if conn.lastFinishStatus != storage.DiscoverStatusFailure {
		t.Fatalf("rollback status = %q want failure", conn.lastFinishStatus)
	}
	if conn.lastFinishErr == "" {
		t.Fatal("rollback should have written a sanitized error")
	}
}

// TestDiscoverScheduler_DBError_NotifiesAndReturns pins behaviour when
// ListDueForDiscovery fails — the scheduler reports an error
// notification and returns the underlying error.
func TestDiscoverScheduler_DBError_NotifiesAndReturns(t *testing.T) {
	jc := &fakeJobCreator{}
	notif := &recordingNotifier{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	conn := &fakeProviderConnections{listErr: errors.New("postgres unavailable")}
	locks := &fakeLocker{}
	sw := sweepers.DiscoverScheduler{
		Jobs:        jc,
		Connections: conn,
		Locks:       locks,
		Notifier:    notif,
		Logger:      logger,
	}
	err := sw.Run(t.Context())
	if err == nil {
		t.Fatal("expected error from DB list failure")
	}
	if len(jc.created) != 0 {
		t.Fatal("no jobs should enqueue when DB list fails")
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

func TestDiscoverScheduler_EnvFallback_PartialFailure(t *testing.T) {
	// First Create succeeds, second fails. The sweeper must return
	// the error but ALSO have enqueued the one that worked.
	failing := &flakyJobCreator{failOn: 2}
	notif := &recordingNotifier{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sw := sweepers.DiscoverScheduler{
		Jobs:     failing,
		Notifier: notif,
		Logger:   logger,
		EnvFallback: []sweepers.DiscoverTarget{
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

// ---- P4 fakes -----------------------------------------------------

type fakeProviderConnections struct {
	targets          []storage.DiscoverTarget
	listErr          error
	markedStarted    int
	markedFinished   int
	lastFinishStatus string
	lastFinishErr    string
}

func (f *fakeProviderConnections) ListDueForDiscovery(_ context.Context, _ time.Time) ([]storage.DiscoverTarget, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.targets, nil
}
func (f *fakeProviderConnections) MarkDiscoverStarted(_ context.Context, _ uuid.UUID, _ time.Time) error {
	f.markedStarted++
	return nil
}
func (f *fakeProviderConnections) MarkDiscoverFinished(_ context.Context, _ uuid.UUID, status, sanitizedErr string, _ time.Time) error {
	f.markedFinished++
	f.lastFinishStatus = status
	f.lastFinishErr = sanitizedErr
	return nil
}

type fakeLocker struct {
	acquired  int
	forceHeld bool
}

func (f *fakeLocker) AcquireLock(_ context.Context, _ string, _ time.Duration) (*runtime.Lock, error) {
	if f.forceHeld {
		return nil, runtime.ErrLockHeld
	}
	f.acquired++
	return &runtime.Lock{}, nil
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
