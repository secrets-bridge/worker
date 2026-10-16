package sweepers_test

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/secrets-bridge/api/pkg/storage"
	"github.com/secrets-bridge/worker/internal/sweepers"
)

func countJobs(t *testing.T, pool *storage.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM sync_jobs`).Scan(&n); err != nil {
		t.Fatalf("count sync_jobs: %v", err)
	}
	return n
}

// WRK-N-05: SB_DISCOVER_TARGETS_JSON is a fallback for deployments that
// have no DB-backed targets at all. A DB target that simply isn't due
// this tick must not let the env targets run.
func TestDiscoverScheduler_PG_EnvFallbackSkippedWhenDBTargetsExist(t *testing.T) {
	pool := testPool(t)
	ctx := t.Context()

	conns := storage.NewProviderConnections(pool)
	pc, err := conns.Create(ctx, storage.ProviderConnectionInput{
		Name:                    "tenant-a-uat-vault",
		Type:                    storage.ProviderConnectionTypeVault,
		AuthMethod:              "kubernetes",
		Scope:                   map[string]string{"address": "https://vault.example.com"},
		Status:                  storage.ProviderConnectionStatusActive,
		ClusterName:             "tenant-a-uat",
		DiscoverEnabled:         true,
		DiscoverIntervalSeconds: 3600,
	})
	if err != nil {
		t.Fatalf("provider connection Create: %v", err)
	}
	// Discovered a minute ago, so not due for another 59 minutes.
	if err := conns.MarkDiscoverFinished(ctx, pc.ID, storage.DiscoverStatusSuccess, "", time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("MarkDiscoverFinished: %v", err)
	}

	sw := sweepers.DiscoverScheduler{
		Jobs:        storage.NewSyncJobs(pool),
		Connections: sweepers.DiscoverConnections{ProviderConnections: conns, DB: pool},
		Locks:       &fakeLocker{},
		EnvFallback: []sweepers.DiscoverTarget{
			{Name: "legacy-vault", Cluster: "tenant-a-uat", ProviderType: "vault"},
		},
		Notifier: &recordingNotifier{},
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if err := sw.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := countJobs(t, pool); n != 0 {
		t.Fatalf("enqueued %d discover job(s) from SB_DISCOVER_TARGETS_JSON while a DB target exists; want 0", n)
	}
}

// The fallback still works for a deployment with no DB targets, and a
// connection that can't be discovered (discovery off) doesn't count.
func TestDiscoverScheduler_PG_EnvFallbackRunsWithoutDBTargets(t *testing.T) {
	pool := testPool(t)
	ctx := t.Context()

	conns := storage.NewProviderConnections(pool)
	if _, err := conns.Create(ctx, storage.ProviderConnectionInput{
		Name:            "tenant-a-uat-vault",
		Type:            storage.ProviderConnectionTypeVault,
		AuthMethod:      "kubernetes",
		Scope:           map[string]string{"address": "https://vault.example.com"},
		Status:          storage.ProviderConnectionStatusActive,
		ClusterName:     "tenant-a-uat",
		DiscoverEnabled: false,
		// Schema CHECK wants an interval even with discovery off.
		DiscoverIntervalSeconds: 3600,
	}); err != nil {
		t.Fatalf("provider connection Create: %v", err)
	}

	sw := sweepers.DiscoverScheduler{
		Jobs:        storage.NewSyncJobs(pool),
		Connections: sweepers.DiscoverConnections{ProviderConnections: conns, DB: pool},
		Locks:       &fakeLocker{},
		EnvFallback: []sweepers.DiscoverTarget{
			{Name: "legacy-vault", Cluster: "tenant-a-uat", ProviderType: "vault"},
		},
		Notifier: &recordingNotifier{},
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if err := sw.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := countJobs(t, pool); n != 1 {
		t.Fatalf("enqueued %d job(s) want 1 from the env fallback", n)
	}
}
