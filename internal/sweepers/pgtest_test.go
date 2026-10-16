package sweepers_test

// Postgres-backed tests. They apply the api's embedded migrations to
// TEST_DATABASE_URL (the same DSN the CI test job exports) and wipe
// the data tables before each test. When TEST_DATABASE_URL is unset
// they skip, so `go test ./...` still works on a laptop without
// Postgres.

import (
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/storage"
)

func testPool(t *testing.T) *storage.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping Postgres-backed sweeper test")
	}
	cfg := storage.Config{DSN: dsn, MaxConns: 5, ConnLifetime: 5 * time.Minute}
	ctx := t.Context()
	if err := storage.Migrate(ctx, cfg); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	pool, err := storage.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(pool.Close)

	// TRUNCATE skips the audit_events no-delete trigger (row-level
	// triggers don't fire), same wipe the api storage tests use.
	const truncate = `
		TRUNCATE TABLE
			audit_events,
			reveal_sessions,
			secret_wraps,
			gitops_observations,
			argocd_endpoints,
			sync_runs,
			sync_jobs,
			approvals,
			access_requests,
			secret_mappings,
			agents,
			provider_connections,
			environments,
			projects,
			team_members,
			teams,
			local_users
		RESTART IDENTITY CASCADE`
	if _, err := pool.Exec(ctx, truncate); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	// TRUNCATE projects CASCADE also wipes policy_rules (FK from
	// migration 0033); put the seeded match-all rule back.
	const reseedSystemPolicy = `
		INSERT INTO policy_rules (name, selector, workflow_id, priority, is_system)
		SELECT 'match-all (system default)', '{}'::jsonb,
		       (SELECT id FROM workflow_definitions WHERE name = 'standard'), 0, true
		WHERE NOT EXISTS (SELECT 1 FROM policy_rules WHERE name = 'match-all (system default)')`
	if _, err := pool.Exec(ctx, reseedSystemPolicy); err != nil {
		t.Fatalf("reseed match-all: %v", err)
	}
	return pool
}

// makeReadRequest inserts a plain read access_request and returns its
// id. Minimal shape: the columns the api's Create and the
// access_requests_target_fields_present CHECK require.
func makeReadRequest(t *testing.T, pool *storage.Pool) uuid.UUID {
	t.Helper()
	wf, err := storage.NewWorkflows(pool).GetDefault(t.Context())
	if err != nil {
		t.Fatalf("default workflow: %v", err)
	}
	req := &storage.AccessRequest{
		RequesterID:        "alice@example.com",
		Type:               storage.AccessRequestTypeRead,
		Justification:      "worker sweeper test",
		WorkflowID:         &wf.ID,
		TargetProviderType: "vault",
		TargetSecretRef:    "team-alpha/uat/db",
	}
	if err := storage.NewAccessRequests(pool).Create(t.Context(), req); err != nil {
		t.Fatalf("access request Create: %v", err)
	}
	return req.ID
}
