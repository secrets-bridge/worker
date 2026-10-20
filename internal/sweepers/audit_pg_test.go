package sweepers_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/storage"
	"github.com/secrets-bridge/worker/internal/sweepers"
)

// WRK-N-02: every state transition the worker makes writes an audit
// row (hard rule 6). These tests run each sweeper against Postgres and
// read audit_events back the way GET /audit would.

// auditMetadataKeys is everything a worker audit row may carry: ids,
// reasons and counts. Anything else is a test failure.
var auditMetadataKeys = map[string]bool{
	"sweeper": true, "reason": true, "request_id": true, "state": true,
	"job_type": true, "stale_after_seconds": true, "wrap_count": true,
	"wraps_expired": true, "wraps_already_closed": true, "wrap_errors": true,
	// gateway observation sweepers (worker#38)
	"task_id": true, "gateway_id": true, "task_state": true, "verdict": true,
	"verdict_reason": true, "attempt_count": true, "failure_count": true,
}

// auditRows returns every audit_events row, oldest first.
func auditRows(t *testing.T, pool *storage.Pool) []*storage.AuditEvent {
	t.Helper()
	rows, err := storage.NewAuditEvents(pool).Query(t.Context(), storage.AuditQuery{Limit: 1000})
	if err != nil {
		t.Fatalf("audit Query: %v", err)
	}
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
	return rows
}

// requireOneAudit returns the single row with the given action and
// resource, after checking actor, correlation and metadata shape.
func requireOneAudit(t *testing.T, rows []*storage.AuditEvent, action, resource string, correlation uuid.UUID) *storage.AuditEvent {
	t.Helper()
	var hits []*storage.AuditEvent
	for _, r := range rows {
		if r.Action == action && r.Resource == resource {
			hits = append(hits, r)
		}
	}
	if len(hits) != 1 {
		t.Fatalf("audit rows for %s %s = %d want 1 (all rows: %s)", action, resource, len(hits), dumpAudit(rows))
	}
	ev := hits[0]
	if ev.Actor != sweepers.AuditActor {
		t.Errorf("actor = %q want %q", ev.Actor, sweepers.AuditActor)
	}
	if correlation != uuid.Nil && ev.CorrelationID != correlation {
		t.Errorf("correlation_id = %s want %s", ev.CorrelationID, correlation)
	}
	for k := range ev.Metadata {
		if !auditMetadataKeys[k] {
			t.Errorf("metadata key %q is not an id, reason or count", k)
		}
	}
	return ev
}

// requireNoValues fails if any banned fixture value shows up anywhere in
// the audit rows.
func requireNoValues(t *testing.T, rows []*storage.AuditEvent, banned ...string) {
	t.Helper()
	raw := dumpAudit(rows)
	for _, b := range banned {
		if strings.Contains(raw, b) {
			t.Fatalf("audit leaked %q: %s", b, raw)
		}
	}
}

func dumpAudit(rows []*storage.AuditEvent) string {
	b, _ := json.Marshal(rows)
	return string(b)
}

// --- cross-team fill window ----------------------------------------

func makeExpiredCrossTeamRequest(t *testing.T, pool *storage.Pool) uuid.UUID {
	t.Helper()
	ctx := t.Context()
	projects := storage.NewProjects(pool)
	src := &storage.Project{Name: "ct-src"}
	dst := &storage.Project{Name: "ct-dst"}
	for _, p := range []*storage.Project{src, dst} {
		if err := projects.Create(ctx, p); err != nil {
			t.Fatalf("project Create: %v", err)
		}
	}
	team := &storage.Team{Name: "team-alpha"}
	if err := storage.NewTeams(pool).Create(ctx, team); err != nil {
		t.Fatalf("team Create: %v", err)
	}
	env := &storage.Environment{ProjectID: dst.ID, Name: "uat", Type: storage.EnvironmentTypeUAT, Kind: storage.EnvironmentKindNonProd}
	if err := storage.NewEnvironments(pool).Create(ctx, env); err != nil {
		t.Fatalf("env Create: %v", err)
	}
	var connID uuid.UUID
	if err := pool.QueryRow(ctx, `
		INSERT INTO provider_connections (name, type, auth_method, scope, status)
		VALUES ('tenant-a-uat-vault', 'vault', 'token', '{}'::jsonb, 'active') RETURNING id`).Scan(&connID); err != nil {
		t.Fatalf("provider connection insert: %v", err)
	}
	wf, err := storage.NewWorkflows(pool).GetDefault(ctx)
	if err != nil {
		t.Fatalf("default workflow: %v", err)
	}
	var ruleID uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT id FROM policy_rules WHERE is_system = true LIMIT 1`).Scan(&ruleID); err != nil {
		t.Fatalf("policy rule: %v", err)
	}
	secApproval, minApprovers := false, int16(1)
	expired := time.Now().Add(-time.Minute).UTC()
	req := &storage.AccessRequest{
		RequesterID:                     "alice@example.com",
		Type:                            storage.AccessRequestTypeCrossTeam,
		Justification:                   "justification-canary-31f",
		Status:                          storage.AccessRequestStatusPendingValues,
		WorkflowID:                      &wf.ID,
		TargetTeamID:                    &team.ID,
		TargetProjectID:                 &dst.ID,
		TargetEnvironmentID:             &env.ID,
		DestinationProviderConnectionID: &connID,
		DestinationSecretRef:            "team-alpha/uat/db",
		DestinationKeys:                 []string{"DB_PASSWORD"},
		FillExpiresAt:                   &expired,
		MatchedPolicyRuleID:             &ruleID,
		SnapRequiresSecurityApproval:    &secApproval,
		SnapMinApprovers:                &minApprovers,
		TargetScope:                     map[string]any{"project_id": src.ID.String(), "environment": "uat"},
	}
	if err := storage.NewAccessRequests(pool).CreateCrossTeam(ctx, req); err != nil {
		t.Fatalf("CreateCrossTeam: %v", err)
	}
	return req.ID
}

func TestAudit_PG_CrossTeamFillExpired(t *testing.T) {
	pool := testPool(t)
	reqID := makeExpiredCrossTeamRequest(t, pool)

	sw := sweepers.CrossTeamFillExpired{
		Repo:     storage.NewAccessRequests(pool),
		Audit:    storage.NewAuditEvents(pool),
		Notifier: &recordingNotifier{},
	}
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rows := auditRows(t, pool)
	ev := requireOneAudit(t, rows, "request.cross_team.expired", "request:"+reqID.String(), reqID)
	if ev.Metadata["reason"] != "fill_window_expired" {
		t.Errorf("metadata = %v", ev.Metadata)
	}
	requireNoValues(t, rows, "team-alpha/uat/db", "justification-canary", "alice@example.com", "DB_PASSWORD")

	// Re-run: nothing new flipped, nothing new audited.
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if n := len(auditRows(t, pool)); n != len(rows) {
		t.Fatalf("idle re-run wrote %d extra audit row(s)", n-len(rows))
	}
}

// --- reveal sessions ------------------------------------------------

func TestAudit_PG_RevealSessionsExpiredAndSelfHeal(t *testing.T) {
	f := newRevealFixture(t)
	ctx := t.Context()
	reqID := makeReadRequest(t, f.pool)

	consumed := f.wrap(t, time.Hour)
	if err := f.wraps.MarkConsumedByUser(ctx, consumed, "alice@example.com", time.Now()); err != nil {
		t.Fatalf("MarkConsumedByUser: %v", err)
	}
	live := f.wrap(t, time.Hour)
	s := &storage.RevealSession{
		UserID: "alice@example.com", ProjectID: f.project, EnvironmentID: f.envID,
		AccessRequestID: &reqID, TTLSeconds: 60,
		ExpiresAt: time.Now().Add(-time.Minute).UTC(), WrapIDs: []uuid.UUID{consumed, live},
	}
	if err := f.sessions.Create(ctx, s); err != nil {
		t.Fatalf("session Create: %v", err)
	}
	// A session an older build expired without shortening its wrap.
	legacyWrap := f.wrap(t, time.Hour)
	legacy := f.session(t, time.Now().Add(-10*time.Minute), legacyWrap)
	if err := f.sessions.MarkExpired(ctx, legacy, time.Now().Add(-9*time.Minute), storage.RevealSessionExpiredTTL); err != nil {
		t.Fatalf("MarkExpired: %v", err)
	}

	sw := sweepers.RevealSessionsExpired{
		Sessions: f.sessions,
		Wraps:    f.wraps,
		Store:    sweepers.RevealSessionStore{DB: f.pool},
		Audit:    storage.NewAuditEvents(f.pool),
		Notifier: &recordingNotifier{},
	}
	if err := sw.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rows := auditRows(t, f.pool)
	ev := requireOneAudit(t, rows, "reveal.session.expired", "reveal_session:"+s.ID.String(), reqID)
	want := map[string]any{"reason": "ttl", "wrap_count": float64(2), "wraps_expired": float64(1), "wraps_already_closed": float64(1), "wrap_errors": float64(0)}
	for k, v := range want {
		if ev.Metadata[k] != v {
			t.Errorf("metadata[%s] = %v want %v (all: %v)", k, ev.Metadata[k], v, ev.Metadata)
		}
	}
	healed := requireOneAudit(t, rows, "wrap.expiry_clamped", "wrap:"+legacyWrap.String(), uuid.Nil)
	if healed.Metadata["reason"] != "reveal_session_ended" {
		t.Errorf("heal metadata = %v", healed.Metadata)
	}
	requireNoValues(t, rows, "DB_PASSWORD", "alice@example.com")

	if err := sw.Run(ctx); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if n := len(auditRows(t, f.pool)); n != len(rows) {
		t.Fatalf("idle re-run wrote %d extra audit row(s)", n-len(rows))
	}
}

// --- jobs recovery --------------------------------------------------

func TestAudit_PG_JobsRecovery(t *testing.T) {
	pool := testPool(t)
	ctx := t.Context()
	jobs := storage.NewSyncJobs(pool)

	mk := func(claimExpires time.Time) *storage.SyncJob {
		j := &storage.SyncJob{
			JobType: storage.JobTypeDiscover,
			Payload: map[string]any{"target_provider_config": map[string]any{"address": "https://vault.example.com/payload-canary"}},
		}
		if err := jobs.Create(ctx, j); err != nil {
			t.Fatalf("job Create: %v", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE sync_jobs SET status = 'claimed', claim_expires_at = $2 WHERE id = $1`, j.ID, claimExpires); err != nil {
			t.Fatalf("claim job: %v", err)
		}
		return j
	}
	dead := mk(time.Now().Add(-time.Minute))
	mk(time.Now().Add(time.Minute)) // still within its claim

	sw := sweepers.JobsRecovery{Pool: pool, Audit: storage.NewAuditEvents(pool), Notifier: &recordingNotifier{}}
	if err := sw.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rows := auditRows(t, pool)
	if len(rows) != 1 {
		t.Fatalf("audit rows = %d want 1: %s", len(rows), dumpAudit(rows))
	}
	ev := requireOneAudit(t, rows, "job.expired", "job:"+dead.ID.String(), dead.CorrelationID)
	if ev.Metadata["job_type"] != "discover" || ev.Metadata["reason"] != "claim_expired" {
		t.Errorf("metadata = %v", ev.Metadata)
	}
	requireNoValues(t, rows, "payload-canary")
}

// --- agents stale ---------------------------------------------------

func TestAudit_PG_AgentsStale(t *testing.T) {
	pool := testPool(t)
	ctx := t.Context()
	var quiet, fresh uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO agents (name, status, secret_hash, last_seen_at) VALUES ('agent-name-canary', 'active', '\x00', now() - interval '1 hour') RETURNING id`).Scan(&quiet); err != nil {
		t.Fatalf("agent insert: %v", err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO agents (name, status, secret_hash, last_seen_at) VALUES ('agent-fresh', 'active', '\x00', now()) RETURNING id`).Scan(&fresh); err != nil {
		t.Fatalf("agent insert: %v", err)
	}

	sw := sweepers.AgentsStale{Pool: pool, StaleAfter: 5 * time.Minute, Audit: storage.NewAuditEvents(pool), Notifier: &recordingNotifier{}}
	if err := sw.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rows := auditRows(t, pool)
	if len(rows) != 1 {
		t.Fatalf("audit rows = %d want 1: %s", len(rows), dumpAudit(rows))
	}
	ev := requireOneAudit(t, rows, "agent.stale", "agent:"+quiet.String(), uuid.Nil)
	if ev.Metadata["reason"] != "heartbeat_missed" || ev.Metadata["stale_after_seconds"] != float64(300) {
		t.Errorf("metadata = %v", ev.Metadata)
	}
	requireNoValues(t, rows, "agent-name-canary", fresh.String())
}
