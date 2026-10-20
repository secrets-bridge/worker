package sweepers_test

// Gateway observation sweepers (worker#38) against Postgres, and the
// two-replica lock against Redis. Fixtures go through the api's storage
// calls (CreateTask, Claim, Renew, Release, RecordHeartbeat) so the
// tests exercise the same fences the api uses.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/runtime"
	"github.com/secrets-bridge/api/pkg/storage"

	"github.com/secrets-bridge/worker/internal/retry"
	"github.com/secrets-bridge/worker/internal/scheduler"
	"github.com/secrets-bridge/worker/internal/sweepers"
)

const testContract = "1"

type gwFixture struct {
	pool   *storage.Pool
	tasks  *storage.ObservationTasks
	envID  uuid.UUID
	connID uuid.UUID
	n      int
}

type testGateway struct {
	id      uuid.UUID
	binding uuid.UUID
}

func newGatewayFixture(t *testing.T) *gwFixture {
	t.Helper()
	pool := testPool(t)
	ctx := t.Context()
	p := &storage.Project{Name: "gw-sweeper-" + uuid.NewString()[:8]}
	if err := storage.NewProjects(pool).Create(ctx, p); err != nil {
		t.Fatalf("project Create: %v", err)
	}
	env := &storage.Environment{ProjectID: p.ID, Name: "uat", Type: storage.EnvironmentTypeUAT}
	if err := storage.NewEnvironments(pool).Create(ctx, env); err != nil {
		t.Fatalf("env Create: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE environments SET observation_source = 'gateway' WHERE id = $1`, env.ID); err != nil {
		t.Fatalf("observation source: %v", err)
	}
	f := &gwFixture{pool: pool, tasks: storage.NewObservationTasks(pool), envID: env.ID}
	if err := pool.QueryRow(ctx, `
		INSERT INTO provider_connections (name, type, auth_method, scope, status)
		VALUES ($1, 'vault', 'token', '{}'::jsonb, 'active') RETURNING id`,
		"conn-"+uuid.NewString()[:8]).Scan(&f.connID); err != nil {
		t.Fatalf("connection: %v", err)
	}
	return f
}

// gateway inserts an enrolled Gateway in status with the given
// last_seen_at offset (nil: never seen), plus an approved Argo CD
// connector and an enabled binding.
func (f *gwFixture) gateway(t *testing.T, status string, lastSeenAgo *time.Duration, enrolledAgo time.Duration) testGateway {
	t.Helper()
	ctx := t.Context()
	var g testGateway
	var lastSeen any
	if lastSeenAgo != nil {
		lastSeen = time.Now().Add(-*lastSeenAgo)
	}
	var secret any = []byte{1}
	var revokedAt any
	switch status {
	case "pending":
		secret = nil
	case "revoked":
		revokedAt = time.Now()
	}
	if err := f.pool.QueryRow(ctx, `
		INSERT INTO gateways (name, boundary, secret_hash, status, enrolled_at, last_seen_at, contract_versions, revoked_at, created_by)
		VALUES ($1, 'tenant-a', $2, $3, $4, $5, '["1"]', $6, 'admin') RETURNING id`,
		"gw-"+uuid.NewString()[:8], secret, status, time.Now().Add(-enrolledAgo), lastSeen, revokedAt).Scan(&g.id); err != nil {
		t.Fatalf("gateway insert: %v", err)
	}
	var conn uuid.UUID
	if err := f.pool.QueryRow(ctx, `
		INSERT INTO gateway_connectors (gateway_id, name, kind, status, advertised_targets)
		VALUES ($1, 'argocd-main', 'argocd', 'approved', '[{"target_id":"app-a","stale":false}]') RETURNING id`,
		g.id).Scan(&conn); err != nil {
		t.Fatalf("connector insert: %v", err)
	}
	if err := f.pool.QueryRow(ctx, `
		INSERT INTO gateway_connector_bindings (gateway_id, environment_id, provider_connection_id, secret_ref,
			expected_stages, argocd_connector_id, argocd_target_id, created_by)
		VALUES ($1, $2, $3, 'team-alpha/uat/db', '["argocd_context"]', $4, 'app-a', 'admin') RETURNING id`,
		g.id, f.envID, f.connID, conn).Scan(&g.binding); err != nil {
		t.Fatalf("binding insert: %v", err)
	}
	return g
}

func ago(d time.Duration) *time.Duration { return &d }

// task creates a queued task for g with the given deadline. Every task
// gets its own secret scope, so creating one never supersedes another.
func (f *gwFixture) task(t *testing.T, g testGateway, deadline time.Time) *storage.ObservationTask {
	t.Helper()
	f.n++
	ref := fmt.Sprintf("team-alpha/uat/db-%d", f.n)
	now := time.Now()
	tk := &storage.ObservationTask{
		RequestID:            makeReadRequest(t, f.pool),
		CorrelationID:        uuid.New(),
		GatewayID:            g.id,
		BindingID:            g.binding,
		EnvironmentID:        f.envID,
		ConnectorRefs:        []storage.ObservationConnectorRef{{ConnectorID: "argocd-main", TargetID: "app-a", Stage: "argocd_context"}},
		ExpectedStages:       []string{"argocd_context"},
		ProviderType:         "vault",
		SecretScopeKey:       f.connID.String() + ":" + ref,
		SecretRef:            ref,
		ProviderConnectionID: &f.connID,
		VersionSource:        "none",
		ExecutedAt:           now.Add(-time.Minute),
		NotBefore:            now.Add(-time.Second),
		DeadlineAt:           deadline,
	}
	created, _, err := f.tasks.CreateTask(t.Context(), tk, now.Add(-time.Minute))
	if err != nil || !created {
		t.Fatalf("CreateTask = %v, %v", created, err)
	}
	return tk
}

// claim leases one task of g to a fresh replica.
func (f *gwFixture) claim(t *testing.T, g testGateway) *storage.ObservationClaim {
	t.Helper()
	c, err := f.tasks.Claim(t.Context(), g.id, uuid.New(), 30*time.Second, 10, testContract)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	return c
}

// expireLease moves a leased task's lease into the past, as if the
// replica died without renewing.
func (f *gwFixture) expireLease(t *testing.T, taskID uuid.UUID) {
	t.Helper()
	if _, err := f.pool.Exec(t.Context(), `
		UPDATE observation_tasks SET lease_expires_at = now() - interval '1 second' WHERE id = $1 AND state = 'leased'`, taskID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
}

func (f *gwFixture) get(t *testing.T, id uuid.UUID) *storage.ObservationTask {
	t.Helper()
	tk, err := f.tasks.Get(t.Context(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	return tk
}

func (f *gwFixture) attemptOutcome(t *testing.T, attemptID uuid.UUID) string {
	t.Helper()
	var outcome *string
	if err := f.pool.QueryRow(t.Context(), `SELECT outcome FROM observation_task_attempts WHERE attempt_id = $1`, attemptID).Scan(&outcome); err != nil {
		t.Fatalf("attempt: %v", err)
	}
	if outcome == nil {
		return ""
	}
	return *outcome
}

func leasesSweeper(f *gwFixture, store interface {
	ExpireLeases(ctx context.Context, now time.Time, limit int) ([]*storage.ObservationTask, error)
	ExpireDeadlines(ctx context.Context, now time.Time, requestID *uuid.UUID, limit int) ([]*storage.ObservationTask, error)
}) sweepers.ObservationLeases {
	return sweepers.ObservationLeases{
		Store: store, Audit: storage.NewAuditEvents(f.pool), Notifier: &recordingNotifier{},
		Clock: sweepers.DBClock{Pool: f.pool}.Now,
	}
}

func requireCleanTaskAudit(t *testing.T, ev *storage.AuditEvent, task *storage.ObservationTask) {
	t.Helper()
	if _, ok := ev.Metadata["_dropped_keys"]; ok {
		t.Errorf("api allowlist dropped keys: %v", ev.Metadata)
	}
	if _, ok := ev.Metadata["_rejected_keys"]; ok {
		t.Errorf("api allowlist rejected keys: %v", ev.Metadata)
	}
	if ev.Metadata["task_id"] != task.ID.String() || ev.Metadata["gateway_id"] != task.GatewayID.String() ||
		ev.Metadata["request_id"] != task.RequestID.String() {
		t.Errorf("metadata ids = %v", ev.Metadata)
	}
}

// Lease expiry requeues the task with no attempt and an unchanged
// failure_count (design 5.3), closes the attempt as lease_expired, and
// the api's fence then refuses the dead attempt's renew, release and
// result. A live lease is left alone.
func TestGateway_PG_LeaseExpiryRequeuesAndFencesLateResult(t *testing.T) {
	f := newGatewayFixture(t)
	ctx := t.Context()
	g := f.gateway(t, "active", ago(0), time.Hour)
	tk := f.task(t, g, time.Now().Add(time.Hour))
	c := f.claim(t, g)
	attemptA := c.Attempt.AttemptID
	sw := leasesSweeper(f, f.tasks)

	// Live lease: nothing happens.
	if err := sw.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := f.get(t, tk.ID); got.State != storage.ObservationTaskLeased || got.CurrentAttemptID == nil || *got.CurrentAttemptID != attemptA {
		t.Fatalf("live lease touched: state %s attempt %v", got.State, got.CurrentAttemptID)
	}

	f.expireLease(t, tk.ID)
	if _, err := f.tasks.Renew(ctx, g.id, tk.ID, attemptA, 30*time.Second); !errors.Is(err, storage.ErrObservationLeaseExpired) {
		t.Fatalf("Renew before the sweep = %v; want ErrObservationLeaseExpired", err)
	}
	if err := sw.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := f.get(t, tk.ID)
	if got.State != storage.ObservationTaskQueued || got.CurrentAttemptID != nil || got.LeaseExpiresAt != nil || got.LeasedReplicaID != nil {
		t.Fatalf("after sweep: state %s attempt %v lease %v replica %v; want queued with no attempt", got.State, got.CurrentAttemptID, got.LeaseExpiresAt, got.LeasedReplicaID)
	}
	if got.FailureCount != 0 || got.Verdict != storage.ObservationVerdictPending || got.AttemptCount != 1 {
		t.Fatalf("after sweep: failure_count %d verdict %s attempt_count %d; want 0, pending, 1", got.FailureCount, got.Verdict, got.AttemptCount)
	}
	if o := f.attemptOutcome(t, attemptA); o != storage.ObservationAttemptLeaseExpired {
		t.Fatalf("attempt outcome = %q want lease_expired", o)
	}

	// The late attempt is fenced out on every path.
	if _, err := f.tasks.Renew(ctx, g.id, tk.ID, attemptA, 30*time.Second); !errors.Is(err, storage.ErrObservationAttemptSuperseded) {
		t.Fatalf("late Renew = %v; want ErrObservationAttemptSuperseded", err)
	}
	if _, _, err := f.tasks.Release(ctx, g.id, tk.ID, attemptA, "shutdown"); !errors.Is(err, storage.ErrObservationAttemptSuperseded) {
		t.Fatalf("late Release = %v; want ErrObservationAttemptSuperseded", err)
	}
	rt, err := f.tasks.BeginResult(ctx, g.id, tk.ID)
	if err != nil {
		t.Fatalf("BeginResult: %v", err)
	}
	// The result path's attempt check (design 4.8 step 4) needs state
	// leased and current_attempt_id = body.attempt_id; neither holds, so
	// the late result is refused as attempt_superseded.
	if rt.Task.State == storage.ObservationTaskLeased || rt.Task.CurrentAttemptID != nil {
		t.Fatalf("late result sees state %s, current attempt %v; want queued with no attempt", rt.Task.State, rt.Task.CurrentAttemptID)
	}
	rt.Rollback(ctx)

	// Reissue: the next claim gets a new attempt and nothing to reclaim.
	c2 := f.claim(t, g)
	if c2.Attempt.AttemptID == attemptA || c2.Reclaimed != nil || c2.Task.AttemptCount != 2 {
		t.Fatalf("reclaim: attempt %s (old %s) reclaimed %v count %d", c2.Attempt.AttemptID, attemptA, c2.Reclaimed, c2.Task.AttemptCount)
	}

	rows := auditRows(t, f.pool)
	ev := requireOneAudit(t, rows, "observation.task.lease_expired", "observation_task:"+tk.ID.String(), tk.CorrelationID)
	requireCleanTaskAudit(t, ev, tk)
	if ev.Metadata["task_state"] != "queued" || ev.Metadata["failure_count"] != float64(0) || ev.Metadata["sweeper"] != "observation-leases" {
		t.Errorf("metadata = %v", ev.Metadata)
	}
	requireNoValues(t, rows, "team-alpha/uat/db", "argocd-main")
}

// Past the deadline a task is done / timed_out, never dead or failed:
// deadline_reached while its Gateway is active and seen, and
// gateway_unavailable when the Gateway is stale. Leased tasks close
// their attempt as deadline. Tasks before their deadline are untouched.
func TestGateway_PG_DeadlineTimesOutNeverFails(t *testing.T) {
	f := newGatewayFixture(t)
	ctx := t.Context()
	live := f.gateway(t, "active", ago(0), time.Hour)
	quiet := f.gateway(t, "stale", ago(time.Hour), 2*time.Hour)

	queued := f.task(t, live, time.Now().Add(-time.Second))
	leased := f.task(t, live, time.Now().Add(time.Hour))
	c := f.claim(t, live)
	if c.Task.ID != leased.ID {
		t.Fatalf("claimed %s want %s", c.Task.ID, leased.ID)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE observation_tasks SET deadline_at = now() - interval '1 second', lease_expires_at = now() - interval '1 second' WHERE id = $1`, leased.ID); err != nil {
		t.Fatal(err)
	}
	future := f.task(t, live, time.Now().Add(time.Hour))
	offline := f.task(t, quiet, time.Now().Add(-time.Second))

	sw := sweepers.ObservationDeadlines{
		Store: f.tasks, Audit: storage.NewAuditEvents(f.pool), Notifier: &recordingNotifier{},
		Clock: sweepers.DBClock{Pool: f.pool}.Now,
	}
	if err := sw.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, c := range []struct {
		tk     *storage.ObservationTask
		reason string
	}{
		{queued, storage.ObservationReasonDeadlineReached},
		{leased, storage.ObservationReasonDeadlineReached},
		{offline, storage.ObservationReasonGatewayUnavailable},
	} {
		got := f.get(t, c.tk.ID)
		if got.State != storage.ObservationTaskDone || got.Verdict != storage.ObservationVerdictTimedOut || got.VerdictReason != c.reason {
			t.Errorf("task %s: state %s verdict %s reason %s; want done timed_out %s", c.tk.ID, got.State, got.Verdict, got.VerdictReason, c.reason)
		}
		if got.TerminalAt == nil || got.CurrentAttemptID != nil || got.FailureCount != 0 {
			t.Errorf("task %s: terminal %v attempt %v failure_count %d", c.tk.ID, got.TerminalAt, got.CurrentAttemptID, got.FailureCount)
		}
		ev := requireOneAudit(t, auditRows(t, f.pool), "observation.task.timed_out", "observation_task:"+c.tk.ID.String(), c.tk.CorrelationID)
		requireCleanTaskAudit(t, ev, c.tk)
		if ev.Metadata["verdict"] != "timed_out" || ev.Metadata["verdict_reason"] != c.reason || ev.Metadata["task_state"] != "done" {
			t.Errorf("task %s audit metadata = %v", c.tk.ID, ev.Metadata)
		}
	}
	if o := f.attemptOutcome(t, c.Attempt.AttemptID); o != storage.ObservationAttemptDeadline {
		t.Errorf("leased task's attempt outcome = %q want deadline", o)
	}
	if got := f.get(t, future.ID); got.State != storage.ObservationTaskQueued || got.Verdict != storage.ObservationVerdictPending {
		t.Errorf("future task: state %s verdict %s; want untouched", got.State, got.Verdict)
	}
	var failed int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM observation_tasks WHERE state = 'dead' OR verdict = 'unverified'`).Scan(&failed); err != nil {
		t.Fatal(err)
	}
	if failed != 0 {
		t.Errorf("%d task(s) dead or unverified; a deadline never fails a task", failed)
	}

	// Idempotent: a second run closes nothing and audits nothing.
	before := len(auditRows(t, f.pool))
	if err := sw.Run(ctx); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if after := len(auditRows(t, f.pool)); after != before {
		t.Errorf("second run wrote %d audit rows", after-before)
	}
}

// An active Gateway goes stale only once it has been quiet past the
// cutoff (a Gateway that never sent a heartbeat is judged on
// enrolled_at); pending, disabled and revoked rows are never touched,
// and the api's next heartbeat flips it back to active.
func TestGateway_PG_StaleAfterMissedHeartbeatsAndNotBefore(t *testing.T) {
	f := newGatewayFixture(t)
	ctx := t.Context()
	fresh := f.gateway(t, "active", ago(90*time.Second), time.Hour)
	quiet := f.gateway(t, "active", ago(3*time.Minute), time.Hour)
	neverSeen := f.gateway(t, "active", nil, 10*time.Minute)
	justEnrolled := f.gateway(t, "active", nil, 30*time.Second)
	pending := f.gateway(t, "pending", nil, time.Hour)
	disabled := f.gateway(t, "disabled", ago(time.Hour), 2*time.Hour)
	revoked := f.gateway(t, "revoked", ago(time.Hour), 2*time.Hour)

	sw := sweepers.GatewaysStale{Pool: f.pool, StaleAfter: 2 * time.Minute, Audit: storage.NewAuditEvents(f.pool), Notifier: &recordingNotifier{}}
	if err := sw.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	status := func(g testGateway) string {
		var s string
		if err := f.pool.QueryRow(ctx, `SELECT status FROM gateways WHERE id = $1`, g.id).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	for g, want := range map[testGateway]string{
		fresh: "active", quiet: "stale", neverSeen: "stale", justEnrolled: "active",
		pending: "pending", disabled: "disabled", revoked: "revoked",
	} {
		if got := status(g); got != want {
			t.Errorf("gateway %s status = %s want %s", g.id, got, want)
		}
	}
	rows := auditRows(t, f.pool)
	if len(rows) != 2 {
		t.Fatalf("audit rows = %d want 2: %s", len(rows), dumpAudit(rows))
	}
	for _, g := range []testGateway{quiet, neverSeen} {
		ev := requireOneAudit(t, rows, "gateway.stale", "gateway:"+g.id.String(), uuid.Nil)
		if ev.Metadata["gateway_id"] != g.id.String() || ev.Metadata["reason"] != "heartbeat_missed" || ev.Metadata["stale_after_seconds"] != float64(120) {
			t.Errorf("metadata = %v", ev.Metadata)
		}
		if _, ok := ev.Metadata["_dropped_keys"]; ok {
			t.Errorf("api allowlist dropped keys: %v", ev.Metadata)
		}
	}

	// Second run: nothing new.
	if err := sw.Run(ctx); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if n := len(auditRows(t, f.pool)); n != 2 {
		t.Errorf("second run audited again: %d rows", n)
	}

	// The api flips a stale Gateway back on its next heartbeat.
	if _, err := storage.NewGateways(f.pool).RecordHeartbeat(ctx, storage.GatewayHeartbeat{
		GatewayID: quiet.id, ReplicaID: uuid.New(), GatewayVersion: "0.1.0", ContractVersions: []string{testContract}, Status: "ok",
		Connectors: []storage.GatewayConnectorHealth{},
	}, time.Now(), time.Now().Add(-10*time.Minute)); err != nil {
		t.Fatalf("RecordHeartbeat: %v", err)
	}
	if got := status(quiet); got != "active" {
		t.Fatalf("after heartbeat status = %s want active", got)
	}
}

// Replica rows older than the retention are pruned, fresher ones kept.
func TestGateway_PG_ReplicasPrune(t *testing.T) {
	f := newGatewayFixture(t)
	ctx := t.Context()
	g := f.gateway(t, "active", ago(0), time.Hour)
	old, fresh := uuid.New(), uuid.New()
	if _, err := f.pool.Exec(ctx, `
		INSERT INTO gateway_replicas (gateway_id, replica_id, first_seen_at, last_seen_at)
		VALUES ($1, $2, now() - interval '3 hours', now() - interval '2 hours'), ($1, $3, now(), now())`, g.id, old, fresh); err != nil {
		t.Fatal(err)
	}
	if err := (sweepers.GatewayReplicasPrune{Pool: f.pool, Retention: time.Hour}).Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var left []uuid.UUID
	rows, err := f.pool.Query(ctx, `SELECT replica_id FROM gateway_replicas WHERE gateway_id = $1`, g.id)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		left = append(left, id)
	}
	rows.Close()
	if len(left) != 1 || left[0] != fresh {
		t.Fatalf("replicas left = %v want only %s", left, fresh)
	}
}

// gatedStore counts ExpireLeases calls and holds the first one until
// released, so a second replica's tick lands while the first is running.
type gatedStore struct {
	*storage.ObservationTasks
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gatedStore) ExpireLeases(ctx context.Context, now time.Time, limit int) ([]*storage.ObservationTask, error) {
	if g.calls.Add(1) == 1 {
		g.once.Do(func() { close(g.entered) })
		<-g.release
	}
	return g.ObservationTasks.ExpireLeases(ctx, now, limit)
}

// Two worker replicas tick the same sweeper at the same time: the Redis
// lock lets exactly one run, so each expired lease is processed and
// audited once.
func TestGateway_PG_TwoReplicasNeverDoubleProcess(t *testing.T) {
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("TEST_REDIS_URL not set; skipping Redis-backed lock test")
	}
	f := newGatewayFixture(t)
	ctx := t.Context()
	g := f.gateway(t, "active", ago(0), time.Hour)
	var ids []uuid.UUID
	for i := 0; i < 3; i++ {
		ids = append(ids, f.task(t, g, time.Now().Add(time.Hour)).ID)
	}
	// Lease all three before expiring any: a claim re-enters an expired
	// lease inline.
	for range ids {
		f.claim(t, g)
	}
	for _, id := range ids {
		f.expireLease(t, id)
	}

	ns := "sbtest-worker-" + uuid.NewString()[:8]
	open := func() *runtime.Client {
		rt, err := runtime.Open(ctx, runtime.Config{URL: url, Namespace: ns})
		if err != nil {
			t.Fatalf("runtime.Open: %v", err)
		}
		t.Cleanup(func() { _ = rt.Close() })
		return rt
	}
	store := &gatedStore{ObservationTasks: f.tasks, entered: make(chan struct{}), release: make(chan struct{})}
	reg := scheduler.TaskRegistration{
		Task:     leasesSweeper(f, store),
		Interval: time.Hour,
		Retry:    retry.Policy{MaxAttempts: 1},
	}
	replicaA := scheduler.New(open(), slog.Default(), nil)
	replicaB := scheduler.New(open(), slog.Default(), nil)
	replicaA.Register(reg)
	replicaB.Register(reg)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = replicaA.TickForTest(context.Background(), "observation-leases")
	}()
	select {
	case <-store.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("replica A never started the sweep")
	}
	bDone := make(chan struct{})
	go func() {
		defer close(bDone)
		_ = replicaB.TickForTest(context.Background(), "observation-leases")
	}()
	select {
	case <-bDone:
	case <-time.After(3 * time.Second):
		// B is running the sweep too (no lock): let both finish so the
		// assertions below report it.
	}
	close(store.release)
	<-done
	<-bDone

	if n := store.calls.Load(); n != 1 {
		t.Fatalf("sweep ran %d times across two replicas; want 1", n)
	}
	rows := auditRows(t, f.pool)
	for _, id := range ids {
		requireOneAudit(t, rows, "observation.task.lease_expired", "observation_task:"+id.String(), uuid.Nil)
		if got := f.get(t, id); got.State != storage.ObservationTaskQueued {
			t.Errorf("task %s state %s want queued", id, got.State)
		}
	}
}
