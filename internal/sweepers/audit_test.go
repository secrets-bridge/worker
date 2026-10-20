package sweepers_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/secrets-bridge/api/pkg/storage"
	"github.com/secrets-bridge/worker/internal/sweepers"
)

// recordingAudit is an in-memory audit sink.
type recordingAudit struct {
	mu     sync.Mutex
	events []storage.AuditEvent
	err    error
}

func (r *recordingAudit) Append(_ context.Context, evt *storage.AuditEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.events = append(r.events, *evt)
	return nil
}

func auditFailureCount(t *testing.T, reg *prometheus.Registry, sweeper string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "worker_sweeper_audit_failures_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "sweeper" && lp.GetValue() == sweeper {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// A failed audit append doesn't fail the sweep (the flip already
// committed), but it is logged with sweeper, action and SQLSTATE only,
// and counted.
func TestAudit_AppendFailureIsLoggedAndCounted(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(sweepers.Collectors()...)
	logs := captureDefaultLog(t)

	reqID := uuid.New()
	sw := sweepers.CrossTeamFillExpired{
		Repo: &fakeCrossTeamFillExpirer{swept: []storage.SweptFillExpired{
			{ID: reqID, RequesterID: "alice@example.com"},
		}},
		Audit: &recordingAudit{err: &pgconn.PgError{
			Code: "23514", Message: "check violation", Detail: "Failing row contains (detail-canary)",
		}},
		Notifier: &recordingNotifier{},
	}
	before := auditFailureCount(t, reg, sw.Name())
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := auditFailureCount(t, reg, sw.Name()) - before; got != 1 {
		t.Fatalf("audit failures counted = %v want 1", got)
	}
	var found map[string]any
	for _, l := range logs.lines(t) {
		if l["msg"] == "sweepers: audit append failed" {
			found = l
		}
	}
	if found == nil || found["sweeper"] != sw.Name() || found["action"] != "request.cross_team.expired" || found["error_class"] != "pg_23514" {
		t.Fatalf("log line = %v; logs:\n%s", found, logs.String())
	}
	for _, banned := range []string{"detail-canary", "alice@example.com", reqID.String()} {
		if strings.Contains(logs.String(), banned) {
			t.Fatalf("log leaked %q:\n%s", banned, logs.String())
		}
	}
}

// A sweeper wired without an audit sink doesn't drop events silently.
func TestAudit_MissingSinkIsLoud(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(sweepers.Collectors()...)
	logs := captureDefaultLog(t)
	sw := sweepers.CrossTeamFillExpired{
		Repo:     &fakeCrossTeamFillExpirer{swept: []storage.SweptFillExpired{{ID: uuid.New()}}},
		Notifier: &recordingNotifier{},
	}
	before := auditFailureCount(t, reg, sw.Name())
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := auditFailureCount(t, reg, sw.Name()) - before; got != 1 {
		t.Fatalf("audit failures counted = %v want 1", got)
	}
	if !strings.Contains(logs.String(), "no audit sink wired") {
		t.Fatalf("missing-sink log line absent:\n%s", logs.String())
	}
}
