package sweepers

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/storage"

	"github.com/secrets-bridge/worker/internal/notifications"
)

// Gateway observation queue sweepers (worker#38, gateway control-plane
// design section 8). The queue itself lives in api/pkg/storage
// (observation_tasks.go); the two sweeps are storage calls that close
// rows in one UPDATE ... RETURNING and hand them back, so the sweeper
// only audits, counts and notifies.
//
// Both calls lock observation_tasks rows only (FOR UPDATE SKIP LOCKED),
// never the gateways row, so they cannot invert the api's lock order
// (gateway row first, then the task). A row the result path or a claim
// holds is skipped and picked up on a later tick.
//
// A retry after an error never duplicates a side effect: each batch is
// one statement that either commits, and then its rows no longer match,
// or does not. That is why neither sweeper returns retry.Permanent.

// observationTaskSweeps is the slice of *storage.ObservationTasks the
// sweepers use. Defined here so unit tests can plug a fake.
type observationTaskSweeps interface {
	ExpireLeases(ctx context.Context, now time.Time, limit int) ([]*storage.ObservationTask, error)
	ExpireDeadlines(ctx context.Context, now time.Time, requestID *uuid.UUID, limit int) ([]*storage.ObservationTask, error)
}

// DefaultObservationBatchSize is the per-statement limit when none is
// configured.
const DefaultObservationBatchSize = 200

// maxObservationBatches bounds the batches one tick runs, so a large
// backlog drains over a few ticks instead of holding the lock.
const maxObservationBatches = 10

// DBClock reads the database clock. The queue's leases, deadlines and
// not_before are written with the database clock (now()), so the sweeps
// judge them on the same clock instead of the worker's.
type DBClock struct {
	Pool pgxRowQuerier
}

// Now returns the database's now().
func (c DBClock) Now(ctx context.Context) (time.Time, error) {
	rows, err := c.Pool.Query(ctx, `SELECT now()`)
	if err != nil {
		return time.Time{}, err
	}
	defer rows.Close()
	var t time.Time
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return time.Time{}, err
		}
		return time.Time{}, fmt.Errorf("sweepers: database clock returned no row")
	}
	if err := rows.Scan(&t); err != nil {
		return time.Time{}, err
	}
	return t, rows.Err()
}

// clockFunc returns the time the sweep is judged at.
type clockFunc func(ctx context.Context) (time.Time, error)

func batchSize(n int) int {
	if n <= 0 {
		return DefaultObservationBatchSize
	}
	return n
}

// drain runs one sweep in batches until a batch comes back short, the
// batch cap is reached or a batch fails. It returns every closed task,
// including those of the batches before a failure, so they are still
// audited.
func drain(ctx context.Context, limit int, sweep func(ctx context.Context, limit int) ([]*storage.ObservationTask, error)) ([]*storage.ObservationTask, error) {
	var all []*storage.ObservationTask
	for i := 0; i < maxObservationBatches; i++ {
		got, err := sweep(ctx, limit)
		all = append(all, got...)
		if err != nil {
			return all, err
		}
		if len(got) < limit {
			break
		}
	}
	return all, nil
}

// taskAuditMetadata is the metadata of a task closed by a sweeper: the
// keys the api writes for the same actions (auditTaskClosed), ids,
// enums and counts only.
func taskAuditMetadata(t *storage.ObservationTask) map[string]any {
	return map[string]any{
		"task_id":        t.ID.String(),
		"request_id":     t.RequestID.String(),
		"gateway_id":     t.GatewayID.String(),
		"task_state":     string(t.State),
		"verdict":        string(t.Verdict),
		"verdict_reason": t.VerdictReason,
		"attempt_count":  t.AttemptCount,
		"failure_count":  t.FailureCount,
	}
}

func taskResource(t *storage.ObservationTask) string {
	return "observation_task:" + t.ID.String()
}

// ObservationLeases puts leased observation tasks whose lease ended
// back to queued and closes the attempt as lease_expired (design
// section 8, observation-lease-expiry). A late result, renew or release
// from that attempt is then refused by the api's fence. failure_count is
// not changed (design section 5.3): replica deaths and rolling restarts
// must not kill a healthy task; the deadline bounds the churn.
//
// Not needed for correctness, since a claim re-enters an expired lease
// inline; it keeps the state the UI shows honest.
type ObservationLeases struct {
	Store observationTaskSweeps
	// Audit receives one observation.task.lease_expired event per task.
	Audit    auditAppender
	Notifier notifications.Notifier
	// Clock is the database clock; tests may replace it.
	Clock     clockFunc
	BatchSize int
}

// Name returns the stable identifier for this sweeper (the lock name).
func (s ObservationLeases) Name() string { return "observation-leases" }

// Run expires the due leases, audits each task and notifies with the
// count.
func (s ObservationLeases) Run(ctx context.Context) error {
	now, err := s.Clock(ctx)
	if err != nil {
		return fmt.Errorf("sweepers: observation leases: read clock: %w", err)
	}
	tasks, err := drain(ctx, batchSize(s.BatchSize), func(ctx context.Context, limit int) ([]*storage.ObservationTask, error) {
		return s.Store.ExpireLeases(ctx, now, limit)
	})
	for _, t := range tasks {
		observationLeasesExpired.Inc()
		recordAudit(ctx, s.Audit, s.Name(), storage.AuditEvent{
			Action:        "observation.task.lease_expired",
			Resource:      taskResource(t),
			CorrelationID: t.CorrelationID,
			Metadata:      taskAuditMetadata(t),
		})
	}
	if n := len(tasks); n > 0 {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityInfo,
			Component: s.Name(),
			Title:     "observation task leases expired; tasks requeued",
			Metadata:  map[string]any{"requeued": int64(n)},
		})
	}
	if err != nil {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityError,
			Component: s.Name(),
			Title:     "expire observation task leases failed",
			Detail:    "error class " + auditErrorClass(err),
		})
		return fmt.Errorf("sweepers: observation leases: %w", err)
	}
	return nil
}

// ObservationDeadlines closes open observation tasks whose deadline
// passed: state done, verdict timed_out, reason deadline_reached or,
// when the Gateway was not seen since the task was created or is not
// active, gateway_unavailable (design sections 5.4 and 8). A task is
// never failed: timed out means "not observed in time", not a failed
// deployment.
type ObservationDeadlines struct {
	Store observationTaskSweeps
	// Audit receives one observation.task.timed_out event per task.
	Audit    auditAppender
	Notifier notifications.Notifier
	// Clock is the database clock; tests may replace it.
	Clock     clockFunc
	BatchSize int
}

// Name returns the stable identifier for this sweeper (the lock name).
func (s ObservationDeadlines) Name() string { return "observation-deadlines" }

// Run times out the due tasks, audits each and notifies with the count.
func (s ObservationDeadlines) Run(ctx context.Context) error {
	now, err := s.Clock(ctx)
	if err != nil {
		return fmt.Errorf("sweepers: observation deadlines: read clock: %w", err)
	}
	tasks, err := drain(ctx, batchSize(s.BatchSize), func(ctx context.Context, limit int) ([]*storage.ObservationTask, error) {
		return s.Store.ExpireDeadlines(ctx, now, nil, limit)
	})
	for _, t := range tasks {
		observationTasksTimedOut.WithLabelValues(t.VerdictReason).Inc()
		recordAudit(ctx, s.Audit, s.Name(), storage.AuditEvent{
			Action:        "observation.task.timed_out",
			Resource:      taskResource(t),
			CorrelationID: t.CorrelationID,
			Metadata:      taskAuditMetadata(t),
		})
	}
	if n := len(tasks); n > 0 {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityInfo,
			Component: s.Name(),
			Title:     "observation tasks timed out (not a failed deployment)",
			Metadata:  map[string]any{"timed_out": int64(n)},
		})
	}
	if err != nil {
		notify(ctx, s.Notifier, notifications.Event{
			Severity:  notifications.SeverityError,
			Component: s.Name(),
			Title:     "expire observation task deadlines failed",
			Detail:    "error class " + auditErrorClass(err),
		})
		return fmt.Errorf("sweepers: observation deadlines: %w", err)
	}
	return nil
}
