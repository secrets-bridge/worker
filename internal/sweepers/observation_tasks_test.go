package sweepers_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/storage"
	"github.com/secrets-bridge/worker/internal/sweepers"
)

// fakeSweeps hands out batches in order; an error entry fails that call.
type fakeSweeps struct {
	batches []int
	failAt  int // 1-based call that fails; 0 never
	calls   int
	limits  []int
}

func (f *fakeSweeps) next(limit int) ([]*storage.ObservationTask, error) {
	f.calls++
	f.limits = append(f.limits, limit)
	if f.calls == f.failAt {
		return nil, errors.New("connection reset")
	}
	if f.calls > len(f.batches) {
		return nil, nil
	}
	out := make([]*storage.ObservationTask, f.batches[f.calls-1])
	for i := range out {
		out[i] = &storage.ObservationTask{ID: uuid.New(), RequestID: uuid.New(), GatewayID: uuid.New(), CorrelationID: uuid.New(),
			State: storage.ObservationTaskQueued, Verdict: storage.ObservationVerdictPending}
	}
	return out, nil
}

func (f *fakeSweeps) ExpireLeases(_ context.Context, _ time.Time, limit int) ([]*storage.ObservationTask, error) {
	return f.next(limit)
}

func (f *fakeSweeps) ExpireDeadlines(_ context.Context, _ time.Time, _ *uuid.UUID, limit int) ([]*storage.ObservationTask, error) {
	return f.next(limit)
}

type countingAudit struct{ n int }

func (c *countingAudit) Append(context.Context, *storage.AuditEvent) error { c.n++; return nil }

func fixedClock(context.Context) (time.Time, error) { return time.Unix(1_800_000_000, 0), nil }

// A full batch is followed by another until one comes back short.
func TestObservationLeases_DrainsFullBatches(t *testing.T) {
	store := &fakeSweeps{batches: []int{2, 2, 1}}
	audit := &countingAudit{}
	sw := sweepers.ObservationLeases{Store: store, Audit: audit, Notifier: &recordingNotifier{}, Clock: fixedClock, BatchSize: 2}
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if store.calls != 3 || audit.n != 5 {
		t.Fatalf("calls %d audits %d; want 3 and 5", store.calls, audit.n)
	}
}

// A failed batch fails the run, but the batches before it committed and
// are still audited, once.
func TestObservationDeadlines_FailedBatchStillAuditsEarlierOnes(t *testing.T) {
	store := &fakeSweeps{batches: []int{2, 2}, failAt: 2}
	audit := &countingAudit{}
	n := &recordingNotifier{}
	sw := sweepers.ObservationDeadlines{Store: store, Audit: audit, Notifier: n, Clock: fixedClock, BatchSize: 2}
	if err := sw.Run(t.Context()); err == nil {
		t.Fatal("Run succeeded; want the batch error")
	}
	if audit.n != 2 {
		t.Fatalf("audits = %d want 2", audit.n)
	}
	if len(n.titles()) != 2 {
		t.Fatalf("notifications = %v; want a count and an error", n.titles())
	}
}

// A clock error stops the run before any sweep.
func TestObservationLeases_ClockErrorSweepsNothing(t *testing.T) {
	store := &fakeSweeps{batches: []int{1}}
	sw := sweepers.ObservationLeases{Store: store, Audit: &countingAudit{}, Clock: func(context.Context) (time.Time, error) {
		return time.Time{}, errors.New("db down")
	}}
	if err := sw.Run(t.Context()); err == nil || store.calls != 0 {
		t.Fatalf("err %v calls %d; want an error and no sweep", err, store.calls)
	}
}
