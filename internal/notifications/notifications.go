// Package notifications publishes worker events to external sinks.
// The worker issues structured Notify calls; the underlying transport
// (webhook, Slack, email, ...) is pluggable behind the Notifier
// interface so adding a new sink doesn't reshape worker logic.
//
// Severity model:
//   - Info: routine events worth surfacing (sweep ran, N rows touched)
//   - Warn: degraded but recoverable (a sweeper failed once; retry pending)
//   - Error: permanent or repeated failure that needs operator attention
//
// Sinks decide their own delivery semantics — webhook fires N
// requests, Slack posts to a channel, email goes through SMTP. The
// worker passes the same Event to every registered Notifier so
// fanout is the sink's choice, not the worker's.
package notifications

import (
	"context"
	"errors"
	"time"
)

// Severity is the worker's lens on event importance.
type Severity string

const (
	SeverityInfo  Severity = "info"
	SeverityWarn  Severity = "warn"
	SeverityError Severity = "error"
)

// Event is the structured payload notifications carry. Title is the
// short human-readable summary; Detail is the longer body. Component
// identifies which worker subsystem produced the event (sweeper name,
// scheduler, etc.). Metadata is a free-form bag for tags / IDs /
// counts; sinks MAY surface it as labels or skip it.
//
// Plaintext secrets MUST NEVER appear in Title, Detail, or Metadata.
// Notifications go to external sinks; treating them as a logging
// surface means honoring the same hard rule as audit + logs.
type Event struct {
	Time      time.Time
	Severity  Severity
	Component string
	Title     string
	Detail    string
	Metadata  map[string]any
}

// Notifier is the sink contract. Notify SHOULD return quickly; the
// worker calls it inline from sweepers so slow notifications would
// stall sweep cycles. Notifier implementations that need network
// retries should use the retry package internally.
type Notifier interface {
	Notify(ctx context.Context, event Event) error
	// Name identifies the sink for logging + telemetry.
	Name() string
}

// Fanout is a Notifier that dispatches to multiple downstream
// Notifiers. A single sink failure does NOT block the others — Fanout
// collects per-sink errors and returns them combined so the caller
// can decide whether any matter.
type Fanout struct {
	Sinks []Notifier
}

// Notify dispatches event to every sink. Errors are joined and
// returned together; a partial failure means some sinks delivered.
func (f *Fanout) Notify(ctx context.Context, event Event) error {
	var errs []error
	for _, s := range f.Sinks {
		if err := s.Notify(ctx, event); err != nil {
			errs = append(errs, sinkErr{sink: s.Name(), err: err})
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

// Name returns "fanout".
func (f *Fanout) Name() string { return "fanout" }

type sinkErr struct {
	sink string
	err  error
}

func (e sinkErr) Error() string { return "notifications: " + e.sink + ": " + e.err.Error() }
func (e sinkErr) Unwrap() error { return e.err }
