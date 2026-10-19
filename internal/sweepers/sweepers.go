// Package sweepers implements the periodic worker tasks.
//
// Each sweeper:
//   - Owns one well-scoped invariant (e.g. "expired wraps don't sit
//     in the database").
//   - Reads + writes a small slice of api/pkg/storage. Where api's
//     repository surface doesn't expose the exact query a sweeper
//     needs, the sweeper falls back to raw SQL on the shared *pgxpool
//     — those queries are worker concerns and don't belong in api's
//     domain layer.
//   - Emits a notification on every run with a count + outcome so the
//     operator sees the sweep cadence in their telemetry pipe.
//   - Returns errors UNwrapped (no value bytes ever in error text);
//     the sweeper's audit footprint is the notification body, not
//     the value of any row it touched.
//
// Sweepers are values (not pointers) where possible — the
// scheduler.TaskRegistration captures them by value, and every field
// is read-only after construction. Mutability lives in the underlying
// repository or pool.
package sweepers

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/secrets-bridge/worker/internal/notifications"
)

// pgxRowQuerier is the slice of the pool the raw-SQL sweepers use
// (agents-stale, jobs-recovery and the worker-side stores). Every
// statement reads rows back (UPDATE ... RETURNING) so each flip can be
// audited. Defined here so tests can inject a fake without booting
// Postgres; *storage.Pool and *pgxpool.Pool satisfy it.
type pgxRowQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Pool is the worker-side alias for the pgxpool used by sweepers
// that fall back to raw SQL. The real type is *pgxpool.Pool from
// api/pkg/storage; we accept the pool because the queries those
// sweepers run don't belong in api's domain layer.
type Pool = pgxpool.Pool

// notify is shared by sweepers as a tiny wrapper to keep the
// boilerplate down. A notification failure never becomes the sweep's
// own outcome, but it is logged and counted: sweeper name, sink and
// an error class only. The error text can carry the webhook URL (and
// its secret path) and the event can carry ids, so neither is logged.
// WRK-N-07. It reports whether the event was delivered, for the
// sweepers that only move on once it was.
func notify(ctx context.Context, n notifications.Notifier, ev notifications.Event) bool {
	if n == nil {
		return false
	}
	if err := n.Notify(ctx, ev); err != nil {
		notifyFailures.WithLabelValues(ev.Component).Inc()
		slog.Default().Warn("sweepers: notification failed",
			"sweeper", ev.Component,
			"sink", n.Name(),
			"error_class", notifications.ErrorClass(err),
		)
		return false
	}
	return true
}
