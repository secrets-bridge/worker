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

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/secrets-bridge/worker/internal/notifications"
)

// pgxQuerier is the minimal slice of pgxpool used by sweepers that
// need raw SQL (agents-stale + jobs-recovery). Defined here so tests
// can inject a fake without booting Postgres; the real *pgxpool.Pool
// satisfies it.
type pgxQuerier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Pool is the worker-side alias for the pgxpool used by sweepers
// that fall back to raw SQL. The real type is *pgxpool.Pool from
// api/pkg/storage; we accept the pool because the queries those
// sweepers run don't belong in api's domain layer.
type Pool = pgxpool.Pool

// notify is shared by sweepers as a tiny wrapper to keep the
// boilerplate down. Returns nil — sweepers never bubble a
// notification failure as their own outcome (notification errors are
// logged + counted by the Fanout itself).
func notify(ctx context.Context, n notifications.Notifier, ev notifications.Event) {
	if n == nil {
		return
	}
	_ = n.Notify(ctx, ev)
}
