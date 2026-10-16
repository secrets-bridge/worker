package sweepers

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/secrets-bridge/api/pkg/storage"

	"github.com/secrets-bridge/worker/internal/notifications"
)

// AuditActor is the actor on every audit event the worker writes. It
// follows the api's system:<kind> convention (system:oidc,
// system:bootstrap, system:policy-engine); the audit UI renders it as
// "System (worker)".
const AuditActor = "system:worker"

// auditAppender is the slice of api/pkg/storage.AuditEvents the
// sweepers use. *storage.AuditEvents satisfies it.
type auditAppender interface {
	Append(ctx context.Context, evt *storage.AuditEvent) error
}

// recordAudit appends one worker audit event (hard rule 6, WRK-N-02).
//
// Metadata must hold ids, reasons and counts only: never values,
// tokens, wrap bodies, secret refs or free text from a provider. The
// sweeper name is added here.
//
// Best effort, like the api's own `_ = audit.Append(...)` calls: the
// state change it describes has already committed and a retry would not
// see the row again, so a failed append doesn't fail the sweep. It is
// logged (sweeper, action, error class) and counted in
// worker_sweeper_audit_failures_total so a broken audit path is
// visible. A nil appender is treated as a failure for the same reason.
func recordAudit(ctx context.Context, a auditAppender, sweeper string, evt storage.AuditEvent) {
	if a == nil {
		auditFailures.WithLabelValues(sweeper).Inc()
		slog.Default().Error("sweepers: audit event dropped, no audit sink wired",
			"sweeper", sweeper, "action", evt.Action)
		return
	}
	evt.Actor = AuditActor
	if evt.Status == "" {
		evt.Status = storage.AuditStatusSuccess
	}
	md := make(map[string]any, len(evt.Metadata)+1)
	for k, v := range evt.Metadata {
		md[k] = v
	}
	md["sweeper"] = sweeper
	evt.Metadata = md
	if err := a.Append(ctx, &evt); err != nil {
		auditFailures.WithLabelValues(sweeper).Inc()
		slog.Default().Error("sweepers: audit append failed",
			"sweeper", sweeper, "action", evt.Action, "error_class", auditErrorClass(err))
	}
}

// auditErrorClass labels an Append error without its text (a pg error
// detail can echo row contents). SQLSTATE codes are safe.
func auditErrorClass(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return "pg_" + pgErr.Code
	}
	return notifications.ErrorClass(err)
}
