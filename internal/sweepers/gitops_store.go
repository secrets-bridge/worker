package sweepers

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/storage"
)

// GitOpsObservationStore is the observation repo the gitops sweepers
// run on: the api repository plus a worker-side terminal transition
// that reports whether it changed the row.
//
// The api's Transition is an idempotent no-op on a row that is already
// terminal and doesn't say which happened. The poller and the timeout
// sweeper can race on the same row, so the worker needs to know whether
// its own call made the flip before it writes an audit event for it.
type GitOpsObservationStore struct {
	*storage.GitOpsObservations
	DB pgxRowQuerier
}

// transitionTerminalSQL is the api's Transition statement plus
// RETURNING, so zero rows means "someone else already finished it".
const transitionTerminalSQL = `
	UPDATE gitops_observations
	SET polling_state = $2, terminal_at = $3
	WHERE id = $1 AND polling_state IN ('queued', 'active')
	RETURNING id`

// TransitionTerminal moves a queued/active observation to state and
// reports whether this call did it.
func (s GitOpsObservationStore) TransitionTerminal(ctx context.Context, id uuid.UUID, state storage.GitOpsObservationState, at time.Time) (bool, error) {
	rows, err := s.DB.Query(ctx, transitionTerminalSQL, id, state, at)
	if err != nil {
		return false, fmt.Errorf("sweepers: transition gitops observation: %w", err)
	}
	defer rows.Close()
	changed := rows.Next()
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("sweepers: transition gitops observation: %w", err)
	}
	return changed, nil
}

// auditGitOpsTransition writes the audit event for a terminal flip this
// replica made. reason is "argocd_status" (poller) or "timeout".
func auditGitOpsTransition(ctx context.Context, a auditAppender, sweeper string, o *storage.GitOpsObservation, state storage.GitOpsObservationState, reason string) {
	status := storage.AuditStatusSuccess
	if state == storage.GitOpsStateFailed {
		status = storage.AuditStatusFailure
	}
	recordAudit(ctx, a, sweeper, storage.AuditEvent{
		Action:        "gitops.observation." + string(state),
		Resource:      "gitops_observation:" + o.ID.String(),
		Status:        status,
		CorrelationID: o.RequestID,
		Metadata: map[string]any{
			"request_id": o.RequestID.String(),
			"state":      string(state),
			"reason":     reason,
		},
	})
}
