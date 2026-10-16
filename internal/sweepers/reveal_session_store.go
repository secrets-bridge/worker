package sweepers

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// RevealSessionStore holds the worker-side SQL the reveal-sessions
// sweeper runs next to api/pkg/storage. The api has no query for
// "wraps of sessions that already ended", and it is a worker concern
// (same reasoning as agents-stale and jobs-recovery).
type RevealSessionStore struct {
	DB pgxRowQuerier
}

// clampEndedSessionWrapsSQL shortens, in one statement, every
// unconsumed and still reachable wrap that belongs to a reveal session
// which has ended, down to the moment that session ended.
//
// A session has ended when expired_at is set (TTL sweep, Hide Now,
// unmount) or when its TTL passed and no sweep has stamped it yet. It
// ended at LEAST(expires_at, expired_at): the TTL deadline for a TTL
// sweep, the click time for an early hide. When a wrap sits in more
// than one ended session the earliest end wins.
//
// It only ever moves expires_at down (w.expires_at > ended_at) and only
// touches wraps a retrieve could still open (unconsumed,
// expires_at > now), so a second run is a no-op.
//
// Cost: reveal_sessions is never pruned and has no index this query
// can use, so it is one sequential scan per tick. The live-wrap set is
// small (wraps-expired deletes the rest), and a session can only hold
// wraps that existed when it was opened, so sessions created before
// the oldest live wrap are filtered out before their wrap_ids are
// unnested. Measured on 200k ended sessions: ~50ms, versus ~1.5s
// without the prune.
const clampEndedSessionWrapsSQL = `
	WITH live AS (
		SELECT id, created_at
		FROM secret_wraps
		WHERE consumed_at IS NULL AND expires_at > $1
	), ended AS (
		SELECT u.wrap_id,
		       MIN(LEAST(rs.expires_at, COALESCE(rs.expired_at, rs.expires_at))) AS ended_at
		FROM reveal_sessions AS rs
		CROSS JOIN LATERAL unnest(rs.wrap_ids) AS u(wrap_id)
		JOIN live ON live.id = u.wrap_id
		WHERE (rs.expired_at IS NOT NULL OR rs.expires_at <= $1)
		  AND rs.created_at >= (SELECT min(created_at) FROM live)
		GROUP BY u.wrap_id
	)
	UPDATE secret_wraps AS w
	SET expires_at = ended.ended_at
	FROM ended
	WHERE w.id = ended.wrap_id
	  AND w.consumed_at IS NULL
	  AND w.expires_at > $1
	  AND w.expires_at > ended.ended_at
	RETURNING w.id`

// ClampEndedSessionWraps runs clampEndedSessionWrapsSQL and returns the
// ids of the wraps it shortened (normally none).
func (s RevealSessionStore) ClampEndedSessionWraps(ctx context.Context, now time.Time) ([]uuid.UUID, error) {
	rows, err := s.DB.Query(ctx, clampEndedSessionWrapsSQL, now)
	if err != nil {
		return nil, fmt.Errorf("clamp ended reveal session wraps: %w", err)
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan clamped wrap id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clamp ended reveal session wraps: %w", err)
	}
	return ids, nil
}

// RequestIDs maps each of the given reveal session ids to its
// access_request_id, so a session's audit event shares the request's
// correlation id. Sessions without a request are left out.
func (s RevealSessionStore) RequestIDs(ctx context.Context, sessionIDs []uuid.UUID) (map[uuid.UUID]uuid.UUID, error) {
	out := map[uuid.UUID]uuid.UUID{}
	if len(sessionIDs) == 0 {
		return out, nil
	}
	const q = `
		SELECT id, access_request_id
		FROM reveal_sessions
		WHERE id = ANY($1) AND access_request_id IS NOT NULL`
	rows, err := s.DB.Query(ctx, q, sessionIDs)
	if err != nil {
		return nil, fmt.Errorf("reveal session request ids: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var sid, rid uuid.UUID
		if err := rows.Scan(&sid, &rid); err != nil {
			return nil, fmt.Errorf("scan reveal session request id: %w", err)
		}
		out[sid] = rid
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reveal session request ids: %w", err)
	}
	return out, nil
}
