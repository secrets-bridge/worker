package sweepers_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/storage"
	"github.com/secrets-bridge/worker/internal/retry"
	"github.com/secrets-bridge/worker/internal/sweepers"
)

// revealFixture is one project + environment to hang reveal sessions
// and wraps off.
type revealFixture struct {
	pool     *storage.Pool
	envID    uuid.UUID
	project  uuid.UUID
	wraps    *storage.SecretWraps
	sessions *storage.RevealSessions
}

func newRevealFixture(t *testing.T) *revealFixture {
	t.Helper()
	pool := testPool(t)
	ctx := t.Context()
	p := &storage.Project{Name: "reveal-sweeper-" + uuid.NewString()[:8]}
	if err := storage.NewProjects(pool).Create(ctx, p); err != nil {
		t.Fatalf("project Create: %v", err)
	}
	env := &storage.Environment{ProjectID: p.ID, Name: "uat", Type: storage.EnvironmentTypeUAT}
	if err := storage.NewEnvironments(pool).Create(ctx, env); err != nil {
		t.Fatalf("env Create: %v", err)
	}
	return &revealFixture{
		pool: pool, envID: env.ID, project: p.ID,
		wraps:    storage.NewSecretWraps(pool),
		sessions: storage.NewRevealSessions(pool),
	}
}

// wrap creates an unconsumed wrap that expires in ttl. The ciphertext
// fields are fake bytes; nothing here is ever decrypted.
func (f *revealFixture) wrap(t *testing.T, ttl time.Duration) uuid.UUID {
	t.Helper()
	w := &storage.SecretWrap{
		EnvironmentID:     &f.envID,
		IssuedVia:         storage.WrapIssuedViaDirectReveal,
		KeyName:           "DB_PASSWORD",
		EncryptedValue:    []byte("ct"),
		Nonce:             []byte("nonce"),
		DataKeyCiphertext: []byte("dek"),
		KMSKeyID:          "local:test",
		ContentHash:       []byte("hash"),
		ByteLength:        8,
		ExpiresAt:         time.Now().Add(ttl).UTC(),
	}
	if err := f.wraps.Create(t.Context(), w); err != nil {
		t.Fatalf("wrap Create: %v", err)
	}
	return w.ID
}

// session creates a reveal session holding wrapIDs whose TTL ends at
// expiresAt.
func (f *revealFixture) session(t *testing.T, expiresAt time.Time, wrapIDs ...uuid.UUID) uuid.UUID {
	t.Helper()
	s := &storage.RevealSession{
		UserID: "alice@example.com", ProjectID: f.project, EnvironmentID: f.envID,
		TTLSeconds: 60, ExpiresAt: expiresAt.UTC(), WrapIDs: wrapIDs,
	}
	if err := f.sessions.Create(t.Context(), s); err != nil {
		t.Fatalf("session Create: %v", err)
	}
	return s.ID
}

func (f *revealFixture) wrapExpiry(t *testing.T, id uuid.UUID) time.Time {
	t.Helper()
	w, err := f.wraps.Get(t.Context(), id)
	if err != nil {
		t.Fatalf("wrap Get: %v", err)
	}
	return w.ExpiresAt
}

// countingSessions counts SweepExpired calls, i.e. sweeper attempts.
type countingSessions struct {
	inner *storage.RevealSessions
	calls atomic.Int32
}

func (c *countingSessions) SweepExpired(ctx context.Context, now time.Time) ([]storage.SweptRevealSession, error) {
	c.calls.Add(1)
	return c.inner.SweepExpired(ctx, now)
}

// flakyWraps fails SetExpiry for one wrap id, as a dropped connection
// or a statement timeout would.
type flakyWraps struct {
	inner  *storage.SecretWraps
	failID uuid.UUID
}

func (f flakyWraps) SetExpiry(ctx context.Context, id uuid.UUID, at time.Time) error {
	if id == f.failID {
		return errors.New("simulated: connection reset by peer")
	}
	return f.inner.SetExpiry(ctx, id, at)
}

// WRK-N-01: a session whose wrap was already consumed (the normal case
// once the user revealed the key) is a clean sweep, not a failure that
// burns a retry under the sweeper lock.
func TestRevealSessionsExpired_PG_ConsumedWrapIsNotAFailure(t *testing.T) {
	f := newRevealFixture(t)
	ctx := t.Context()

	consumed := f.wrap(t, time.Hour)
	if err := f.wraps.MarkConsumedByUser(ctx, consumed, "alice@example.com", time.Now()); err != nil {
		t.Fatalf("MarkConsumedByUser: %v", err)
	}
	live := f.wrap(t, time.Hour)
	f.session(t, time.Now().Add(-time.Minute), consumed, live)

	sessions := &countingSessions{inner: f.sessions}
	sw := sweepers.RevealSessionsExpired{
		Sessions: sessions,
		Wraps:    f.wraps,
		Store:    sweepers.RevealSessionStore{DB: f.pool},
		Notifier: &recordingNotifier{},
	}
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := retry.DefaultPolicy().Run(runCtx, sw.Run); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := sessions.calls.Load(); n != 1 {
		t.Fatalf("sweeper ran %d times; a consumed wrap must not trigger a retry", n)
	}
	if exp := f.wrapExpiry(t, live); exp.After(time.Now()) {
		t.Fatalf("live wrap still reachable until %v", exp)
	}
}

// WRK-N-01: SweepExpired commits expired_at before the wraps are
// shortened. If one SetExpiry fails, the next tick no longer returns
// that session, so without a self-heal the wrap keeps its full TTL.
func TestRevealSessionsExpired_PG_SelfHealsWrapLeftLiveByPartialFailure(t *testing.T) {
	f := newRevealFixture(t)
	ctx := t.Context()

	stuck := f.wrap(t, time.Hour)
	ok := f.wrap(t, time.Hour)
	sessionEnd := time.Now().Add(-time.Minute).UTC()
	f.session(t, sessionEnd, stuck, ok)

	// Tick 1: the update for `stuck` fails after the session row has
	// already been marked expired, and the database blip also takes out
	// that tick's self-heal.
	blip := errors.New("simulated: connection reset by peer")
	tick1 := sweepers.RevealSessionsExpired{
		Sessions: f.sessions,
		Wraps:    flakyWraps{inner: f.wraps, failID: stuck},
		Store:    &fakeClamper{err: blip},
		Notifier: &recordingNotifier{},
	}
	if err := tick1.Run(ctx); err == nil {
		t.Fatal("tick 1 hid the failure")
	}
	if exp := f.wrapExpiry(t, stuck); !exp.After(time.Now().Add(50 * time.Minute)) {
		t.Fatalf("fixture: stuck wrap should still have its 1h TTL after tick 1, got %v", exp)
	}
	if exp := f.wrapExpiry(t, ok); exp.After(time.Now()) {
		t.Fatalf("healthy wrap not expired on tick 1: %v", exp)
	}

	// Tick 2: storage healthy again.
	tick2 := sweepers.RevealSessionsExpired{
		Sessions: f.sessions,
		Wraps:    f.wraps,
		Store:    sweepers.RevealSessionStore{DB: f.pool},
		Notifier: &recordingNotifier{},
	}
	if err := tick2.Run(ctx); err != nil {
		t.Fatalf("tick 2 Run: %v", err)
	}
	exp := f.wrapExpiry(t, stuck)
	if exp.After(sessionEnd.Add(time.Second)) {
		t.Fatalf("wrap from expired session still reachable until %v (session ended %v)", exp, sessionEnd)
	}
}

// Scope of the self-heal query against a mix of rows, including ones
// written before this fix (an expired session whose wrap kept its TTL).
func TestRevealSessionStore_PG_ClampScope(t *testing.T) {
	f := newRevealFixture(t)
	ctx := t.Context()
	now := time.Now().UTC()

	// Legacy: TTL-swept session, wrap never shortened.
	legacyWrap := f.wrap(t, time.Hour)
	legacyEnd := now.Add(-10 * time.Minute)
	legacy := f.session(t, legacyEnd, legacyWrap)
	if err := f.sessions.MarkExpired(ctx, legacy, now.Add(-9*time.Minute), storage.RevealSessionExpiredTTL); err != nil {
		t.Fatalf("MarkExpired legacy: %v", err)
	}

	// Hide Now: ended before its TTL, at the click.
	hiddenWrap := f.wrap(t, time.Hour)
	hidden := f.session(t, now.Add(4*time.Minute), hiddenWrap)
	hiddenAt := now.Add(-time.Minute)
	if err := f.sessions.MarkExpired(ctx, hidden, hiddenAt, storage.RevealSessionExpiredUserHide); err != nil {
		t.Fatalf("MarkExpired hidden: %v", err)
	}

	// Active session: untouched.
	activeWrap := f.wrap(t, time.Hour)
	f.session(t, now.Add(2*time.Minute), activeWrap)

	// Consumed wrap in an ended session: untouched (consumed_at wins).
	consumedWrap := f.wrap(t, time.Hour)
	if err := f.wraps.MarkConsumedByUser(ctx, consumedWrap, "alice@example.com", now); err != nil {
		t.Fatalf("MarkConsumedByUser: %v", err)
	}
	f.session(t, now.Add(-time.Minute), consumedWrap)

	// Wrap in no session at all: untouched.
	looseWrap := f.wrap(t, time.Hour)

	// Wrap already expired earlier than its session's end: never moved up.
	earlyWrap := f.wrap(t, -30*time.Minute)
	f.session(t, now.Add(-time.Minute), earlyWrap)

	before := map[uuid.UUID]time.Time{}
	for _, id := range []uuid.UUID{activeWrap, consumedWrap, looseWrap, earlyWrap} {
		before[id] = f.wrapExpiry(t, id)
	}

	store := sweepers.RevealSessionStore{DB: f.pool}
	healed, err := store.ClampEndedSessionWraps(ctx, now)
	if err != nil {
		t.Fatalf("ClampEndedSessionWraps: %v", err)
	}
	if len(healed) != 2 {
		t.Fatalf("healed %d wraps (%v) want 2 (legacy + hidden)", len(healed), healed)
	}
	if got := f.wrapExpiry(t, legacyWrap); !got.Equal(legacyEnd.Truncate(time.Microsecond)) {
		t.Errorf("legacy wrap expires_at = %v want session TTL end %v", got, legacyEnd)
	}
	if got := f.wrapExpiry(t, hiddenWrap); !got.Equal(hiddenAt.Truncate(time.Microsecond)) {
		t.Errorf("hidden wrap expires_at = %v want hide time %v", got, hiddenAt)
	}
	for id, want := range before {
		if got := f.wrapExpiry(t, id); !got.Equal(want) {
			t.Errorf("wrap %s moved from %v to %v; must be untouched", id, want, got)
		}
	}

	again, err := store.ClampEndedSessionWraps(ctx, now)
	if err != nil || len(again) != 0 {
		t.Fatalf("second run healed %v (err %v); want a no-op", again, err)
	}
}
