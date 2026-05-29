package sweepers

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/argocd"
	"github.com/secrets-bridge/api/pkg/keymgmt"
	"github.com/secrets-bridge/api/pkg/storage"
	"github.com/secrets-bridge/worker/internal/notifications"
)

// gitopsObservationRepo is the slice of api/pkg/storage.GitOpsObservations
// the poller needs. Defined here so unit tests can inject a fake.
type gitopsObservationRepo interface {
	ClaimNextActive(ctx context.Context, limit int, claimedBy uuid.UUID) ([]*storage.GitOpsObservation, error)
	RecordPoll(ctx context.Context, id uuid.UUID, observed map[string]any, pollErr string, polledAt time.Time) error
	Transition(ctx context.Context, id uuid.UUID, state storage.GitOpsObservationState, at time.Time) error
}

// gitopsEndpointRepo is the slice of api/pkg/storage.ArgoCDEndpoints
// the poller needs.
type gitopsEndpointRepo interface {
	Get(ctx context.Context, id uuid.UUID) (*storage.ArgoCDEndpoint, error)
	UpdateHealth(ctx context.Context, id uuid.UUID, healthAt time.Time, healthErr string) error
}

// ArgoClient is the slice of *argocd.Client used by the poller.
// Exported so tests in other packages (and the worker's main) can
// swap a fake.
type ArgoClient interface {
	GetApplicationResourceTree(ctx context.Context, name string) (*argocd.Application, error)
}

// ArgoClientFactory builds an argocd.Client for a given endpoint +
// resolved token. The default factory is BuildArgoClient; tests
// inject a fake.
type ArgoClientFactory func(ctx context.Context, e *storage.ArgoCDEndpoint, token []byte, httpTimeout time.Duration) (ArgoClient, error)

// BuildArgoClient is the production factory.
func BuildArgoClient(_ context.Context, e *storage.ArgoCDEndpoint, token []byte, httpTimeout time.Duration) (ArgoClient, error) {
	return argocd.New(argocd.Config{
		BaseURL:       e.BaseURL,
		Token:         string(token),
		TLSCAPEM:      e.TLSCAPEM,
		TLSServerName: e.TLSServerName,
		Timeout:       httpTimeout,
	})
}

// TokenResolver returns the plaintext ArgoCD bearer token for an
// endpoint. The default implementation (BuildTokenResolver) uses the
// KeyManager + AES-GCM to unwrap the persisted envelope. Tests inject
// a fake.
type TokenResolver func(ctx context.Context, e *storage.ArgoCDEndpoint) ([]byte, error)

// BuildTokenResolver returns the production token resolver that
// unwraps the persisted envelope via the configured KeyManager.
func BuildTokenResolver(km keymgmt.KeyManager) TokenResolver {
	return func(ctx context.Context, e *storage.ArgoCDEndpoint) ([]byte, error) {
		return unwrapToken(ctx, km, e)
	}
}

// GitOpsPoller claims active gitops_observations, polls ArgoCD per
// observation, records the observed_state snapshot, and transitions
// terminal rows to applied / failed when the observed state warrants
// it. Timeout transitions are handled by GitOpsTimeoutSweeper.
//
// The poller does NOT decrypt the token plaintext into memory longer
// than the single HTTP call. After argocd.Client construction the
// plaintext is zeroed; the constructed client carries the token
// internally as a string for the Authorization header.
type GitOpsPoller struct {
	Observations  gitopsObservationRepo
	Endpoints     gitopsEndpointRepo
	ResolveToken  TokenResolver // required; nil → fail loud
	ClientFactory ArgoClientFactory
	Notifier      notifications.Notifier
	BatchSize     int
	HTTPTimeout   time.Duration
	// ReplicaID identifies this worker replica for telemetry; the
	// observation rows don't carry a claimed_by today.
	ReplicaID uuid.UUID
	Now       func() time.Time
}

// Name returns the stable identifier for this sweeper.
func (g GitOpsPoller) Name() string { return "gitops-poller" }

// Run claims up to BatchSize observation rows and polls each.
func (g GitOpsPoller) Run(ctx context.Context) error {
	now := time.Now
	if g.Now != nil {
		now = g.Now
	}
	factory := g.ClientFactory
	if factory == nil {
		factory = BuildArgoClient
	}
	batch := g.BatchSize
	if batch <= 0 {
		batch = 10
	}

	claimed, err := g.Observations.ClaimNextActive(ctx, batch, g.ReplicaID)
	if err != nil {
		notify(ctx, g.Notifier, notifications.Event{
			Severity:  notifications.SeverityError,
			Component: g.Name(),
			Title:     "claim gitops observations failed",
			Detail:    err.Error(),
		})
		return fmt.Errorf("sweepers: claim gitops observations: %w", err)
	}
	if len(claimed) == 0 {
		return nil
	}

	succeeded, failed := 0, 0
	for _, o := range claimed {
		if err := g.pollOne(ctx, factory, o, now()); err != nil {
			failed++
			continue
		}
		succeeded++
	}
	if succeeded > 0 || failed > 0 {
		notify(ctx, g.Notifier, notifications.Event{
			Severity:  notifications.SeverityInfo,
			Component: g.Name(),
			Title:     "gitops observations polled",
			Metadata: map[string]any{
				"succeeded": succeeded,
				"failed":    failed,
			},
		})
	}
	return nil
}

// pollOne handles one observation row: resolve endpoint → unwrap
// token → build argocd client → fetch app + resources → record poll →
// transition if terminal.
func (g GitOpsPoller) pollOne(ctx context.Context, factory ArgoClientFactory, o *storage.GitOpsObservation, now time.Time) error {
	ep, err := g.Endpoints.Get(ctx, o.ArgoCDEndpointID)
	if err != nil {
		_ = g.Observations.RecordPoll(ctx, o.ID, nil, "endpoint lookup: "+err.Error(), now)
		return fmt.Errorf("endpoint lookup: %w", err)
	}
	if !ep.Enabled {
		_ = g.Observations.RecordPoll(ctx, o.ID, nil, "endpoint disabled", now)
		return nil // no error bubble; row stays in active for the next tick
	}
	if g.ResolveToken == nil {
		_ = g.Observations.RecordPoll(ctx, o.ID, nil, "no token resolver configured", now)
		return errors.New("no token resolver configured")
	}
	token, err := g.ResolveToken(ctx, ep)
	if err != nil {
		_ = g.Observations.RecordPoll(ctx, o.ID, nil, "unwrap token: "+err.Error(), now)
		return fmt.Errorf("unwrap token: %w", err)
	}
	client, err := factory(ctx, ep, token, g.HTTPTimeout)
	for i := range token {
		token[i] = 0
	}
	if err != nil {
		_ = g.Observations.RecordPoll(ctx, o.ID, nil, "build argocd client: "+err.Error(), now)
		return fmt.Errorf("build client: %w", err)
	}

	app, err := client.GetApplicationResourceTree(ctx, o.ApplicationName)
	if err != nil {
		_ = g.Observations.RecordPoll(ctx, o.ID, nil, "argocd fetch: "+err.Error(), now)
		_ = g.Endpoints.UpdateHealth(ctx, ep.ID, now, err.Error())
		return fmt.Errorf("argocd fetch: %w", err)
	}
	_ = g.Endpoints.UpdateHealth(ctx, ep.ID, now, "")

	observed := observedStateFromApp(app)
	if err := g.Observations.RecordPoll(ctx, o.ID, observed, "", now); err != nil {
		return fmt.Errorf("record poll: %w", err)
	}

	// Terminal-state decision. "applied" requires health=Healthy AND
	// sync=Synced AND operation phase=Succeeded (or no operation
	// running). "failed" requires health=Degraded AND operation
	// phase=Failed. Otherwise the row stays in active.
	state := decideTerminalState(app)
	if state != "" {
		if err := g.Observations.Transition(ctx, o.ID, state, now); err != nil {
			return fmt.Errorf("transition: %w", err)
		}
	}
	return nil
}

// observedStateFromApp distills an argocd.Application into the
// metadata-only snapshot stored in observed_state. Per BRD §26.4 we
// surface only filtered status fields — never raw manifests.
func observedStateFromApp(app *argocd.Application) map[string]any {
	resources := make([]map[string]any, 0, len(app.Resources))
	for _, r := range app.Resources {
		resources = append(resources, map[string]any{
			"kind":      r.Kind,
			"name":      r.Name,
			"namespace": r.Namespace,
			"health":    r.Health,
			"message":   r.Message,
		})
	}
	return map[string]any{
		"health_status":   app.HealthStatus,
		"health_message":  app.HealthMessage,
		"sync_status":     app.SyncStatus,
		"sync_revision":   app.SyncRevision,
		"operation_phase": app.OperationPhase,
		"resources":       resources,
		"resource_count":  len(resources),
	}
}

// decideTerminalState maps an observed application to applied /
// failed / "" (keep active).
func decideTerminalState(app *argocd.Application) storage.GitOpsObservationState {
	healthy := app.HealthStatus == "Healthy"
	synced := app.SyncStatus == "Synced"
	opIdleOrSucceeded := app.OperationPhase == "" || app.OperationPhase == "Succeeded"
	opFailed := app.OperationPhase == "Failed" || app.OperationPhase == "Error"

	switch {
	case healthy && synced && opIdleOrSucceeded:
		return storage.GitOpsStateApplied
	case opFailed:
		return storage.GitOpsStateFailed
	default:
		return ""
	}
}

// unwrapToken mirrors api/internal/services.ArgoCDEndpointService.ResolveToken
// so the worker can decrypt the same envelope shape without dragging
// the api's internal/services package (which it can't import).
//
// Caller MUST zero the returned slice after use.
func unwrapToken(ctx context.Context, km keymgmt.KeyManager, e *storage.ArgoCDEndpoint) ([]byte, error) {
	if km == nil {
		return nil, errors.New("sweepers: KeyManager not configured")
	}
	dek, err := km.DecryptDataKey(ctx, e.TokenDataKeyCiphertext, e.TokenKMSKeyID)
	if err != nil {
		return nil, fmt.Errorf("decrypt dek: %w", err)
	}
	defer func() {
		for i := range dek {
			dek[i] = 0
		}
	}()
	if len(dek) != 32 {
		return nil, fmt.Errorf("dek must be 32 bytes, got %d", len(dek))
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, fmt.Errorf("aes cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("gcm: %w", err)
	}
	plaintext, err := gcm.Open(nil, e.TokenNonce, e.TokenCiphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("aes-gcm open: %w", err)
	}
	return plaintext, nil
}
