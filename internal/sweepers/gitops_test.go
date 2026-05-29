package sweepers_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/argocd"
	"github.com/secrets-bridge/api/pkg/storage"
	"github.com/secrets-bridge/worker/internal/sweepers"
)

// --- fakes ----------------------------------------------------------

type fakeGitOpsObsRepo struct {
	mu          sync.Mutex
	claim       []*storage.GitOpsObservation
	claimErr    error
	recordCalls []recordCall
	recordErr   error
	transitions []transitionCall
	transErr    error

	// for timeout sweeper
	findRows []*storage.GitOpsObservation
	findErr  error
}

type recordCall struct {
	id       uuid.UUID
	observed map[string]any
	errMsg   string
}

type transitionCall struct {
	id    uuid.UUID
	state storage.GitOpsObservationState
}

func (f *fakeGitOpsObsRepo) ClaimNextActive(_ context.Context, _ int, _ uuid.UUID) ([]*storage.GitOpsObservation, error) {
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	return f.claim, nil
}

func (f *fakeGitOpsObsRepo) RecordPoll(_ context.Context, id uuid.UUID, observed map[string]any, errMsg string, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordCalls = append(f.recordCalls, recordCall{id: id, observed: observed, errMsg: errMsg})
	return f.recordErr
}

func (f *fakeGitOpsObsRepo) Transition(_ context.Context, id uuid.UUID, state storage.GitOpsObservationState, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.transitions = append(f.transitions, transitionCall{id: id, state: state})
	return f.transErr
}

func (f *fakeGitOpsObsRepo) FindTimedOut(_ context.Context, _ time.Time, _ int) ([]*storage.GitOpsObservation, error) {
	return f.findRows, f.findErr
}

type fakeEndpointRepo struct {
	endpoint *storage.ArgoCDEndpoint
	getErr   error
	health   string
}

func (f *fakeEndpointRepo) Get(_ context.Context, _ uuid.UUID) (*storage.ArgoCDEndpoint, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.endpoint, nil
}

func (f *fakeEndpointRepo) UpdateHealth(_ context.Context, _ uuid.UUID, _ time.Time, errMsg string) error {
	f.health = errMsg
	return nil
}

type fakeArgoClient struct {
	app *argocd.Application
	err error
}

func (f *fakeArgoClient) GetApplicationResourceTree(_ context.Context, _ string) (*argocd.Application, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.app, nil
}

// --- GitOpsPoller ---------------------------------------------------

func TestGitOpsPoller_NoObservations_NoOp(t *testing.T) {
	obs := &fakeGitOpsObsRepo{}
	p := sweepers.GitOpsPoller{
		Observations:  obs,
		Endpoints:     &fakeEndpointRepo{},
		ClientFactory: func(context.Context, *storage.ArgoCDEndpoint, []byte, time.Duration) (sweepers.ArgoClient, error) {
			return nil, errors.New("should not be called")
		},
	}
	if err := p.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestGitOpsPoller_HappyPath_TransitionToApplied(t *testing.T) {
	endpointID := uuid.New()
	obsID := uuid.New()
	obs := &fakeGitOpsObsRepo{
		claim: []*storage.GitOpsObservation{
			{ID: obsID, ArgoCDEndpointID: endpointID, ApplicationName: "billing-api", PollingState: storage.GitOpsStateActive},
		},
	}
	ep := &fakeEndpointRepo{endpoint: &storage.ArgoCDEndpoint{
		ID: endpointID, BaseURL: "https://argocd.example.com", Enabled: true,
		TokenCiphertext: []byte("ct"), TokenDataKeyCiphertext: []byte("dek"), TokenNonce: []byte("nonce"), TokenKMSKeyID: "local:test",
	}}
	app := &argocd.Application{
		Name: "billing-api", HealthStatus: "Healthy", SyncStatus: "Synced",
		SyncRevision: "abc123", OperationPhase: "Succeeded",
		Resources: []argocd.ApplicationResource{
			{Kind: "Deployment", Name: "billing-api", Health: "Healthy"},
		},
	}
	p := sweepers.GitOpsPoller{
		Observations: obs,
		Endpoints:    ep,
		ResolveToken: func(_ context.Context, _ *storage.ArgoCDEndpoint) ([]byte, error) { return []byte("fake-token"), nil },
		ClientFactory: func(_ context.Context, _ *storage.ArgoCDEndpoint, _ []byte, _ time.Duration) (sweepers.ArgoClient, error) {
			return &fakeArgoClient{app: app}, nil
		},
		BatchSize: 10,
	}
	if err := p.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(obs.recordCalls) != 1 {
		t.Fatalf("recordCalls = %d want 1", len(obs.recordCalls))
	}
	if obs.recordCalls[0].observed["health_status"] != "Healthy" {
		t.Fatalf("observed = %+v", obs.recordCalls[0].observed)
	}
	if obs.recordCalls[0].observed["sync_revision"] != "abc123" {
		t.Fatalf("sync_revision missing: %+v", obs.recordCalls[0].observed)
	}
	if len(obs.transitions) != 1 || obs.transitions[0].state != storage.GitOpsStateApplied {
		t.Fatalf("transitions = %+v want applied", obs.transitions)
	}
}

func TestGitOpsPoller_DegradedFailedPhase_TransitionToFailed(t *testing.T) {
	endpointID := uuid.New()
	obs := &fakeGitOpsObsRepo{
		claim: []*storage.GitOpsObservation{
			{ID: uuid.New(), ArgoCDEndpointID: endpointID, ApplicationName: "x"},
		},
	}
	ep := &fakeEndpointRepo{endpoint: &storage.ArgoCDEndpoint{
		ID: endpointID, BaseURL: "https://argocd.example.com", Enabled: true,
		TokenCiphertext: []byte("c"), TokenDataKeyCiphertext: []byte("d"), TokenNonce: []byte("n"), TokenKMSKeyID: "local:test",
	}}
	app := &argocd.Application{
		Name: "x", HealthStatus: "Degraded", SyncStatus: "OutOfSync", OperationPhase: "Failed",
	}
	p := sweepers.GitOpsPoller{
		Observations:  obs,
		Endpoints:     ep,
		ResolveToken:  func(_ context.Context, _ *storage.ArgoCDEndpoint) ([]byte, error) { return []byte("fake-token"), nil },
		ClientFactory: func(context.Context, *storage.ArgoCDEndpoint, []byte, time.Duration) (sweepers.ArgoClient, error) {
			return &fakeArgoClient{app: app}, nil
		},
	}
	if err := p.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(obs.transitions) != 1 || obs.transitions[0].state != storage.GitOpsStateFailed {
		t.Fatalf("transitions = %+v want failed", obs.transitions)
	}
}

func TestGitOpsPoller_InProgress_StaysActive(t *testing.T) {
	endpointID := uuid.New()
	obs := &fakeGitOpsObsRepo{
		claim: []*storage.GitOpsObservation{
			{ID: uuid.New(), ArgoCDEndpointID: endpointID, ApplicationName: "x"},
		},
	}
	ep := &fakeEndpointRepo{endpoint: &storage.ArgoCDEndpoint{
		ID: endpointID, BaseURL: "https://argocd.example.com", Enabled: true,
		TokenCiphertext: []byte("c"), TokenDataKeyCiphertext: []byte("d"), TokenNonce: []byte("n"), TokenKMSKeyID: "local:test",
	}}
	app := &argocd.Application{
		Name: "x", HealthStatus: "Progressing", SyncStatus: "Synced", OperationPhase: "Running",
	}
	p := sweepers.GitOpsPoller{
		Observations:  obs,
		Endpoints:     ep,
		ResolveToken:  func(_ context.Context, _ *storage.ArgoCDEndpoint) ([]byte, error) { return []byte("fake-token"), nil },
		ClientFactory: func(context.Context, *storage.ArgoCDEndpoint, []byte, time.Duration) (sweepers.ArgoClient, error) {
			return &fakeArgoClient{app: app}, nil
		},
	}
	if err := p.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(obs.transitions) != 0 {
		t.Fatalf("transitions = %+v want none (still active)", obs.transitions)
	}
	if len(obs.recordCalls) != 1 || obs.recordCalls[0].observed["health_status"] != "Progressing" {
		t.Fatalf("recordCalls = %+v", obs.recordCalls)
	}
}

func TestGitOpsPoller_EndpointDisabled_NoArgoCDCall(t *testing.T) {
	endpointID := uuid.New()
	obs := &fakeGitOpsObsRepo{
		claim: []*storage.GitOpsObservation{
			{ID: uuid.New(), ArgoCDEndpointID: endpointID, ApplicationName: "x"},
		},
	}
	ep := &fakeEndpointRepo{endpoint: &storage.ArgoCDEndpoint{
		ID: endpointID, Enabled: false,
	}}
	p := sweepers.GitOpsPoller{
		Observations:  obs,
		Endpoints:     ep,
		ResolveToken:  func(_ context.Context, _ *storage.ArgoCDEndpoint) ([]byte, error) { return []byte("fake-token"), nil },
		ClientFactory: func(context.Context, *storage.ArgoCDEndpoint, []byte, time.Duration) (sweepers.ArgoClient, error) {
			t.Fatal("ClientFactory called on a disabled endpoint")
			return nil, nil
		},
	}
	if err := p.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(obs.recordCalls) != 1 || !strings.Contains(obs.recordCalls[0].errMsg, "endpoint disabled") {
		t.Fatalf("recordCalls = %+v", obs.recordCalls)
	}
	if len(obs.transitions) != 0 {
		t.Fatalf("transitions = %+v want none", obs.transitions)
	}
}

func TestGitOpsPoller_ArgoCDError_UpdatesEndpointHealth(t *testing.T) {
	endpointID := uuid.New()
	obs := &fakeGitOpsObsRepo{
		claim: []*storage.GitOpsObservation{
			{ID: uuid.New(), ArgoCDEndpointID: endpointID, ApplicationName: "x"},
		},
	}
	ep := &fakeEndpointRepo{endpoint: &storage.ArgoCDEndpoint{
		ID: endpointID, BaseURL: "https://argocd.example.com", Enabled: true,
		TokenCiphertext: []byte("c"), TokenDataKeyCiphertext: []byte("d"), TokenNonce: []byte("n"), TokenKMSKeyID: "local:test",
	}}
	p := sweepers.GitOpsPoller{
		Observations:  obs,
		Endpoints:     ep,
		ResolveToken:  func(_ context.Context, _ *storage.ArgoCDEndpoint) ([]byte, error) { return []byte("fake-token"), nil },
		ClientFactory: func(context.Context, *storage.ArgoCDEndpoint, []byte, time.Duration) (sweepers.ArgoClient, error) {
			return &fakeArgoClient{err: errors.New("connection refused")}, nil
		},
	}
	if err := p.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(ep.health, "connection refused") {
		t.Fatalf("endpoint health = %q want endpoint-update with the error", ep.health)
	}
	if len(obs.transitions) != 0 {
		t.Fatalf("transitions = %+v want none on transient error", obs.transitions)
	}
}

// --- GitOpsTimeoutSweeper ------------------------------------------

func TestGitOpsTimeoutSweeper_FlipsRows(t *testing.T) {
	rows := []*storage.GitOpsObservation{
		{ID: uuid.New(), ApplicationName: "a", PollingState: storage.GitOpsStateActive},
		{ID: uuid.New(), ApplicationName: "b", PollingState: storage.GitOpsStateQueued},
	}
	obs := &fakeGitOpsObsRepo{findRows: rows}
	sw := sweepers.GitOpsTimeoutSweeper{Repo: obs}
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(obs.transitions) != 2 {
		t.Fatalf("transitions = %d want 2", len(obs.transitions))
	}
	for _, tr := range obs.transitions {
		if tr.state != storage.GitOpsStateAppliedUnverified {
			t.Fatalf("transition state = %q want applied_unverified", tr.state)
		}
	}
}

func TestGitOpsTimeoutSweeper_NoTimeouts_NoOp(t *testing.T) {
	obs := &fakeGitOpsObsRepo{}
	sw := sweepers.GitOpsTimeoutSweeper{Repo: obs}
	if err := sw.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(obs.transitions) != 0 {
		t.Fatalf("transitions = %+v", obs.transitions)
	}
}

func TestGitOpsTimeoutSweeper_FindError(t *testing.T) {
	obs := &fakeGitOpsObsRepo{findErr: errors.New("db down")}
	sw := sweepers.GitOpsTimeoutSweeper{Repo: obs}
	err := sw.Run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "db down") {
		t.Fatalf("err = %v", err)
	}
}
