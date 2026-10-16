package sweepers_test

import (
	"context"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
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

// WRK-01: per-resource "message" and app-level "health_message" are
// free text that can carry secret-shaped substrings (e.g. a token in a
// readiness probe failure). They are dropped from observed_state, not
// scrubbed. See also TestGitOpsPoller_PG_ObservedStateHasNoMessageFields.
func TestGitOpsPoller_DropsMessageFields(t *testing.T) {
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
	const awsKeyCanary = "AKIAIOSFODNN7EXAMPLE"
	longMessage := strings.Repeat("readiness probe failed: ", 30) + "credential " + awsKeyCanary + " rejected"
	app := &argocd.Application{
		Name: "x", HealthStatus: "Degraded", SyncStatus: "OutOfSync",
		HealthMessage: longMessage,
		Resources: []argocd.ApplicationResource{
			{Kind: "Pod", Name: "x-0", Health: "Degraded", Message: longMessage},
		},
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
	if len(obs.recordCalls) != 1 {
		t.Fatalf("recordCalls = %d want 1", len(obs.recordCalls))
	}
	observed := obs.recordCalls[0].observed

	if _, ok := observed["health_message"]; ok {
		t.Fatalf("observed_state still carries health_message: %+v", observed)
	}
	resources, ok := observed["resources"].([]map[string]any)
	if !ok || len(resources) != 1 {
		t.Fatalf("resources = %+v", observed["resources"])
	}
	if _, ok := resources[0]["message"]; ok {
		t.Fatalf("resource still carries message: %+v", resources[0])
	}
	for _, v := range resources[0] {
		if s, _ := v.(string); strings.Contains(s, awsKeyCanary) {
			t.Fatalf("resource leaked credential: %+v", resources[0])
		}
	}

	// Status fields still pass through untouched.
	if observed["health_status"] != "Degraded" {
		t.Fatalf("health_status = %+v", observed["health_status"])
	}
	if resources[0]["kind"] != "Pod" || resources[0]["health"] != "Degraded" {
		t.Fatalf("resource fields altered: %+v", resources[0])
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

// --- WRK-N-04: plain http ArgoCD endpoints --------------------------

// argoProbe records every request that reaches the fake ArgoCD and
// whether it carried an Authorization header.
type argoProbe struct {
	hits     atomic.Int32
	authSeen atomic.Value // string
}

func (p *argoProbe) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.hits.Add(1)
		if a := r.Header.Get("Authorization"); a != "" {
			p.authSeen.Store(a)
		}
		http.NotFound(w, r)
	})
}

func (p *argoProbe) auth() string {
	s, _ := p.authSeen.Load().(string)
	return s
}

func pollOnceAgainst(t *testing.T, ep *storage.ArgoCDEndpoint, configure func(*sweepers.GitOpsPoller)) (*fakeGitOpsObsRepo, *fakeEndpointRepo, int) {
	t.Helper()
	obs := &fakeGitOpsObsRepo{claim: []*storage.GitOpsObservation{
		{ID: uuid.New(), ArgoCDEndpointID: ep.ID, ApplicationName: "billing-api"},
	}}
	eps := &fakeEndpointRepo{endpoint: ep}
	resolved := 0
	p := sweepers.GitOpsPoller{
		Observations: obs,
		Endpoints:    eps,
		ResolveToken: func(context.Context, *storage.ArgoCDEndpoint) ([]byte, error) {
			resolved++
			return []byte("fake-token"), nil
		},
		// ClientFactory nil: the production argocd client does the HTTP.
		HTTPTimeout: 5 * time.Second,
	}
	if configure != nil {
		configure(&p)
	}
	if err := p.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return obs, eps, resolved
}

func TestGitOpsPoller_PlainHTTPEndpointRefusedByDefault(t *testing.T) {
	probe := &argoProbe{}
	srv := httptest.NewServer(probe.handler())
	t.Cleanup(srv.Close)

	ep := &storage.ArgoCDEndpoint{ID: uuid.New(), BaseURL: srv.URL, Enabled: true}
	obs, eps, resolved := pollOnceAgainst(t, ep, nil)

	if n := probe.hits.Load(); n != 0 {
		t.Fatalf("plain http endpoint got %d request(s), Authorization=%q; want none", n, probe.auth())
	}
	if resolved != 0 {
		t.Fatalf("token resolved %d time(s) for a refused endpoint; want 0", resolved)
	}
	if !strings.Contains(eps.health, "https") {
		t.Fatalf("endpoint health = %q want an https refusal", eps.health)
	}
	if len(obs.recordCalls) != 1 || !strings.Contains(obs.recordCalls[0].errMsg, "https") {
		t.Fatalf("recordCalls = %+v want one poll error naming https", obs.recordCalls)
	}
	for _, s := range []string{eps.health, obs.recordCalls[0].errMsg} {
		if strings.Contains(s, "fake-token") {
			t.Fatalf("token leaked into a stored error: %q", s)
		}
	}
	if len(obs.transitions) != 0 {
		t.Fatalf("transitions = %+v want none", obs.transitions)
	}
}

// Control for the test above: an https endpoint still gets polled with
// the bearer token, so the harness does see headers when they're sent.
func TestGitOpsPoller_HTTPSEndpointStillPolled(t *testing.T) {
	probe := &argoProbe{}
	srv := httptest.NewTLSServer(probe.handler())
	t.Cleanup(srv.Close)

	ep := &storage.ArgoCDEndpoint{
		ID: uuid.New(), BaseURL: srv.URL, Enabled: true,
		TLSCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})),
	}
	_, _, resolved := pollOnceAgainst(t, ep, nil)
	if probe.hits.Load() == 0 || probe.auth() != "Bearer fake-token" {
		t.Fatalf("https endpoint: hits=%d auth=%q want a request with the bearer token", probe.hits.Load(), probe.auth())
	}
	if resolved != 1 {
		t.Fatalf("token resolved %d time(s) want 1", resolved)
	}
}

// SB_ENV=dev keeps the local-compose path working: main sets
// AllowPlainHTTP only in dev.
func TestGitOpsPoller_PlainHTTPAllowedInDev(t *testing.T) {
	probe := &argoProbe{}
	srv := httptest.NewServer(probe.handler())
	t.Cleanup(srv.Close)

	ep := &storage.ArgoCDEndpoint{ID: uuid.New(), BaseURL: srv.URL, Enabled: true}
	_, _, resolved := pollOnceAgainst(t, ep, func(p *sweepers.GitOpsPoller) { p.AllowPlainHTTP = true })
	if probe.hits.Load() == 0 || resolved != 1 {
		t.Fatalf("dev http endpoint: hits=%d resolved=%d want it polled", probe.hits.Load(), resolved)
	}
}

// Anything that isn't an absolute https URL is refused before the token
// is resolved.
func TestGitOpsPoller_NonHTTPSSchemesRefused(t *testing.T) {
	for _, base := range []string{"ftp://argocd.example.com", "argocd.example.com", "https://", "://bad"} {
		t.Run(base, func(t *testing.T) {
			ep := &storage.ArgoCDEndpoint{ID: uuid.New(), BaseURL: base, Enabled: true}
			_, eps, resolved := pollOnceAgainst(t, ep, nil)
			if resolved != 0 || !strings.Contains(eps.health, "https") {
				t.Fatalf("resolved=%d health=%q want refusal before token resolve", resolved, eps.health)
			}
		})
	}
}
