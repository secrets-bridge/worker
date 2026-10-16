package sweepers_test

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/storage"
	"github.com/secrets-bridge/worker/internal/sweepers"
)

// messageCanary is shaped like nothing sanitize.DiscoverError knows
// (it only matches four token formats), so a scrub-based fix would
// let it through. Fake value, placeholder host.
const messageCanary = "postgres://app:hunter2-canary-7f3a@db.example.com:5432/app"

// fakeArgoCD serves the two GET paths GetApplicationResourceTree hits
// for application billing-api in a Degraded / OutOfSync / Running state.
// Both carry free-text health messages holding messageCanary.
func fakeArgoCD(t *testing.T) *httptest.Server {
	t.Helper()
	return fakeArgoCDWith(t, "Degraded", "OutOfSync", "Running")
}

func fakeArgoCDWith(t *testing.T, health, sync, phase string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/applications/billing-api", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{
			"metadata": {"name": "billing-api", "namespace": "argocd"},
			"status": {
				"health": {"status": "` + health + `", "message": "readiness failed: ` + messageCanary + `"},
				"sync": {"status": "` + sync + `", "revision": "abc123"},
				"operationState": {"phase": "` + phase + `", "message": "` + messageCanary + `"}
			}
		}`))
	})
	mux.HandleFunc("/api/v1/applications/billing-api/resource-tree", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"nodes": [{
			"kind": "Deployment", "name": "billing-api", "namespace": "billing",
			"health": {"status": "` + health + `", "message": "probe: ` + messageCanary + `"}
		}]}`))
	})
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// makeArgoEndpoint stores an enabled endpoint pointing at srv, trusting
// its test certificate. The token envelope is fake bytes; tests resolve
// the token with a stub.
func makeArgoEndpoint(t *testing.T, pool *storage.Pool, srv *httptest.Server) *storage.ArgoCDEndpoint {
	t.Helper()
	ep := &storage.ArgoCDEndpoint{
		Name:    "argocd-" + uuid.NewString()[:8],
		BaseURL: srv.URL,
		TLSCAPEM: string(pem.EncodeToMemory(&pem.Block{
			Type: "CERTIFICATE", Bytes: srv.Certificate().Raw,
		})),
		TokenCiphertext:        []byte("ct"),
		TokenDataKeyCiphertext: []byte("dek"),
		TokenNonce:             []byte("nonce"),
		TokenKMSKeyID:          "local:test",
		Enabled:                true,
	}
	if err := storage.NewArgoCDEndpoints(pool).Create(t.Context(), ep); err != nil {
		t.Fatalf("endpoint Create: %v", err)
	}
	return ep
}

// makeObservation stores a queued observation of billing-api.
func makeObservation(t *testing.T, pool *storage.Pool, endpointID uuid.UUID, timeoutAt time.Time) *storage.GitOpsObservation {
	t.Helper()
	obs := &storage.GitOpsObservation{
		RequestID:        makeReadRequest(t, pool),
		ArgoCDEndpointID: endpointID,
		ApplicationName:  "billing-api",
		TimeoutAt:        &timeoutAt,
	}
	if err := storage.NewGitOpsObservations(pool).Create(t.Context(), obs); err != nil {
		t.Fatalf("observation Create: %v", err)
	}
	return obs
}

func stubToken(context.Context, *storage.ArgoCDEndpoint) ([]byte, error) {
	return []byte("fake-token"), nil
}

// WRK-01: observed_state is returned to requesters by
// GET /requests/:id/gitops, so ArgoCD free text (resource "message",
// app "health_message") must not be persisted at all. Drives the real
// argocd client over TLS and the real gitops_observations table.
func TestGitOpsPoller_PG_ObservedStateHasNoMessageFields(t *testing.T) {
	pool := testPool(t)
	ctx := t.Context()
	srv := fakeArgoCD(t)

	ep := makeArgoEndpoint(t, pool, srv)
	obs := makeObservation(t, pool, ep.ID, time.Now().Add(30*time.Minute))
	observations := storage.NewGitOpsObservations(pool)

	p := sweepers.GitOpsPoller{
		Observations: gitopsStore(pool),
		Endpoints:    storage.NewArgoCDEndpoints(pool),
		ResolveToken: stubToken,
		BatchSize:    10,
		HTTPTimeout:  5 * time.Second,
		ReplicaID:    uuid.New(),
	}
	if err := p.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, err := observations.Get(ctx, obs.ID)
	if err != nil {
		t.Fatalf("observation Get: %v", err)
	}
	if got.LastError != "" {
		t.Fatalf("poll recorded an error, fixture is wrong: %q", got.LastError)
	}
	raw, _ := json.Marshal(got.ObservedState)
	if strings.Contains(string(raw), "hunter2-canary") {
		t.Fatalf("observed_state leaked ArgoCD free text: %s", raw)
	}
	if _, ok := got.ObservedState["health_message"]; ok {
		t.Fatalf("observed_state still has health_message: %s", raw)
	}
	resources, _ := got.ObservedState["resources"].([]any)
	if len(resources) != 1 {
		t.Fatalf("resources = %s", raw)
	}
	res, _ := resources[0].(map[string]any)
	if _, ok := res["message"]; ok {
		t.Fatalf("resource still has message: %s", raw)
	}
	// Status and sync fields stay.
	for key, want := range map[string]any{
		"health_status":   "Degraded",
		"sync_status":     "OutOfSync",
		"sync_revision":   "abc123",
		"operation_phase": "Running",
	} {
		if got.ObservedState[key] != want {
			t.Errorf("%s = %v want %v", key, got.ObservedState[key], want)
		}
	}
	if res["kind"] != "Deployment" || res["name"] != "billing-api" || res["health"] != "Degraded" {
		t.Errorf("resource fields lost: %v", res)
	}
}

// gitopsStore is the production observation repo the gitops sweepers
// are wired with.
func gitopsStore(pool *storage.Pool) sweepers.GitOpsObservationStore {
	return sweepers.GitOpsObservationStore{GitOpsObservations: storage.NewGitOpsObservations(pool), DB: pool}
}
