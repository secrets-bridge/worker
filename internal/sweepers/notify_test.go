package sweepers_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/secrets-bridge/worker/internal/notifications"
	"github.com/secrets-bridge/worker/internal/sweepers"
)

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(b.buf.String()), "\n") {
		if l == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("log line not JSON: %q", l)
		}
		out = append(out, m)
	}
	return out
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureDefaultLog routes slog.Default to a buffer for one test.
func captureDefaultLog(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// webhookSecret stands in for the token a Slack-style incoming webhook
// carries in its path. Placeholder value.
const webhookSecret = "hook-secret-canary-9d2c"

// WRK-N-07: a notification that fails to deliver is logged with the
// sweeper name and an error class, never the error text (a webhook
// transport error embeds the URL, secret path included) or the event
// payload.
func TestNotify_FailureIsLoggedWithoutPayload(t *testing.T) {
	refusing := httptest.NewServer(http.NotFoundHandler())
	refusedURL := refusing.URL + "/services/T000/B000/" + webhookSecret
	refusing.Close() // connection refused from here on

	forbidding := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(forbidding.Close)

	for _, tc := range []struct {
		name      string
		url       string
		wantClass string
	}{
		{"transport error", refusedURL, "transport"},
		{"http 403", forbidding.URL + "/services/T000/B000/" + webhookSecret, "http_4xx"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureDefaultLog(t)
			sink := &notifications.Fanout{Sinks: []notifications.Notifier{
				&notifications.Webhook{URL: tc.url},
			}}
			sw := sweepers.WrapsExpired{Repo: &fakeWrapDeleter{deleted: 3}, Notifier: sink}
			if err := sw.Run(t.Context()); err != nil {
				t.Fatalf("Run: %v (a notification failure must not fail the sweep)", err)
			}

			var found map[string]any
			for _, l := range logs.lines(t) {
				if l["msg"] == "sweepers: notification failed" {
					found = l
				}
			}
			if found == nil {
				t.Fatalf("no notification-failure log line; logs:\n%s", logs.String())
			}
			if found["sweeper"] != "wraps-expired" || found["error_class"] != tc.wantClass {
				t.Fatalf("log line = %v want sweeper=wraps-expired error_class=%s", found, tc.wantClass)
			}
			raw := logs.String()
			for _, banned := range []string{webhookSecret, "expired wraps purged", `"deleted"`} {
				if strings.Contains(raw, banned) {
					t.Fatalf("log leaked %q:\n%s", banned, raw)
				}
			}
		})
	}
}

// notifyFailureCount reads worker_sweeper_notify_failures_total for one
// sweeper from a registry holding sweepers.Collectors().
func notifyFailureCount(t *testing.T, reg *prometheus.Registry, sweeper string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "worker_sweeper_notify_failures_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "sweeper" && lp.GetValue() == sweeper {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func TestNotify_FailureIsCounted(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(sweepers.Collectors()...)
	captureDefaultLog(t)

	forbidding := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(forbidding.Close)

	before := notifyFailureCount(t, reg, "wraps-expired")
	sw := sweepers.WrapsExpired{
		Repo:     &fakeWrapDeleter{deleted: 1},
		Notifier: &notifications.Webhook{URL: forbidding.URL},
	}
	for range 2 {
		if err := sw.Run(t.Context()); err != nil {
			t.Fatalf("Run: %v", err)
		}
	}
	if got := notifyFailureCount(t, reg, "wraps-expired") - before; got != 2 {
		t.Fatalf("notify failures counted = %v want 2", got)
	}
}
