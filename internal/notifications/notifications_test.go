package notifications_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/secrets-bridge/worker/internal/notifications"
)

func TestWebhook_HappyPath(t *testing.T) {
	var got struct {
		body string
		ct   string
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got.body = string(b)
		got.ct = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	wh := &notifications.Webhook{URL: srv.URL, HTTPClient: srv.Client()}
	err := wh.Notify(t.Context(), notifications.Event{
		Severity:  notifications.SeverityInfo,
		Component: "test",
		Title:     "hello",
	})
	if err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if got.ct != "application/json" {
		t.Fatalf("Content-Type = %q", got.ct)
	}
	if !strings.Contains(got.body, `"title":"hello"`) {
		t.Fatalf("body missing title: %s", got.body)
	}
}

func TestWebhook_SlackFormat(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	wh := &notifications.Webhook{URL: srv.URL, HTTPClient: srv.Client(), FormatSlack: true}
	if err := wh.Notify(t.Context(), notifications.Event{
		Severity: notifications.SeverityWarn,
		Title:    "stale",
		Detail:   "5 agents",
	}); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if !strings.Contains(got, `"text"`) || !strings.Contains(got, "[warn] stale") {
		t.Fatalf("slack body unexpected: %s", got)
	}
}

func TestWebhook_PermanentVsTransient(t *testing.T) {
	var status atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(int(status.Load()))
	}))
	t.Cleanup(srv.Close)

	wh := &notifications.Webhook{URL: srv.URL, HTTPClient: srv.Client()}

	status.Store(http.StatusUnauthorized)
	err := wh.Notify(t.Context(), notifications.Event{Severity: notifications.SeverityInfo, Title: "x"})
	var perm *notifications.PermanentStatusError
	if !errors.As(err, &perm) || perm.Status != http.StatusUnauthorized {
		t.Fatalf("got %v, want permanent 401", err)
	}

	status.Store(http.StatusServiceUnavailable)
	err = wh.Notify(t.Context(), notifications.Event{Severity: notifications.SeverityInfo, Title: "x"})
	var trans *notifications.StatusError
	if !errors.As(err, &trans) || trans.Status != http.StatusServiceUnavailable {
		t.Fatalf("got %v, want transient 503", err)
	}
}

func TestFanout_PartialFailureReturnsJoinedErrors(t *testing.T) {
	good := &mockSink{name: "good"}
	bad := &mockSink{name: "bad", err: errors.New("boom")}
	f := &notifications.Fanout{Sinks: []notifications.Notifier{good, bad}}
	err := f.Notify(t.Context(), notifications.Event{Severity: notifications.SeverityInfo, Title: "hello"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "bad") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v", err)
	}
	if good.calls != 1 || bad.calls != 1 {
		t.Fatalf("calls: good=%d bad=%d, want both 1", good.calls, bad.calls)
	}
}

type mockSink struct {
	name  string
	err   error
	calls int
}

func (m *mockSink) Notify(_ context.Context, _ notifications.Event) error {
	m.calls++
	return m.err
}
func (m *mockSink) Name() string { return m.name }
