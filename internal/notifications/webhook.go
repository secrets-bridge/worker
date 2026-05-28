package notifications

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Webhook posts a JSON-encoded Event to a configured URL. Works for
// Slack incoming webhooks, generic ops dashboards, and any service
// that accepts a POST. The wire shape is a flat envelope:
//
//	{
//	  "time": "RFC3339Nano",
//	  "severity": "info|warn|error",
//	  "component": "...",
//	  "title": "...",
//	  "detail": "...",
//	  "metadata": {...}
//	}
//
// For Slack-incoming-webhook compatibility, set FormatSlack=true to
// emit `{"text": "<title> — <detail>"}` instead.
type Webhook struct {
	URL         string
	HTTPClient  *http.Client // optional; defaults to a 10s-timeout client
	FormatSlack bool
	Headers     http.Header // optional extra headers (e.g. signing tokens)
}

// Notify posts the event to the webhook URL. 2xx → nil. 4xx → wrapped
// in retry.Permanent shape so the caller's retry loop bails out (a
// 401 on the webhook won't fix itself by retrying). 5xx + network
// errors → transient.
func (w *Webhook) Notify(ctx context.Context, event Event) error {
	if w.URL == "" {
		return errors.New("notifications: webhook URL is empty")
	}
	cli := w.HTTPClient
	if cli == nil {
		cli = &http.Client{Timeout: 10 * time.Second}
	}

	body, err := w.encode(event)
	if err != nil {
		return fmt.Errorf("notifications: encode webhook body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("notifications: build webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, vv := range w.Headers {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}

	resp, err := cli.Do(req)
	if err != nil {
		return fmt.Errorf("notifications: webhook POST: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// Permanent: re-trying a 401/403/404 won't change the outcome.
		return &PermanentStatusError{Status: resp.StatusCode}
	default:
		return &StatusError{Status: resp.StatusCode}
	}
}

// Name returns "webhook".
func (w *Webhook) Name() string { return "webhook" }

func (w *Webhook) encode(event Event) ([]byte, error) {
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	if w.FormatSlack {
		return json.Marshal(map[string]string{
			"text": fmt.Sprintf("[%s] %s — %s", event.Severity, event.Title, event.Detail),
		})
	}
	return json.Marshal(struct {
		Time      string         `json:"time"`
		Severity  Severity       `json:"severity"`
		Component string         `json:"component"`
		Title     string         `json:"title"`
		Detail    string         `json:"detail,omitempty"`
		Metadata  map[string]any `json:"metadata,omitempty"`
	}{
		Time:      event.Time.UTC().Format(time.RFC3339Nano),
		Severity:  event.Severity,
		Component: event.Component,
		Title:     event.Title,
		Detail:    event.Detail,
		Metadata:  event.Metadata,
	})
}

// StatusError signals a transient (retryable) non-2xx response.
type StatusError struct{ Status int }

func (e *StatusError) Error() string { return fmt.Sprintf("webhook returned HTTP %d (transient)", e.Status) }

// PermanentStatusError signals a 4xx response. Retrying won't help.
type PermanentStatusError struct{ Status int }

func (e *PermanentStatusError) Error() string {
	return fmt.Sprintf("webhook returned HTTP %d (permanent)", e.Status)
}
