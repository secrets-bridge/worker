package notifications

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"
)

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestErrorClass(t *testing.T) {
	refused := &url.Error{Op: "Post", URL: "https://hooks.example.com/services/T000/B000/secret", Err: errors.New("connection refused")}
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"4xx", fmt.Errorf("wrapped: %w", &PermanentStatusError{Status: 403}), "http_4xx"},
		{"5xx", &StatusError{Status: 502}, "http_5xx"},
		{"canceled", fmt.Errorf("post: %w", context.Canceled), "canceled"},
		{"deadline", context.DeadlineExceeded, "timeout"},
		{"net timeout", &url.Error{Op: "Post", URL: "https://hooks.example.com/x", Err: timeoutErr{}}, "timeout"},
		{"transport", fmt.Errorf("notifications: webhook POST: %w", refused), "transport"},
		{"fanout join", errors.Join(sinkErr{sink: "webhook", err: &StatusError{Status: 500}}), "http_5xx"},
		{"other", errors.New("encode failed"), "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ErrorClass(tc.err); got != tc.want {
				t.Fatalf("ErrorClass(%v) = %q want %q", tc.err, got, tc.want)
			}
		})
	}
}
