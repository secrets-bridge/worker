package notifications

import (
	"context"
	"errors"
	"net"
)

// ErrorClass maps a Notify error to a short, payload-free label for
// logs and metrics. The error text itself is never safe to log: a
// webhook transport error embeds the full URL, and incoming-webhook
// URLs carry their secret in the path.
func ErrorClass(err error) string {
	var (
		permanent *PermanentStatusError
		transient *StatusError
		netErr    net.Error
	)
	switch {
	case err == nil:
		return ""
	case errors.As(err, &permanent):
		return "http_4xx"
	case errors.As(err, &transient):
		return "http_5xx"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.As(err, &netErr):
		if netErr.Timeout() {
			return "timeout"
		}
		return "transport"
	default:
		return "other"
	}
}
