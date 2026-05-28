package notifications

import (
	"context"
	"log/slog"
)

// NoOp is a Notifier that records nothing externally; it just logs
// the event at the matching slog level. Used when no real sink is
// configured — keeps the worker's Notify calls safe to make
// unconditionally.
type NoOp struct {
	Logger *slog.Logger
}

// Notify logs the event at info / warn / error per its severity.
func (n *NoOp) Notify(_ context.Context, event Event) error {
	if n.Logger == nil {
		return nil
	}
	args := []any{
		"component", event.Component,
		"title", event.Title,
	}
	if event.Detail != "" {
		args = append(args, "detail", event.Detail)
	}
	for k, v := range event.Metadata {
		args = append(args, k, v)
	}
	switch event.Severity {
	case SeverityWarn:
		n.Logger.Warn("notification", args...)
	case SeverityError:
		n.Logger.Error("notification", args...)
	default:
		n.Logger.Info("notification", args...)
	}
	return nil
}

// Name returns "noop".
func (n *NoOp) Name() string { return "noop" }
