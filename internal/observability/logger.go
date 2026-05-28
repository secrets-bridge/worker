// Package observability builds the structured logger used across the
// worker. Mirrors the api repo's shape so a future operator dashboard
// can grep for the same field names.
package observability

import (
	"log/slog"
	"os"
	"strings"
)

// NewLogger returns a JSON slog logger writing to stderr at the level
// parsed from levelStr ("debug" / "info" / "warn" / "error"; empty →
// info).
func NewLogger(levelStr string) *slog.Logger {
	var level slog.Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	}))
}
