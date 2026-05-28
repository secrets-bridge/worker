package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config carries the worker's runtime configuration. Each knob has a
// safe default — the worker boots with `docker compose up` and only
// SB_DISCOVER_TARGETS_JSON / webhook URL need touching for
// non-trivial setups.
type Config struct {
	LocalAddr      string
	ShutdownGrace  time.Duration

	// Sweep cadences. Defaults err on the side of "won't spam your
	// audit log" — operators tighten them as the deployment scales.
	WrapsExpiredInterval     time.Duration
	SecretsStaleInterval     time.Duration
	AgentsStaleInterval      time.Duration
	JobsRecoveryInterval     time.Duration
	DiscoverInterval         time.Duration

	// Cutoffs: how old a row must be before a sweeper flags it.
	SecretsStaleAfter time.Duration
	AgentsStaleAfter  time.Duration

	// SB_DISCOVER_TARGETS_JSON — see internal/sweepers.ParseTargets.
	DiscoverTargetsJSON string

	// Notifications.
	WebhookURL        string
	WebhookSlackFormat bool
}

// loadConfig reads the env vars and applies defaults. Returns an
// error only for malformed values; missing optional vars take
// defaults.
func loadConfig() (Config, error) {
	cfg := Config{
		LocalAddr:                "127.0.0.1:8091",
		ShutdownGrace:            15 * time.Second,
		WrapsExpiredInterval:     1 * time.Minute,
		SecretsStaleInterval:     5 * time.Minute,
		AgentsStaleInterval:      1 * time.Minute,
		JobsRecoveryInterval:     30 * time.Second,
		DiscoverInterval:         1 * time.Hour,
		SecretsStaleAfter:        24 * time.Hour,
		AgentsStaleAfter:         5 * time.Minute,
	}

	if v := os.Getenv("SB_WORKER_LOCAL_ADDR"); v != "" {
		cfg.LocalAddr = v
	}
	for _, b := range []struct {
		env string
		dst *time.Duration
	}{
		{"SB_WORKER_SHUTDOWN_GRACE", &cfg.ShutdownGrace},
		{"SB_WORKER_WRAPS_EXPIRED_INTERVAL", &cfg.WrapsExpiredInterval},
		{"SB_WORKER_SECRETS_STALE_INTERVAL", &cfg.SecretsStaleInterval},
		{"SB_WORKER_AGENTS_STALE_INTERVAL", &cfg.AgentsStaleInterval},
		{"SB_WORKER_JOBS_RECOVERY_INTERVAL", &cfg.JobsRecoveryInterval},
		{"SB_WORKER_DISCOVER_INTERVAL", &cfg.DiscoverInterval},
		{"SB_WORKER_SECRETS_STALE_AFTER", &cfg.SecretsStaleAfter},
		{"SB_WORKER_AGENTS_STALE_AFTER", &cfg.AgentsStaleAfter},
	} {
		if v := os.Getenv(b.env); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				return Config{}, fmt.Errorf("config: %s: %w", b.env, err)
			}
			*b.dst = d
		}
	}
	cfg.DiscoverTargetsJSON = os.Getenv("SB_DISCOVER_TARGETS_JSON")
	cfg.WebhookURL = os.Getenv("SB_WORKER_WEBHOOK_URL")
	if v := os.Getenv("SB_WORKER_WEBHOOK_SLACK_FORMAT"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: SB_WORKER_WEBHOOK_SLACK_FORMAT: %w", err)
		}
		cfg.WebhookSlackFormat = b
	}
	return cfg, nil
}
