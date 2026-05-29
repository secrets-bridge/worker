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
// Deployment mode. Mirrors the api repo's SB_ENV — production is the
// safe default. Worker only consults it to forward into
// keymgmt.FromEnv when the GitOps poller initializes its KeyManager;
// the rest of the worker is unaffected by mode today.
const (
	ModeDev        = "dev"
	ModeProduction = "production"
)

type Config struct {
	LocalAddr      string
	ShutdownGrace  time.Duration

	// Env is the deployment mode (SB_ENV). Recognised: "dev" or
	// "production"; default "production" so a missing/forgotten env
	// fails closed against LocalKMS in the api-shared keymgmt
	// resolver.
	Env string

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

	// GitOps observation poller (BRD §26). Opt-in via
	// SB_WORKER_GITOPS_ENABLED=true so disabled deployments don't
	// register the sweeper and don't even open the argocd_endpoints
	// repo. When the api side has SB_GITOPS_ENABLED=false the worker
	// MUST also be off — otherwise the poller would scan a table that
	// will never receive rows.
	GitOpsEnabled        bool
	GitOpsPollInterval   time.Duration // default 15s during active rollout
	GitOpsTimeoutInterval time.Duration // default 1m for the timeout sweeper
	GitOpsBatchSize      int            // observations per tick; default 20
	GitOpsHTTPTimeout    time.Duration  // per-ArgoCD-call ceiling; default 15s
}

// loadConfig reads the env vars and applies defaults. Returns an
// error only for malformed values; missing optional vars take
// defaults.
func loadConfig() (Config, error) {
	cfg := Config{
		LocalAddr:                "127.0.0.1:8091",
		ShutdownGrace:            15 * time.Second,
		Env:                      ModeProduction,
		WrapsExpiredInterval:     1 * time.Minute,
		SecretsStaleInterval:     5 * time.Minute,
		AgentsStaleInterval:      1 * time.Minute,
		JobsRecoveryInterval:     30 * time.Second,
		DiscoverInterval:         1 * time.Hour,
		SecretsStaleAfter:        24 * time.Hour,
		AgentsStaleAfter:         5 * time.Minute,
		GitOpsPollInterval:       15 * time.Second,
		GitOpsTimeoutInterval:    1 * time.Minute,
		GitOpsBatchSize:          20,
		GitOpsHTTPTimeout:        15 * time.Second,
	}

	if v := os.Getenv("SB_WORKER_LOCAL_ADDR"); v != "" {
		cfg.LocalAddr = v
	}
	if v := os.Getenv("SB_ENV"); v != "" {
		switch v {
		case ModeDev, ModeProduction:
			cfg.Env = v
		default:
			return Config{}, fmt.Errorf("config: SB_ENV=%q is not recognised (allowed: %s, %s)", v, ModeDev, ModeProduction)
		}
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
		{"SB_WORKER_GITOPS_POLL_INTERVAL", &cfg.GitOpsPollInterval},
		{"SB_WORKER_GITOPS_TIMEOUT_INTERVAL", &cfg.GitOpsTimeoutInterval},
		{"SB_WORKER_GITOPS_HTTP_TIMEOUT", &cfg.GitOpsHTTPTimeout},
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
	if v := os.Getenv("SB_WORKER_GITOPS_ENABLED"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: SB_WORKER_GITOPS_ENABLED: %w", err)
		}
		cfg.GitOpsEnabled = b
	}
	if v := os.Getenv("SB_WORKER_GITOPS_BATCH_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: SB_WORKER_GITOPS_BATCH_SIZE: %w", err)
		}
		cfg.GitOpsBatchSize = n
	}
	return cfg, nil
}
