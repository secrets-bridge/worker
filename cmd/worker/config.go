package main

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config carries the worker's runtime configuration. Each knob has a
// safe default — the worker boots with `docker compose up` and only
// SB_DISCOVER_TARGETS_JSON / webhook URL need touching for
// non-trivial setups.
// Deployment mode. Mirrors the api repo's SB_ENV; production is the
// safe default. The worker consults it for the SB_PUBLIC_URL rule only.
const (
	ModeDev        = "dev"
	ModeProduction = "production"
)

type Config struct {
	LocalAddr     string
	ShutdownGrace time.Duration

	// Env is the deployment mode (SB_ENV). Recognised: "dev" or
	// "production"; default "production".
	Env string

	// Sweep cadences. Defaults err on the side of "won't spam your
	// audit log" — operators tighten them as the deployment scales.
	WrapsExpiredInterval          time.Duration
	SecretsStaleInterval          time.Duration
	AgentsStaleInterval           time.Duration
	JobsRecoveryInterval          time.Duration
	DiscoverInterval              time.Duration
	RevealSessionsExpiredInterval time.Duration
	CrossTeamFillExpiredInterval  time.Duration
	UsersPendingInterval          time.Duration

	// Cutoffs: how old a row must be before a sweeper flags it.
	SecretsStaleAfter time.Duration
	AgentsStaleAfter  time.Duration

	// SB_DISCOVER_TARGETS_JSON — see internal/sweepers.ParseTargets.
	DiscoverTargetsJSON string

	// Notifications.
	WebhookURL         string
	WebhookSlackFormat bool

	// PublicURL is SB_PUBLIC_URL, the address people open the console
	// with, without a trailing slash. The users-pending message links
	// to Admin → Users with it. Unset, the address saved in the console
	// is used, as in the api.
	PublicURL string

	// RetiredEnvSet names the retired settings that are still set
	// (retiredEnvSet). Names only, never values.
	RetiredEnvSet []string

	// Gateway observation sweepers (worker#38): observation-leases,
	// observation-deadlines, gateways-stale and gateway-replicas-prune.
	// Opt-in via SB_WORKER_GATEWAY_SWEEPERS_ENABLED=true. Needed
	// whenever any environment's observation source is gateway. Off,
	// the gateway and observation tables are never touched.
	GatewaySweepersEnabled       bool
	ObservationLeasesInterval    time.Duration // default 30s
	ObservationDeadlinesInterval time.Duration // default 30s
	ObservationBatchSize         int           // rows per statement; default 200
	GatewaysStaleInterval        time.Duration // default 1m
	GatewaysStaleAfter           time.Duration // default 2m (four missed 30s heartbeats)
	GatewayReplicasPruneInterval time.Duration // default 1h
	GatewayReplicasRetention     time.Duration // default 24h
}

// Floors for the gateway sweepers. The Gateway heartbeats every 30s
// (api gatewayHeartbeatIntervalSec), so a stale cutoff under a minute
// would flag a healthy Gateway after one late heartbeat. The api counts
// replicas seen within the last 10 minutes towards max_replicas
// (services.GatewayReplicaWindow); pruning inside that window would let
// a Gateway exceed the cap. The interval floors keep the sweeps from
// hammering Postgres.
const (
	minObservationSweepInterval    = 5 * time.Second
	minGatewaysStaleInterval       = 10 * time.Second
	minGatewaysStaleAfter          = time.Minute
	minGatewayReplicasPruneEvery   = time.Minute
	minGatewayReplicasRetention    = 10 * time.Minute
	maxObservationBatchSize        = 1000
	defaultObservationBatchSizeCfg = 200
)

// loadConfig reads the env vars and applies defaults. Returns an
// error only for malformed values; missing optional vars take
// defaults.
func loadConfig() (Config, error) {
	cfg := Config{
		LocalAddr:            "127.0.0.1:8091",
		ShutdownGrace:        15 * time.Second,
		Env:                  ModeProduction,
		WrapsExpiredInterval: 1 * time.Minute,
		SecretsStaleInterval: 5 * time.Minute,
		AgentsStaleInterval:  1 * time.Minute,
		JobsRecoveryInterval: 30 * time.Second,
		DiscoverInterval:     1 * time.Hour,
		// Reveal sessions are user-facing — a session ending late means
		// a wrap could still be retrievable after the SPA already nuked
		// its plaintext display. 5s keeps the gap close to invisible
		// without thrashing Postgres (the SweepExpired query is a
		// single indexed UPDATE … RETURNING).
		RevealSessionsExpiredInterval: 5 * time.Second,
		// fill_ttl_seconds is measured in hours (workflow default
		// 24h); 30s precision is comfortable + matches the natural
		// cadence of non-user-facing sweepers. The api service-layer
		// Fill check rejects late writers anyway via
		// ErrFillWindowExpired, so this sweeper is defense in depth.
		CrossTeamFillExpiredInterval: 30 * time.Second,
		// One message per run at most, so this is also the most often
		// admins hear about new accounts.
		UsersPendingInterval: 1 * time.Minute,
		SecretsStaleAfter:    24 * time.Hour,
		AgentsStaleAfter:     5 * time.Minute,

		ObservationLeasesInterval:    30 * time.Second,
		ObservationDeadlinesInterval: 30 * time.Second,
		ObservationBatchSize:         defaultObservationBatchSizeCfg,
		GatewaysStaleInterval:        time.Minute,
		GatewaysStaleAfter:           2 * time.Minute,
		GatewayReplicasPruneInterval: time.Hour,
		GatewayReplicasRetention:     24 * time.Hour,
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
		{"SB_WORKER_REVEAL_SESSIONS_EXPIRED_INTERVAL", &cfg.RevealSessionsExpiredInterval},
		{"SB_WORKER_CROSS_TEAM_FILL_EXPIRED_INTERVAL", &cfg.CrossTeamFillExpiredInterval},
		{"SB_WORKER_USERS_PENDING_INTERVAL", &cfg.UsersPendingInterval},
		{"SB_WORKER_SECRETS_STALE_AFTER", &cfg.SecretsStaleAfter},
		{"SB_WORKER_AGENTS_STALE_AFTER", &cfg.AgentsStaleAfter},
		{"SB_WORKER_OBSERVATION_LEASES_INTERVAL", &cfg.ObservationLeasesInterval},
		{"SB_WORKER_OBSERVATION_DEADLINES_INTERVAL", &cfg.ObservationDeadlinesInterval},
		{"SB_WORKER_GATEWAYS_STALE_INTERVAL", &cfg.GatewaysStaleInterval},
		{"SB_WORKER_GATEWAYS_STALE_AFTER", &cfg.GatewaysStaleAfter},
		{"SB_WORKER_GATEWAY_REPLICAS_PRUNE_INTERVAL", &cfg.GatewayReplicasPruneInterval},
		{"SB_WORKER_GATEWAY_REPLICAS_RETENTION", &cfg.GatewayReplicasRetention},
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
	if v := os.Getenv("SB_PUBLIC_URL"); v != "" {
		u, err := normalizePublicURL(v, cfg.Env == ModeDev)
		if err != nil {
			return Config{}, fmt.Errorf("config: SB_PUBLIC_URL %w", err)
		}
		cfg.PublicURL = u
	}
	if v := os.Getenv("SB_WORKER_GATEWAY_SWEEPERS_ENABLED"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: SB_WORKER_GATEWAY_SWEEPERS_ENABLED: %w", err)
		}
		cfg.GatewaySweepersEnabled = b
	}
	if v := os.Getenv("SB_WORKER_OBSERVATION_BATCH_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: SB_WORKER_OBSERVATION_BATCH_SIZE: %w", err)
		}
		cfg.ObservationBatchSize = n
	}
	cfg.RetiredEnvSet = retiredEnvSet()
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// retiredEnv are settings the worker no longer reads. They configured
// the control-plane Argo CD poller, removed when the Gateway became the
// only component that talks to Argo CD (secrets-bridge/gateway#13).
// Setting one does nothing: there is no switch to turn the poller back
// on.
var retiredEnv = []string{
	"SB_WORKER_GITOPS_ENABLED",
	"SB_WORKER_GITOPS_POLL_INTERVAL",
	"SB_WORKER_GITOPS_TIMEOUT_INTERVAL",
	"SB_WORKER_GITOPS_BATCH_SIZE",
	"SB_WORKER_GITOPS_HTTP_TIMEOUT",
}

// retiredEnvSet returns the names of the retiredEnv entries that are
// set to a non-empty value. It never returns or reads out the values.
func retiredEnvSet() []string {
	var set []string
	for _, k := range retiredEnv {
		if v, ok := os.LookupEnv(k); ok && v != "" {
			set = append(set, k)
		}
	}
	return set
}

// normalizePublicURL is the api's rule for SB_PUBLIC_URL
// (services.NormalizePublicURL), so a value the api accepts works here
// and one it refuses stops the worker too: https, or plain http for
// localhost or a loopback address only with SB_ENV=dev (dev); no user,
// query or fragment; no trailing slash.
func normalizePublicURL(raw string, dev bool) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return "", errors.New("must be an absolute address such as https://secrets.example.com")
	}
	host := u.Hostname()
	ip := net.ParseIP(host)
	loopback := host == "localhost" || (ip != nil && ip.IsLoopback())
	switch {
	case u.Scheme == "https":
	case u.Scheme == "http" && loopback && dev:
	default:
		return "", errors.New("must use https (http is allowed for localhost with SB_ENV=dev only)")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("must not carry user info, a query or a fragment")
	}
	return strings.TrimRight(u.Scheme+"://"+u.Host+u.EscapedPath(), "/"), nil
}

// minUsersPendingInterval keeps the waiting-account messages from
// turning into a stream: at most one message per run.
const minUsersPendingInterval = 30 * time.Second

// validate rejects values that would break the worker after it already
// reported ready: time.NewTicker panics on a non-positive interval, a
// non-positive *_STALE_AFTER moves the cutoff to now or later (every
// row goes stale).
// Checked for every knob, including the Gateway ones while those
// sweepers are off, so turning them on later can't surface a latent bad
// value. WRK-N-06.
func (c Config) validate() error {
	for _, d := range []struct {
		env string
		val time.Duration
	}{
		{"SB_WORKER_SHUTDOWN_GRACE", c.ShutdownGrace},
		{"SB_WORKER_WRAPS_EXPIRED_INTERVAL", c.WrapsExpiredInterval},
		{"SB_WORKER_SECRETS_STALE_INTERVAL", c.SecretsStaleInterval},
		{"SB_WORKER_AGENTS_STALE_INTERVAL", c.AgentsStaleInterval},
		{"SB_WORKER_JOBS_RECOVERY_INTERVAL", c.JobsRecoveryInterval},
		{"SB_WORKER_DISCOVER_INTERVAL", c.DiscoverInterval},
		{"SB_WORKER_REVEAL_SESSIONS_EXPIRED_INTERVAL", c.RevealSessionsExpiredInterval},
		{"SB_WORKER_CROSS_TEAM_FILL_EXPIRED_INTERVAL", c.CrossTeamFillExpiredInterval},
		{"SB_WORKER_USERS_PENDING_INTERVAL", c.UsersPendingInterval},
		{"SB_WORKER_SECRETS_STALE_AFTER", c.SecretsStaleAfter},
		{"SB_WORKER_AGENTS_STALE_AFTER", c.AgentsStaleAfter},
		{"SB_WORKER_OBSERVATION_LEASES_INTERVAL", c.ObservationLeasesInterval},
		{"SB_WORKER_OBSERVATION_DEADLINES_INTERVAL", c.ObservationDeadlinesInterval},
		{"SB_WORKER_GATEWAYS_STALE_INTERVAL", c.GatewaysStaleInterval},
		{"SB_WORKER_GATEWAYS_STALE_AFTER", c.GatewaysStaleAfter},
		{"SB_WORKER_GATEWAY_REPLICAS_PRUNE_INTERVAL", c.GatewayReplicasPruneInterval},
		{"SB_WORKER_GATEWAY_REPLICAS_RETENTION", c.GatewayReplicasRetention},
	} {
		if d.val <= 0 {
			return fmt.Errorf("config: %s must be greater than zero (got %s)", d.env, d.val)
		}
	}
	if c.UsersPendingInterval < minUsersPendingInterval {
		return fmt.Errorf("config: SB_WORKER_USERS_PENDING_INTERVAL must be at least %s (got %s)", minUsersPendingInterval, c.UsersPendingInterval)
	}
	for _, f := range []struct {
		env      string
		val, min time.Duration
	}{
		{"SB_WORKER_OBSERVATION_LEASES_INTERVAL", c.ObservationLeasesInterval, minObservationSweepInterval},
		{"SB_WORKER_OBSERVATION_DEADLINES_INTERVAL", c.ObservationDeadlinesInterval, minObservationSweepInterval},
		{"SB_WORKER_GATEWAYS_STALE_INTERVAL", c.GatewaysStaleInterval, minGatewaysStaleInterval},
		{"SB_WORKER_GATEWAYS_STALE_AFTER", c.GatewaysStaleAfter, minGatewaysStaleAfter},
		{"SB_WORKER_GATEWAY_REPLICAS_PRUNE_INTERVAL", c.GatewayReplicasPruneInterval, minGatewayReplicasPruneEvery},
		{"SB_WORKER_GATEWAY_REPLICAS_RETENTION", c.GatewayReplicasRetention, minGatewayReplicasRetention},
	} {
		if f.val < f.min {
			return fmt.Errorf("config: %s must be at least %s (got %s)", f.env, f.min, f.val)
		}
	}
	if c.ObservationBatchSize < 1 || c.ObservationBatchSize > maxObservationBatchSize {
		return fmt.Errorf("config: SB_WORKER_OBSERVATION_BATCH_SIZE must be between 1 and %d (got %d)", maxObservationBatchSize, c.ObservationBatchSize)
	}
	return nil
}
