package main

import (
	"strings"
	"testing"
	"time"
)

// durationVars is every duration knob loadConfig reads.
var durationVars = []string{
	"SB_WORKER_SHUTDOWN_GRACE",
	"SB_WORKER_WRAPS_EXPIRED_INTERVAL",
	"SB_WORKER_SECRETS_STALE_INTERVAL",
	"SB_WORKER_AGENTS_STALE_INTERVAL",
	"SB_WORKER_JOBS_RECOVERY_INTERVAL",
	"SB_WORKER_DISCOVER_INTERVAL",
	"SB_WORKER_REVEAL_SESSIONS_EXPIRED_INTERVAL",
	"SB_WORKER_CROSS_TEAM_FILL_EXPIRED_INTERVAL",
	"SB_WORKER_USERS_PENDING_INTERVAL",
	"SB_WORKER_SECRETS_STALE_AFTER",
	"SB_WORKER_AGENTS_STALE_AFTER",
	"SB_WORKER_GITOPS_POLL_INTERVAL",
	"SB_WORKER_GITOPS_TIMEOUT_INTERVAL",
	"SB_WORKER_GITOPS_HTTP_TIMEOUT",
	"SB_WORKER_OBSERVATION_LEASES_INTERVAL",
	"SB_WORKER_OBSERVATION_DEADLINES_INTERVAL",
	"SB_WORKER_GATEWAYS_STALE_INTERVAL",
	"SB_WORKER_GATEWAYS_STALE_AFTER",
	"SB_WORKER_GATEWAY_REPLICAS_PRUNE_INTERVAL",
	"SB_WORKER_GATEWAY_REPLICAS_RETENTION",
}

// floorVars are the duration knobs with a floor above 90s, which the
// override test sets to a value that clears it.
var floorVars = map[string]string{
	"SB_WORKER_GATEWAY_REPLICAS_RETENTION": "15m",
}

func TestLoadConfig_DefaultsAreValid(t *testing.T) {
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig with no env: %v", err)
	}
	if cfg.AgentsStaleAfter != 5*time.Minute || cfg.RevealSessionsExpiredInterval != 5*time.Second {
		t.Fatalf("defaults changed: %+v", cfg)
	}
}

func TestLoadConfig_PositiveOverridesAccepted(t *testing.T) {
	for _, name := range durationVars {
		if v, ok := floorVars[name]; ok {
			t.Setenv(name, v)
			continue
		}
		t.Setenv(name, "90s")
	}
	t.Setenv("SB_WORKER_GITOPS_BATCH_SIZE", "5")
	t.Setenv("SB_WORKER_OBSERVATION_BATCH_SIZE", "50")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.AgentsStaleAfter != 90*time.Second || cfg.GitOpsHTTPTimeout != 90*time.Second || cfg.GitOpsBatchSize != 5 ||
		cfg.GatewaysStaleAfter != 90*time.Second || cfg.GatewayReplicasRetention != 15*time.Minute || cfg.ObservationBatchSize != 50 {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
}

// WRK-N-06: a zero or negative interval panics time.NewTicker after the
// worker already reported ready, a negative *_STALE_AFTER puts the
// cutoff in the future (every row goes stale), and a negative HTTP
// timeout disables the timeout. All of them must stop the boot with a
// message naming the variable.
func TestLoadConfig_RejectsNonPositiveDurations(t *testing.T) {
	for _, name := range durationVars {
		for _, bad := range []string{"0s", "-1s", "-24h"} {
			t.Run(name+"="+bad, func(t *testing.T) {
				t.Setenv(name, bad)
				_, err := loadConfig()
				if err == nil {
					t.Fatalf("loadConfig accepted %s=%s", name, bad)
				}
				if !strings.Contains(err.Error(), name) {
					t.Fatalf("error %q does not name %s", err, name)
				}
			})
		}
	}
}

func TestLoadConfig_RejectsNonPositiveBatchSize(t *testing.T) {
	for _, bad := range []string{"0", "-5"} {
		t.Run(bad, func(t *testing.T) {
			t.Setenv("SB_WORKER_GITOPS_BATCH_SIZE", bad)
			_, err := loadConfig()
			if err == nil || !strings.Contains(err.Error(), "SB_WORKER_GITOPS_BATCH_SIZE") {
				t.Fatalf("err = %v want a SB_WORKER_GITOPS_BATCH_SIZE bounds error", err)
			}
		})
	}
}

// The users-pending message links to <SB_PUBLIC_URL>/admin/users, so a
// value that can't make a plain link stops the boot, like in the api.
func TestLoadConfig_PublicURL(t *testing.T) {
	t.Setenv("SB_PUBLIC_URL", "https://secrets.example.com/")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.PublicURL != "https://secrets.example.com" {
		t.Fatalf("PublicURL = %q, want the address without the trailing slash", cfg.PublicURL)
	}
	t.Setenv("SB_PUBLIC_URL", " https://secrets.example.com/console/ ")
	if cfg, err := loadConfig(); err != nil || cfg.PublicURL != "https://secrets.example.com/console" {
		t.Errorf("loadConfig = %q, %v; want the normalized address", cfg.PublicURL, err)
	}
	// Plain http for localhost and loopback only with SB_ENV=dev, as in
	// the api.
	for in, want := range map[string]string{"http://localhost:5173/": "http://localhost:5173", "http://127.0.0.1:8080": "http://127.0.0.1:8080", "http://[::1]:8080": "http://[::1]:8080"} {
		t.Setenv("SB_PUBLIC_URL", in)
		t.Setenv("SB_ENV", ModeDev)
		if cfg, err := loadConfig(); err != nil || cfg.PublicURL != want {
			t.Errorf("loadConfig(%q) in dev = %q, %v; want %q", in, cfg.PublicURL, err, want)
		}
		t.Setenv("SB_ENV", ModeProduction)
		if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "SB_PUBLIC_URL") {
			t.Errorf("loadConfig(%q) in production = %v; want an error naming SB_PUBLIC_URL", in, err)
		}
	}
	t.Setenv("SB_ENV", "")
	for _, bad := range []string{"secrets.example.com", "javascript:alert(1)", "http://secrets.example.com", "https://user@secrets.example.com", "https://secrets.example.com/?x=1", "https://secrets.example.com/#x"} {
		t.Run(bad, func(t *testing.T) {
			t.Setenv("SB_PUBLIC_URL", bad)
			if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "SB_PUBLIC_URL") {
				t.Fatalf("loadConfig(%q) = %v, want an error naming SB_PUBLIC_URL", bad, err)
			}
		})
	}
}

// At most one message per run, so a tiny interval would stream them.
func TestLoadConfig_UsersPendingIntervalFloor(t *testing.T) {
	t.Setenv("SB_WORKER_USERS_PENDING_INTERVAL", "10s")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "SB_WORKER_USERS_PENDING_INTERVAL") {
		t.Fatalf("loadConfig accepted a 10s interval: %v", err)
	}
	t.Setenv("SB_WORKER_USERS_PENDING_INTERVAL", "30s")
	if _, err := loadConfig(); err != nil {
		t.Fatalf("loadConfig refused 30s: %v", err)
	}
}

// The gateway sweepers are off unless asked for, and their defaults are
// the documented ones.
func TestLoadConfig_GatewaySweepersDefaults(t *testing.T) {
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.GatewaySweepersEnabled {
		t.Fatal("gateway sweepers on by default")
	}
	if cfg.ObservationLeasesInterval != 30*time.Second || cfg.ObservationDeadlinesInterval != 30*time.Second ||
		cfg.GatewaysStaleInterval != time.Minute || cfg.GatewaysStaleAfter != 2*time.Minute ||
		cfg.GatewayReplicasPruneInterval != time.Hour || cfg.GatewayReplicasRetention != 24*time.Hour ||
		cfg.ObservationBatchSize != 200 {
		t.Fatalf("gateway sweeper defaults changed: %+v", cfg)
	}
	t.Setenv("SB_WORKER_GATEWAY_SWEEPERS_ENABLED", "true")
	if cfg, err := loadConfig(); err != nil || !cfg.GatewaySweepersEnabled {
		t.Fatalf("enable = %v, %v", cfg.GatewaySweepersEnabled, err)
	}
	t.Setenv("SB_WORKER_GATEWAY_SWEEPERS_ENABLED", "maybe")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "SB_WORKER_GATEWAY_SWEEPERS_ENABLED") {
		t.Fatalf("bad bool accepted: %v", err)
	}
}

// Below the floor a gateway knob stops the boot, at the floor it is
// accepted. A stale cutoff under two heartbeats or a retention inside
// the api's replica window would misbehave quietly.
func TestLoadConfig_GatewaySweeperFloors(t *testing.T) {
	for name, c := range map[string]struct{ below, at string }{
		"SB_WORKER_OBSERVATION_LEASES_INTERVAL":     {"4s", "5s"},
		"SB_WORKER_OBSERVATION_DEADLINES_INTERVAL":  {"4s", "5s"},
		"SB_WORKER_GATEWAYS_STALE_INTERVAL":         {"9s", "10s"},
		"SB_WORKER_GATEWAYS_STALE_AFTER":            {"59s", "1m"},
		"SB_WORKER_GATEWAY_REPLICAS_PRUNE_INTERVAL": {"59s", "1m"},
		"SB_WORKER_GATEWAY_REPLICAS_RETENTION":      {"9m", "10m"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, c.below)
			if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("%s=%s accepted: %v", name, c.below, err)
			}
			t.Setenv(name, c.at)
			if _, err := loadConfig(); err != nil {
				t.Fatalf("%s=%s refused: %v", name, c.at, err)
			}
		})
	}
	for _, bad := range []string{"0", "-1", "1001", "x"} {
		t.Run("batch="+bad, func(t *testing.T) {
			t.Setenv("SB_WORKER_OBSERVATION_BATCH_SIZE", bad)
			if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "SB_WORKER_OBSERVATION_BATCH_SIZE") {
				t.Fatalf("batch %s accepted: %v", bad, err)
			}
		})
	}
}
