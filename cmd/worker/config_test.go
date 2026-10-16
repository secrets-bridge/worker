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
	"SB_WORKER_SECRETS_STALE_AFTER",
	"SB_WORKER_AGENTS_STALE_AFTER",
	"SB_WORKER_GITOPS_POLL_INTERVAL",
	"SB_WORKER_GITOPS_TIMEOUT_INTERVAL",
	"SB_WORKER_GITOPS_HTTP_TIMEOUT",
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
		t.Setenv(name, "90s")
	}
	t.Setenv("SB_WORKER_GITOPS_BATCH_SIZE", "5")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.AgentsStaleAfter != 90*time.Second || cfg.GitOpsHTTPTimeout != 90*time.Second || cfg.GitOpsBatchSize != 5 {
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
