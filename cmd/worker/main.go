// Command worker is the Secrets Bridge background worker.
//
// It runs alongside the api as a separate K8s Deployment. Owns the
// periodic sweepers (expired wraps, stale agents, stuck jobs, stale
// discovered secrets) and the discover-job scheduler. Multiple
// replicas can run concurrently — Redis-backed locks ensure each
// sweep fires from exactly one replica per tick.
//
// Hard rules (BRD §15, §24, NFR-08):
//   - Stateless (all state lives in Postgres + Redis)
//   - No secret values logged, audited, or notified
//   - Notifications go to external sinks; treated as a logging surface
//     w.r.t. the "no plaintext on the wire" rule
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/secrets-bridge/api/pkg/runtime"
	"github.com/secrets-bridge/api/pkg/storage"

	"github.com/secrets-bridge/worker/internal/notifications"
	"github.com/secrets-bridge/worker/internal/observability"
	"github.com/secrets-bridge/worker/internal/probes"
	"github.com/secrets-bridge/worker/internal/retry"
	"github.com/secrets-bridge/worker/internal/scheduler"
	"github.com/secrets-bridge/worker/internal/sweepers"
)

// buildVersion is set at link time.
var buildVersion = "dev"

// revealSessionsRetry bounds the retries inside one reveal-sessions
// tick (worst case about 1.8s of backoff). The sweeper ticks every few
// seconds and its self-heal redoes missed work on the next tick, while
// DefaultPolicy's growing backoff (up to 1h per wait, over an hour in
// total) would keep the sweeper lock and stall every later sweep after
// a database blip. WRK-N-01.
func revealSessionsRetry() retry.Policy {
	return retry.Policy{
		InitialDelay: 500 * time.Millisecond,
		MaxDelay:     time.Second,
		MaxAttempts:  3,
	}
}

// gatewaySweepersRetry bounds the retries inside one tick of the
// gateway observation sweepers. Each one redoes any missed work on its
// next tick (seconds to a minute away), so DefaultPolicy's backoff of up
// to an hour would only hold the sweeper lock and stall later ticks
// after a database blip.
func gatewaySweepersRetry() retry.Policy {
	return retry.Policy{
		InitialDelay: 500 * time.Millisecond,
		MaxDelay:     2 * time.Second,
		MaxAttempts:  3,
	}
}

// logRetiredEnv notes the retired settings that are still set. The
// control-plane Argo CD poller is gone: the Gateway is the only
// component that talks to Argo CD, and nothing here turns the poller
// back on. It logs the names only, never the values.
func logRetiredEnv(logger *slog.Logger, set []string) {
	if len(set) == 0 {
		return
	}
	logger.Info("settings no longer used can be removed: the Argo CD poller was removed, the Gateway is the only Argo CD client",
		"env", set)
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		// logger isn't built yet — write to stderr directly.
		_, _ = os.Stderr.WriteString("config error: " + err.Error() + "\n")
		os.Exit(1)
	}
	logger := observability.NewLogger(os.Getenv("LOG_LEVEL"))
	slog.SetDefault(logger)

	logger.Info("starting secrets-bridge worker",
		"version", buildVersion,
		"local_addr", cfg.LocalAddr,
	)
	logRetiredEnv(logger, cfg.RetiredEnvSet)

	bootCtx, cancelBoot := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelBoot()

	// Open Postgres + Redis. Both are required — the worker has no
	// useful mode without either of them.
	storageCfg, err := storage.LoadConfig()
	if err != nil {
		logger.Error("storage config", "error", err)
		os.Exit(1)
	}
	pool, err := storage.Open(bootCtx, storageCfg)
	if err != nil {
		logger.Error("storage open", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	runtimeCfg, err := runtime.LoadConfig()
	if err != nil {
		logger.Error("runtime config", "error", err)
		os.Exit(1)
	}
	rt, err := runtime.Open(bootCtx, runtimeCfg)
	if err != nil {
		logger.Error("runtime open", "error", err)
		os.Exit(1)
	}
	defer func() { _ = rt.Close() }()

	// Parse the discover-targets env var early so a misconfig surfaces
	// at boot, not three sweeper-intervals later.
	targets, err := sweepers.ParseTargets(cfg.DiscoverTargetsJSON)
	if err != nil {
		logger.Error("discover targets", "error", err)
		os.Exit(1)
	}

	// Notifier: webhook if configured, NoOp otherwise.
	var notifier notifications.Notifier = &notifications.NoOp{Logger: logger}
	var webhook notifications.Notifier
	if cfg.WebhookURL != "" {
		webhook = &notifications.Webhook{URL: cfg.WebhookURL, FormatSlack: cfg.WebhookSlackFormat}
		notifier = &notifications.Fanout{Sinks: []notifications.Notifier{
			&notifications.NoOp{Logger: logger},
			webhook,
		}}
		logger.Info("notifications: webhook configured",
			"slack_format", cfg.WebhookSlackFormat)
	}

	// Register sweepers.
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registry.MustRegister(collectors.NewGoCollector())
	registry.MustRegister(sweepers.Collectors()...)
	sched := scheduler.New(rt, logger, registry)

	wrapRepo := storage.NewSecretWraps(pool)
	secretRepo := storage.NewSecrets(pool)
	jobRepo := storage.NewSyncJobs(pool)
	// WRK-N-02: every state flip a sweeper makes is audited as
	// system:worker (hard rule 6).
	auditRepo := storage.NewAuditEvents(pool)

	sched.Register(scheduler.TaskRegistration{
		Task: sweepers.WrapsExpired{
			Repo:     wrapRepo,
			Notifier: notifier,
		},
		Interval: cfg.WrapsExpiredInterval,
		Retry:    retry.DefaultPolicy(),
	})
	sched.Register(scheduler.TaskRegistration{
		Task: sweepers.SecretsStale{
			Repo:       secretRepo,
			Notifier:   notifier,
			StaleAfter: cfg.SecretsStaleAfter,
		},
		Interval: cfg.SecretsStaleInterval,
		Retry:    retry.DefaultPolicy(),
	})
	sched.Register(scheduler.TaskRegistration{
		Task: sweepers.AgentsStale{
			Pool:       pool,
			Notifier:   notifier,
			StaleAfter: cfg.AgentsStaleAfter,
			Audit:      auditRepo,
		},
		Interval: cfg.AgentsStaleInterval,
		Retry:    retry.DefaultPolicy(),
	})
	sched.Register(scheduler.TaskRegistration{
		Task: sweepers.JobsRecovery{
			Pool:     pool,
			Notifier: notifier,
			Audit:    auditRepo,
		},
		Interval: cfg.JobsRecoveryInterval,
		Retry:    retry.DefaultPolicy(),
	})
	// Slice M3 — reveal sessions expired sweeper. Closes the session
	// window for any reveal_sessions row past TTL by advancing the
	// underlying wraps' expires_at so a post-expire retrieve returns
	// ErrExpired even if the SPA still holds the wrap_id.
	revealSessionRepo := storage.NewRevealSessions(pool)
	sched.Register(scheduler.TaskRegistration{
		Task: sweepers.RevealSessionsExpired{
			Sessions: revealSessionRepo,
			Wraps:    wrapRepo,
			Store:    sweepers.RevealSessionStore{DB: pool},
			Audit:    auditRepo,
			Notifier: notifier,
		},
		Interval: cfg.RevealSessionsExpiredInterval,
		Retry:    revealSessionsRetry(),
	})

	// Slice N4 — cross-team fill-window expiry sweeper. Flips
	// pending_values cross_team requests past fill_expires_at to
	// 'expired'. Defense in depth (the api Fill check already rejects
	// late writers); ensures observable state catches up quickly even
	// when no fill attempt arrives.
	accessRequestRepo := storage.NewAccessRequests(pool)
	sched.Register(scheduler.TaskRegistration{
		Task: sweepers.CrossTeamFillExpired{
			Repo:     accessRequestRepo,
			Audit:    auditRepo,
			Notifier: notifier,
		},
		Interval: cfg.CrossTeamFillExpiredInterval,
		Retry:    retry.DefaultPolicy(),
	})

	// Accounts waiting for approval (worker#34). The message names
	// people, so it goes to the webhook alone: the NoOp sink in the
	// fanout would write it to the log. No webhook, nothing to send.
	if webhook != nil {
		sched.Register(scheduler.TaskRegistration{
			Task: sweepers.UsersPending{
				Pool:      pool,
				Cursor:    sweepers.RedisUsersPendingCursor{Client: rt},
				Notifier:  webhook,
				PublicURL: cfg.PublicURL,
			},
			Interval: cfg.UsersPendingInterval,
			Retry:    retry.DefaultPolicy(),
		})
	} else {
		logger.Info("waiting-account messages off (set SB_WORKER_WEBHOOK_URL to send them)")
	}

	// Gateway observation sweepers (worker#38). OFF by default; turn on
	// with SB_WORKER_GATEWAY_SWEEPERS_ENABLED=true whenever any
	// environment's observation source is gateway.
	if cfg.GatewaySweepersEnabled {
		registerGatewaySweepers(sched, cfg, pool, auditRepo, notifier)
		logger.Info("gateway observation sweepers enabled",
			"leases_interval", cfg.ObservationLeasesInterval,
			"deadlines_interval", cfg.ObservationDeadlinesInterval,
			"stale_interval", cfg.GatewaysStaleInterval,
			"stale_after", cfg.GatewaysStaleAfter,
			"replicas_prune_interval", cfg.GatewayReplicasPruneInterval,
			"replicas_retention", cfg.GatewayReplicasRetention,
		)
	} else {
		logger.Info("gateway observation sweepers disabled (set SB_WORKER_GATEWAY_SWEEPERS_ENABLED=true to enable)")
	}

	// Slice P4: DB-backed discover scheduler. Reads from
	// provider_connections via api/pkg/storage; the env-var path
	// (SB_DISCOVER_TARGETS_JSON) is now a deprecated fallback used
	// ONLY when no DB-backed discover target exists at all.
	pcRepo := storage.NewProviderConnections(pool)
	sched.Register(scheduler.TaskRegistration{
		Task: sweepers.DiscoverScheduler{
			Jobs:        jobRepo,
			Connections: sweepers.DiscoverConnections{ProviderConnections: pcRepo, DB: pool},
			Locks:       rt,
			EnvFallback: targets,
			Notifier:    notifier,
			Logger:      logger,
		},
		Interval: cfg.DiscoverInterval,
		Retry:    retry.DefaultPolicy(),
	})
	logger.Info("discover scheduler configured (DB-backed)",
		"env_fallback_targets", len(targets))

	// Probes.
	probeSrv := probes.New(cfg.LocalAddr, registry)
	probeSrv.AddReadinessCheck("postgres", func(ctx context.Context) error {
		return pool.Ping(ctx)
	})
	probeSrv.AddReadinessCheck("redis", func(ctx context.Context) error {
		return rt.Ping(ctx)
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := probeSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("probe server exited", "error", err)
		}
	}()
	probeSrv.SetReady(true)
	logger.Info("worker ready", "addr", cfg.LocalAddr)

	done := sched.Start(ctx)
	<-ctx.Done()
	logger.Info("shutdown signal received; draining scheduler")
	<-done
	logger.Info("scheduler drained; stopping probe server")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()
	_ = probeSrv.Shutdown(shutdownCtx)
	logger.Info("shutdown complete")
}

// registerGatewaySweepers registers the four gateway observation
// sweepers, each under its own Redis lock.
func registerGatewaySweepers(sched *scheduler.Scheduler, cfg Config, pool *storage.Pool, audit *storage.AuditEvents, notifier notifications.Notifier) {
	tasks := storage.NewObservationTasks(pool)
	clock := sweepers.DBClock{Pool: pool}.Now
	sched.Register(scheduler.TaskRegistration{
		Task: sweepers.ObservationLeases{
			Store: tasks, Audit: audit, Notifier: notifier, Clock: clock, BatchSize: cfg.ObservationBatchSize,
		},
		Interval: cfg.ObservationLeasesInterval,
		Retry:    gatewaySweepersRetry(),
	})
	sched.Register(scheduler.TaskRegistration{
		Task: sweepers.ObservationDeadlines{
			Store: tasks, Audit: audit, Notifier: notifier, Clock: clock, BatchSize: cfg.ObservationBatchSize,
		},
		Interval: cfg.ObservationDeadlinesInterval,
		Retry:    gatewaySweepersRetry(),
	})
	sched.Register(scheduler.TaskRegistration{
		Task: sweepers.GatewaysStale{
			Pool: pool, StaleAfter: cfg.GatewaysStaleAfter, Audit: audit, Notifier: notifier,
		},
		Interval: cfg.GatewaysStaleInterval,
		Retry:    gatewaySweepersRetry(),
	})
	sched.Register(scheduler.TaskRegistration{
		Task:     sweepers.GatewayReplicasPrune{Pool: pool, Retention: cfg.GatewayReplicasRetention},
		Interval: cfg.GatewayReplicasPruneInterval,
		Retry:    gatewaySweepersRetry(),
	})
}
