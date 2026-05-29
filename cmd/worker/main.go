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

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/keymgmt"
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
	if cfg.WebhookURL != "" {
		notifier = &notifications.Fanout{Sinks: []notifications.Notifier{
			&notifications.NoOp{Logger: logger},
			&notifications.Webhook{URL: cfg.WebhookURL, FormatSlack: cfg.WebhookSlackFormat},
		}}
		logger.Info("notifications: webhook configured",
			"slack_format", cfg.WebhookSlackFormat)
	}

	// Register sweepers.
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registry.MustRegister(collectors.NewGoCollector())
	sched := scheduler.New(rt, logger, registry)

	wrapRepo := storage.NewSecretWraps(pool)
	secretRepo := storage.NewSecrets(pool)
	jobRepo := storage.NewSyncJobs(pool)

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
		},
		Interval: cfg.AgentsStaleInterval,
		Retry:    retry.DefaultPolicy(),
	})
	sched.Register(scheduler.TaskRegistration{
		Task: sweepers.JobsRecovery{
			Pool:     pool,
			Notifier: notifier,
		},
		Interval: cfg.JobsRecoveryInterval,
		Retry:    retry.DefaultPolicy(),
	})

	// GitOps observation poller (BRD §26). OFF by default — must be
	// opt-in via SB_WORKER_GITOPS_ENABLED=true AND the api side must
	// have SB_GITOPS_ENABLED=true. When disabled, the gitops_observations
	// table is never read.
	if cfg.GitOpsEnabled {
		km, err := keymgmt.FromEnv(bootCtx)
		if err != nil {
			logger.Error("gitops poller: KeyManager init failed", "error", err)
			os.Exit(1)
		}
		obsRepo := storage.NewGitOpsObservations(pool)
		endpointRepo := storage.NewArgoCDEndpoints(pool)
		sched.Register(scheduler.TaskRegistration{
			Task: sweepers.GitOpsPoller{
				Observations:  obsRepo,
				Endpoints:     endpointRepo,
				ResolveToken:  sweepers.BuildTokenResolver(km),
				ClientFactory: sweepers.BuildArgoClient,
				Notifier:      notifier,
				BatchSize:     cfg.GitOpsBatchSize,
				HTTPTimeout:   cfg.GitOpsHTTPTimeout,
				ReplicaID:     uuid.New(),
			},
			Interval: cfg.GitOpsPollInterval,
			Retry:    retry.DefaultPolicy(),
		})
		sched.Register(scheduler.TaskRegistration{
			Task: sweepers.GitOpsTimeoutSweeper{
				Repo:     obsRepo,
				Notifier: notifier,
			},
			Interval: cfg.GitOpsTimeoutInterval,
			Retry:    retry.DefaultPolicy(),
		})
		logger.Info("gitops observation poller enabled (BRD §26)",
			"poll_interval", cfg.GitOpsPollInterval,
			"timeout_interval", cfg.GitOpsTimeoutInterval,
			"batch_size", cfg.GitOpsBatchSize,
			"kms_key_id", km.CurrentKeyID(),
		)
	} else {
		logger.Info("gitops observation poller disabled (set SB_WORKER_GITOPS_ENABLED=true to enable)")
	}
	if len(targets) > 0 {
		sched.Register(scheduler.TaskRegistration{
			Task: sweepers.DiscoverScheduler{
				Jobs:     jobRepo,
				Targets:  targets,
				Notifier: notifier,
			},
			Interval: cfg.DiscoverInterval,
			Retry:    retry.DefaultPolicy(),
		})
		logger.Info("discover scheduler configured", "targets", len(targets))
	} else {
		logger.Info("no SB_DISCOVER_TARGETS_JSON configured — discover scheduler disabled")
	}

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
