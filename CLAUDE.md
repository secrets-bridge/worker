# CLAUDE.md: secrets-bridge/worker

Instructions for Claude Code and contributors working in this repo. For the whole platform, see https://secrets-bridge.io.

## What this repo is

The Secrets Bridge background worker. It runs periodic sweepers against the control plane's Postgres and Redis. Each sweeper takes its own Redis lock per tick, so running several replicas is safe.

## Layout

| Path | Purpose |
|---|---|
| `cmd/worker` | Config, wiring, sweeper registration |
| `internal/sweepers` | One file per sweeper (below) |
| `internal/scheduler` | Leader-elected periodic runner with lock renewal and metrics |
| `internal/retry` | Exponential backoff with a cap and a `Permanent` shortcut |
| `internal/notifications` | `Notifier` interface: no-op, webhook (optional Slack format), fan-out |
| `internal/probes`, `internal/observability` | Loopback health/metrics server, JSON logging |

| Sweeper | Job |
|---|---|
| `wraps_expired` | Deletes expired, unopened wraps |
| `reveal_sessions_expired` | Closes reveal sessions past their TTL |
| `cross_team_fill_expired` | Expires cross-team requests whose fill window passed |
| `secrets_stale` | Marks catalog rows `missing` when discovery stops seeing them |
| `agents_stale` | Marks agents stale after missed heartbeats |
| `jobs_recovery` | Flags jobs stuck in a claimed state |
| `discover_scheduler` | Enqueues discovery jobs |
| `gitops_poller`, `gitops_timeout` | Optional read-only ArgoCD observation; enable with `SB_WORKER_GITOPS_ENABLED` (must match the api setting) |

## Build and test

This module imports `github.com/secrets-bridge/api/pkg/...` through a `replace` directive pointing at `../api`. Clone `api` next to this repo first.

```bash
go build ./... && go vet ./...
go test -race -count=1 ./...
golangci-lint run
go mod tidy   # CI fails on a diff
```

## Configuration

`DATABASE_URL`, `REDIS_URL`, `SB_ENV`, one `SB_WORKER_<SWEEPER>_INTERVAL` per sweeper, `SB_WORKER_SECRETS_STALE_AFTER`, `SB_WORKER_AGENTS_STALE_AFTER`, optional `SB_WORKER_WEBHOOK_URL`.

## Rules for this repo

- Stateless: all state lives in Postgres and Redis.
- Sweepers must be idempotent and safe to run concurrently with other replicas.
- Never log, audit or notify secret values, tokens or row contents. Report counts, cutoffs and sweeper names.
- No provider SDKs here. Anything that touches a provider runs in the agent.
- A losing lock is normal (N-1 replicas skip each tick); count it in metrics, do not log it as a warning.
- If a change needs a new query or table in `api`, land the `api` PR first.

## Contributing

Feature branch off `main`, conventional commits, small PRs, CI green, then open a PR for maintainer review. Never push to `main`.

This repo is public. Keep account IDs, hostnames, cluster names, real secret paths and company names out of code, commits, PRs and issues.
