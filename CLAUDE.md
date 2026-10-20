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
| `observation_tasks` (`observation-leases`, `observation-deadlines`), `gateways_stale` (`gateways-stale`, `gateway-replicas-prune`) | Gateway observation queue upkeep: requeue expired leases, time out tasks past their deadline (never failed), flag quiet Gateways stale, prune old replica rows. Enable with `SB_WORKER_GATEWAY_SWEEPERS_ENABLED`; needed whenever any environment's observation source is `gateway` |
| `users_pending` | One webhook message per run for accounts that started waiting for approval; only with a webhook |

## Build and test

This module imports `github.com/secrets-bridge/api/pkg/...` through a `replace` directive pointing at `../api`. Clone `api` next to this repo first.

```bash
go build ./... && go vet ./...
go test -race -count=1 ./...
golangci-lint run
go mod tidy   # CI fails on a diff
```

## Configuration

`DATABASE_URL`, `REDIS_URL`, `SB_ENV`, one `SB_WORKER_<SWEEPER>_INTERVAL` per sweeper, `SB_WORKER_SECRETS_STALE_AFTER`, `SB_WORKER_AGENTS_STALE_AFTER`, the Gateway sweeper knobs (`SB_WORKER_GATEWAY_SWEEPERS_ENABLED`, `SB_WORKER_GATEWAYS_STALE_AFTER`, `SB_WORKER_GATEWAY_REPLICAS_RETENTION`, `SB_WORKER_OBSERVATION_BATCH_SIZE`; floors in `cmd/worker/config.go`, table in the README), optional `SB_WORKER_WEBHOOK_URL`, optional `SB_PUBLIC_URL` (the link in the `users_pending` message).

## Rules for this repo

- Stateless: all state lives in Postgres and Redis.
- Sweepers must be idempotent and safe to run concurrently with other replicas.
- Never log, audit or notify secret values, tokens or row contents. Report counts, cutoffs and sweeper names. The one exception is `users_pending`, which lists the waiting accounts' emails in its webhook message (api#265). It sends to the webhook alone, never the log sink, and never includes display names.
- No provider SDKs here. Anything that touches a provider runs in the agent.
- No Argo CD client here. The Gateway is the only component that talks to Argo CD (gateway#13). `internal/archguard` fails on an import of an Argo CD package or an Argo CD API path in non-test code. `SB_WORKER_GITOPS_*` are retired settings: logged by name if set, never a switch.
- A losing lock is normal (N-1 replicas skip each tick); count it in metrics, do not log it as a warning.
- If a change needs a new query or table in `api`, land the `api` PR first.
- Lock order with the api: a sweeper that locks a `gateways` row and observation tasks together locks the Gateway row first (`FOR NO KEY UPDATE` or `FOR SHARE`), as every api path does. `ExpireLeases` and `ExpireDeadlines` lock task rows only.
- Postgres tests run when `TEST_DATABASE_URL` is set, Redis ones when `TEST_REDIS_URL` is set (both in CI). Run them with `-p 1`: the Postgres tests wipe shared tables.

## Contributing

Feature branch off `main`, conventional commits, small PRs, CI green, then open a PR for maintainer review. Never push to `main`.

This repo is public. Keep account IDs, hostnames, cluster names, real secret paths and company names out of code, commits, PRs and issues.

**No AI attribution.** Commits and PRs describe the change, nothing else. Don't add `Co-Authored-By` trailers for AI assistants, "Generated with ..." lines or links to AI sessions to commit messages, PR titles or PR bodies. A squash merge copies the PR body into the commit on `main`, where it can't be edited later. If you use a coding assistant, turn off its commit and PR attribution in its settings.
