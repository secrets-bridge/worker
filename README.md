<p align="center">
  <a href="https://github.com/secrets-bridge"><img src="https://raw.githubusercontent.com/secrets-bridge/.github/main/profile/logo.svg" alt="Secrets Bridge" width="520" /></a>
</p>

<p align="center">
  <b>The brain behind your secrets.</b><br/>
  Unified secrets control plane for cloud-native teams.<br/>
  <a href="https://secrets-bridge.io">secrets-bridge.io</a> · <a href="https://github.com/secrets-bridge">all repos</a>
</p>

---
# secrets-bridge / worker

Background workers for the secrets-bridge Control Plane. Runs alongside the api as a separate K8s Deployment. Owns the periodic sweepers (expired wraps, stale agents, stuck jobs, stale discovered secrets) and the discover-job scheduler.

Multiple replicas can run concurrently. Redis-backed locks ensure each sweep fires from exactly one replica per tick.

## Layout

```
cmd/worker/         main + config
internal/
  observability/    slog JSON logger
  probes/           /healthz /readyz /metrics on a loopback listener
  retry/            exponential-backoff retry policy
  notifications/    pluggable sink interface + Webhook + NoOp + Fanout
  scheduler/        Redis-lock leader-elected periodic runner
  sweepers/         WrapsExpired, SecretsStale, AgentsStale, JobsRecovery, DiscoverScheduler, UsersPending
```

The worker imports `github.com/secrets-bridge/api/pkg/{storage,runtime}` per the REFACTOR_PLAN §4 polyrepo rule. A local `replace` directive keeps cross-repo iteration fast while interfaces stabilize.

## Sweepers

| Sweeper | Owns | Default cadence | Default cutoff |
|---|---|---|---|
| `wraps-expired` | Purge `secret_wraps` rows past `expires_at` | 1m | none |
| `secrets-stale` | Flip discovered-secret rows to `missing` after extended absence | 5m | 24h |
| `agents-stale` | Flip agents from `active` to `stale` when heartbeat has stopped | 1m | 5m |
| `jobs-recovery` | Flip claimed sync_jobs to `expired` when claim_expires_at passed | 30s | none |
| `discover-scheduler` | Enqueue one discover job per configured target every interval | 1h | none |
| `gitops-poller` | Poll ArgoCD per active observation, record observed_state, transition terminal states (applied / failed) | 15s | none (opt-in via `SB_WORKER_GITOPS_ENABLED`) |
| `gitops-timeout` | Flip rows past `timeout_at` to `applied_unverified` | 1m | none (opt-in via `SB_WORKER_GITOPS_ENABLED`) |
| `users-pending` | Post one webhook message for accounts that started waiting for approval (below) | 1m | none (only with `SB_WORKER_WEBHOOK_URL`) |

Each sweeper:
- Runs under a Redis lock (`worker:sweeper:<name>`), so with multiple replicas only one runs per tick.
- Retries transient failures per `retry.DefaultPolicy()` (exponential, 20% jitter, 1h cap). `reveal-sessions-expired` is the exception: at most 3 tries (about 1.8s) per tick, since its self-heal redoes missed work on the next tick.
- Writes one audit event per row it changes, as actor `system:worker`, with ids, reasons and counts only.
- Emits a structured notification on every meaningful outcome. A notification that fails to send is logged with the sweeper name and an error class (never the error text) and counted in `worker_sweeper_notify_failures_total`.
- NEVER logs / audits / notifies a secret value.

## Env vars

| Var | Default | Notes |
|---|---|---|
| `DATABASE_URL` | required | Same DSN shape as the api |
| `REDIS_URL` | required | Same URL shape as the api |
| `SB_WORKER_LOCAL_ADDR` | `127.0.0.1:8091` | Probes + metrics |
| `SB_WORKER_SHUTDOWN_GRACE` | `15s` | |
| `SB_WORKER_WRAPS_EXPIRED_INTERVAL` | `1m` | |
| `SB_WORKER_SECRETS_STALE_INTERVAL` | `5m` | |
| `SB_WORKER_AGENTS_STALE_INTERVAL` | `1m` | |
| `SB_WORKER_JOBS_RECOVERY_INTERVAL` | `30s` | |
| `SB_WORKER_DISCOVER_INTERVAL` | `1h` | |
| `SB_WORKER_SECRETS_STALE_AFTER` | `24h` | Flip cutoff |
| `SB_WORKER_AGENTS_STALE_AFTER` | `5m` | Flip cutoff |
| `SB_DISCOVER_TARGETS_JSON` | (unset) | Discover scheduler targets (see below) |
| `SB_WORKER_WEBHOOK_URL` | (unset) | If set, notifications go to this URL |
| `SB_WORKER_WEBHOOK_SLACK_FORMAT` | `false` | Use Slack's `{"text": ...}` shape |
| `SB_WORKER_USERS_PENDING_INTERVAL` | `1m` | How often `users-pending` looks for new waiting accounts; at most one message per run. At least `30s` |
| `SB_PUBLIC_URL` | (unset) | The address people open the console with, for the link in the `users-pending` message. Unset, the address saved in the console is used. Same variable and rule as the api's: `https://`, or `http://` for localhost only with `SB_ENV=dev` |
| `SB_WORKER_GITOPS_ENABLED` | `false` | Opt-in for the GitOps observation poller + timeout sweeper (BRD §26). Must match api's `SB_GITOPS_ENABLED`. When enabled, requires a configured `KeyManager` (`SB_KMS_BACKEND` etc.) to unwrap stored ArgoCD tokens. |
| `SB_WORKER_GITOPS_POLL_INTERVAL` | `15s` | Tick cadence of the gitops-poller sweeper |
| `SB_WORKER_GITOPS_TIMEOUT_INTERVAL` | `1m` | Tick cadence of the gitops-timeout sweeper |
| `SB_WORKER_GITOPS_BATCH_SIZE` | `20` | Max observations claimed per gitops-poller tick |
| `SB_WORKER_GITOPS_HTTP_TIMEOUT` | `15s` | Per-ArgoCD-call HTTP timeout |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |

Every duration above must be greater than zero and `SB_WORKER_GITOPS_BATCH_SIZE` at least 1. Anything else stops the worker at boot with an error naming the variable.

### Accounts waiting for approval

Someone who signs in with Google for the first time gets an account that waits until an admin approves it on Admin → Users. `users-pending` tells the admins: each run posts at most one message to the webhook with the accounts that started waiting since the last one, up to 10 by email, then "and N more", and a link to `<public address>/admin/users`.

- **Emails, never display names.** A display name comes from the person's Google account and can be anything, and Slack turns URL-shaped text into links and reads `*`, `_` and `~` as formatting. An email had to pass Google's `hd` check and the allowed domains.
- **Webhook only.** The message still names people, so it never goes to the log: the worker logs the counts. Without `SB_WORKER_WEBHOOK_URL` the sweeper isn't registered, and the console's badge and Overview banner show who is waiting.
- **Once each.** A cursor in Redis (`<ns>:cursor:users-pending`) remembers the newest account announced. It moves only after the webhook accepts the message, so a failed post is retried on the next run. With no cursor yet, the accounts already waiting are announced once. A cursor that can't be saved after a post ends the tick without a retry, so the message isn't sent twice in one tick; the next tick sends it once more.
- **Slack escaping.** The Slack format escapes `&`, `<` and `>` in every message, so event text can't mention `@channel` or dress up a link. Emails also lose control and format characters before they go in.
- Accounts younger than 30 seconds wait for the next run, so one whose transaction commits late can't slip behind the cursor.

ArgoCD endpoints must use `https://`. Plain `http://` is only polled when `SB_ENV=dev`; otherwise the endpoint is skipped before its token is unwrapped and a health error is recorded on it.

## Discover targets

`SB_DISCOVER_TARGETS_JSON` is a JSON array. Example:

```json
[
  {
    "name": "prod-eu-vault",
    "cluster": "prod-eu",
    "provider_type": "vault",
    "provider_config": {"address": "https://vault.example.com", "kvMount": "secret"},
    "scope": {"project": "billing"}
  },
  {
    "name": "prod-us-aws",
    "cluster": "prod-us",
    "provider_type": "aws-sm",
    "provider_config": {"region": "us-east-1"}
  }
]
```

`cluster` must match the target agent's `SB_CLUSTER_NAME`. This env var is a deprecated fallback: discovery targets now live in `provider_connections`, and the env targets only run when no eligible connection exists at all (active, discovery enabled, cluster name set), not just when none is due this tick.

## Notifications

Sink contract:
```go
type Notifier interface {
    Notify(ctx context.Context, event Event) error
    Name() string
}
```

Built-in sinks: `NoOp` (logs at the event's severity), `Webhook` (POSTs JSON; supports Slack format). `Fanout` dispatches one event to many sinks; per-sink errors are joined but don't block siblings.

## Local dev

The api repo's `docker-compose.yml` brings up Postgres + Redis. From the api repo:

```
docker compose up -d postgres redis
cd ../worker
DATABASE_URL=postgres://secrets_bridge:devpass@localhost:5432/secrets_bridge?sslmode=disable \
REDIS_URL=redis://localhost:6379/0 \
go run ./cmd/worker
```

## Hard rules

- Stateless: all state lives in Postgres + Redis (NFR-08)
- No secret values logged, audited, or notified
- Worker authenticates to Postgres + Redis only; no provider SDKs imported

## Status

| Slice | Scope | Status |
|---|---|---|
| Step 11 ([#1](https://github.com/secrets-bridge/worker/issues/1)) | Scaffold + retry + notifications + scheduler + 5 sweepers (wraps-expired / secrets-stale / agents-stale / jobs-recovery / discover-scheduler) | ✅ merged |
| BRD §26 GitOps poller ([#3](https://github.com/secrets-bridge/worker/pull/3)) | `gitops-poller` (15 s) + `gitops-timeout` (1 m) sweepers; opt-in via `SB_WORKER_GITOPS_ENABLED=true`, must match api's `SB_GITOPS_ENABLED` | ✅ merged |

See [`skills/PROGRESS.md`](https://github.com/secrets-bridge/skills/blob/main/PROGRESS.md) for the slice-by-slice log.
