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
| `observation-leases` | Put Gateway observation tasks whose lease ended back to `queued` (below) | 30s | none (opt-in via `SB_WORKER_GATEWAY_SWEEPERS_ENABLED`) |
| `observation-deadlines` | Close open observation tasks past `deadline_at` as `timed_out` | 30s | none (opt-in) |
| `gateways-stale` | Flip Gateways from `active` to `stale` after missed heartbeats | 1m | 2m (opt-in) |
| `gateway-replicas-prune` | Delete `gateway_replicas` rows not seen within the retention | 1h | 24h (opt-in) |

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
| `SB_WORKER_GATEWAY_SWEEPERS_ENABLED` | `false` | Opt-in for the four Gateway observation sweepers. Turn it on together with the api's `SB_GATEWAY_ENABLED`. Off, the gateway and observation tables are never read |
| `SB_WORKER_OBSERVATION_LEASES_INTERVAL` | `30s` | Tick of `observation-leases`. At least `5s` |
| `SB_WORKER_OBSERVATION_DEADLINES_INTERVAL` | `30s` | Tick of `observation-deadlines`. At least `5s` |
| `SB_WORKER_OBSERVATION_BATCH_SIZE` | `200` | Tasks per statement; a tick runs up to 10 batches. 1 to 1000 |
| `SB_WORKER_GATEWAYS_STALE_INTERVAL` | `1m` | Tick of `gateways-stale`. At least `10s` |
| `SB_WORKER_GATEWAYS_STALE_AFTER` | `2m` | Flip cutoff: four missed 30s heartbeats. At least `1m` |
| `SB_WORKER_GATEWAY_REPLICAS_PRUNE_INTERVAL` | `1h` | Tick of `gateway-replicas-prune`. At least `1m` |
| `SB_WORKER_GATEWAY_REPLICAS_RETENTION` | `24h` | Replica rows not seen for this long are deleted. At least `10m`, the api's replica-limit window |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |

Every duration above must be greater than zero, `SB_WORKER_GITOPS_BATCH_SIZE` at least 1, and the Gateway knobs at or above their floors. Anything else stops the worker at boot with an error naming the variable. The floors are checked even while the Gateway sweepers are off, so turning them on later can't surface a bad value.

### Gateway observation sweepers

The Gateway observation queue lives in the api (`observation_tasks`, api#294). These sweepers clean up after a Gateway that stops calling in or a replica that dies:

- **`observation-leases`** calls `ExpireLeases`: a leased task whose lease ended goes back to `queued` with no attempt, and the attempt is closed as `lease_expired`. A late renew, release or result from that attempt is then refused by the api's attempt fence. `failure_count` is not touched, so replica restarts can't kill a healthy task; the deadline bounds the churn. Audit: `observation.task.lease_expired` per task.
- **`observation-deadlines`** calls `ExpireDeadlines`: an open task past `deadline_at` becomes `done` with verdict `timed_out` and reason `deadline_reached`, or `gateway_unavailable` when its Gateway was not seen since the task was created or is not active. It is never `dead` or failed: timed out means "not observed in time", not a failed deployment. Audit: `observation.task.timed_out` per task.
- **`gateways-stale`** flips an `active` Gateway to `stale` once its `last_seen_at` (or `enrolled_at`, if it never sent a heartbeat) is older than `SB_WORKER_GATEWAYS_STALE_AFTER`. The Gateway keeps its credential and can still call in; the api flips it back to `active` on the next accepted heartbeat. Audit: `gateway.stale` per Gateway.
- **`gateway-replicas-prune`** deletes old `gateway_replicas` rows. No audit row: a replica row is a heartbeat cache.

The task sweeps lock task rows only (`FOR UPDATE SKIP LOCKED`). The Gateway sweeps lock Gateway rows first (`FOR NO KEY UPDATE` / `FOR SHARE`, `SKIP LOCKED`), the same order the api uses, so a sweep never deadlocks with a claim, result, heartbeat or revoke. Times are judged on the database clock, the clock the api writes leases and deadlines with. Each tick retries at most 3 times (a few seconds): the next tick redoes any missed work. Metrics: `worker_observation_task_leases_expired_total`, `worker_observation_tasks_timed_out_total{reason}`, `worker_gateways_stale_total`, `worker_gateway_replicas_pruned_total`. Logs and notifications carry counts only.

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
| Gateway R1 ([#38](https://github.com/secrets-bridge/worker/issues/38)) | `observation-leases`, `observation-deadlines`, `gateways-stale`, `gateway-replicas-prune`; opt-in via `SB_WORKER_GATEWAY_SWEEPERS_ENABLED=true` | in review |

See [`skills/PROGRESS.md`](https://github.com/secrets-bridge/skills/blob/main/PROGRESS.md) for the slice-by-slice log.
