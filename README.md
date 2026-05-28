# secrets-bridge / worker

Background workers for the secrets-bridge Control Plane. Runs alongside the api as a separate K8s Deployment. Owns the periodic sweepers (expired wraps, stale agents, stuck jobs, stale discovered secrets) and the discover-job scheduler.

Multiple replicas can run concurrently — Redis-backed locks ensure each sweep fires from exactly one replica per tick.

## Layout

```
cmd/worker/         main + config
internal/
  observability/    slog JSON logger
  probes/           /healthz /readyz /metrics on a loopback listener
  retry/            exponential-backoff retry policy
  notifications/    pluggable sink interface + Webhook + NoOp + Fanout
  scheduler/        Redis-lock leader-elected periodic runner
  sweepers/         WrapsExpired, SecretsStale, AgentsStale, JobsRecovery, DiscoverScheduler
```

The worker imports `github.com/secrets-bridge/api/pkg/{storage,runtime}` per the REFACTOR_PLAN §4 polyrepo rule. A local `replace` directive keeps cross-repo iteration fast while interfaces stabilize.

## Sweepers

| Sweeper | Owns | Default cadence | Default cutoff |
|---|---|---|---|
| `wraps-expired` | Purge `secret_wraps` rows past `expires_at` | 1m | — |
| `secrets-stale` | Flip discovered-secret rows to `missing` after extended absence | 5m | 24h |
| `agents-stale` | Flip agents from `active` to `stale` when heartbeat has stopped | 1m | 5m |
| `jobs-recovery` | Flip claimed sync_jobs to `expired` when claim_expires_at passed | 30s | — |
| `discover-scheduler` | Enqueue one discover job per configured target every interval | 1h | — |

Each sweeper:
- Runs under a Redis lock (`worker:sweeper:<name>`) — multiple replicas mean only one runs per tick.
- Retries transient failures per `retry.DefaultPolicy()` (exponential, 20% jitter, 1h cap).
- Emits a structured notification on every meaningful outcome.
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
| `SB_DISCOVER_TARGETS_JSON` | (unset) | Discover scheduler targets — see below |
| `SB_WORKER_WEBHOOK_URL` | (unset) | If set, notifications go to this URL |
| `SB_WORKER_WEBHOOK_SLACK_FORMAT` | `false` | Use Slack's `{"text": ...}` shape |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |

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

`cluster` must match the target agent's `SB_CLUSTER_NAME`. A future PR will replace this env var with a `provider_connections` admin API.

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

- Stateless — all state lives in Postgres + Redis (NFR-08)
- No secret values logged, audited, or notified
- Worker authenticates to Postgres + Redis only — no provider SDKs imported

🚧 Step 11 of the REFACTOR_PLAN. See [issue #1](https://github.com/secrets-bridge/worker/issues/1).
