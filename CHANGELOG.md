# Changelog

All notable changes to `secrets-bridge/worker` are tracked here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the project uses [SemVer](https://semver.org/).

Until `v0.1.0` ships, every change lands under `[Unreleased]`. The
rolling `:dev` container tag (published by
`.github/workflows/docker-publish.yml`) reflects whatever is currently
on `main`.

## [Unreleased]

### Removed
- The control-plane Argo CD poller (`gitops-poller`, `gitops-timeout`
  and their store). The Gateway is the only component that talks to
  Argo CD (secrets-bridge/gateway#13). `SB_WORKER_GITOPS_ENABLED`,
  `SB_WORKER_GITOPS_POLL_INTERVAL`, `SB_WORKER_GITOPS_TIMEOUT_INTERVAL`,
  `SB_WORKER_GITOPS_BATCH_SIZE` and `SB_WORKER_GITOPS_HTTP_TIMEOUT` are
  retired: logged by name at startup if set, never read. The worker no
  longer needs KMS settings or access.

### Security
- gitops-poller no longer stores ArgoCD `message` / `health_message`
  free text in `observed_state` (WRK-01).
- ArgoCD bearer token is never sent to a plain `http://` endpoint
  outside `SB_ENV=dev` (WRK-N-04).
- reveal-sessions-expired self-heals wraps left live by a partial
  failure, treats consumed or deleted wraps as closed and caps its
  retries per tick (WRK-N-01).
- Sweepers write audit events (`system:worker`) for cross-team
  expiry, reveal session TTL, job expiry, GitOps terminal states and
  stale agents (WRK-N-02).
- `SB_DISCOVER_TARGETS_JSON` only runs when no DB discover target
  exists (WRK-N-05).
- Non-positive durations and batch size fail at boot (WRK-N-06).
- Failed notifications are logged and counted (WRK-N-07).

### Added
- `internal/archguard`: fails if non-test code imports an Argo CD
  client or builds an Argo CD API path.
- Docker image publishing workflow — multi-arch (amd64 + arm64),
  pushed to `ghcr.io/secrets-bridge/worker`. Tags: rolling `dev` on
  every push to `main`; semver tags + `latest` activate post-`v0.1.0`.
  Because `go.mod` declares `replace github.com/secrets-bridge/api =>
  ../api` for local polyrepo dev, the workflow checks out the api
  repo into `worker/.deps/api/` and rewrites the replace path before
  building so the Dockerfile's `COPY . .` brings both repos into the
  build stage. The rewrite is on a checkout copy only — `main`'s
  `go.mod` keeps the `../api` form for local dev.

---

## How to cut a release (post-v0.1.0)

1. Land all release-bound work on `main`.
2. Update this file: move `[Unreleased]` entries under a new
   `[vX.Y.Z] — YYYY-MM-DD` heading. Add a fresh empty `[Unreleased]`
   on top.
3. `git tag vX.Y.Z && git push origin vX.Y.Z`.
4. The `docker-publish` workflow picks up the tag and publishes:
   - `ghcr.io/secrets-bridge/worker:vX.Y.Z`
   - `ghcr.io/secrets-bridge/worker:vX.Y`
   - `ghcr.io/secrets-bridge/worker:latest` (only for non-prerelease)
5. Create the GitHub Release pointing at the tag.
