# Changelog

All notable changes to `secrets-bridge/worker` are tracked here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the project uses [SemVer](https://semver.org/).

Until `v0.1.0` ships, every change lands under `[Unreleased]`. The
rolling `:dev` container tag (published by
`.github/workflows/docker-publish.yml`) reflects whatever is currently
on `main`.

## [Unreleased]

### Added
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
