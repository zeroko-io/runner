# Zeroko runner

Runs the JavaScript HTTP services that agents deploy through its API: a Go
binary (gateway on :8080, API on :8081, service data) and one workerd
process per service, in one container. Only a runner: no UI, no users, and
no settings in the environment beyond the root token and the user it runs
as; the app that manages it (plane, the orchestrator, or the open source
panel, `zeroko-io/panel`) sets the rest through the API.
Read the architecture page of the docs before changing how requests,
processes or bindings work.

## Principles

- Less is more. Few, generic pieces; remove what is unused. No dependency
  without a strong reason: there is one (`mattn/go-sqlite3`). Only the
  Docker image is supported.
- workerd is only the runtime: it runs services' code and holds no state.
  Every piece of state goes through a binding to the runner: `db`, `files`,
  `jobs`, `realtime`. These are the runner's own APIs, not Cloudflare's.
- What agents deploy are services (not apps): use the word in code, API and
  docs.
- The API is the contract: plane and the panels depend on it, and the
  agents' skill (in `zeroko-io/panel`) describes it. Change it with care, and
  note every change of it in the changelog.
- No docs in this repository: every doc is on the website, in
  `zeroko-io/cloud` (`content/docs`, public: architecture, settings,
  limits, API, security; `content/internal`, the team's: roadmap,
  releases). Update them with any change they describe. Keep the
  README short.

## Layout

- `cmd/runner`: startup, and the user the runner runs as.
- `internal/runtime`: workerd processes (config, start, readiness, logs, CPU
  watchdog, idle stop, process cap); `js/` holds the entry wrapper and the
  bindings Worker that run inside every process.
- `internal/platform`: product logic (deploy, rollback, secrets, vars,
  limits, logs, jobs, crons, locks, backups).
- `internal/data`: services' databases and files, and their quota.
  `internal/realtime`: the connection hub and the WebSocket implementation.
- `internal/egress`: the proxy every outbound request of a service goes
  through.
- `internal/server`: gateway, API, auth, the control socket for bindings.
- `internal/store`: `runner.db`. Migrations are append-only; never edit a
  released one.
- `test/e2e`: end-to-end tests (build tag `e2e`) of a running runner,
  through its API and gateway: `TestE2E` on a fresh one, `TestSmoke` as a
  short check of an image.

## Commands

The runner only runs as its Docker image (Linux, cgo SQLite, workerd).
Build and test on the development VM:

```sh
go vet ./... && go test ./...
docker build -t ghcr.io/zeroko-io/runner:local .
docker run -d -p 8082:8080 -p 8083:8081 \
  -e ZEROKO_TOKEN=e2e-root-token-0123456789abcdef0123456789 ghcr.io/zeroko-io/runner:local
RUNNER_GATEWAY=127.0.0.1:8082 RUNNER_API=127.0.0.1:8083 \
  go test -tags e2e -count=1 ./test/e2e -run TestE2E   # on an empty runner
```

## Releases

CI (`.github/workflows/ci.yml`) builds each commit once. On `main` (and pull
requests) it runs the tests, builds both images, and runs the whole e2e
suite against each: amd64 there, arm64 on an arm64 machine (the same
image, handed over as an artifact); on `main`, once both pass, it publishes
them as `sha-<commit>` and, while the commit is `main`'s newest, `dev`. The Dockerfile cross-compiles: no stage
runs emulated, keep it so.

Versions come from git: the binary's is `git describe` of the `vX.Y.Z`
tags (`0.1.0`, or `0.1.0-3-gabc1234` after it). To release: turn
`## Unreleased` in `CHANGELOG.md` into `## X.Y.Z`, commit, tag `vX.Y.Z` and
push both at once (`git push --atomic origin main vX.Y.Z`). The tag's run
builds nothing: once `main`'s run of the commit passes, it names that tested
image `X.Y.Z`, `X.Y` and `latest` and makes the GitHub release from the
changelog. It fails at once if the changelog has no `## X.Y.Z` or the image
was built without the tag.
