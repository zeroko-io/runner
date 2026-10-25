# Changelog

## 0.2.0

- `PATCH /api/services/<name>` takes `name` too: it renames the service,
  its address. Its data, versions and logs stay; it answers at the new
  host at once, and no longer at the old one. Services that call it
  through a `service:<name>` binding need deploying with the new name.

## 0.1.0

The first version of the runner.

- Services: JavaScript modules run by workerd, one process per service,
  started on demand and stopped when idle, each at `<name>.<domain>` (or
  `<name>--<domain>`, one level under a parent domain's wildcard
  certificate, with the `separator` setting).
- Deployments through the API, with rollback, the last 10 kept, and each one
  downloadable as the body that deploys it again.
- Bindings: `db` (SQLite, with `lock` for changes from concurrent requests),
  `files` (with ranges and conditional requests), `jobs` (retries, crons,
  keys, cancelling), `realtime` (WebSocket and Server-Sent Events, with
  presence), `assets` (the deployment's `public/` files) and
  `service:<name>` (calls between services).
- Secrets, encrypted at rest; vars in the deployment and on the service.
- Egress through the runner: public addresses only, and per service any
  host, a list of hosts or none.
- A disk quota per service, a heap limit, a CPU watchdog and a cap on
  processes running at once.
- Logs per service by source, metrics per minute (and every service's
  traffic with the list of services, `?period=`), the runner's own log with
  every API call that changes something.
- Backups of the whole runner and exports of a service's data, while it
  runs.
- Two ports: services' traffic on 8080, the API on 8081 for the app that
  manages the runner.
- Settings in the runner's database, through the API; the environment holds
  only the root token (`ZEROKO_TOKEN`) and the user the runner runs as.
- Auth by bearer tokens: the root token, and tokens made through the API.
- Images for amd64 and arm64, running as `ZEROKO_UID`:`ZEROKO_GID`.
- Each process's own memory in `/api/system` (its anonymous pages), not the
  pages of workerd's binary that every process shares.
- Hosts reached out to are resolved as named, fully qualified: the
  machine's search domains never apply.
