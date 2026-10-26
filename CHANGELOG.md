# Changelog

## 0.3.2

- The reads on every request (a service, its deployments, a token, logs)
  have connections of their own to `runner.db`: they no longer wait behind
  a batch of logs being written. A service's lookup under a steady write
  load went from 11 ms (p50) and 150 ms (p99) to well under a millisecond.

## 0.3.1

- WebSocket control frames (ping, pong, close) are at most 125 bytes and
  never fragmented, as RFC 6455 says: others close the connection (1002).
- Pruning logs runs as a short statement per service, so requests don't
  wait behind one over every service's logs.
- A service holds up to 10,000 jobs, queued and dead: past it, sending one
  answers 400. Dead jobs are deleted after 7 days, a cron's after 1 day.
- A token's `last_used_at` is to the minute: the API no longer writes to
  `runner.db` on every request.

## 0.3.0

- A client, for rate limits, is the last of `X-Forwarded-For` only when
  the request comes from a private or loopback address (a proxy in front);
  otherwise the connection's, which a client can't make up.
- Outbound sockets from `connect()` work: each service process has an
  egress socket of its own, which says whose its requests are (workerd
  doesn't add the process's token to a socket's CONNECT). Plain TCP only:
  TLS on a socket isn't proxied yet; HTTPS goes through `fetch`.
- `cpu_seconds_per_minute` (30 by default): the CPU a service process may
  use in any minute; past it, the process is paused (SIGSTOP) until the
  minute fits, and says so in its logs. Its requests wait, they don't fail.
- `realtime_connections` (1000 by default): the realtime connections a
  service holds at once; past it, a new one answers 503.
- `rate_limit` (none by default), and a service's own (`PATCH
  /api/services/<name>` `{"rate_limit": n}`: 0 for the runner's, -1 for
  none): requests a second per client, by address, with bursts of twice
  as many; past it, 429 with `Retry-After`. Calls from other services
  aren't counted. A migration adds it to every service.
- Services calling each other in a cycle are stopped: a request goes
  through up to 16 service bindings in a row (`X-Zeroko-Hops`, carried for
  each request by the service's entry module); past it, the call answers
  508.
- Tests: outbound sockets and WebSockets through the egress proxy, and an
  upgrade from the last release on the same volume (in CI).

- The API as an MCP server (the Model Context Protocol), at `POST /mcp`
  with the same tokens: tools to list, deploy, inspect, roll back, stop,
  start and delete services, read their logs and metrics, set secrets and
  vars, query their databases, list their files and run their crons. Each
  tool is an API call made as the caller, logged as any other.
  Streamable HTTP, answered as JSON.

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
