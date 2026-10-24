# Zeroko runner

Runs the small HTTP services that agents build, as JavaScript modules on
[workerd](https://github.com/cloudflare/workerd), each at
`https://<name>.your-domain`. It is the engine of Zeroko: it has an API and
no UI. An app manages it through the API: the open source panel, or plane,
Zeroko's orchestrator.

Each service takes what it needs through bindings:

- **db**: a SQLite database, with `lock` for changes from concurrent requests
- **files**: file storage
- **jobs**: background work, retries and crons
- **realtime**: WebSocket and Server-Sent Events connections
- **assets**: the deployment's static files
- **service:&lt;name&gt;**: calls to another service of the runner

One container, one volume. Each service runs in its own process, with its
own disk quota and its own list of hosts it may reach.

## Start

Set a root token in `docker-compose.yml`, then:

```sh
docker compose up -d
curl -X PATCH -H "Authorization: Bearer $ZEROKO_TOKEN" \
  -d '{"domain": "zeroko.example.com"}' http://127.0.0.1:8081/api/settings
```

Point `*.zeroko.example.com` at an HTTPS proxy in front of port 8080. Port
8081 is the API, for the app that manages the runner.

## Learn more

The docs are at [zeroko.io/docs](https://zeroko.io/docs): self-hosting,
the runner's settings and limits, its API, security and architecture. What
changed is in the [changelog](CHANGELOG.md).

Licensed under the GNU AGPL v3.0.
