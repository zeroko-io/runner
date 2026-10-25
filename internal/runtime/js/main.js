// The runner's entry module for a service. The public socket serves the service's
// default export; the internal socket, which only the runner can reach, serves
// __ZerokoInternal, which runs the service's job() and message() handlers.
//
// A service with service:<name> bindings passes on, to each call through
// them, how many calls the request came through (X-Zeroko-Hops, kept for
// the request in an AsyncLocalStorage): the runner stops services that call
// each other in a cycle.
import { WorkerEntrypoint } from "cloudflare:workers";
import { AsyncLocalStorage } from "node:async_hooks";
import service from "__ZEROKO_MAIN__";

const bindings = "__ZEROKO_SERVICES__";
const hops = new AsyncLocalStorage();

// env, with each service binding saying how far its call has come.
function withHops(env) {
  if (bindings.length === 0) return env;
  const out = { ...env };
  for (const name of bindings) {
    const target = env[name];
    if (!target) continue;
    out[name] = {
      fetch(input, init) {
        const req = new Request(input, init);
        const headers = new Headers(req.headers);
        headers.set("x-zeroko-hops", String(hops.getStore() ?? 0));
        return target.fetch(new Request(req, { headers }));
      },
    };
  }
  return out;
}

const handler = service?.fetch;

export default bindings.length && typeof handler === "function"
  ? {
      ...service,
      fetch(request, env, ctx) {
        const n = Number(request.headers.get("x-zeroko-hops")) || 0;
        return hops.run(n, () => handler.call(service, request, withHops(env), ctx));
      },
    }
  : service;

export class __ZerokoInternal extends WorkerEntrypoint {
  async fetch(req) {
    const handler = new URL(req.url).pathname.slice(1);
    if (handler === "ready") return new Response("ok");
    if (handler !== "job" && handler !== "message") return new Response("unknown handler", { status: 404 });
    if (typeof service?.[handler] !== "function") {
      return Response.json({ error: `the service does not export a ${handler}() handler` }, { status: 501 });
    }
    try {
      const body = await req.json();
      await hops.run(0, () => service[handler](body, withHops(this.env), this.ctx));
      return Response.json({ ok: true });
    } catch (e) {
      console.error(`${handler}() failed:`, e?.stack || String(e));
      return Response.json({ error: e?.message || String(e) }, { status: 500 });
    }
  }
}
