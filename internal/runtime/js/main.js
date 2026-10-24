// The runner's entry module for a service. The public socket serves the service's
// default export; the internal socket, which only the runner can reach, serves
// __ZerokoInternal, which runs the service's job() and message() handlers.
import { WorkerEntrypoint } from "cloudflare:workers";
import service from "__ZEROKO_MAIN__";

export default service;

export class __ZerokoInternal extends WorkerEntrypoint {
  async fetch(req) {
    const handler = new URL(req.url).pathname.slice(1);
    if (handler === "ready") return new Response("ok");
    if (handler !== "job" && handler !== "message") return new Response("unknown handler", { status: 404 });
    if (typeof service?.[handler] !== "function") {
      return Response.json({ error: `the service does not export a ${handler}() handler` }, { status: 501 });
    }
    try {
      await service[handler](await req.json(), this.env, this.ctx);
      return Response.json({ ok: true });
    } catch (e) {
      console.error(`${handler}() failed:`, e?.stack || String(e));
      return Response.json({ error: e?.message || String(e) }, { status: 500 });
    }
  }
}
