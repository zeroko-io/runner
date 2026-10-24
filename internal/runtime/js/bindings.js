// The runner's bindings. This Worker runs in every service process next to the
// service, whose env holds one of these entrypoints per binding it declares,
// with the binding's name in props. Each call becomes a request to the runner over
// CONTROL, authenticated with this process's token, which the service never
// sees.
import { WorkerEntrypoint } from "cloudflare:workers";

async function call(env, path, init = {}) {
  const headers = new Headers(init.headers);
  headers.set("x-zeroko-token", env.TOKEN);
  const res = await env.CONTROL.fetch("http://zeroko" + path, { ...init, headers });
  if (res.ok || res.status === 404 || init.pass?.includes(res.status)) return res;
  let message = `${res.status} ${res.statusText}`;
  try { message = (await res.json()).error || message; } catch {}
  throw new Error(message);
}

async function json(env, path, body) {
  const res = await call(env, path, { method: "POST", body: JSON.stringify(body), headers: { "content-type": "application/json" } });
  return res.json();
}

const B64 = "$blob";

function encodeValue(v) {
  if (v instanceof ArrayBuffer) v = new Uint8Array(v);
  if (ArrayBuffer.isView(v)) {
    const bytes = new Uint8Array(v.buffer, v.byteOffset, v.byteLength);
    let s = "";
    for (let i = 0; i < bytes.length; i += 0x8000) s += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
    return { [B64]: btoa(s) };
  }
  if (typeof v === "bigint") return v.toString();
  if (typeof v === "boolean") return v ? 1 : 0;
  if (v === undefined) return null;
  if (v instanceof Date) return v.toISOString();
  return v;
}

function decodeValue(v) {
  if (v && typeof v === "object" && B64 in v) {
    const s = atob(v[B64]);
    const out = new Uint8Array(s.length);
    for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i);
    return out;
  }
  return v;
}

function decodeResult(r) {
  if (r.rows) r.rows = r.rows.map((row) => Object.fromEntries(Object.entries(row).map(([k, v]) => [k, decodeValue(v)])));
  return r;
}

function statement(s) {
  if (!Array.isArray(s) || typeof s[0] !== "string") throw new TypeError("batch() takes [sql, ...params] arrays");
  return { sql: s[0], params: s.slice(1).map(encodeValue) };
}

export class Db extends WorkerEntrypoint {
  path(op) { return `/db/${encodeURIComponent(this.ctx.props.binding)}/${op}`; }

  // query runs one statement and returns its rows as objects.
  async query(sql, ...params) {
    const [r] = await json(this.env, this.path("batch"), { statements: [statement([sql, ...params])] });
    return decodeResult(r).rows;
  }

  // run runs one statement and returns what it changed.
  async run(sql, ...params) {
    const [r] = await json(this.env, this.path("batch"), { statements: [statement([sql, ...params])] });
    return { changes: r.changes, lastRowId: r.lastRowId };
  }

  // batch runs statements in one transaction: all apply or none.
  async batch(statements) {
    return (await json(this.env, this.path("batch"), { statements: statements.map(statement) })).map(decodeResult);
  }

  // lock runs fn alone among the callers that lock the same key, in the
  // order they asked, and returns what it returns.
  async lock(key, fn) {
    if (typeof key !== "string" || !key) throw new TypeError("lock keys are non-empty strings");
    if (typeof fn !== "function") throw new TypeError("lock(key, fn) takes the function to run");
    const { id } = await json(this.env, this.path("lock"), { key });
    try {
      return await fn();
    } finally {
      await call(this.env, this.path("unlock"), { method: "POST", body: JSON.stringify({ key, id }), headers: { "content-type": "application/json" } });
    }
  }
}

const CONDITIONS = ["range", "if-range", "if-none-match", "if-match", "if-modified-since", "if-unmodified-since"];

export class Files extends WorkerEntrypoint {
  path(key, q) {
    if (typeof key !== "string" || !key) throw new TypeError("file keys are non-empty strings");
    return `/files/${encodeURIComponent(this.ctx.props.binding)}?key=${encodeURIComponent(key)}${q || ""}`;
  }

  async put(key, body, options = {}) {
    if (typeof body === "string" || body instanceof ArrayBuffer || ArrayBuffer.isView(body) || body instanceof Blob) {
      body = new Response(body).body;
    }
    if (!(body instanceof ReadableStream)) throw new TypeError("put() takes a string, ArrayBuffer, typed array, Blob or ReadableStream");
    const headers = {};
    if (options.type) headers["content-type"] = options.type;
    const res = await call(this.env, this.path(key), { method: "PUT", body, headers, duplex: "half" });
    return res.json();
  }

  // get returns the file as a Response, or null. With a request (or its
  // headers), its range and conditions apply: the Response is then a 206, a
  // 304 or a 416 to return as it is.
  async get(key, request) {
    const from = new Headers(request instanceof Request ? request.headers : request);
    const headers = {};
    for (const h of CONDITIONS) if (from.has(h)) headers[h] = from.get(h);
    const res = await call(this.env, this.path(key), { headers, pass: [304, 412, 416] });
    if (res.status === 404) return null;
    return new Response(res.body, { status: res.status, headers: res.headers });
  }

  async head(key) {
    const res = await call(this.env, this.path(key, "&meta=1"));
    return res.status === 404 ? null : res.json();
  }

  async delete(key) {
    const res = await call(this.env, this.path(key), { method: "DELETE" });
    return res.status !== 404;
  }

  async list(options = {}) {
    const q = new URLSearchParams();
    if (options.prefix) q.set("prefix", options.prefix);
    if (options.cursor) q.set("cursor", options.cursor);
    if (options.limit) q.set("limit", String(options.limit));
    const res = await call(this.env, `/files/${encodeURIComponent(this.ctx.props.binding)}/list?${q}`);
    return res.json();
  }
}

// Assets answers a request with the deployment's file under public/ at its
// path (x, x.html or x/index.html; index.html for a path ending in /), with
// its range and conditions applied, or a 404.
export class Assets extends WorkerEntrypoint {
  async fetch(request) {
    const headers = {};
    for (const h of CONDITIONS) if (request.headers.has(h)) headers[h] = request.headers.get(h);
    const method = request.method === "HEAD" ? "HEAD" : "GET";
    const res = await call(this.env, `/assets?path=${encodeURIComponent(new URL(request.url).pathname)}`, { method, headers, pass: [304, 412, 416] });
    return new Response(res.body, { status: res.status, headers: res.headers });
  }
}

// Service sends a request to another service of this runner, as a request to
// its URL would arrive, without leaving the runner.
export class Service extends WorkerEntrypoint {
  async fetch(request) {
    const url = new URL(request.url);
    const headers = new Headers(request.headers);
    headers.set("x-zeroko-token", this.env.TOKEN);
    return this.env.CONTROL.fetch(`http://zeroko/call/${this.ctx.props.service}${url.pathname}${url.search}`, {
      method: request.method, headers, body: request.body, redirect: "manual",
    });
  }
}

export class Jobs extends WorkerEntrypoint {
  // send queues a job for the service's job() handler.
  async send(payload, options = {}) {
    return json(this.env, "/jobs/send", { payload: payload === undefined ? null : payload, delay: options.delay || 0, attempts: options.attempts || 0, key: options.key || "" });
  }

  // cancel deletes a queued job, by its id or its key; true if there was one.
  async cancel(job) {
    return json(this.env, "/jobs/cancel", typeof job === "string" ? { key: job } : { id: job });
  }
}

export class Realtime extends WorkerEntrypoint {
  // subscribe returns the response that turns the current request into a
  // realtime connection: a WebSocket if the client asked for one, otherwise
  // a stream of Server-Sent Events.
  async subscribe(channels, options = {}) {
    if (typeof channels === "string") channels = [channels];
    if (!Array.isArray(channels) || !channels.length || channels.some((c) => typeof c !== "string" || !c)) {
      throw new TypeError("subscribe() takes a channel name or an array of them");
    }
    return new Response(null, {
      status: 200,
      headers: { "x-zeroko-realtime": JSON.stringify({ channels, meta: options.meta ?? null }) },
    });
  }

  async publish(channel, data) {
    return json(this.env, "/realtime/publish", { channel, data: typeof data === "string" ? data : JSON.stringify(data) });
  }

  async send(connection, data) {
    const r = await json(this.env, "/realtime/send", { connection, data: typeof data === "string" ? data : JSON.stringify(data) });
    return r.sent;
  }

  // connections lists who is connected to a channel: [{connection, meta}],
  // up to 1000.
  async connections(channel) {
    return (await json(this.env, "/realtime/connections", { channel })).connections;
  }
}

export default {
  fetch() {
    return new Response("not found", { status: 404 });
  },
};
