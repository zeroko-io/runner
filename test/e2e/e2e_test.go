//go:build e2e

// Package e2e tests a running runner from outside, through its API and its
// gateway. Start an empty runner with the test's root token, then:
//
//	docker run -d -p 8080:8080 -p 8081:8081 -e ZEROKO_TOKEN=e2e-root-token-0123456789abcdef0123456789 runner
//	go test -tags e2e ./test/e2e -run TestE2E
//
// TestSmoke is a short check of an image (CI runs the arm64 one emulated).
// RUNNER_API, RUNNER_GATEWAY and RUNNER_TOKEN change the addresses and the
// token. TestE2E restores a backup, so the runner restarts once.
package e2e

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const domain = "zeroko.localhost"

var (
	apiAddr   = env("RUNNER_API", "127.0.0.1:8081")
	gwAddr    = env("RUNNER_GATEWAY", "127.0.0.1:8080")
	rootToken = env("RUNNER_TOKEN", "e2e-root-token-0123456789abcdef0123456789")
	client    = &http.Client{Timeout: 120 * time.Second}
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// resp is a response, read whole.
type resp struct {
	status int
	body   []byte
	header http.Header
}

func (r resp) text() string { return string(r.body) }

// obj and arr decode a JSON body; a body that is not one gives nil.
func (r resp) obj() map[string]any {
	var v map[string]any
	json.Unmarshal(r.body, &v)
	return v
}

func (r resp) arr() []any {
	var v []any
	json.Unmarshal(r.body, &v)
	return v
}

// do sends a request to addr with the Host header host. A body that is not
// a string or bytes is sent as JSON.
func do(method, addr, host, path, token string, body any, header map[string]string) resp {
	var rd io.Reader
	ctype := ""
	switch b := body.(type) {
	case nil:
	case []byte:
		rd = bytes.NewReader(b)
	case string:
		rd = strings.NewReader(b)
	default:
		j, _ := json.Marshal(b)
		rd, ctype = bytes.NewReader(j), "application/json"
	}
	req, err := http.NewRequest(method, "http://"+addr+path, rd)
	if err != nil {
		return resp{}
	}
	req.Host = host
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	res, err := client.Do(req)
	if err != nil {
		return resp{}
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return resp{res.StatusCode, b, res.Header}
}

// suite holds the token the API calls use and counts what fails.
type suite struct {
	t     *testing.T
	token string
}

func (s *suite) api(method, path string, body any) resp {
	return do(method, apiAddr, "runner", path, s.token, body, nil)
}

func (s *suite) apiH(method, path string, body any, header map[string]string) resp {
	return do(method, apiAddr, "runner", path, s.token, body, header)
}

// gw sends a request to a service, by its name.
func gw(service, method, path string, body any, header map[string]string) resp {
	return do(method, gwAddr, service+"."+domain, path, "", body, header)
}

// t1 sends a GET, or a request with a body, to the test's main service.
func t1(path string) resp { return gw("t1", "GET", path, nil, nil) }

func (s *suite) check(name string, ok bool, extra ...any) {
	s.t.Helper()
	if ok {
		s.t.Log("ok   " + name)
	} else {
		s.t.Errorf("FAIL %s %v", name, extra)
	}
}

func waitFor(timeout, every time.Duration, fn func() bool) bool {
	for end := time.Now().Add(timeout); time.Now().Before(end); time.Sleep(every) {
		if fn() {
			return true
		}
	}
	return false
}

func healthy() bool { return do("GET", apiAddr, "runner", "/healthz", "", nil, nil).status == 200 }

// configure sets the settings the tests run with; the runner restarts.
func (s *suite) configure() {
	r := s.api("PATCH", "/api/settings", map[string]any{
		"domain": domain, "public_scheme": "http",
		"cpu_seconds": 3, "max_processes": 3, "realtime_connections": 2,
	})
	s.check("settings: set, and the runner restarts", r.status == 202, r.text())
	time.Sleep(time.Second)
	s.check("settings: the runner is back", waitFor(30*time.Second, 250*time.Millisecond, healthy))
}

const service = `
import page from "./page.html";
import { double } from "./lib/math.js";

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const p = url.pathname;
    if (p === "/") return new Response(page, { headers: { "content-type": "text/html" } });
    if (p === "/env") return Response.json({ v: env.VERSION, greeting: env.GREETING, secret: env.API_KEY ?? null, two: double(1), host: url.host, proto: url.protocol, ip: req.headers.get("x-forwarded-for") });
    if (p === "/lock/add") {
      return Response.json(await env.DB.lock("counter", async () => {
        const [{ n }] = await env.DB.query("SELECT n FROM counter");
        await new Promise((r) => setTimeout(r, 10)); // others read meanwhile, without the lock
        await env.DB.run("UPDATE counter SET n = ?", n + 1);
        return n + 1;
      }));
    }
    if (p === "/lock/throw") return env.DB.lock("counter", () => { throw new Error("inside the lock"); });
    if (p === "/db/setup") {
      await env.DB.run("CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, text TEXT, data BLOB); CREATE TABLE IF NOT EXISTS done (job INTEGER, cron TEXT); CREATE TABLE IF NOT EXISTS counter (n INTEGER); INSERT INTO counter VALUES (0)");
      return new Response("ok");
    }
    if (p === "/db/notes" && req.method === "POST") {
      const { text } = await req.json();
      return Response.json(await env.DB.run("INSERT INTO notes (text, data) VALUES (?, ?)", text, new Uint8Array([7, 8])));
    }
    if (p === "/db/notes") {
      const rows = await env.DB.query("SELECT id, text, data FROM notes ORDER BY id");
      return Response.json(rows.map((r) => ({ ...r, data: r.data ? Array.from(r.data) : null })));
    }
    if (p === "/db/batch") {
      try {
        await env.DB.batch([["INSERT INTO notes (text) VALUES ('in batch')"], ["INSERT INTO missing VALUES (1)"]]);
        return new Response("committed");
      } catch (e) { return new Response("rolled back: " + e.message); }
    }
    if (p === "/db/attach") {
      try { await env.DB.run("ATTACH DATABASE '/data/runner.db' AS c"); return new Response("attached"); }
      catch (e) { return new Response("refused"); }
    }
    if (p === "/files/put") return Response.json(await env.FILES.put(url.searchParams.get("key"), req.body, { type: req.headers.get("content-type") }));
    if (p === "/files/get") return (await env.FILES.get(url.searchParams.get("key"))) ?? new Response("missing", { status: 404 });
    if (p === "/files/serve") return (await env.FILES.get(url.searchParams.get("key"), req)) ?? new Response("missing", { status: 404 });
    if (p === "/files/part") return env.FILES.get(url.searchParams.get("key"), { range: url.searchParams.get("r") });
    if (p === "/files/head") return Response.json(await env.FILES.head(url.searchParams.get("key")));
    if (p === "/files/list") return Response.json(await env.FILES.list({ prefix: url.searchParams.get("prefix") || "", limit: 2, cursor: url.searchParams.get("cursor") || undefined }));
    if (p === "/files/delete") return Response.json(await env.FILES.delete(url.searchParams.get("key")));
    if (p === "/jobs/later") return Response.json(await env.JOBS.send({ later: true }, { delay: 3600, key: url.searchParams.get("key") || undefined }));
    if (p === "/jobs/cancel") return Response.json(await env.JOBS.cancel(url.searchParams.get("key") || Number(url.searchParams.get("id"))));
    if (p === "/jobs/send") return Response.json(await env.JOBS.send(await req.json(), { attempts: Number(url.searchParams.get("attempts") || 3) }));
    if (p === "/live") return env.LIVE.subscribe(["room", "user:" + url.searchParams.get("u")], { meta: { u: url.searchParams.get("u") } });
    if (p === "/live/publish") return Response.json(await env.LIVE.publish("room", { hello: "room" }));
    if (p === "/live/who") return Response.json((await env.LIVE.connections("room")).map((c) => c.meta.u).sort());
    if (p === "/stream") {
      const enc = new TextEncoder();
      return new Response(new ReadableStream({
        async start(c) { for (let i = 0; i < 3; i++) { c.enqueue(enc.encode("part" + i + ";")); await new Promise((r) => setTimeout(r, 300)); } c.close(); },
      }));
    }
    if (p === "/outbound") {
      try {
        const r = await fetch(url.searchParams.get("u"));
        return new Response(r.status + " " + (r.headers.get("x-zeroko-egress") ?? "-") + " " + (await r.text()).trim());
      } catch (e) { return new Response("threw " + e.message); }
    }
    if (p === "/log") { console.log("hello from the service", url.searchParams.get("m")); return new Response("logged"); }
    if (p === "/throw") throw new Error("thrown on purpose");
    if (p === "/spin") { while (true) {} }
    return env.ASSETS.fetch(req);
  },
  async job(job, env) {
    if (job.payload?.fail) throw new Error("failing on purpose " + job.attempt);
    await env.DB.run("INSERT INTO done VALUES (?, ?)", job.id, job.cron ?? null);
  },
  async message(e, env) {
    if (e.type === "message") await env.LIVE.send(e.connection, "echo " + e.meta.u + ": " + e.data);
    if (e.type === "close") console.log("closed", e.meta.u);
  },
};
`

func deployment(version int) map[string]any {
	return map[string]any{
		"files": map[string]any{
			"index.js":         service,
			"lib/math.js":      "export const double = (n) => n * 2;",
			"page.html":        "<h1>e2e</h1>",
			"public/hello.txt": "static hello",
		},
		"bindings": map[string]string{"DB": "db", "FILES": "files", "JOBS": "jobs", "LIVE": "realtime", "ASSETS": "assets"},
		"vars":     map[string]string{"VERSION": fmt.Sprint(version), "GREETING": "hi"},
		"crons":    []any{map[string]any{"schedule": "* * * * *", "payload": map[string]int{"tick": 1}}},
	}
}

// num reads a JSON number.
func num(v any) int {
	f, _ := v.(float64)
	return int(f)
}

func keys(list []any, key string) []string {
	var out []string
	for _, x := range list {
		if m, ok := x.(map[string]any); ok {
			out = append(out, fmt.Sprint(m[key]))
		}
	}
	return out
}

func TestE2E(t *testing.T) {
	s := &suite{t: t}

	// Before it is configured, the runner serves no service.
	s.check("health", healthy())
	s.check("no domain: no service is served", gw("t1", "GET", "/", nil, nil).status == 404)
	s.check("the API needs auth", s.api("GET", "/api/services", nil).status == 401)
	for _, bad := range []string{"nope", "zk_nope", rootToken + "x"} {
		r := do("GET", apiAddr, "runner", "/api/services", bad, nil, nil)
		s.check("a wrong token is refused ("+bad[:4]+"…)", r.status == 401, r.status)
	}
	s.token = rootToken
	s.check("the root token works", len(s.api("GET", "/api/services", nil).arr()) == 0)
	r := s.api("PATCH", "/api/settings", map[string]any{"heap_mb": 1})
	s.check("settings: invalid ones are refused", r.status == 400 && strings.Contains(r.text(), "heap_mb"), r.text())
	s.configure()
	set := s.api("GET", "/api/settings", nil).obj()
	s.check("settings: read", set["domain"] == domain && num(set["max_processes"]) == 3 && num(set["heap_mb"]) == 128, set)
	s.check("an unknown host is a 404", do("GET", gwAddr, "a.b.example", "/", "", nil, nil).status == 404)
	s.check("the domain itself serves nothing", do("GET", gwAddr, domain, "/api/services", "", nil, nil).status == 404)
	s.check("the API is not on the gateway", gw("api", "GET", "/api/services", nil, nil).status == 404)
	tok := s.api("POST", "/api/tokens", map[string]any{"name": "e2e", "expires_in_days": 1}).obj()
	secret, _ := tok["secret"].(string)
	token, _ := tok["token"].(map[string]any)
	s.check("the root token makes a token", strings.HasPrefix(secret, "zk_") && float64(num(token["expires_at"])) > float64(time.Now().UnixMilli()), tok)
	s.token = secret
	s.check("the token works", len(s.api("GET", "/api/services", nil).arr()) == 0)

	// Deployments.
	r = s.api("POST", "/api/services/Bad_Name/deployments", deployment(1))
	s.check("an invalid service name is a 400", r.status == 400, r.text())
	r = s.api("POST", "/api/services/t1/deployments", map[string]any{"files": map[string]string{"index.js": "export default {"}})
	d, _ := r.obj()["deployment"].(map[string]any)
	s.check("a syntax error fails the deployment with 422", r.status == 422 && d["status"] == "failed" && strings.Contains(fmt.Sprint(d["error"]), "SyntaxError"), r.text())
	s.check("a failed first deployment serves nothing", t1("/").status == 404)
	r = s.api("POST", "/api/services/t1/deployments", deployment(1))
	d, _ = r.obj()["deployment"].(map[string]any)
	s.check("deploy", r.status == 201 && d["status"] == "ready" && r.obj()["url"] == "http://t1."+domain, r.text())
	first := fmt.Sprint(d["id"])
	r = t1("/")
	s.check("serve HTML from a text module", r.status == 200 && r.text() == "<h1>e2e</h1>", r.text())
	e := t1("/env").obj()
	s.check("vars, relative imports, public URL", e["v"] == "1" && num(e["two"]) == 2 && e["host"] == "t1."+domain && e["proto"] == "http:" && e["ip"] != nil, e)

	// Secrets apply to the running version.
	s.check("set a secret", s.api("PUT", "/api/services/t1/secrets/API_KEY", map[string]string{"value": "s3"}).status == 204)
	s.check("the service sees the secret", t1("/env").obj()["secret"] == "s3")
	secrets := s.api("GET", "/api/services/t1/secrets", nil).arr()
	first0, _ := secrets[0].(map[string]any)
	_, hasValue := first0["value"]
	s.check("secrets list names only", first0["name"] == "API_KEY" && !hasValue, secrets)
	s.check("a secret cannot shadow a var", s.api("PUT", "/api/services/t1/secrets/VERSION", map[string]string{"value": "x"}).status == 400)

	// Vars set on the service go over the deployment's, without a deployment.
	s.check("set a var", s.api("PUT", "/api/services/t1/vars/GREETING", map[string]string{"value": "hello"}).status == 204)
	s.check("the service sees the var", t1("/env").obj()["greeting"] == "hello")
	vars := s.api("GET", "/api/services/t1/vars", nil).arr()
	v0, _ := vars[0].(map[string]any)
	s.check("vars list values", v0["name"] == "GREETING" && v0["value"] == "hello", vars)
	s.check("a var cannot be a secret", s.api("PUT", "/api/services/t1/vars/API_KEY", map[string]string{"value": "x"}).status == 400)
	s.check("a secret cannot be a var", s.api("PUT", "/api/services/t1/secrets/GREETING", map[string]string{"value": "x"}).status == 400)
	s.check("a var cannot be a binding", s.api("PUT", "/api/services/t1/vars/DB", map[string]string{"value": "x"}).status == 400)
	s.check("delete a var", s.api("DELETE", "/api/services/t1/vars/GREETING", nil).status == 204)
	s.check("the deployment's var is back", t1("/env").obj()["greeting"] == "hi")

	s.dbChecks()
	s.fileChecks()
	s.jobChecks()
	s.realtimeChecks()

	// Responses stream.
	req, _ := http.NewRequest("GET", "http://"+gwAddr+"/stream", nil)
	req.Host = "t1." + domain
	if res, err := client.Do(req); err == nil {
		t0 := time.Now()
		buf := make([]byte, 100)
		n, _ := res.Body.Read(buf)
		s.check("responses stream", string(buf[:n]) == "part0;" && time.Since(t0) < 250*time.Millisecond, string(buf[:n]))
		res.Body.Close()
	} else {
		s.check("responses stream", false, err)
	}

	s.egressChecks()
	s.mcpChecks()
	s.check("a thrown error is a 500", t1("/throw").status == 500)
	t1("/log?m=marker1")

	// A second service keeps working while the first spins.
	r = s.api("POST", "/api/services/t2/deployments", map[string]any{"files": map[string]string{"index.js": "export default { fetch() { return new Response('t2') } }"}})
	s.check("deploy a second service", r.status == 201, r.text())
	r = s.api("PATCH", "/api/services/t2", map[string]any{"name": "t2-renamed"})
	s.check("rename a service", r.status == 200 && r.obj()["name"] == "t2-renamed", r.text())
	s.check("a renamed service answers at its new host", gw("t2-renamed", "GET", "/", nil, nil).text() == "t2")
	s.check("and no longer at its old one", gw("t2", "GET", "/", nil, nil).status == 404)
	s.check("a service can't take another's name", s.api("PATCH", "/api/services/t2-renamed", map[string]any{"name": "t1"}).status == 400)
	s.check("rename it back", s.api("PATCH", "/api/services/t2-renamed", map[string]any{"name": "t2"}).status == 200 && gw("t2", "GET", "/", nil, nil).text() == "t2")
	spun := make(chan int, 1)
	go func() { spun <- t1("/spin").status }()
	time.Sleep(500 * time.Millisecond)
	t0 := time.Now()
	r = gw("t2", "GET", "/", nil, nil)
	s.check("services are isolated: t2 answers while t1 spins", r.text() == "t2" && time.Since(t0) < time.Second, r.text())
	select {
	case st := <-spun:
		s.check("the spinning service is restarted", st == 502 || st == 503, st)
	case <-time.After(20 * time.Second):
		s.check("the spinning service is restarted", false, "no answer in 20 s")
	}
	s.check("and serves again", t1("/env").status == 200)

	s.logChecks()

	// Rollback, stop, start.
	r = s.api("POST", "/api/services/t1/deployments", deployment(2))
	s.check("deploy version 2", r.status == 201 && t1("/env").obj()["v"] == "2")
	s.check("data survives a deployment", len(t1("/db/notes").arr()) == 1)
	r = s.api("POST", "/api/services/t1/deployments/"+first+"/activate", nil)
	s.check("activate the first deployment", r.status == 200 && t1("/env").obj()["v"] == "1", r.text())
	ds := s.api("GET", "/api/services/t1/deployments", nil).arr()
	s.check("deployments list", slices.Equal(keys(ds, "number"), []string{"3", "2", "1"}) && slices.Equal(keys(ds, "active"), []string{"false", "true", "false"}), ds)
	r = s.api("POST", "/api/services/t1/stop", nil)
	s.check("stop", r.status == 200 && r.obj()["status"] == "stopped" && t1("/").status == 503, r.text())
	r = s.api("POST", "/api/services/t1/start", nil)
	s.check("start: running before its process is up", r.status == 200 && r.obj()["status"] == "running" && t1("/").status == 200, r.text())
	a := s.api("GET", "/api/services/t1", nil).obj()
	bindings, _ := a["bindings"].([]any)
	names := keys(bindings, "name")
	sort.Strings(names)
	dep, _ := a["deployment"].(map[string]any)
	s.check("service detail", a["status"] == "running" && slices.Equal(names, []string{"ASSETS", "DB", "FILES", "JOBS", "LIVE"}) && num(dep["number"]) == 2, a)

	s.operatorChecks(first)
	s.quotaChecks()
	s.bundleChecks(first)
	s.assetsChecks()
	s.serviceCallChecks()
	s.processCapChecks()
	s.backupChecks()

	// Crons run every minute.
	s.check("crons run the job handler", waitFor(75*time.Second, 2*time.Second, func() bool {
		r := s.api("POST", "/api/services/t1/db/DB/query", map[string]any{"sql": "SELECT cron FROM done WHERE cron IS NOT NULL"})
		return strings.Contains(r.text(), `"cron":"* * * * *"`)
	}))

	s.limitChecks()

	// Deleting a service deletes its data.
	s.check("delete the service", s.api("DELETE", "/api/services/t1", nil).status == 204 && t1("/").status == 404)
	s.api("POST", "/api/services/t1/deployments", deployment(1))
	t1("/db/setup")
	s.check("a new service with the same name starts empty", len(t1("/db/notes").arr()) == 0 && t1("/files/head?key=b/1").text() == "null")
	s.check("revoke the token", s.api("DELETE", "/api/tokens/"+fmt.Sprint(token["id"]), nil).status == 204 && s.api("GET", "/api/services", nil).status == 401)
}

func (s *suite) dbChecks() {
	s.check("db: schema script", t1("/db/setup").text() == "ok")
	r := gw("t1", "POST", "/db/notes", map[string]string{"text": "first"}, nil).obj()
	s.check("db: insert", num(r["changes"]) == 1 && num(r["lastRowId"]) == 1, r)
	var notes []struct {
		ID   int    `json:"id"`
		Text string `json:"text"`
		Data []int  `json:"data"`
	}
	json.Unmarshal(t1("/db/notes").body, &notes)
	s.check("db: query with a blob", len(notes) == 1 && notes[0].ID == 1 && notes[0].Text == "first" && slices.Equal(notes[0].Data, []int{7, 8}), notes)
	body := t1("/db/batch").text()
	s.check("db: a failing batch rolls back", strings.HasPrefix(body, "rolled back") && len(t1("/db/notes").arr()) == 1, body)
	s.check("db: ATTACH is refused", t1("/db/attach").text() == "refused")
	r2 := s.api("POST", "/api/services/t1/db/DB/query", map[string]any{"sql": "SELECT text FROM notes WHERE id = ?", "params": []any{1}})
	s.check("db: API query", r2.status == 200 && strings.Contains(r2.text(), `"rows":[{"text":"first"}]`), r2.text())
	tables := keys(s.api("GET", "/api/services/t1/db/DB/tables", nil).arr(), "name")
	sort.Strings(tables)
	s.check("db: API tables", slices.Equal(tables, []string{"counter", "done", "notes"}), tables)
	s.check("db: API error is 400", s.api("POST", "/api/services/t1/db/DB/query", map[string]string{"sql": "SELEC"}).status == 400)

	// db.lock: concurrent read-modify-writes of one key take turns.
	var mu sync.Mutex
	var got []int
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var n int
			json.Unmarshal(t1("/lock/add").body, &n)
			mu.Lock()
			got = append(got, n)
			mu.Unlock()
		}()
	}
	wg.Wait()
	sort.Ints(got)
	want := make([]int, 20)
	for i := range want {
		want[i] = i + 1
	}
	s.check("lock: concurrent changes all apply", slices.Equal(got, want), got)
	s.check("lock: a throwing function releases it", t1("/lock/throw").status == 500 && t1("/lock/add").text() == "21")
}

func (s *suite) fileChecks() {
	big := make([]byte, 3<<20)
	rand.Read(big)
	f := gw("t1", "PUT", "/files/put?key=a/big.bin", big, map[string]string{"Content-Type": "application/x-test"}).obj()
	s.check("files: put a streamed body", num(f["size"]) == len(big) && f["type"] == "application/x-test", f)
	r := t1("/files/get?key=a/big.bin")
	s.check("files: get", r.status == 200 && bytes.Equal(r.body, big) && r.header.Get("Content-Type") == "application/x-test")
	for _, k := range []string{"a/1", "a/2", "b/1"} {
		gw("t1", "PUT", "/files/put?key="+k, k, nil)
	}
	l := t1("/files/list?prefix=a/").obj()
	files, _ := l["files"].([]any)
	s.check("files: list pages", slices.Equal(keys(files, "key"), []string{"a/1", "a/2"}) && l["cursor"] == "a/2", l)
	l = t1("/files/list?prefix=a/&cursor=a/2").obj()
	files, _ = l["files"].([]any)
	_, more := l["cursor"]
	s.check("files: second page", slices.Equal(keys(files, "key"), []string{"a/big.bin"}) && !more, l)
	s.check("files: head", num(t1("/files/head?key=a/1").obj()["size"]) == 3)
	s.check("files: missing is null", t1("/files/get?key=nope").status == 404 && t1("/files/head?key=nope").text() == "null")
	s.check("files: delete", t1("/files/delete?key=a/1").text() == "true" && t1("/files/delete?key=a/1").text() == "false")
	r = s.api("GET", "/api/services/t1/files/FILES/file?key=b/1", nil)
	s.check("files: API download", r.status == 200 && r.text() == "b/1")
	r = s.apiH("GET", "/api/services/t1/files/FILES/file?key=a/big.bin", nil, map[string]string{"Range": "bytes=10-19"})
	s.check("files: API ranges", r.status == 206 && bytes.Equal(r.body, big[10:20]))
	r = gw("t1", "GET", "/files/serve?key=a/big.bin", nil, map[string]string{"Range": "bytes=100-"})
	s.check("files: get with the request's range", r.status == 206 && bytes.Equal(r.body, big[100:]) && r.header.Get("Content-Range") == fmt.Sprintf("bytes 100-%d/%d", len(big)-1, len(big)), r.status)
	r2 := gw("t1", "GET", "/files/serve?key=a/big.bin", nil, map[string]string{"If-None-Match": r.header.Get("Etag")})
	s.check("files: get with the request's conditions", r2.status == 304 && len(r2.body) == 0, r2.status)
	r = t1("/files/part?key=a/big.bin&r=bytes=-5")
	s.check("files: get a range by headers", r.status == 206 && bytes.Equal(r.body, big[len(big)-5:]), r.status)
	s.check("files: a range past the end", t1("/files/part?key=a/big.bin&r=bytes=99999999-").status == 416)
}

func (s *suite) jobChecks() {
	j := gw("t1", "POST", "/jobs/send", map[string]int{"n": 1}, nil).obj()
	id := num(j["id"])
	s.check("jobs: send", id > 0, j)
	s.check("jobs: the job handler ran", waitFor(20*time.Second, 250*time.Millisecond, func() bool {
		r := s.api("POST", "/api/services/t1/db/DB/query", map[string]any{"sql": "SELECT job FROM done WHERE job = ?", "params": []any{id}})
		return strings.Contains(r.text(), fmt.Sprintf(`"job":%d`, id))
	}))
	j = gw("t1", "POST", "/jobs/send?attempts=2", map[string]bool{"fail": true}, nil).obj()
	id = num(j["id"])
	var dead map[string]any
	waitFor(40*time.Second, 250*time.Millisecond, func() bool {
		for _, x := range s.api("GET", "/api/services/t1/jobs?status=dead", nil).arr() {
			if m, _ := x.(map[string]any); num(m["id"]) == id {
				dead = m
				return true
			}
		}
		return false
	})
	s.check("jobs: a failing job retries, then is dead", dead != nil && num(dead["attempts"]) == 2 && strings.Contains(fmt.Sprint(dead["error"]), "failing on purpose 2"), dead)
	s.check("jobs: retry a dead job", s.api("POST", fmt.Sprintf("/api/services/t1/jobs/%d/retry", id), nil).status == 204)
	s.check("jobs: delete", waitFor(40*time.Second, 250*time.Millisecond, func() bool {
		return s.api("DELETE", fmt.Sprintf("/api/services/t1/jobs/%d", id), nil).status == 204
	}))
	a, b := t1("/jobs/later?key=once").obj(), t1("/jobs/later?key=once").obj()
	_, dup := a["duplicate"]
	s.check("jobs: one job per key", !dup && num(b["id"]) == num(a["id"]) && b["duplicate"] == true, a, b)
	r := s.api("POST", "/api/services/t1/jobs", map[string]any{"payload": 1, "delay": 3600, "key": "once"})
	c := r.obj()
	s.check("jobs: one per key from the API too", r.status == 200 && num(c["id"]) == num(a["id"]) && c["key"] == "once", r.text())
	s.check("jobs: cancel by key", t1("/jobs/cancel?key=once").text() == "true" && t1("/jobs/cancel?key=once").text() == "false")
	d := t1("/jobs/later?key=once").obj()
	_, dup = d["duplicate"]
	s.check("jobs: the key is free again", !dup && num(d["id"]) != num(a["id"]), d)
	e := t1("/jobs/later").obj()
	s.check("jobs: cancel by id", t1(fmt.Sprintf("/jobs/cancel?id=%d", num(e["id"]))).text() == "true" && t1(fmt.Sprintf("/jobs/cancel?id=%d", num(d["id"]))).text() == "true")
}

func (s *suite) realtimeChecks() {
	ws, wr, status := wsConnect("/live?u=ana", "t1."+domain)
	s.check("realtime: WebSocket upgrade", strings.Contains(status, "101"), status)
	if ws == nil {
		return
	}
	defer ws.Close()
	wsSend(ws, "hi")
	s.check("realtime: message() and send()", wsRecv(ws, wr) == "echo ana: hi")

	req, _ := http.NewRequest("GET", "http://"+gwAddr+"/live?u=bo", nil)
	req.Host = "t1." + domain
	sse, err := (&http.Client{}).Do(req)
	if err != nil {
		s.check("realtime: SSE", false, err)
		return
	}
	defer sse.Body.Close()
	s.check("realtime: SSE", sse.StatusCode == 200 && sse.Header.Get("Content-Type") == "text/event-stream")
	lines := bufio.NewReader(sse.Body)
	line := func() string {
		l, _ := lines.ReadString('\n')
		return l
	}
	time.Sleep(300 * time.Millisecond)
	s.check("realtime: publish reaches both", t1("/live/publish").text() == `{"delivered":2}`)
	s.check("realtime: WebSocket receives JSON", wsRecv(ws, wr) == `{"hello":"room"}`)
	s.check("realtime: SSE receives it", line() == "data: {\"hello\":\"room\"}\n")
	s.check("realtime: who is connected", t1("/live/who").text() == `["ana","bo"]`)
	s.check("realtime: past the runner's realtime_connections, 503", t1("/live?u=cy").status == 503)
	r := s.api("POST", "/api/services/t1/realtime/publish", map[string]string{"channel": "user:bo", "data": "just bo"})
	s.check("realtime: API publish", r.text() == "{\"delivered\":1}\n" && line() == "\n" && line() == "data: just bo\n", r.text())
}

func (s *suite) egressChecks() {
	out := func() string { return t1("/outbound?u=http://nothing.invalid/").text() }
	b := t1("/outbound?u=http://127.0.0.1:8080/").text()
	s.check("egress: private addresses are blocked", strings.HasPrefix(b, "403 blocked") && strings.Contains(b, "not a public internet address"), b)
	b = t1("/outbound?u=http://localhost:8080/").text()
	// Blocked as private once resolved, or not resolved at all (as the
	// network's DNS has it): never reached either way.
	s.check("egress: names of private addresses are blocked", strings.HasPrefix(b, "502 error") && (strings.Contains(b, "not a public internet address") || strings.Contains(b, "no such host")), b)
	b = out()
	s.check("egress: a failed request is a 502 with the cause", strings.HasPrefix(b, "502 error") && strings.Contains(b, "nothing.invalid"), b)
	r := s.api("PATCH", "/api/services/t1", map[string]any{"egress": []string{"*.example.com"}})
	s.check("egress: set allowed hosts", r.status == 200 && strings.Contains(r.text(), `"egress":["*.example.com"]`), r.text())
	b = out()
	s.check("egress: other hosts are blocked", strings.HasPrefix(b, "403 blocked") && strings.Contains(b, "does not allow nothing.invalid"), b)
	s.api("PATCH", "/api/services/t1", map[string]any{"egress": []string{}})
	b = out()
	s.check("egress: no hosts disables outbound requests", strings.Contains(b, "disabled"), b)
	s.check("egress: invalid hosts are refused", s.api("PATCH", "/api/services/t1", map[string]any{"egress": []string{"http://x"}}).status == 400)
	r = s.api("PATCH", "/api/services/t1", map[string]any{"egress": []string{"*"}})
	s.check("egress: back to any host", strings.Contains(r.text(), `"egress":["*"]`), r.text())
}

const limited = `
import { connect } from "cloudflare:sockets";
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/burn") { const end = Date.now() + Number(url.searchParams.get("ms")); while (Date.now() < end) {} return new Response("burnt"); }
    if (url.pathname === "/cycle") return env.NEXT.fetch("https://next/cycle");
    if (url.pathname === "/connect") {
      try {
        const sock = connect({ hostname: url.searchParams.get("host"), port: 80 });
        const w = sock.writable.getWriter();
        await w.write(new TextEncoder().encode("HEAD / HTTP/1.0\r\nHost: " + url.searchParams.get("host") + "\r\n\r\n"));
        const { value } = await sock.readable.getReader().read();
        await sock.close();
        return new Response(new TextDecoder().decode(value).split("\r\n")[0]);
      } catch (e) { return new Response("failed: " + e.message); }
    }
    if (url.pathname === "/ws") {
      try {
        const res = await fetch("https://echo.websocket.org/", { headers: { Upgrade: "websocket" } });
        const ws = res.webSocket;
        if (!ws) return new Response("no websocket: " + res.status);
        ws.accept();
        const got = new Promise((resolve) => ws.addEventListener("message", (e) => { if (String(e.data) === "ping") resolve("echo ping"); }));
        ws.send("ping");
        const out = await Promise.race([got, new Promise((r) => setTimeout(() => r("no echo"), 8000))]);
        ws.close();
        return new Response(out);
      } catch (e) { return new Response("failed: " + e.message); }
    }
    return new Response("ok");
  },
};
`

// limitChecks: each client's rate, a cycle of services stopped, outbound
// sockets and WebSockets through the egress proxy, and the CPU a service
// may use in a minute (last: it restarts the runner).
func (s *suite) limitChecks() {
	deploy := func(name string, bindings map[string]string) bool {
		return s.api("POST", "/api/services/"+name+"/deployments", map[string]any{"files": map[string]string{"index.js": limited}, "bindings": bindings}).status == 201
	}
	s.check("limits: deploy", deploy("lim", nil) && deploy("cyc1", map[string]string{"NEXT": "service:cyc2"}) && deploy("cyc2", map[string]string{"NEXT": "service:cyc1"}))

	// Each client at its rate: 2 a second, a burst of 4.
	r := s.api("PATCH", "/api/services/lim", map[string]any{"rate_limit": 2})
	s.check("rate limit: set on a service", r.status == 200 && strings.Contains(r.text(), `"rate_limit":2`), r.text())
	limited, retry := 0, ""
	for range 10 {
		r := gw("lim", "GET", "/", nil, nil)
		if r.status == 429 {
			limited++
			retry = r.header.Get("Retry-After")
		}
	}
	s.check("rate limit: past it, 429 with Retry-After", limited >= 5 && retry == "1", limited, retry)
	other := gw("lim", "GET", "/", nil, map[string]string{"X-Forwarded-For": "203.0.113.9"})
	s.check("rate limit: per client", other.status == 200, other.status)
	s.api("PATCH", "/api/services/lim", map[string]any{"rate_limit": 0})
	time.Sleep(1100 * time.Millisecond)
	s.check("rate limit: 0 is the runner's, none here", gw("lim", "GET", "/", nil, nil).status == 200)

	// Services that call each other in a cycle are stopped.
	c := gw("cyc1", "GET", "/cycle", nil, nil)
	s.check("bindings: a cycle of services is stopped with 508", c.status == 508, c.status, c.text())

	// Outbound sockets and WebSockets go through the egress proxy, held to
	// the hosts the service may reach.
	b := gw("lim", "GET", "/connect?host=example.com", nil, nil).text()
	if strings.Contains(b, "failed") && !strings.Contains(b, "blocked") {
		s.t.Logf("skip egress connect(): no internet here? %s", b)
	} else {
		s.check("egress: connect() reaches a public host", strings.HasPrefix(b, "HTTP/1."), b)
		s.api("PATCH", "/api/services/lim", map[string]any{"egress": []string{"example.org"}})
		b = gw("lim", "GET", "/connect?host=example.com", nil, nil).text()
		s.check("egress: connect() to another host is refused", strings.HasPrefix(b, "failed"), b)
		s.api("PATCH", "/api/services/lim", map[string]any{"egress": []string{"*"}})
		w := gw("lim", "GET", "/ws", nil, nil).text()
		s.check("egress: an outbound WebSocket", w == "echo ping", w)
	}

	// The CPU a service may use in a minute: 4 s here. Past it, it waits.
	r = s.api("PATCH", "/api/settings", map[string]any{"cpu_seconds_per_minute": 4})
	s.check("cpu: set cpu_seconds_per_minute", r.status == 202, r.text())
	time.Sleep(time.Second)
	waitFor(30*time.Second, 250*time.Millisecond, healthy)
	for range 3 {
		gw("lim", "GET", "/burn?ms=1500", nil, nil)
		time.Sleep(1200 * time.Millisecond)
	}
	go gw("lim", "GET", "/burn?ms=1500", nil, nil) // it may wait a minute
	s.check("cpu: past a minute's CPU, a service waits, and says so", waitFor(10*time.Second, 500*time.Millisecond, func() bool {
		return strings.Contains(s.api("GET", "/api/services/lim/logs?limit=50", nil).text(), "more than the 4s it may")
	}))
	for _, n := range []string{"lim", "cyc1", "cyc2"} {
		s.api("DELETE", "/api/services/"+n, nil)
	}
	s.api("PATCH", "/api/settings", map[string]any{"cpu_seconds_per_minute": 30})
	time.Sleep(time.Second)
	waitFor(30*time.Second, 250*time.Millisecond, healthy)
}

// mcpChecks: the API as an MCP server, at /mcp with the same token.
func (s *suite) mcpChecks() {
	rpc := func(id int, method string, params any) map[string]any {
		r := s.api("POST", "/mcp", map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
		return r.obj()
	}
	init := rpc(1, "initialize", map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "e2e", "version": "1"}})
	res, _ := init["result"].(map[string]any)
	s.check("mcp: initialize", res["protocolVersion"] == "2025-06-18" && res["instructions"] != nil, init)
	n := s.api("POST", "/mcp", map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"})
	s.check("mcp: a notification gets 202", n.status == 202, n.status)
	list := rpc(2, "tools/list", map[string]any{})
	tools, _ := list["result"].(map[string]any)["tools"].([]any)
	names := map[string]bool{}
	for _, t := range tools {
		names[t.(map[string]any)["name"].(string)] = true
	}
	s.check("mcp: tools", len(tools) > 10 && names["deploy"] && names["get_logs"] && names["query_db"], len(tools))
	call := func(id int, tool string, args map[string]any) (string, bool) {
		out := rpc(id, "tools/call", map[string]any{"name": tool, "arguments": args})
		r, _ := out["result"].(map[string]any)
		content, _ := r["content"].([]any)
		text := ""
		if len(content) > 0 {
			text, _ = content[0].(map[string]any)["text"].(string)
		}
		isErr, _ := r["isError"].(bool)
		return text, isErr
	}
	text, isErr := call(3, "deploy", map[string]any{"name": "mcp1", "files": map[string]any{"index.js": "export default { fetch: () => new Response('from mcp') }"}})
	s.check("mcp: deploy", !isErr && strings.Contains(text, `"status":"ready"`), text)
	s.check("mcp: the deployed service answers", gw("mcp1", "GET", "/", nil, nil).text() == "from mcp")
	text, isErr = call(4, "list_services", nil)
	s.check("mcp: list_services", !isErr && strings.Contains(text, `"mcp1"`), text)
	text, isErr = call(5, "get_service", map[string]any{"name": "nope"})
	s.check("mcp: an API error is the tool's", isErr && strings.Contains(text, "not_found"), text)
	text, isErr = call(6, "delete_service", map[string]any{"name": "mcp1"})
	s.check("mcp: delete_service", !isErr, text)
	bad := rpc(7, "tools/call", map[string]any{"name": "nothing"})
	s.check("mcp: an unknown tool", bad["error"] != nil, bad)
	s.check("mcp: no token, no MCP", do("POST", apiAddr, "runner", "/mcp", "", map[string]any{"jsonrpc": "2.0", "id": 1, "method": "ping"}, nil).status == 401)
}

func (s *suite) logChecks() {
	logs := s.api("GET", "/api/services/t1/logs?limit=500", nil).arr()
	has := func(list []any, fn func(m map[string]any) bool) bool {
		for _, x := range list {
			if m, ok := x.(map[string]any); ok && fn(m) {
				return true
			}
		}
		return false
	}
	msg := func(sub string) func(m map[string]any) bool {
		return func(m map[string]any) bool { return strings.Contains(fmt.Sprint(m["message"]), sub) }
	}
	s.check("logs: console output", has(logs, msg("hello from the service marker1")))
	s.check("logs: uncaught errors", has(logs, msg("thrown on purpose")))
	s.check("logs: CPU restarts", waitFor(5*time.Second, 250*time.Millisecond, func() bool {
		return has(s.api("GET", "/api/services/t1/logs?limit=500", nil).arr(), msg("kept the CPU busy"))
	}))
	s.check("logs: process starts", has(logs, func(m map[string]any) bool { return m["source"] == "runtime" && msg("started in")(m) }))
	s.check("logs: WebSocket and SSE close events", has(logs, msg("closed ana")) && has(logs, msg("closed bo")))
	last, _ := logs[len(logs)-1].(map[string]any)
	after := num(last["id"])
	time.AfterFunc(500*time.Millisecond, func() { t1("/log?m=marker2") })
	seen := false
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end) && !seen; {
		got := s.api("GET", fmt.Sprintf("/api/services/t1/logs?after=%d&wait=10", after), nil).arr()
		seen = has(got, msg("marker2"))
		if len(got) > 0 {
			m, _ := got[len(got)-1].(map[string]any)
			after = num(m["id"])
		}
	}
	s.check("logs: long poll", seen)
}

func (s *suite) operatorChecks(first string) {
	r := s.api("POST", "/api/services", map[string]string{"name": "empty"})
	s.check("create an empty service", r.status == 201 && r.obj()["status"] == "not_deployed", r.text())
	s.check("a service name is taken once", s.api("POST", "/api/services", map[string]string{"name": "empty"}).status == 400)
	r = s.api("POST", "/api/services", map[string]any{})
	name := fmt.Sprint(r.obj()["name"])
	s.check("a service without a name gets one", r.status == 201 && regexp.MustCompile(`^service-[a-z0-9]{4}$`).MatchString(name), r.text())
	s.api("DELETE", "/api/services/"+name, nil)
	src := s.api("GET", "/api/services/t1/deployments/"+first+"/source", nil).obj()
	files, _ := src["files"].(map[string]any)
	s.check("deployment source", src["main"] == "index.js" && files["page.html"] == "<h1>e2e</h1>", src)
	m := s.api("GET", "/api/services/t1/metrics?period=1h", nil).obj()
	points, _ := m["points"].([]any)
	totals, _ := m["totals"].(map[string]any)
	s.check("metrics count requests", len(points) == 60 && num(totals["requests"]) > 10 && num(totals["errors"]) >= 1 && num(totals["exceptions"]) >= 1, totals)
	logs := s.api("GET", "/api/services/t1/logs?limit=500", nil).text()
	s.check("logs: request lines", strings.Contains(logs, `"source":"request","level":"info","message":"GET /env 200`), "")
	s.check("logs: outbound requests", regexp.MustCompile(`"source":"fetch"[^}]*nothing\.invalid`).MatchString(logs))
	s.check("metrics count outbound requests", num(totals["outbound"]) >= 3 && num(totals["outbound_errors"]) >= 3, totals)
	var listed map[string]any
	for _, v := range s.api("GET", "/api/services?period=1h", nil).arr() {
		if sv, _ := v.(map[string]any); sv["name"] == "t1" {
			listed, _ = sv["traffic"].(map[string]any)
		}
	}
	s.check("services list with traffic", listed != nil && num(listed["requests"]) == num(totals["requests"]) && num(listed["errors"]) == num(totals["errors"]), listed)
	r = s.api("GET", "/api/services?period=2h", nil)
	s.check("services list: an unknown period is refused", r.status == 400, r.text())
	r = s.api("POST", "/api/services/t1/crons/run", map[string]string{"schedule": "* * * * *"})
	s.check("run a cron now", r.status == 201 && r.obj()["cron"] == "* * * * *", r.text())
	stores := s.api("GET", "/api/services/t1/storage", nil).text()
	s.check("storage lists bound stores", strings.Contains(stores, `"type":"db","binding":"DB"`) && strings.Contains(stores, `"type":"files","binding":"FILES"`) && strings.Count(stores, `"bound":true`) == 2, stores)
	archive := s.api("GET", "/api/services/t1/export", nil)
	s.check("export", archive.status == 200 && bytes.HasPrefix(archive.body, []byte{0x1f, 0x8b}))
	s.check("clear a store", s.api("DELETE", "/api/services/t1/storage/db/DB", nil).status == 204 && t1("/db/notes").status == 500)
	r = s.apiH("POST", "/api/services/t1/import", archive.body, map[string]string{"Content-Type": "application/gzip"})
	s.check("import restores the stores", r.status == 200 && strings.Contains(r.text(), `"stores":["db:DB","files:FILES"]`) && len(t1("/db/notes").arr()) == 1 && num(t1("/files/head?key=b/1").obj()["size"]) == 3, r.text())
	sys := s.api("GET", "/api/system", nil).obj()
	procs, _ := sys["processes"].([]any)
	running := false
	for _, p := range procs {
		if m, _ := p.(map[string]any); m["service"] == "t1" && num(m["memory"]) > 0 {
			running = true
		}
	}
	disk, _ := sys["disk"].(map[string]any)
	s.check("system", num(sys["services"]) == 3 && running && num(disk["total"]) > 0 && num(sys["data_bytes"]) > 0, sys)
	lines := s.api("GET", "/api/system/logs", nil).text()
	s.check("the runner's log", strings.Contains(lines, "runner started") && strings.Contains(lines, "by=root"))
	s.check("the runner's log holds no service lines", !strings.Contains(lines, "service="))
}

func (s *suite) quotaChecks() {
	a := s.api("PATCH", "/api/services/t1", map[string]any{"disk_mb": 1}).obj()
	s.check("quota: set", num(a["disk_quota"]) == 1<<20 && num(a["bytes"]) > 1<<20, a)
	s.check("quota: a full service cannot write files", gw("t1", "PUT", "/files/put?key=over", strings.Repeat("x", 1000), nil).status == 500)
	r := s.api("POST", "/api/services/t1/db/DB/query", map[string]string{"sql": "INSERT INTO notes (text) VALUES (zeroblob(100000))"})
	s.check("quota: nor grow its databases", r.status == 400 && strings.Contains(r.text(), "disk quota"), r.text())
	s.api("PATCH", "/api/services/t1", map[string]any{"disk_mb": 0})
	s.check("quota: back to the default", gw("t1", "PUT", "/files/put?key=over", "x", nil).status == 200)
}

func (s *suite) bundleChecks(first string) {
	r := s.api("GET", "/api/services/t1/deployments/"+first+"/bundle", nil)
	b := r.obj()
	files, _ := b["files"].(map[string]any)
	bindings, _ := b["bindings"].(map[string]any)
	s.check("export a deployment", r.status == 200 && strings.Contains(r.header.Get("Content-Disposition"), "attachment") && files["page.html"] == "<h1>e2e</h1>" && bindings["DB"] == "db")
	r = s.api("POST", "/api/services/t3/deployments", json.RawMessage(r.body))
	s.check("an exported deployment deploys again", r.status == 201 && gw("t3", "GET", "/", nil, nil).text() == "<h1>e2e</h1>", r.text())
	s.api("DELETE", "/api/services/t3", nil)
}

const site = `
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/api/hi") return new Response("from the code");
    const res = await env.ASSETS.fetch(req);
    // A single-page app: paths that are no file get the app.
    return res.status === 404 && !url.pathname.includes(".") ? env.ASSETS.fetch(new URL("/", url)) : res;
  },
};
`

func (s *suite) assetsChecks() {
	s.check("assets: public/ files through the code", t1("/hello.txt").text() == "static hello" && t1("/").text() == "<h1>e2e</h1>")
	files := map[string]string{"public/index.html": "<h1>site</h1>", "public/css/a.css": "a{}", "public/lib.js": "import x from 'three';", "public/docs/index.html": "docs"}
	s.check("public/ files need code", s.api("POST", "/api/services/site/deployments", map[string]any{"files": files}).status == 400)
	files["index.js"] = site
	r := s.api("POST", "/api/services/site/deployments", map[string]any{"files": files, "bindings": map[string]string{"ASSETS": "assets"}})
	s.check("a site deploys", r.status == 201, r.text())
	get := func(path string, h map[string]string) resp { return gw("site", "GET", path, nil, h) }
	r = get("/", nil)
	s.check("assets: index.html", r.status == 200 && r.text() == "<h1>site</h1>" && strings.HasPrefix(r.header.Get("Content-Type"), "text/html"), r.status)
	r = get("/lib.js", nil)
	s.check("assets: browser JS is served, not loaded", r.status == 200 && strings.Contains(r.text(), "three") && strings.Contains(r.header.Get("Content-Type"), "javascript"), r.status)
	s.check("assets: /x and /x/ serve x/index.html", get("/docs/", nil).text() == "docs" && get("/docs", nil).text() == "docs")
	s.check("assets: conditional requests", get("/lib.js", map[string]string{"If-None-Match": r.header.Get("Etag")}).status == 304)
	r = get("/lib.js", map[string]string{"Range": "bytes=0-5"})
	s.check("assets: ranges", r.status == 206 && r.text() == "import", r.status)
	s.check("assets: the code decides the rest", get("/app/route", nil).text() == "<h1>site</h1>" && get("/api/hi", nil).text() == "from the code")
	s.check("assets: a missing file is a 404", get("/nope.png", nil).status == 404)
}

const callee = `
export default {
  async fetch(req) {
    const url = new URL(req.url);
    return Response.json({ from: req.headers.get("x-zeroko-from"), ip: req.headers.get("x-forwarded-for"), path: url.pathname + url.search, host: url.host, method: req.method, body: await req.text() });
  },
};
`

const caller = `
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/unbound") return Response.json(await (await env.OTHER.fetch("http://x/")).status);
    return env.API.fetch("http://callee/echo?x=1", { method: "POST", body: "ping" });
  },
};
`

func (s *suite) serviceCallChecks() {
	st1 := s.api("POST", "/api/services/callee/deployments", map[string]any{"files": map[string]string{"index.js": callee}}).status
	st2 := s.api("POST", "/api/services/caller/deployments", map[string]any{"files": map[string]string{"index.js": caller}, "bindings": map[string]string{"API": "service:callee", "OTHER": "service:nobody"}}).status
	s.check("service bindings deploy", st1 == 201 && st2 == 201, st1, st2)
	r := gw("caller", "GET", "/", nil, nil)
	s.check("a service calls another through its binding", r.text() == `{"from":"caller","ip":null,"path":"/echo?x=1","host":"callee.`+domain+`","method":"POST","body":"ping"}`, r.text())
	r = gw("callee", "GET", "/", nil, map[string]string{"X-Zeroko-From": "caller"})
	o := r.obj()
	s.check("X-Zeroko-From cannot be forged from outside", r.status == 200 && o["from"] == nil && o["ip"] != nil, r.text())
	s.check("a missing service is a 404", gw("caller", "GET", "/unbound", nil, nil).text() == "404")
	s.check("a service cannot bind to itself", s.api("POST", "/api/services/caller/deployments", map[string]any{"files": map[string]string{"index.js": caller}, "bindings": map[string]string{"API": "service:caller"}}).status == 400)
	s.check("service bindings need a valid name", s.api("POST", "/api/services/caller/deployments", map[string]any{"files": map[string]string{"index.js": caller}, "bindings": map[string]string{"API": "service:Bad_Name"}}).status == 400)
	s.api("DELETE", "/api/services/callee", nil)
	s.api("DELETE", "/api/services/caller", nil)
}

func (s *suite) processCapChecks() {
	for i := range 4 {
		s.api("POST", fmt.Sprintf("/api/services/cap%d/deployments", i), map[string]any{"files": map[string]string{"index.js": fmt.Sprintf("export default { fetch() { return new Response('cap%d') } }", i)}})
	}
	served := true
	for i := range 4 {
		served = served && gw(fmt.Sprintf("cap%d", i), "GET", "/", nil, nil).text() == fmt.Sprintf("cap%d", i)
	}
	procs, _ := s.api("GET", "/api/system", nil).obj()["processes"].([]any)
	s.check("process cap: services take turns", served && len(procs) <= 3, procs)
	for i := range 4 {
		s.api("DELETE", fmt.Sprintf("/api/services/cap%d", i), nil)
	}
}

func (s *suite) backupChecks() {
	archive := s.api("GET", "/api/system/backup", nil)
	s.check("backup", archive.status == 200 && bytes.HasPrefix(archive.body, []byte{0x1f, 0x8b}), archive.status)
	s.api("DELETE", "/api/services/t2", nil)
	gz := map[string]string{"Content-Type": "application/gzip"}
	s.check("restore refuses other archives", s.apiH("POST", "/api/system/restore", []byte("nope"), gz).status == 400)
	r := s.apiH("POST", "/api/system/restore", archive.body, gz)
	s.check("restore restarts the runner", r.status == 202, r.text())
	time.Sleep(time.Second)
	s.check("the runner is back", waitFor(30*time.Second, 250*time.Millisecond, healthy))
	s.check("restore brings deleted services back", gw("t2", "GET", "/", nil, nil).text() == "t2")
	s.check("and their data", len(t1("/db/notes").arr()) == 1)
	s.check("and their public files", gw("site", "GET", "/", nil, nil).text() == "<h1>site</h1>")
}

// wsConnect opens a WebSocket to the gateway and returns the response's
// status line.
func wsConnect(path, host string) (net.Conn, *bufio.Reader, string) {
	c, err := net.DialTimeout("tcp", gwAddr, 10*time.Second)
	if err != nil {
		return nil, nil, err.Error()
	}
	c.SetDeadline(time.Now().Add(30 * time.Second))
	key := make([]byte, 16)
	rand.Read(key)
	fmt.Fprintf(c, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n",
		path, host, base64.StdEncoding.EncodeToString(key))
	r := bufio.NewReader(c)
	status, _ := r.ReadString('\n')
	for {
		l, err := r.ReadString('\n')
		if err != nil || l == "\r\n" {
			break
		}
	}
	return c, r, strings.TrimSpace(status)
}

// wsSend sends a masked text frame, as clients do.
func wsSend(c net.Conn, text string) {
	p, mask := []byte(text), make([]byte, 4)
	rand.Read(mask)
	frame := []byte{0x81, 0x80 | byte(len(p))}
	frame = append(frame, mask...)
	for i, b := range p {
		frame = append(frame, b^mask[i%4])
	}
	c.Write(frame)
}

// wsRecv reads a text frame, skipping pings.
func wsRecv(c net.Conn, r *bufio.Reader) string {
	for {
		h := make([]byte, 2)
		if _, err := io.ReadFull(r, h); err != nil {
			return err.Error()
		}
		n := int(h[1] & 0x7f)
		if n == 126 {
			ext := make([]byte, 2)
			io.ReadFull(r, ext)
			n = int(binary.BigEndian.Uint16(ext))
		}
		data := make([]byte, n)
		io.ReadFull(r, data)
		if h[0]&0x0f != 0x9 {
			return string(data)
		}
	}
}

const smokeService = `
export default {
  async fetch(req, env) {
    await env.DB.run("CREATE TABLE IF NOT EXISTS hits (n INTEGER)");
    await env.DB.run("INSERT INTO hits VALUES (1)");
    const [{ n }] = await env.DB.query("SELECT count(*) AS n FROM hits");
    if (new URL(req.url).pathname === "/hits") return Response.json(n);
    return env.ASSETS.fetch(req);
  },
};
`

// TestSmoke checks that an image starts and serves a service: settings, a
// deployment, a database and static files.
func TestSmoke(t *testing.T) {
	s := &suite{t: t, token: rootToken}
	s.check("health", healthy())
	s.configure()
	r := s.api("POST", "/api/services/smoke/deployments", map[string]any{
		"files":    map[string]string{"index.js": smokeService, "public/index.html": "<h1>smoke</h1>"},
		"bindings": map[string]string{"DB": "db", "ASSETS": "assets"},
	})
	s.check("deploy", r.status == 201, r.text())
	s.check("a database through a binding", gw("smoke", "GET", "/hits", nil, nil).text() == "1")
	s.check("files through the assets binding", gw("smoke", "GET", "/", nil, nil).text() == "<h1>smoke</h1>")
	s.api("DELETE", "/api/services/smoke", nil)
}
