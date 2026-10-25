//go:build e2e

package e2e

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestUpgrade: a runner of one version, with a service and its data, moved
// to another on the same volume, as plane moves every runner to each
// release: everything stays, and the new version's migrations apply.
//
//	RUNNER_OLD_IMAGE=ghcr.io/zeroko-io/runner:latest RUNNER_NEW_IMAGE=ghcr.io/zeroko-io/runner:local \
//	  go test -tags e2e -count=1 ./test/e2e -run TestUpgrade
//
// It runs the runners itself, with Docker, on ports 8282 and 8283.
func TestUpgrade(t *testing.T) {
	oldImage, newImage := os.Getenv("RUNNER_OLD_IMAGE"), os.Getenv("RUNNER_NEW_IMAGE")
	if oldImage == "" || newImage == "" {
		t.Skip("RUNNER_OLD_IMAGE and RUNNER_NEW_IMAGE are required")
	}
	const name, volume = "zeroko-upgrade-e2e", "zeroko-upgrade-e2e"
	docker := func(args ...string) string {
		out, err := exec.Command("docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("docker %s: %v %s", strings.Join(args, " "), err, out)
		}
		return string(out)
	}
	exec.Command("docker", "rm", "-f", name).Run()
	exec.Command("docker", "volume", "rm", name).Run()
	defer func() {
		exec.Command("docker", "rm", "-f", name).Run()
		exec.Command("docker", "volume", "rm", volume).Run()
	}()
	apiAddr, gwAddr = "127.0.0.1:8283", "127.0.0.1:8282"
	start := func(image string) {
		docker("run", "-d", "--name", name, "-p", "8282:8080", "-p", "8283:8081",
			"-v", volume+":/data", "-e", "ZEROKO_TOKEN="+rootToken, image)
		if !waitFor(60*time.Second, 250*time.Millisecond, healthy) {
			t.Fatalf("%s didn't start: %s", image, docker("logs", name))
		}
	}
	s := &suite{t: t, token: rootToken}

	start(oldImage)
	s.configure()
	r := s.api("POST", "/api/services/keep/deployments", map[string]any{
		"files": map[string]string{"index.js": `export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/write") {
      await env.DB.run("CREATE TABLE IF NOT EXISTS notes (text TEXT)");
      await env.DB.run("INSERT INTO notes VALUES ('kept')");
      await env.FILES.put("a.txt", "file kept");
      return new Response("written");
    }
    const [row] = await env.DB.query("SELECT text FROM notes");
    const file = await (await env.FILES.get("a.txt")).text();
    return new Response(row.text + " " + file + " " + env.SECRET);
  },
};`},
		"bindings": map[string]string{"DB": "db", "FILES": "files"},
	})
	s.check("upgrade: deploy on the old version", r.status == 201, r.text())
	s.check("upgrade: write data", gw("keep", "GET", "/write", nil, nil).text() == "written")
	s.api("PUT", "/api/services/keep/secrets/SECRET", map[string]string{"value": "secret kept"})
	tok := s.api("POST", "/api/tokens", map[string]any{"name": "kept"}).obj()
	agent, _ := tok["secret"].(string)
	s.check("upgrade: the old version serves it all", gw("keep", "GET", "/", nil, nil).text() == "kept file kept secret kept")

	docker("stop", "-t", "10", name)
	docker("rm", name)
	start(newImage)
	s.check("upgrade: the new version serves the same data, files and secret", gw("keep", "GET", "/", nil, nil).text() == "kept file kept secret kept")
	r = do("GET", apiAddr, "runner", "/api/services", agent, nil, nil)
	s.check("upgrade: an agent's token still works", r.status == 200 && strings.Contains(r.text(), `"keep"`), r.text())
	r = s.api("GET", "/api/services/keep", nil)
	s.check("upgrade: the new version's migrations applied", r.status == 200 && strings.Contains(r.text(), `"rate_limit":0`), r.text())
	s.check("upgrade: its settings kept", strings.Contains(s.api("GET", "/api/settings", nil).text(), `"domain":"`+domain+`"`))
	r = s.api("POST", "/api/services/keep/deployments", map[string]any{"files": map[string]string{"index.js": `export default { fetch: () => new Response("v2") }`}})
	s.check("upgrade: and deploys anew", r.status == 201 && gw("keep", "GET", "/", nil, nil).text() == "v2", r.text())
}
