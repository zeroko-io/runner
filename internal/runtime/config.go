package runtime

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/zeroko-io/runner/internal/deploy"
)

// CompatibilityDate is the workerd compatibility date every service runs with.
const CompatibilityDate = "2026-10-01"

var (
	//go:embed js/bindings.js
	bindingsJS string
	//go:embed js/main.js
	mainJS string
)

var entrypoints = map[string]string{
	deploy.DB:       "Db",
	deploy.Files:    "Files",
	deploy.Jobs:     "Jobs",
	deploy.Realtime: "Realtime",
	deploy.Assets:   "Assets",
}

// writeConfig writes a process's directory: its workerd config and every
// module it embeds. The service reaches the runner only through its bindings, and
// the outside only through the runner's egress proxy.
func writeConfig(dir string, spec *Spec, token string, opt *Options) error {
	if err := os.MkdirAll(filepath.Join(dir, "m"), 0o700); err != nil {
		return err
	}
	main, _ := json.Marshal("./" + spec.Manifest.Main)
	files := map[string]string{
		"main.js":     strings.Replace(mainJS, `"__ZEROKO_MAIN__"`, string(main), 1),
		"bindings.js": bindingsJS,
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			return err
		}
	}

	var b strings.Builder
	b.WriteString("using Workerd = import \"/workerd/workerd.capnp\";\n")
	b.WriteString("const config :Workerd.Config = (\n  services = [\n")
	b.WriteString("    (name = \"main\", worker = (\n      modules = [\n")
	b.WriteString("        (name = \"__zeroko_main.js\", esModule = embed \"main.js\"),\n")
	for i, m := range spec.Modules {
		if m.Type == "asset" {
			continue // served by the runner, not loaded by workerd
		}
		file := fmt.Sprintf("m/%d", i)
		if err := os.WriteFile(filepath.Join(dir, file), m.Content, 0o600); err != nil {
			return err
		}
		fmt.Fprintf(&b, "        (name = %s, %s = embed %s),\n", quote(m.Name), m.Type, quote(file))
	}
	b.WriteString("      ],\n")
	fmt.Fprintf(&b, "      compatibilityDate = %s,\n", quote(CompatibilityDate))
	b.WriteString("      bindings = [\n")
	for _, name := range sortedKeys(spec.Manifest.Bindings) {
		typ, props := spec.Manifest.Bindings[name], map[string]string{"binding": name}
		entrypoint := entrypoints[typ]
		if target, ok := strings.CutPrefix(typ, deploy.ServicePrefix); ok {
			entrypoint, props["service"] = "Service", target
		}
		js, _ := json.Marshal(props)
		fmt.Fprintf(&b, "        (name = %s, service = (name = \"zeroko\", entrypoint = %s, props = (json = %s))),\n",
			quote(name), quote(entrypoint), quote(string(js)))
	}
	for _, name := range sortedKeys(spec.Env) {
		fmt.Fprintf(&b, "        (name = %s, text = %s),\n", quote(name), quote(spec.Env[name]))
	}
	b.WriteString("      ],\n      globalOutbound = \"egress\",\n    )),\n")
	b.WriteString("    (name = \"zeroko\", worker = (\n")
	b.WriteString("      modules = [(name = \"bindings.js\", esModule = embed \"bindings.js\")],\n")
	fmt.Fprintf(&b, "      compatibilityDate = %s,\n", quote(CompatibilityDate))
	fmt.Fprintf(&b, "      bindings = [(name = \"TOKEN\", text = %s), (name = \"CONTROL\", service = \"control\")],\n", quote(token))
	b.WriteString("      globalOutbound = \"control\",\n    )),\n")
	fmt.Fprintf(&b, "    (name = \"control\", external = (address = %s, http = ())),\n", quote("unix:"+opt.ControlSock))
	// Requests in proxy form, with the absolute URL; Go's server wants a Host
	// header besides, which it ignores for those.
	fmt.Fprintf(&b, "    (name = \"egress\", external = (address = %s, http = (style = proxy, injectRequestHeaders = [(name = \"x-zeroko-token\", value = %s), (name = \"host\", value = \"egress\")]))),\n",
		quote("unix:"+opt.EgressSock), quote(token))
	b.WriteString("  ],\n  sockets = [\n")
	fmt.Fprintf(&b, "    (name = \"public\", address = %s, http = (style = host, forwardedProtoHeader = \"x-forwarded-proto\"), service = \"main\"),\n",
		quote("unix:"+filepath.Join(dir, "public.sock")))
	fmt.Fprintf(&b, "    (name = \"internal\", address = %s, http = (), service = (name = \"main\", entrypoint = \"__ZerokoInternal\")),\n",
		quote("unix:"+filepath.Join(dir, "internal.sock")))
	b.WriteString("  ],\n")
	fmt.Fprintf(&b, "  v8Flags = [%s],\n", quote(fmt.Sprintf("--max-old-space-size=%d", opt.HeapMB)))
	b.WriteString("  structuredLogging = true,\n);\n")
	return os.WriteFile(filepath.Join(dir, "config.capnp"), []byte(b.String()), 0o600)
}

// quote writes s as a Cap'n Proto text literal.
func quote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"' || c == '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		case c < 0x20 || c == 0x7f:
			fmt.Fprintf(&b, "\\x%02x", c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
	return b.String()
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
