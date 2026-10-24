// Package deploy defines what a deployment is: a service's modules and its
// manifest (bindings, crons, vars), and how both are validated.
package deploy

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
)

// Binding types.
const (
	DB       = "db"
	Files    = "files"
	Jobs     = "jobs"
	Realtime = "realtime"
	Assets   = "assets" // the deployment's public/ files

	// ServicePrefix starts a binding to another service: "service:<name>".
	ServicePrefix = "service:"
)

const (
	MaxFiles       = 2000
	MaxBundleBytes = 50 << 20
	MaxCrons       = 20
	MaxVars        = 100
	MaxVarBytes    = 64 << 10
)

// Manifest is everything about a deployment except its code.
type Manifest struct {
	Main     string            `json:"main"`
	Bindings map[string]string `json:"bindings,omitempty"` // env name -> binding type
	Crons    []Cron            `json:"crons,omitempty"`
	Vars     map[string]string `json:"vars,omitempty"`
	Assets   int               `json:"assets,omitempty"` // files under public/, counted by Parse
}

type Cron struct {
	Schedule string          `json:"schedule"`
	Payload  json.RawMessage `json:"payload,omitempty"`
}

// Module is one file of the service. Type follows workerd's module types,
// plus asset: a file under public/, served as it is and never loaded by
// workerd.
type Module struct {
	Name    string `json:"name"`
	Type    string `json:"type"` // esModule, text, data, json, wasm or asset
	Content []byte `json:"content"`
}

// AssetPrefix is the directory of the files a service serves as they are.
const AssetPrefix = "public/"

// Request is the body of a deployment: the manifest plus the files, each
// either a string (UTF-8 content) or {"base64": "..."}.
type Request struct {
	Manifest
	Files map[string]json.RawMessage `json:"files"`
}

type Error string

func (e Error) Error() string { return string(e) }

func invalid(format string, a ...any) error { return Error(fmt.Sprintf(format, a...)) }

var (
	envName     = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)
	moduleName  = regexp.MustCompile(`^[A-Za-z0-9_\-.@+/]+$`)
	ServiceName = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?$`)
)

// ValidEnvName reports whether name can be a binding, var or secret name.
func ValidEnvName(name string) bool {
	return envName.MatchString(name) && !strings.HasPrefix(name, "__ZEROKO")
}

func moduleType(name string) string {
	if strings.HasPrefix(name, AssetPrefix) {
		return "asset"
	}
	switch strings.ToLower(path.Ext(name)) {
	case ".js", ".mjs":
		return "esModule"
	case ".json":
		return "json"
	case ".wasm":
		return "wasm"
	case ".txt", ".html", ".htm", ".css", ".md", ".svg", ".xml", ".csv", ".map":
		return "text"
	}
	return "data"
}

// Parse validates a deployment request and returns its manifest and modules.
func Parse(r *Request) (*Manifest, []Module, error) {
	m := r.Manifest
	if len(r.Files) == 0 {
		return nil, nil, invalid("files is required: a map of path to content")
	}
	if len(r.Files) > MaxFiles {
		return nil, nil, invalid("at most %d files", MaxFiles)
	}
	names := make([]string, 0, len(r.Files))
	for name := range r.Files {
		names = append(names, name)
	}
	sort.Strings(names)
	var modules []Module
	total := 0
	for _, name := range names {
		if !moduleName.MatchString(name) || len(name) > 256 || strings.HasPrefix(name, "/") || path.Clean(name) != name || strings.HasPrefix(name, "__zeroko") {
			return nil, nil, invalid("invalid file path %q: use a relative path such as src/index.js", name)
		}
		var content []byte
		raw := r.Files[name]
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			content = []byte(s)
		} else {
			var b struct {
				Base64 string `json:"base64"`
			}
			if err := json.Unmarshal(raw, &b); err != nil {
				return nil, nil, invalid("file %q: content must be a string or {\"base64\": \"...\"}", name)
			}
			if content, err = base64.StdEncoding.DecodeString(b.Base64); err != nil {
				return nil, nil, invalid("file %q: invalid base64", name)
			}
		}
		total += len(content)
		mod := Module{Name: name, Type: moduleType(name), Content: content}
		if mod.Type == "json" && !json.Valid(content) {
			return nil, nil, invalid("file %q is not valid JSON", name)
		}
		modules = append(modules, mod)
	}
	if total > MaxBundleBytes {
		return nil, nil, invalid("the files add up to more than %d MB", MaxBundleBytes>>20)
	}

	if m.Main == "" {
		for _, c := range []string{"index.js", "src/index.js", "worker.js", "main.js"} {
			if _, ok := r.Files[c]; ok {
				m.Main = c
				break
			}
		}
		if m.Main == "" {
			return nil, nil, invalid("main is required: the path of the module that exports the service")
		}
	}
	if _, ok := r.Files[m.Main]; !ok || moduleType(m.Main) != "esModule" {
		return nil, nil, invalid("main %q must be one of the files, a .js or .mjs module outside public/", m.Main)
	}

	used := map[string]bool{}
	for name, typ := range m.Bindings {
		if !ValidEnvName(name) {
			return nil, nil, invalid("invalid binding name %q", name)
		}
		switch typ {
		case DB, Files, Jobs, Realtime, Assets:
		default:
			if target, ok := strings.CutPrefix(typ, ServicePrefix); !ok || !ServiceName.MatchString(target) {
				return nil, nil, invalid("binding %s: unknown type %q (db, files, jobs, realtime, assets or service:<name>)", name, typ)
			}
		}
		used[name] = true
	}
	if len(m.Vars) > MaxVars {
		return nil, nil, invalid("at most %d vars", MaxVars)
	}
	for name, v := range m.Vars {
		if !ValidEnvName(name) {
			return nil, nil, invalid("invalid var name %q", name)
		}
		if used[name] {
			return nil, nil, invalid("%s is both a binding and a var", name)
		}
		if len(v) > MaxVarBytes {
			return nil, nil, invalid("var %s is larger than %d KB", name, MaxVarBytes>>10)
		}
	}
	if len(m.Crons) > MaxCrons {
		return nil, nil, invalid("at most %d crons", MaxCrons)
	}
	for i, c := range m.Crons {
		if _, err := ParseCron(c.Schedule); err != nil {
			return nil, nil, invalid("cron %q: %v", c.Schedule, err)
		}
		if len(c.Payload) == 0 {
			m.Crons[i].Payload = json.RawMessage("null")
		} else if !json.Valid(c.Payload) {
			return nil, nil, invalid("cron %q: payload is not valid JSON", c.Schedule)
		}
	}
	m.Assets = 0
	for _, mod := range modules {
		if mod.Type == "asset" {
			m.Assets++
		}
	}
	return &m, modules, nil
}
