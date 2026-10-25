package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
)

// The runner's API as an MCP server (the Model Context Protocol), at
// POST /mcp with the same bearer tokens: each tool is a call to the API,
// made in the process with the caller's own token, so it's checked,
// limited and logged as any other. Streamable HTTP, answered as JSON; no
// sessions and no server-sent events: every tool answers at once.

// mcpVersions are the protocol versions the runner speaks, newest first.
var mcpVersions = []string{"2025-11-25", "2025-06-18", "2025-03-26"}

const mcpInstructions = `Zeroko runs JavaScript HTTP services (ES modules on workerd) with SQLite, files, jobs, crons and realtime as bindings. Deploy with the deploy tool: every deployment sends every file. A failed deployment leaves the running version serving; read its error, fix the code and deploy again. Then request the service's url and read its logs. The skill describes the runtime, the bindings and the limits in full: ask the user for it if you don't have it.`

type mcpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type mcpError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// mcpTool is a tool and the API call it makes from its arguments.
type mcpTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
	Annotations map[string]any `json:"annotations,omitempty"`
	call        func(args map[string]any) (method, path string, body any)
}

// schema is a JSON schema of an object: its properties, and those required.
func schema(props map[string]any, required ...string) map[string]any {
	s := map[string]any{"type": "object", "properties": props}
	if len(required) > 0 {
		s["required"] = required
	}
	return s
}

func str(desc string) map[string]any { return map[string]any{"type": "string", "description": desc} }

var (
	serviceArg = str("The service's name.")
	readOnly   = map[string]any{"readOnlyHint": true}
	destroys   = map[string]any{"destructiveHint": true}
)

func arg(args map[string]any, k string) string {
	switch v := args[k].(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
	return ""
}

func svc(args map[string]any, rest string) string {
	return "/api/services/" + url.PathEscape(arg(args, "name")) + rest
}

// pick is the arguments named, those given, as a body.
func pick(args map[string]any, keys ...string) map[string]any {
	out := map[string]any{}
	for _, k := range keys {
		if v, ok := args[k]; ok {
			out[k] = v
		}
	}
	return out
}

var mcpTools = []mcpTool{
	{
		Name:        "list_services",
		Description: "The services: name, url, status and the active deployment.",
		InputSchema: schema(map[string]any{}),
		Annotations: readOnly,
		call:        func(map[string]any) (string, string, any) { return "GET", "/api/services", nil },
	},
	{
		Name:        "get_service",
		Description: "A service: status, active deployment, manifest, bindings, usage, disk quota and process.",
		InputSchema: schema(map[string]any{"name": serviceArg}, "name"),
		Annotations: readOnly,
		call:        func(a map[string]any) (string, string, any) { return "GET", svc(a, ""), nil },
	},
	{
		Name: "deploy",
		Description: "Deploys a new version of a service, creating it if it doesn't exist. Every deployment sends every file. " +
			"Answers the deployment and the service's url; a failed one has status failed and its error, and the previous version keeps serving.",
		InputSchema: schema(map[string]any{
			"name": str("The service's name: 1 to 40 lowercase letters, digits and hyphens."),
			"main": str("The main module (index.js by default)."),
			"files": map[string]any{
				"type":                 "object",
				"description":          `Path to content: a string, or {"base64": "..."} for binary files. Files under public/ are served as they are by an assets binding.`,
				"additionalProperties": true,
			},
			"bindings": map[string]any{
				"type":                 "object",
				"description":          `Env name to binding type: db, files, jobs, realtime, assets or service:<name>.`,
				"additionalProperties": map[string]any{"type": "string"},
			},
			"crons": map[string]any{
				"type":        "array",
				"description": `Schedules that queue a job: [{"schedule": "0 * * * *", "payload": {...}}], in UTC.`,
				"items":       map[string]any{"type": "object"},
			},
			"vars": map[string]any{
				"type":                 "object",
				"description":          "Plain strings in env. Use set_secret for credentials.",
				"additionalProperties": map[string]any{"type": "string"},
			},
		}, "name", "files"),
		call: func(a map[string]any) (string, string, any) {
			return "POST", svc(a, "/deployments"), pick(a, "main", "files", "bindings", "crons", "vars")
		},
	},
	{
		Name:        "get_logs",
		Description: "A service's logs, oldest first: requests, console output and errors, outbound requests, and the runtime's (starts, restarts, failed deployments).",
		InputSchema: schema(map[string]any{
			"name":  serviceArg,
			"limit": map[string]any{"type": "integer", "description": "How many entries, up to 1000 (200 by default)."},
			"after": map[string]any{"type": "integer", "description": "Only the entries after this id."},
		}, "name"),
		Annotations: readOnly,
		call: func(a map[string]any) (string, string, any) {
			q := url.Values{}
			for _, k := range []string{"limit", "after"} {
				if v := arg(a, k); v != "" {
					q.Set(k, v)
				}
			}
			return "GET", svc(a, "/logs?"+q.Encode()), nil
		},
	},
	{
		Name:        "get_metrics",
		Description: "A service's requests, 5xx, exceptions, latency (p50, p95) and outbound requests over a period.",
		InputSchema: schema(map[string]any{
			"name":   serviceArg,
			"period": map[string]any{"type": "string", "enum": []string{"1h", "24h", "7d", "30d"}},
		}, "name"),
		Annotations: readOnly,
		call: func(a map[string]any) (string, string, any) {
			p := arg(a, "period")
			if p == "" {
				p = "24h"
			}
			return "GET", svc(a, "/metrics?period="+url.QueryEscape(p)), nil
		},
	},
	{
		Name:        "list_deployments",
		Description: "A service's deployments, newest first: the last 10 are kept.",
		InputSchema: schema(map[string]any{"name": serviceArg}, "name"),
		Annotations: readOnly,
		call:        func(a map[string]any) (string, string, any) { return "GET", svc(a, "/deployments"), nil },
	},
	{
		Name:        "get_deployment_source",
		Description: "The files of a deployment, as deployed: each one's text, or null for binary files.",
		InputSchema: schema(map[string]any{"name": serviceArg, "deployment": str("The deployment's id.")}, "name", "deployment"),
		Annotations: readOnly,
		call: func(a map[string]any) (string, string, any) {
			return "GET", svc(a, "/deployments/"+url.PathEscape(arg(a, "deployment"))+"/source"), nil
		},
	},
	{
		Name:        "activate_deployment",
		Description: "Rolls a service back (or forward) to one of its ready deployments.",
		InputSchema: schema(map[string]any{"name": serviceArg, "deployment": str("The deployment's id.")}, "name", "deployment"),
		call: func(a map[string]any) (string, string, any) {
			return "POST", svc(a, "/deployments/"+url.PathEscape(arg(a, "deployment"))+"/activate"), nil
		},
	},
	{
		Name:        "update_service",
		Description: "Renames a service (its address changes), or sets the hosts it may reach (egress) or its disk quota.",
		InputSchema: schema(map[string]any{
			"name":     serviceArg,
			"new_name": str("A new name: the service answers at the new address only."),
			"egress":   map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": `Hosts it may reach: "api.example.com", "*.example.com", or "*" for any; empty for none.`},
			"disk_mb":  map[string]any{"type": "integer", "description": "Its disk quota in MB; 0 for the default."},
		}, "name"),
		call: func(a map[string]any) (string, string, any) {
			body := pick(a, "egress", "disk_mb")
			if v, ok := a["new_name"]; ok {
				body["name"] = v
			}
			return "PATCH", svc(a, ""), body
		},
	},
	{
		Name:        "stop_service",
		Description: "Stops a service: it answers 503 and runs no jobs until started.",
		InputSchema: schema(map[string]any{"name": serviceArg}, "name"),
		call:        func(a map[string]any) (string, string, any) { return "POST", svc(a, "/stop"), nil },
	},
	{
		Name:        "start_service",
		Description: "Starts a stopped service.",
		InputSchema: schema(map[string]any{"name": serviceArg}, "name"),
		call:        func(a map[string]any) (string, string, any) { return "POST", svc(a, "/start"), nil },
	},
	{
		Name:        "delete_service",
		Description: "Deletes a service and all its data, for good. Ask the user first.",
		InputSchema: schema(map[string]any{"name": serviceArg}, "name"),
		Annotations: destroys,
		call:        func(a map[string]any) (string, string, any) { return "DELETE", svc(a, ""), nil },
	},
	{
		Name:        "set_secret",
		Description: "Sets a secret of a service: a string in env, applied at once, never shown again. The service must exist.",
		InputSchema: schema(map[string]any{"name": serviceArg, "secret": str("The secret's name, as in env."), "value": str("Its value.")}, "name", "secret", "value"),
		call: func(a map[string]any) (string, string, any) {
			return "PUT", svc(a, "/secrets/"+url.PathEscape(arg(a, "secret"))), map[string]any{"value": a["value"]}
		},
	},
	{
		Name:        "list_secrets",
		Description: "A service's secrets' names (never their values).",
		InputSchema: schema(map[string]any{"name": serviceArg}, "name"),
		Annotations: readOnly,
		call:        func(a map[string]any) (string, string, any) { return "GET", svc(a, "/secrets"), nil },
	},
	{
		Name:        "delete_secret",
		Description: "Deletes a secret of a service.",
		InputSchema: schema(map[string]any{"name": serviceArg, "secret": str("The secret's name.")}, "name", "secret"),
		Annotations: destroys,
		call: func(a map[string]any) (string, string, any) {
			return "DELETE", svc(a, "/secrets/"+url.PathEscape(arg(a, "secret"))), nil
		},
	},
	{
		Name:        "set_var",
		Description: "Sets a var of a service without a deployment: a plain string in env, over the deployment's var of the same name.",
		InputSchema: schema(map[string]any{"name": serviceArg, "var": str("The var's name."), "value": str("Its value.")}, "name", "var", "value"),
		call: func(a map[string]any) (string, string, any) {
			return "PUT", svc(a, "/vars/"+url.PathEscape(arg(a, "var"))), map[string]any{"value": a["value"]}
		},
	},
	{
		Name:        "query_db",
		Description: "Runs SQL on a service's db binding: reads and writes alike.",
		InputSchema: schema(map[string]any{
			"name":    serviceArg,
			"binding": str("The db binding's name, as in env (such as DB)."),
			"sql":     str("One or more statements."),
			"params":  map[string]any{"type": "array", "description": "Values for the ? placeholders."},
		}, "name", "binding", "sql"),
		call: func(a map[string]any) (string, string, any) {
			return "POST", svc(a, "/db/"+url.PathEscape(arg(a, "binding"))+"/query"), pick(a, "sql", "params")
		},
	},
	{
		Name:        "list_tables",
		Description: "The tables of a service's db binding: name, schema and rows.",
		InputSchema: schema(map[string]any{"name": serviceArg, "binding": str("The db binding's name.")}, "name", "binding"),
		Annotations: readOnly,
		call: func(a map[string]any) (string, string, any) {
			return "GET", svc(a, "/db/"+url.PathEscape(arg(a, "binding"))+"/tables"), nil
		},
	},
	{
		Name:        "list_files",
		Description: "The files of a service's files binding, by key, up to 1000; pass the cursor back for more.",
		InputSchema: schema(map[string]any{
			"name":    serviceArg,
			"binding": str("The files binding's name."),
			"prefix":  str("Only keys that start with it."),
			"cursor":  str("Where the last list ended."),
		}, "name", "binding"),
		Annotations: readOnly,
		call: func(a map[string]any) (string, string, any) {
			q := url.Values{}
			for _, k := range []string{"prefix", "cursor"} {
				if v := arg(a, k); v != "" {
					q.Set(k, v)
				}
			}
			return "GET", svc(a, "/files/"+url.PathEscape(arg(a, "binding"))+"?"+q.Encode()), nil
		},
	},
	{
		Name:        "run_cron",
		Description: "Queues the job of one of a service's crons now, by its schedule.",
		InputSchema: schema(map[string]any{"name": serviceArg, "schedule": str("The cron's schedule, as deployed.")}, "name", "schedule"),
		call: func(a map[string]any) (string, string, any) {
			return "POST", svc(a, "/crons/run"), pick(a, "schedule")
		},
	},
}

// mcp serves POST /mcp: one JSON-RPC message, answered as JSON.
func (s *Server) mcp(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "MCP is POST /mcp, answered as JSON.")
		return
	}
	var req mcpRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 100<<20)).Decode(&req); err != nil || req.Method == "" {
		mcpReply(w, nil, nil, &mcpError{-32700, "The body is one JSON-RPC message."})
		return
	}
	// A notification asks for no answer.
	if len(req.ID) == 0 {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	switch req.Method {
	case "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		json.Unmarshal(req.Params, &p)
		version := mcpVersions[0]
		for _, v := range mcpVersions {
			if v == p.ProtocolVersion {
				version = v
			}
		}
		mcpReply(w, req.ID, map[string]any{
			"protocolVersion": version,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "zeroko", "title": "Zeroko", "version": s.p.Version},
			"instructions":    mcpInstructions,
		}, nil)
	case "ping":
		mcpReply(w, req.ID, map[string]any{}, nil)
	case "tools/list":
		mcpReply(w, req.ID, map[string]any{"tools": mcpTools}, nil)
	case "tools/call":
		var p struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			mcpReply(w, req.ID, nil, &mcpError{-32602, "params are {name, arguments}."})
			return
		}
		for _, t := range mcpTools {
			if t.Name == p.Name {
				mcpReply(w, req.ID, s.mcpCall(r, t, p.Arguments), nil)
				return
			}
		}
		mcpReply(w, req.ID, nil, &mcpError{-32602, "No tool named " + p.Name + "."})
	default:
		mcpReply(w, req.ID, nil, &mcpError{-32601, "No method " + req.Method + "."})
	}
}

// mcpCall makes a tool's API call, as the caller, and answers what the API
// answered: its JSON, or its error as the tool's.
func (s *Server) mcpCall(r *http.Request, t mcpTool, args map[string]any) map[string]any {
	if args == nil {
		args = map[string]any{}
	}
	method, path, body := t.call(args)
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	inner, _ := http.NewRequestWithContext(r.Context(), method, path, rd)
	inner.Header.Set("Authorization", r.Header.Get("Authorization"))
	inner.Header.Set("Content-Type", "application/json")
	inner.RemoteAddr = r.RemoteAddr
	for _, h := range []string{"X-Forwarded-For", "X-Real-Ip"} {
		if v := r.Header.Get(h); v != "" {
			inner.Header.Set(h, v)
		}
	}
	rec := httptest.NewRecorder()
	s.api.ServeHTTP(rec, inner)
	text := strings.TrimSpace(rec.Body.String())
	if text == "" {
		text = http.StatusText(rec.Code)
	}
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": text}},
		"isError": rec.Code >= 400,
	}
}

func mcpReply(w http.ResponseWriter, id json.RawMessage, result any, e *mcpError) {
	msg := map[string]any{"jsonrpc": "2.0", "id": id}
	if id == nil {
		msg["id"] = nil
	}
	if e != nil {
		msg["error"] = e
	} else {
		msg["result"] = result
	}
	writeJSON(w, http.StatusOK, msg)
}
