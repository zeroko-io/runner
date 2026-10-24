package deploy

import (
	"encoding/json"
	"testing"
	"time"
)

func TestCron(t *testing.T) {
	at := func(s string) time.Time {
		v, err := time.Parse("2006-01-02 15:04", s)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	cases := []struct {
		expr string
		at   string
		want bool
	}{
		{"* * * * *", "2026-10-21 10:07", true},
		{"*/15 * * * *", "2026-10-21 10:30", true},
		{"*/15 * * * *", "2026-10-21 10:31", false},
		{"0 9 * * 1-5", "2026-10-21 09:00", true},  // Wednesday
		{"0 9 * * 1-5", "2026-10-25 09:00", false}, // Sunday
		{"0 0 * * 7", "2026-10-25 00:00", true},    // 7 is Sunday
		{"0 0 1,15 * *", "2026-10-15 00:00", true},
		{"0 0 1 * 1", "2026-10-26 00:00", true}, // either day field may match
		{"5-10/5 * * * *", "2026-10-21 10:10", true},
	}
	for _, c := range cases {
		s, err := ParseCron(c.expr)
		if err != nil {
			t.Fatalf("%s: %v", c.expr, err)
		}
		if got := s.Matches(at(c.at)); got != c.want {
			t.Errorf("%s at %s: got %v", c.expr, c.at, got)
		}
	}
	for _, bad := range []string{"* * * *", "60 * * * *", "*/0 * * * *", "a * * * *", "5-1 * * * *"} {
		if _, err := ParseCron(bad); err == nil {
			t.Errorf("%q: want an error", bad)
		}
	}
}

func TestParse(t *testing.T) {
	files := func(m map[string]string) map[string]json.RawMessage {
		out := map[string]json.RawMessage{}
		for k, v := range m {
			b, _ := json.Marshal(v)
			out[k] = b
		}
		return out
	}
	m, mods, err := Parse(&Request{Files: files(map[string]string{"src/index.js": "export default {}", "a.html": "<p>", "d.json": "{}"})})
	if err != nil || m.Main != "src/index.js" || len(mods) != 3 {
		t.Fatalf("%+v %v", m, err)
	}
	types := map[string]string{}
	for _, mod := range mods {
		types[mod.Name] = mod.Type
	}
	if types["a.html"] != "text" || types["d.json"] != "json" || types["src/index.js"] != "esModule" {
		t.Fatalf("types: %v", types)
	}
	bad := []Request{
		{},
		{Files: files(map[string]string{"../x.js": ""})},
		{Files: files(map[string]string{"/x.js": ""})},
		{Files: files(map[string]string{"__zeroko_main.js": ""}), Manifest: Manifest{Main: "__zeroko_main.js"}},
		{Files: files(map[string]string{"x.txt": ""})},
		{Files: files(map[string]string{"index.js": ""}), Manifest: Manifest{Bindings: map[string]string{"DB": "kv"}}},
		{Files: files(map[string]string{"index.js": ""}), Manifest: Manifest{Bindings: map[string]string{"DB": "db"}, Vars: map[string]string{"DB": "x"}}},
		{Files: files(map[string]string{"index.js": ""}), Manifest: Manifest{Crons: []Cron{{Schedule: "nope"}}}},
		{Files: files(map[string]string{"index.js": "", "d.json": "{"})},
	}
	for i, r := range bad {
		if _, _, err := Parse(&r); err == nil {
			t.Errorf("case %d: want an error", i)
		}
	}
}
