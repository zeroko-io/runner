package store

import (
	"context"
	"path/filepath"
	"testing"
)

// Pruning keeps each service's newest entries, and none past their age.
func TestPruneLogs(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "runner.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	var ids []string
	for _, name := range []string{"a", "b"} {
		sv, err := st.CreateService(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, sv.ID)
		var logs []Log
		for i := range 10 {
			logs = append(logs, Log{TS: int64(100 + i), Source: "console", Level: "info", Message: "x"})
		}
		if err := st.AddLogs(ctx, sv.ID, logs); err != nil {
			t.Fatal(err)
		}
	}
	// 4 newest each, and none before ts 107: a keeps 3 (107-109), b too.
	if err := st.PruneLogs(ctx, 4, 107); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		logs, err := st.Logs(ctx, id, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(logs) != 3 || logs[0].TS != 107 {
			t.Fatalf("service %s kept %d entries from %v; want 3 from 107", id, len(logs), logs)
		}
	}
	if err := st.PruneLogs(ctx, 2, 0); err != nil {
		t.Fatal(err)
	}
	if logs, _ := st.Logs(ctx, ids[0], 0, 100); len(logs) != 2 || logs[1].TS != 109 {
		t.Fatalf("kept %v; want the 2 newest", logs)
	}
}
