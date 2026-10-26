package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

func stmt(sql string, params ...any) Statement {
	s := Statement{SQL: sql}
	for _, p := range params {
		b, _ := json.Marshal(p)
		s.Params = append(s.Params, b)
	}
	return s
}

func TestBatch(t *testing.T) {
	d, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx := context.Background()
	run := func(stmts ...Statement) ([]Result, error) { return d.Batch(ctx, "service", "DB", stmts) }

	// A script of several statements runs whole.
	if _, err := run(stmt("CREATE TABLE a (id INTEGER PRIMARY KEY, v TEXT, b BLOB); CREATE TABLE c (x)")); err != nil {
		t.Fatal(err)
	}
	r, err := run(stmt("INSERT INTO a (v, b) VALUES (?, ?)", "one", map[string]string{"$blob": "AQID"}))
	if err != nil || r[0].Changes != 1 || r[0].LastRowID != 1 {
		t.Fatalf("insert: %+v %v", r, err)
	}
	r, err = run(stmt("SELECT v, b FROM a WHERE id = ?", 1))
	if err != nil || len(r[0].Rows) != 1 || r[0].Rows[0]["v"] != "one" || r[0].Changes != 0 {
		t.Fatalf("select: %+v %v", r, err)
	}
	if b, _ := json.Marshal(r[0].Rows[0]["b"]); string(b) != `{"$blob":"AQID"}` {
		t.Fatalf("blob: %s", b)
	}

	// Triggers hold semicolons in their body.
	if _, err := run(stmt("CREATE TRIGGER t AFTER INSERT ON c BEGIN UPDATE c SET x = x; SELECT 1; END; -- done")); err != nil {
		t.Fatal(err)
	}
	for q, want := range map[string]bool{
		"SELECT 1":                     false,
		"SELECT 1;":                    false,
		"SELECT 1; -- x\n/* y */ ":     false,
		"SELECT ';'":                   false,
		"SELECT 1; SELECT 2":           true,
		"INSERT INTO a VALUES (';');x": true,
	} {
		if multiple(q) != want {
			t.Errorf("multiple(%q) = %v", q, !want)
		}
	}

	// A failing statement rolls the whole batch back.
	_, err = run(stmt("INSERT INTO a (v) VALUES ('two')"), stmt("INSERT INTO nope VALUES (1)"))
	var se SQLError
	if !errors.As(err, &se) || !strings.Contains(err.Error(), "statement 2") {
		t.Fatalf("want a SQL error in statement 2, got %v", err)
	}
	r, _ = run(stmt("SELECT count(*) AS n FROM a"))
	if n := r[0].Rows[0]["n"]; n != int64(1) {
		t.Fatalf("batch not rolled back: %v rows", n)
	}

	// The service cannot escape its database or hold a transaction open.
	for _, q := range []string{"ATTACH DATABASE '/tmp/x.db' AS x", "BEGIN", "PRAGMA journal_mode = DELETE", "PRAGMA max_page_count = 1", "SAVEPOINT s"} {
		if _, err := run(stmt(q)); !errors.As(err, &se) {
			t.Errorf("%s: want refused, got %v", q, err)
		}
	}
	if _, err := run(stmt("PRAGMA table_info(a)")); err != nil {
		t.Errorf("table_info: %v", err)
	}
	// Batches still work after refused statements.
	if _, err := run(stmt("INSERT INTO c VALUES (1)"), stmt("INSERT INTO c VALUES (2)")); err != nil {
		t.Fatal(err)
	}
	tables, err := d.Tables(ctx, "service", "DB")
	if err != nil || len(tables) != 2 || tables[1].Rows != 2 {
		t.Fatalf("tables: %+v %v", tables, err)
	}
}

func TestFiles(t *testing.T) {
	d, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx := context.Background()
	for _, k := range []string{"a/1", "a/2", "a/3", "b/1"} {
		if _, err := d.PutFile(ctx, "service", "F", k, "text/plain", strings.NewReader("content "+k)); err != nil {
			t.Fatal(err)
		}
	}
	f, err := d.PutFile(ctx, "service", "F", "a/1", "", strings.NewReader("replaced"))
	if err != nil || f.Size != 8 || f.Type != "application/octet-stream" {
		t.Fatalf("replace: %+v %v", f, err)
	}
	_, r, err := d.OpenFile(ctx, "service", "F", "a/1")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) != "replaced" {
		t.Fatalf("content %q", b)
	}
	l, err := d.ListFiles(ctx, "service", "F", "a/", "", 2)
	if err != nil || len(l.Files) != 2 || l.Cursor != "a/2" {
		t.Fatalf("list: %+v %v", l, err)
	}
	l, _ = d.ListFiles(ctx, "service", "F", "a/", l.Cursor, 2)
	if len(l.Files) != 1 || l.Files[0].Key != "a/3" || l.Cursor != "" {
		t.Fatalf("page 2: %+v", l)
	}
	if err := d.DeleteFile(ctx, "service", "F", "a/3"); err != nil {
		t.Fatal(err)
	}
	if _, err := d.StatFile(ctx, "service", "F", "a/3"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted file: %v", err)
	}
	if n, size, _ := d.FilesUsage(ctx, "service", "F"); n != 3 || size != 8+11+11 {
		t.Fatalf("usage: %d %d", n, size)
	}
	if err := d.DeleteService("service"); err != nil {
		t.Fatal(err)
	}
	if n, _, _ := d.FilesUsage(ctx, "service", "F"); n != 0 {
		t.Fatalf("files left after DeleteService: %d", n)
	}
}

func TestQuota(t *testing.T) {
	d, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	d.Quota = func(string) int64 { return 1 << 20 }
	ctx := context.Background()
	full := func(err error) bool { return err != nil && strings.Contains(err.Error(), "quota") }

	if _, err := d.PutFile(ctx, "service", "F", "a", "", strings.NewReader(strings.Repeat("x", 600<<10))); err != nil {
		t.Fatal(err)
	}
	if _, err := d.PutFile(ctx, "service", "F", "b", "", strings.NewReader(strings.Repeat("x", 600<<10))); !full(err) {
		t.Fatalf("a file over the quota: %v", err)
	}
	// Replacing a file counts the room it frees.
	if _, err := d.PutFile(ctx, "service", "F", "a", "", strings.NewReader(strings.Repeat("y", 700<<10))); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if u := d.Usage("service"); u != 700<<10 {
		t.Fatalf("usage %d", u)
	}
	if _, err := d.Batch(ctx, "service", "DB", []Statement{stmt("CREATE TABLE t (v BLOB)")}); err != nil {
		t.Fatal(err)
	}
	_, err = d.Batch(ctx, "service", "DB", []Statement{stmt("INSERT INTO t VALUES (zeroblob(400000))")})
	if !full(err) {
		t.Fatalf("a row over the quota: %v", err)
	}
	if err := d.DeleteFile(ctx, "service", "F", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Batch(ctx, "service", "DB", []Statement{stmt("INSERT INTO t VALUES (zeroblob(400000))")}); err != nil {
		t.Fatalf("after freeing room: %v", err)
	}
}

// Uploads at once share the quota: they can't each take the room left.
func TestQuotaUploadsAtOnce(t *testing.T) {
	d, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	d.Quota = func(string) int64 { return 1000 }
	errs := make(chan error, 10)
	for i := range 10 {
		go func() {
			_, err := d.PutFile(context.Background(), "s", "F", fmt.Sprint(i), "", strings.NewReader(strings.Repeat("x", 400)))
			errs <- err
		}()
	}
	for range 10 {
		<-errs
	}
	if u := d.Usage("s"); u > 1000 {
		t.Fatalf("usage %d past the quota of 1000", u)
	}
}
