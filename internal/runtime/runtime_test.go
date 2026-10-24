package runtime

import "testing"

func TestExplain(t *testing.T) {
	var p Proc
	cause := "e = rust/worker/ffi.rs:110: failed: failed to lookup address information: Name or service not known\n" +
		"stack: /usr/bin/workerd@2662e30 /usr/bin/workerd@366f330; sentryErrorContext = jsgInternalError; wdErrId = mjt7lnk2tgh4ftpon89qfh2v"
	if got, want := p.explain(cause), "internal error mjt7lnk2tgh4ftpon89qfh2v: failed to lookup address information: Name or service not known"; got != want {
		t.Errorf("cause line:\n got %q\nwant %q", got, want)
	}
	service := "job() failed: Error: internal error; reference = mjt7lnk2tgh4ftpon89qfh2v\n    at async Object.job (index.js:85:17)"
	want := "job() failed: Error: failed to lookup address information: Name or service not known (internal error mjt7lnk2tgh4ftpon89qfh2v)\n    at async Object.job (index.js:85:17)"
	if got := p.explain(service); got != want {
		t.Errorf("service line:\n got %q\nwant %q", got, want)
	}
	unknown := "Error: internal error; reference = otherid"
	if got := p.explain(unknown); got != unknown {
		t.Errorf("unknown reference changed: %q", got)
	}
}
