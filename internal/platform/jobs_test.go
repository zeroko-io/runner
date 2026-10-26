package platform

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/zeroko-io/runner/internal/store"
)

// A service keeps at most maxJobs, and dead ones go after their retention:
// they're in runner.db, outside its disk quota.
func TestJobsBounded(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "runner.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	sv, err := st.CreateService(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	p := &Platform{Store: st, jobsWake: make(chan struct{}, 1)}
	for range maxJobs {
		if _, err := st.Enqueue(ctx, &store.Job{ServiceID: sv.ID, Payload: "null", MaxAttempts: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := p.SendJob(ctx, sv.ID, nil, time.Hour, 0, ""); err == nil {
		t.Fatal("a job past maxJobs was queued")
	}

	// Dead, a while ago: a cron's after a day, any other after a week.
	if _, err := st.DB.Exec(`UPDATE jobs SET status = 'dead', updated_at = ?, cron = CASE WHEN id % 2 = 0 THEN '* * * * *' ELSE '' END`, time.Now().Add(-2*24*time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := st.PruneDeadJobs(ctx, time.Now().Add(-deadRetention).UnixMilli(), time.Now().Add(-deadCronRetention).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if n, _ := st.JobCount(ctx, sv.ID); n != maxJobs/2 {
		t.Fatalf("%d jobs left; want the %d that aren't crons", n, maxJobs/2)
	}
}
