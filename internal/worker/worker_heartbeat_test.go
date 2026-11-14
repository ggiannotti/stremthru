package worker

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MunifTanjim/stremthru/internal/db"
	"github.com/madflojo/tasks"
)

// Exercise the real worker and job_log SQLite UPSERT while deterministically
// delaying an in-flight heartbeat, as SQLite contention can do in production.
func TestWorkerTerminalStatusWaitsForHeartbeat(t *testing.T) {
	for _, mode := range []string{"done", "error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			sqlDB, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "jobs.db")+"?_journal_mode=WAL&_busy_timeout=5000")
			if err != nil {
				t.Fatal(err)
			}
			db.SetForTesting(sqlDB, t.Cleanup)
			_, err = sqlDB.Exec(`CREATE TABLE job_log (
                name TEXT NOT NULL, id TEXT NOT NULL, status TEXT NOT NULL,
                data TEXT, error TEXT, created_at INTEGER NOT NULL DEFAULT (unixepoch()),
                updated_at INTEGER NOT NULL DEFAULT (unixepoch()), expires_at INTEGER,
                PRIMARY KEY (name, id))`)
			if err != nil {
				t.Fatal(err)
			}

			heartbeatEntered := make(chan struct{})
			heartbeatRelease := make(chan struct{})
			heartbeatReturned := make(chan struct{})
			executorReturned := make(chan struct{})
			abort := make(chan struct{})
			terminalWrite := make(chan struct{}, 1)
			var startedWrites atomic.Int32
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(heartbeatRelease) }) }
			t.Cleanup(release)
			originalExec := db.Exec
			db.Exec = func(query string, args ...any) (sql.Result, error) {
				if strings.HasPrefix(query, "INSERT INTO job_log ") {
					status := args[2].(string)
					if status == "started" && startedWrites.Add(1) == 2 {
						close(heartbeatEntered)
						<-heartbeatRelease
						defer close(heartbeatReturned)
					} else if status != "started" {
						terminalWrite <- struct{}{}
					}
				}
				return originalExec(query, args...)
			}
			t.Cleanup(func() { db.Exec = originalExec })

			w := NewWorker(&WorkerConfig{
				Name: "sync-imdb", Interval: time.Hour, HeartbeatInterval: time.Millisecond,
				Executor: func(*Worker) error {
					select {
					case <-heartbeatEntered:
					case <-abort:
						return errors.New("test aborted")
					}
					close(executorReturned)
					switch mode {
					case "error":
						return errors.New("executor failed")
					case "panic":
						panic("executor panic")
					default:
						return nil
					}
				},
			})
			t.Cleanup(w.scheduler.Stop)
			var task *tasks.Task
			for _, scheduled := range w.scheduler.Tasks() {
				task = scheduled
			}
			if task == nil {
				t.Fatal("worker task was not scheduled")
			}
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				if err := task.TaskFunc(); err != nil {
					task.ErrFunc(err)
				}
			}()
			t.Cleanup(func() {
				close(abort)
				release()
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Error("worker did not finish during cleanup")
				}
			})
			select {
			case <-executorReturned:
			case <-time.After(5 * time.Second):
				t.Fatal("executor did not reach the in-flight heartbeat")
			}
			select {
			case <-terminalWrite:
				t.Error("terminal status was written while a heartbeat could still overwrite it")
				// Let the terminal write complete before releasing the old heartbeat.
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Fatal("terminal write did not finish")
				}
			case <-time.After(50 * time.Millisecond):
			}
			release()
			select {
			case <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("worker did not finish after heartbeat was released")
			}
			select {
			case <-heartbeatReturned:
			case <-time.After(5 * time.Second):
				t.Fatal("heartbeat did not finish after release")
			}
			var status string
			if err := sqlDB.QueryRow("SELECT status FROM job_log WHERE name = ?", "sync-imdb").Scan(&status); err != nil {
				t.Fatal(err)
			}
			want := "done"
			if mode != "done" {
				want = "failed"
			}
			if status != want {
				t.Errorf("persisted status = %q, want %q", status, want)
			}
		})
	}
}
