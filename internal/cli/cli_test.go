package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/ymertaliskan0/durable-tasks/internal/pgtest"
	"github.com/ymertaliskan0/durable-tasks/internal/storage"
)

func TestMain(m *testing.M) { pgtest.Main(m) }

func TestDSNFlag_DefaultsToBuiltIn(t *testing.T) {
	t.Setenv("DURABLE_TASKS_DSN", "") // empty counts as unset

	got, err := NewRootCmd().PersistentFlags().GetString("dsn")
	if err != nil {
		t.Fatalf("no --dsn persistent flag: %v", err)
	}
	if got != defaultDSN {
		t.Fatalf("--dsn default = %q, want %q", got, defaultDSN)
	}
}

func TestDSNFlag_DefaultsToEnv(t *testing.T) {
	t.Setenv("DURABLE_TASKS_DSN", "postgres://from-env/db")

	got, err := NewRootCmd().PersistentFlags().GetString("dsn")
	if err != nil {
		t.Fatalf("no --dsn persistent flag: %v", err)
	}
	if got != "postgres://from-env/db" {
		t.Fatalf("--dsn default = %q, want the value of DURABLE_TASKS_DSN", got)
	}
}

func TestMigrateCommand(t *testing.T) {
	dsn := pgtest.NewDB(t)

	// Run twice: the second run must be a no-op, not an error.
	for i := range 2 {
		var out bytes.Buffer
		cmd := NewRootCmd()
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs([]string{"migrate", "--dsn", dsn})

		if err := cmd.ExecuteContext(t.Context()); err != nil {
			t.Fatalf("migrate run #%d: %v\noutput:\n%s", i+1, err, out.String())
		}
	}

	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(t.Context()) }()

	var n int
	if err := conn.QueryRow(t.Context(), "SELECT count(*) FROM tasks").Scan(&n); err != nil {
		t.Fatalf("tasks table missing after `migrate`: %v", err)
	}
}

func TestMigrateCommand_RejectsArguments(t *testing.T) {
	migrate, _, err := NewRootCmd().Find([]string{"migrate"})
	if err != nil || migrate.Name() != "migrate" {
		t.Fatalf("no `migrate` subcommand registered on the root command (err=%v)", err)
	}
	if migrate.Args == nil {
		t.Fatal("migrate.Args is nil; set it to cobra.NoArgs")
	}
	if err := migrate.Args(migrate, []string{"unexpected-arg"}); err == nil {
		t.Fatal("migrate accepted a positional argument; want an error")
	}
}

// run executes the CLI with args and returns its output.
func run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	cmd := NewRootCmd()
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(t.Context())
	return out.String(), err
}

func mustRun(t *testing.T, args ...string) string {
	t.Helper()
	out, err := run(t, args...)
	if err != nil {
		t.Fatalf("%v: %v\noutput:\n%s", args, err, out)
	}
	return out
}

func TestTaskCommands(t *testing.T) {
	dsn := pgtest.NewDB(t)
	mustRun(t, "migrate", "--dsn", dsn)

	out := mustRun(t, "submit", "sleep", "--dsn", dsn, "--steps", "3", "--idempotency-key", "k1")
	id, note, _ := strings.Cut(strings.TrimSpace(out), "\t")
	if note != "created" {
		t.Fatalf("submit output %q, want <id>\\tcreated", out)
	}
	if out := mustRun(t, "submit", "sleep", "--dsn", dsn, "--steps", "3", "--idempotency-key", "k1"); !strings.Contains(out, id+"\talready existed") {
		t.Errorf("resubmitting the same key printed %q, want the same id, already existed", out)
	}

	if out := mustRun(t, "status", id, "--dsn", dsn); !strings.Contains(out, "ready_to_run") || !strings.Contains(out, `"steps":3`) {
		t.Errorf("status output missing status or request:\n%s", out)
	}
	if out := mustRun(t, "list", "--dsn", dsn, "--status", "ready_to_run"); !strings.Contains(out, id) {
		t.Errorf("list output missing the task:\n%s", out)
	}

	mustRun(t, "cancel", id, "--dsn", dsn)
	if out := mustRun(t, "status", id, "--dsn", dsn); !strings.Contains(out, "ready_to_cancel") {
		t.Errorf("status after cancel:\n%s\nwant ready_to_cancel", out)
	}

	if _, err := run(t, "status", "not-a-uuid", "--dsn", dsn); err == nil {
		t.Error("status accepted an invalid id")
	}
	if _, err := run(t, "status", "00000000-0000-0000-0000-000000000001", "--dsn", dsn); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("status of a missing task: err = %v, want ErrNotFound", err)
	}
}
