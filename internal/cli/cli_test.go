package cli

import (
	"bytes"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/ymertaliskan/durable-tasks/internal/pgtest"
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
