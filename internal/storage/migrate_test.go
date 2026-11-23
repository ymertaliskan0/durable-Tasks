package storage

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// ---------------------------------------------------------------------------
// Unit tests (no Docker): loadMigrations and the embedded files.
// ---------------------------------------------------------------------------

func TestLoadMigrations(t *testing.T) {
	file := func(s string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(s)} }

	tests := []struct {
		name         string
		fsys         fstest.MapFS
		wantVersions []int
		wantErr      bool
	}{
		{
			name: "sorts numerically, not lexically",
			fsys: fstest.MapFS{
				"10_c.sql":   file("SELECT 10"),
				"2_b.sql":    file("SELECT 2"),
				"0001_a.sql": file("SELECT 1"),
			},
			wantVersions: []int{1, 2, 10},
		},
		{
			name: "ignores non-sql files and subdirectories",
			fsys: fstest.MapFS{
				"0001_a.sql":        file("SELECT 1"),
				"README.md":         file("docs"),
				"nested/0002_b.sql": file("SELECT 2"),
			},
			wantVersions: []int{1},
		},
		{
			name:         "empty directory is fine",
			fsys:         fstest.MapFS{},
			wantVersions: nil,
		},
		{name: "missing underscore", fsys: fstest.MapFS{"0001.sql": file("")}, wantErr: true},
		{name: "missing description", fsys: fstest.MapFS{"0001_.sql": file("")}, wantErr: true},
		{name: "non-numeric version", fsys: fstest.MapFS{"abc_init.sql": file("")}, wantErr: true},
		{name: "version zero", fsys: fstest.MapFS{"0000_init.sql": file("")}, wantErr: true},
		{
			name: "duplicate version",
			fsys: fstest.MapFS{
				"0001_a.sql": file("SELECT 1"),
				"1_b.sql":    file("SELECT 1"),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadMigrations(tt.fsys)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("got %d migrations and no error; want an error", len(got))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			var versions []int
			for _, m := range got {
				versions = append(versions, m.version)
			}
			if !equalInts(versions, tt.wantVersions) {
				t.Fatalf("versions = %v, want %v", versions, tt.wantVersions)
			}
		})
	}
}

func TestLoadMigrations_KeepsNameAndContents(t *testing.T) {
	got, err := loadMigrations(fstest.MapFS{
		"0007_add_index.sql": &fstest.MapFile{Data: []byte("CREATE INDEX x ON t (c);")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d migrations, want 1", len(got))
	}
	m := got[0]
	if m.version != 7 || m.name != "0007_add_index.sql" || m.sql != "CREATE INDEX x ON t (c);" {
		t.Fatalf("got %+v", m)
	}
}

// Proves the //go:embed directive is in place and points at the right files.
func TestEmbeddedMigrations(t *testing.T) {
	ms, err := embeddedMigrations()
	if err != nil {
		t.Fatalf("embeddedMigrations: %v", err)
	}
	if len(ms) == 0 {
		t.Fatal("no embedded migrations found; is the //go:embed directive in place?")
	}
	if ms[0].version != 1 || ms[0].name != "0001_create_tasks.sql" {
		t.Fatalf("first migration = %d %q, want 1 \"0001_create_tasks.sql\"", ms[0].version, ms[0].name)
	}
	if !strings.Contains(ms[0].sql, "CREATE TABLE tasks") {
		t.Fatal("0001_create_tasks.sql does not contain CREATE TABLE tasks")
	}
}

// ---------------------------------------------------------------------------
// Integration tests (real Postgres via testcontainers).
// ---------------------------------------------------------------------------

func TestMigrate_CreatesSchema(t *testing.T) {
	p, _ := openTestDB(t)
	ctx := t.Context()

	if err := p.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	var n int
	if err := p.pool.QueryRow(ctx, "SELECT count(*) FROM tasks").Scan(&n); err != nil {
		t.Fatalf("tasks table not usable after Migrate: %v", err)
	}
	if got, want := appliedVersions(t, p), allEmbeddedVersions(t); !equalInts(got, want) {
		t.Fatalf("schema_migrations versions = %v, want %v", got, want)
	}
}

func TestMigrate_IsIdempotent(t *testing.T) {
	p, _ := openTestDB(t)

	for i := range 3 {
		if err := p.Migrate(t.Context()); err != nil {
			t.Fatalf("Migrate #%d: %v", i+1, err)
		}
	}
	if got, want := appliedVersions(t, p), allEmbeddedVersions(t); !equalInts(got, want) {
		t.Fatalf("schema_migrations versions = %v, want %v (each exactly once)", got, want)
	}
}

// Five workers booting at the same moment must not race each other. Without
// the advisory lock, this fails with errors such as `relation "tasks" already
// exists` or a duplicate key in schema_migrations.
func TestMigrate_ConcurrentWorkersDoNotRace(t *testing.T) {
	_, dsn := openTestDB(t)
	const workers = 5

	stores := make([]*Postgres, workers)
	for i := range stores {
		p, err := Open(t.Context(), dsn) // separate pool = separate "process"
		if err != nil {
			t.Fatalf("Open #%d: %v", i, err)
		}
		t.Cleanup(p.Close)
		stores[i] = p
	}

	start := make(chan struct{})
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i, p := range stores {
		wg.Go(func() {
			<-start // release all goroutines at once
			errs[i] = p.Migrate(t.Context())
		})
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("worker %d: Migrate: %v", i, err)
		}
	}
	if got, want := appliedVersions(t, stores[0]), allEmbeddedVersions(t); !equalInts(got, want) {
		t.Fatalf("schema_migrations versions = %v, want %v", got, want)
	}
}

// Pins the constraints in 0001_create_tasks.sql. Later milestones rely on
// the database rejecting these states.
func TestSchema_Constraints(t *testing.T) {
	p, _ := openTestDB(t)
	ctx := t.Context()
	if err := p.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	const (
		checkViolation  = "23514"
		uniqueViolation = "23505"
	)
	type row struct {
		status string
		leased bool
		key    *string
		reason string // cancel_reason; "" means NULL
	}
	key := func(s string) *string { return &s }

	tests := []struct {
		name     string
		setup    []row // inserted first; each must succeed
		insert   row
		wantCode string // "" means success
	}{
		{name: "ready_to_run without lease", insert: row{status: "ready_to_run"}},
		{name: "running with lease", insert: row{status: "running", leased: true}},
		{name: "cancelling with lease and reason", insert: row{status: "cancelling", leased: true, reason: "requested"}},
		{name: "running without lease", insert: row{status: "running"}, wantCode: checkViolation},
		{name: "ready_to_run with lease", insert: row{status: "ready_to_run", leased: true}, wantCode: checkViolation},
		{name: "finished with lease", insert: row{status: "finished", leased: true}, wantCode: checkViolation},
		{name: "unknown status", insert: row{status: "paused"}, wantCode: checkViolation},
		{name: "ready_to_cancel without cancel_reason", insert: row{status: "ready_to_cancel"}, wantCode: checkViolation},
		{
			name:   "two tasks without idempotency key",
			setup:  []row{{status: "ready_to_run"}},
			insert: row{status: "ready_to_run"},
		},
		{
			name:     "duplicate idempotency key",
			setup:    []row{{status: "ready_to_run", key: key("k-dup")}},
			insert:   row{status: "ready_to_run", key: key("k-dup")},
			wantCode: uniqueViolation,
		},
	}

	insert := func(r row) error {
		var owner *string
		var expires *time.Time
		if r.leased {
			o, e := "test-worker", time.Now().Add(time.Minute)
			owner, expires = &o, &e
		}
		_, err := p.pool.Exec(ctx, `
			INSERT INTO tasks (id, task_type, request, status, idempotency_key, lease_owner, lease_expires_at, cancel_reason)
			VALUES (gen_random_uuid(), 'test', $1, $2, $3, $4, $5, NULLIF($6, ''))`,
			[]byte{}, r.status, r.key, owner, expires, r.reason)
		return err
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, r := range tt.setup {
				if err := insert(r); err != nil {
					t.Fatalf("setup insert %+v: %v", r, err)
				}
			}

			err := insert(tt.insert)
			if tt.wantCode == "" {
				if err != nil {
					t.Fatalf("insert failed: %v", err)
				}
				return
			}

			// errors.As is Go's version of `catch (PSQLException e)`: it walks
			// the wrap chain looking for an error of the target's type.
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) {
				t.Fatalf("err = %v, want a *pgconn.PgError with code %s", err, tt.wantCode)
			}
			if pgErr.Code != tt.wantCode {
				t.Fatalf("SQLSTATE = %s (%s), want %s", pgErr.Code, pgErr.Message, tt.wantCode)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func appliedVersions(t *testing.T, p *Postgres) []int {
	t.Helper()
	rows, err := p.pool.Query(t.Context(), "SELECT version FROM schema_migrations ORDER BY version")
	if err != nil {
		t.Fatalf("read schema_migrations: %v", err)
	}
	defer rows.Close()

	var vs []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		vs = append(vs, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return vs
}

func allEmbeddedVersions(t *testing.T) []int {
	t.Helper()
	ms, err := embeddedMigrations()
	if err != nil {
		t.Fatalf("embeddedMigrations: %v", err)
	}
	vs := make([]int, len(ms))
	for i, m := range ms {
		vs[i] = m.version
	}
	return vs
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
