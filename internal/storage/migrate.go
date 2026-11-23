package storage

import (
	"cmp"
	"context"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// migrationsFS holds the SQL files from ./migrations, compiled into the
// binary.
//
//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrationLockKey identifies the Postgres advisory lock that serializes
// Migrate across processes. Every process must use the same value; these are
// the ASCII bytes of "durable".
const migrationLockKey int64 = 0x647572_61626c65

// migration is one SQL migration file.
type migration struct {
	version int    // from the file name: 0007_x.sql gives 7
	name    string // the full file name, e.g. "0007_x.sql"
	sql     string // the file contents
}

// loadMigrations reads the migration files at the root of fsys.
//
// A migration file is named NNNN_description.sql, e.g. 0001_create_tasks.sql:
// the version is the positive integer before the first "_", and the
// description must not be empty. Files without the .sql extension and
// subdirectories are ignored.
//
// Parameters:
//   - fsys: the file system holding the migration files.
//
// Returns the migrations sorted numerically by version, so 2_x.sql comes
// before 10_x.sql. Returns an error if a file name breaks the naming rule or
// two files share a version.
func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("storage: list migrations: %w", err)
	}

	var migrations []migration
	seen := make(map[int]string) // version -> file name, to detect duplicates

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || path.Ext(name) != ".sql" {
			continue
		}

		// Split at the first "_" only; the description may contain more.
		prefix, description, found := strings.Cut(strings.TrimSuffix(name, ".sql"), "_")
		if !found || description == "" {
			return nil, fmt.Errorf("storage: migration %q: want NNNN_description.sql", name)
		}

		version, err := strconv.Atoi(prefix)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("storage: migration %q: version must be a positive integer", name)
		}

		if other, dup := seen[version]; dup {
			return nil, fmt.Errorf("storage: migrations %q and %q share version %d", other, name, version)
		}
		seen[version] = name

		contents, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("storage: read migration %q: %w", name, err)
		}

		migrations = append(migrations, migration{version: version, name: name, sql: string(contents)})
	}

	// fs.ReadDir returns entries sorted by *name*, which is lexical order
	// ("10_c" < "2_b"). Re-sort by the parsed number.
	slices.SortFunc(migrations, func(a, b migration) int {
		return cmp.Compare(a.version, b.version)
	})
	return migrations, nil
}

// embeddedMigrations loads the migrations compiled into the binary, sorted
// by version.
func embeddedMigrations() ([]migration, error) {
	sub, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("storage: open embedded migrations: %w", err)
	}
	return loadMigrations(sub)
}

// Migrate brings the database schema up to date by applying every embedded
// migration that has not been applied yet.
//
// All pending migrations run in one transaction, so a failure leaves the
// schema unchanged. Concurrent calls from several processes are safe: they
// run one after another, and every call after the first finds nothing to do.
//
// Returns an error naming the migration that failed, if any.
func (p *Postgres) Migrate(ctx context.Context) error {
	migrations, err := embeddedMigrations()
	if err != nil {
		return err
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("storage: migrate: begin: %w", err)
	}
	// A no-op after a successful Commit; undoes everything on any early return.
	defer func() { _ = tx.Rollback(ctx) }()

	// Lock before anything else: concurrent CREATE TABLE IF NOT EXISTS is not
	// safe in Postgres. The lock is released when the transaction ends.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationLockKey); err != nil {
		return fmt.Errorf("storage: migrate: acquire lock: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    integer     PRIMARY KEY,
			name       text        NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("storage: migrate: create schema_migrations: %w", err)
	}

	rows, err := tx.Query(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return fmt.Errorf("storage: migrate: read applied versions: %w", err)
	}
	appliedVersions, err := pgx.CollectRows(rows, pgx.RowTo[int]) // also closes rows
	if err != nil {
		return fmt.Errorf("storage: migrate: read applied versions: %w", err)
	}
	applied := make(map[int]bool, len(appliedVersions))
	for _, v := range appliedVersions {
		applied[v] = true
	}

	for _, m := range migrations {
		if applied[m.version] {
			continue
		}
		// Without arguments pgx uses the simple protocol, which accepts a
		// multi-statement file in one Exec.
		if _, err := tx.Exec(ctx, m.sql); err != nil {
			return fmt.Errorf("storage: apply migration %s: %w", m.name, err)
		}
		if _, err := tx.Exec(ctx,
			"INSERT INTO schema_migrations (version, name) VALUES ($1, $2)",
			m.version, m.name,
		); err != nil {
			return fmt.Errorf("storage: record migration %s: %w", m.name, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("storage: migrate: commit: %w", err)
	}
	return nil
}
