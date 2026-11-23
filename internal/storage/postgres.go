// Package storage persists tasks in PostgreSQL.
//
// The store is the single source of truth for every task: its status, its
// saved state and which worker currently owns it. Ownership is enforced with
// generation-fenced writes, so a worker that lost a task cannot overwrite it.
package storage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres is the task store backed by a connection pool. It is safe for
// concurrent use; create one per process and share it.
type Postgres struct {
	pool *pgxpool.Pool
}

// Open connects to PostgreSQL and verifies that the server is reachable, so
// a bad DSN or an unavailable server is reported at startup.
//
// Parameters:
//   - dsn: a PostgreSQL connection string, e.g.
//     postgres://user:pass@host:5432/db?sslmode=disable.
//
// Returns a ready store, which the caller must Close.
func Open(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("storage: create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close() // don't leak the pool on the error path
		return nil, fmt.Errorf("storage: ping postgres: %w", err)
	}

	return &Postgres{pool: pool}, nil
}

// Close closes all pooled connections. The store must not be used afterwards.
func (p *Postgres) Close() {
	p.pool.Close()
}
