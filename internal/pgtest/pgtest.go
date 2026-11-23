// Package pgtest gives integration tests a real, throwaway PostgreSQL.
//
// Each test binary (Go builds one per package) starts at most one container,
// on first use. Every call to NewDB then creates a fresh, empty database in
// that container, so tests never see each other's rows and can use
// t.Parallel().
//
// Usage, in any package's tests:
//
//	func TestMain(m *testing.M) { pgtest.Main(m) }
//
//	func TestSomething(t *testing.T) {
//	    dsn := pgtest.NewDB(t) // skips the test under `go test -short`
//	    ...
//	}
package pgtest

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Image matches docker-compose.yml so tests run against the same server.
const Image = "postgres:17-alpine"

var (
	once      sync.Once
	container *postgres.PostgresContainer
	adminDSN  string
	startErr  error
	dbCounter atomic.Int64
)

// Main runs the tests and then removes the container, if one was started.
// Use it as the whole body of TestMain.
func Main(m *testing.M) {
	code := m.Run()
	if container != nil {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "pgtest: terminate container: %v\n", err)
		}
	}
	os.Exit(code)
}

// NewDB returns a connection string for a new, empty database. Under
// `go test -short` it skips the calling test instead.
func NewDB(t testing.TB) string {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: skipped with -short")
	}

	once.Do(start)
	if startErr != nil {
		t.Fatalf("pgtest: start postgres container: %v", startErr)
	}

	name := fmt.Sprintf("test_%d", dbCounter.Add(1))
	ctx := t.Context()

	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("pgtest: connect as admin: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	// CREATE DATABASE cannot take bind parameters; name is generated above.
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("pgtest: create database %s: %v", name, err)
	}

	u, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("pgtest: parse admin DSN: %v", err)
	}
	u.Path = "/" + name
	return u.String()
}

func start() {
	// Generous timeout: the first run may need to pull the image.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c, err := postgres.Run(ctx, Image,
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		startErr = err
		return
	}
	container = c

	adminDSN, startErr = c.ConnectionString(ctx, "sslmode=disable")
}
