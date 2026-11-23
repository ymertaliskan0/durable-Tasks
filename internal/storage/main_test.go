package storage

import (
	"testing"

	"github.com/ymertaliskan0/durable-tasks/internal/pgtest"
)

func TestMain(m *testing.M) { pgtest.Main(m) }

// openTestDB opens a *Postgres on a fresh, empty database and closes it when
// the test ends.
func openTestDB(t *testing.T) (*Postgres, string) {
	t.Helper()
	dsn := pgtest.NewDB(t)
	p, err := Open(t.Context(), dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(p.Close)
	return p, dsn
}
