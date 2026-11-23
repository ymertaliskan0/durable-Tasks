package cli

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/ymertaliskan0/durable-tasks/internal/storage"
)

// openStore connects to the database named by --dsn. The caller must Close
// the returned store.
func openStore(ctx context.Context, opts *rootOptions) (*storage.Postgres, error) {
	return storage.Open(ctx, opts.dsn)
}

// parseTaskID parses a task ID given on the command line.
func parseTaskID(arg string) (uuid.UUID, error) {
	id, err := uuid.Parse(arg)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid task id %q: %w", arg, err)
	}
	return id, nil
}
