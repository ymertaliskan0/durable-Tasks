package nemesis

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// scratchDB creates a new, empty database on the server dsn points at, so a
// run never touches tasks that already exist.
//
// Parameters:
//   - dsn: a URL-style connection string whose user may create databases.
//   - prefix: the start of the new database's name.
//
// Returns the new database's DSN and a function that drops it.
func scratchDB(ctx context.Context, dsn, prefix string) (string, func(), error) {
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" {
		return "", nil, fmt.Errorf("nemesis: need a URL-style DSN (postgres://...), got %q", dsn)
	}
	name := fmt.Sprintf("%s_%s", prefix, time.Now().Format("20060102_150405_000"))
	name = strings.ReplaceAll(name, ".", "_")

	admin := adminDSN(u)
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		return "", nil, fmt.Errorf("nemesis: connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	// CREATE DATABASE takes no bind parameters; name is generated above.
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		return "", nil, fmt.Errorf("nemesis: create scratch database: %w", err)
	}

	u.Path = "/" + name
	drop := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if conn, err := pgx.Connect(ctx, admin); err == nil {
			_, _ = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
			_ = conn.Close(ctx)
		}
	}
	return u.String(), drop, nil
}

// adminDSN returns u without pool_* parameters. pgxpool understands those,
// but a single pgx connection would send them to the server, which rejects
// them. The other parameters are kept exactly as written, because
// re-encoding them would turn spaces into "+", which pgx does not decode.
func adminDSN(u *url.URL) string {
	c := *u
	var kept []string
	for _, part := range strings.Split(c.RawQuery, "&") {
		if part != "" && !strings.HasPrefix(part, "pool_") {
			kept = append(kept, part)
		}
	}
	c.RawQuery = strings.Join(kept, "&")
	return c.String()
}
