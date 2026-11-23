package storage

import (
	"context"
	"testing"
	"time"
)

func TestOpen_PingsTheServer(t *testing.T) {
	p, _ := openTestDB(t)

	var one int
	if err := p.pool.QueryRow(t.Context(), "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("query through pool: %v", err)
	}
}

// Open must fail fast on an unreachable server instead of returning a pool
// that only fails on first use. This needs no Docker: nothing listens on
// port 1.
func TestOpen_UnreachableServerFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	p, err := Open(ctx, "postgres://nobody:nothing@127.0.0.1:1/none?sslmode=disable&connect_timeout=2")
	if err == nil {
		p.Close()
		t.Fatal("Open succeeded against an unreachable server; want an error")
	}
}

func TestOpen_InvalidDSNFails(t *testing.T) {
	p, err := Open(t.Context(), "this is not a dsn ::")
	if err == nil {
		p.Close()
		t.Fatal("Open accepted an invalid DSN; want an error")
	}
}
