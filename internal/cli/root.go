// Package cli implements the durable-tasks command-line interface.
//
// Each subcommand lives in its own file and is built by a newXxxCmd
// constructor, so every call to NewRootCmd returns an independent command
// tree.
package cli

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

// defaultDSN is the connection string for the Postgres in docker-compose.yml.
const defaultDSN = "postgres://durable:durable@localhost:5432/durable_tasks?sslmode=disable"

// rootOptions holds the flag values shared by every subcommand.
type rootOptions struct {
	dsn string
}

// NewRootCmd builds the durable-tasks command and all its subcommands.
//
// The --dsn flag applies to every subcommand. Its default is
// $DURABLE_TASKS_DSN if set, otherwise defaultDSN.
//
// Returns the root command, ready to execute.
func NewRootCmd() *cobra.Command {
	opts := &rootOptions{}

	cmd := &cobra.Command{
		Use:          "durable-tasks",
		Short:        "A fault-tolerant distributed task scheduler backed by PostgreSQL",
		SilenceUsage: true, // runtime errors print just the error, not the usage text
	}

	// The default is resolved here so --help shows it and --dsn overrides it.
	cmd.PersistentFlags().StringVar(&opts.dsn, "dsn",
		envOr("DURABLE_TASKS_DSN", defaultDSN),
		"PostgreSQL connection string (default from $DURABLE_TASKS_DSN)")

	cmd.AddCommand(newMigrateCmd(opts))
	return cmd
}

// Execute runs the CLI with the process arguments.
//
// The context passed to subcommands is cancelled on Ctrl-C (SIGINT) or
// SIGTERM, so long-running commands can shut down gracefully.
//
// Returns the error from the subcommand, which has already been printed.
func Execute() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop() // unregister the signal handler on the way out
	return NewRootCmd().ExecuteContext(ctx)
}

// envOr returns the value of the environment variable key.
//
// Parameters:
//   - key: the variable name.
//   - fallback: the value to use when the variable is unset or empty.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
