package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ymertaliskan/durable-tasks/internal/storage"
)

// newMigrateCmd builds `durable-tasks migrate`, which applies pending
// database migrations and exits. It takes no arguments.
//
// Parameters:
//   - opts: the shared flag values; read when the command runs.
func newMigrateCmd(opts *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "migrate",
		Short: "Apply pending database migrations",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			store, err := storage.Open(ctx, opts.dsn)
			if err != nil {
				return err
			}
			defer store.Close()

			if err := store.Migrate(ctx); err != nil {
				return err
			}

			_, err = fmt.Fprintln(cmd.OutOrStdout(), "migrations applied")
			return err
		},
	}
}
