package cli

import (
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/ymertaliskan0/durable-tasks/internal/storage"
)

// newStatusCmd builds `durable-tasks status <id>`, which prints one task.
func newStatusCmd(opts *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "status <task-id>",
		Short: "Show one task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseTaskID(args[0])
			if err != nil {
				return err
			}
			store, err := openStore(cmd.Context(), opts)
			if err != nil {
				return err
			}
			defer store.Close()

			t, err := store.GetTask(cmd.Context(), id)
			if err != nil {
				return err
			}
			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			for _, row := range [][2]string{
				{"id", t.ID.String()},
				{"type", t.Type},
				{"status", string(t.Status)},
				{"generation", fmt.Sprint(t.Generation)},
				{"retries", fmt.Sprint(t.RetriableErrorCount)},
				{"last error", t.LastError},
				{"cancel reason", string(t.CancelReason)},
				{"lease owner", t.LeaseOwner},
				{"lease expires", formatTime(t.LeaseExpiresAt)},
				{"run after", formatTime(t.RunAfter)},
				{"request", string(t.Request)},
				{"state", string(t.State)},
				{"created", formatTime(t.CreatedAt)},
				{"modified", formatTime(t.ModifiedAt)},
			} {
				_, _ = fmt.Fprintf(w, "%s:\t%s\n", row[0], row[1]) // write errors surface in Flush
			}
			return w.Flush()
		},
	}
}

// newListCmd builds `durable-tasks list`, which prints tasks oldest first.
func newListCmd(opts *rootOptions) *cobra.Command {
	var (
		status string
		limit  int
	)
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List tasks, oldest first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, err := openStore(cmd.Context(), opts)
			if err != nil {
				return err
			}
			defer store.Close()

			list, err := store.ListTasks(cmd.Context(), storage.Status(status), limit)
			if err != nil {
				return err
			}
			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			// Writes go to the tabwriter's buffer; errors surface in Flush.
			_, _ = fmt.Fprintln(w, "ID\tTYPE\tSTATUS\tGEN\tRETRIES\tOWNER\tMODIFIED")
			for _, t := range list {
				_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d\t%s\t%s\n",
					t.ID, t.Type, t.Status, t.Generation, t.RetriableErrorCount, t.LeaseOwner, formatTime(t.ModifiedAt))
			}
			return w.Flush()
		},
	}
	f := cmd.Flags()
	f.StringVar(&status, "status", "", "only list tasks in this status")
	f.IntVar(&limit, "limit", 50, "maximum number of tasks to list")
	return cmd
}

// newCancelCmd builds `durable-tasks cancel <id>`, which requests
// cancellation of a task.
func newCancelCmd(opts *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "cancel <task-id>",
		Short: "Request cancellation of a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseTaskID(args[0])
			if err != nil {
				return err
			}
			store, err := openStore(cmd.Context(), opts)
			if err != nil {
				return err
			}
			defer store.Close()

			if err := store.RequestCancel(cmd.Context(), id); err != nil {
				return err
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), "cancellation requested")
			return err
		},
	}
}

// formatTime prints t in RFC 3339 with milliseconds, or "-" for the zero
// time.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Local().Format("2006-01-02T15:04:05.000Z07:00")
}
