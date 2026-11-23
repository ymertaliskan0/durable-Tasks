package cli

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/ymertaliskan0/durable-tasks/internal/storage"
	"github.com/ymertaliskan0/durable-tasks/internal/tasks/sleeptask"
)

// newSubmitCmd builds `durable-tasks submit`, which has one subcommand per
// task type.
func newSubmitCmd(opts *rootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "submit",
		Short: "Submit a task",
	}
	cmd.AddCommand(newSubmitSleepCmd(opts))
	return cmd
}

// newSubmitSleepCmd builds `durable-tasks submit sleep`, which submits a
// sleeptask and prints its ID.
func newSubmitSleepCmd(opts *rootOptions) *cobra.Command {
	var (
		req sleeptask.Request
		key string
	)
	cmd := &cobra.Command{
		Use:   "sleep",
		Short: "Submit a task that sleeps for a number of checkpointed steps",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			request, err := json.Marshal(req)
			if err != nil {
				return err
			}
			store, err := openStore(cmd.Context(), opts)
			if err != nil {
				return err
			}
			defer store.Close()

			id, created, err := store.CreateTask(cmd.Context(), storage.NewTask{
				Type: sleeptask.Type, Request: request, IdempotencyKey: key,
			})
			if err != nil {
				return err
			}
			note := "created"
			if !created {
				note = "already existed"
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\n", id, note)
			return err
		},
	}
	f := cmd.Flags()
	f.IntVar(&req.Steps, "steps", 5, "number of steps")
	f.DurationVar(&req.StepDuration, "step-duration", time.Second, "how long each step sleeps")
	f.StringVar(&key, "idempotency-key", "", "submitting the same key again returns the existing task")
	return cmd
}
