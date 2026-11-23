package cli

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/ymertaliskan0/durable-tasks/internal/storage"
	"github.com/ymertaliskan0/durable-tasks/internal/tasks/chunkcopy"
	"github.com/ymertaliskan0/durable-tasks/internal/tasks/sleeptask"
)

// newSubmitCmd builds `durable-tasks submit`, which has one subcommand per
// task type.
func newSubmitCmd(opts *rootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "submit",
		Short: "Submit a task",
	}
	cmd.AddCommand(newSubmitSleepCmd(opts), newSubmitCopyCmd(opts))
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
			return submitTask(cmd, opts, sleeptask.Type, req, key)
		},
	}
	f := cmd.Flags()
	f.IntVar(&req.Steps, "steps", 5, "number of steps")
	f.DurationVar(&req.StepDuration, "step-duration", time.Second, "how long each step sleeps")
	f.StringVar(&key, "idempotency-key", "", "submitting the same key again returns the existing task")
	return cmd
}

// newSubmitCopyCmd builds `durable-tasks submit copy`, which submits a
// chunked file copy and prints its ID.
func newSubmitCopyCmd(opts *rootOptions) *cobra.Command {
	var (
		req chunkcopy.Request
		key string
	)
	cmd := &cobra.Command{
		Use:   "copy --src FILE --dst FILE",
		Short: "Submit a chunked, checkpointed file copy",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			// Workers resolve paths from their own working directory.
			if req.Src, err = filepath.Abs(req.Src); err != nil {
				return err
			}
			if req.Dst, err = filepath.Abs(req.Dst); err != nil {
				return err
			}
			return submitTask(cmd, opts, chunkcopy.Type, req, key)
		},
	}
	f := cmd.Flags()
	f.StringVar(&req.Src, "src", "", "file to copy (required)")
	f.StringVar(&req.Dst, "dst", "", "destination file (required)")
	f.Int64Var(&req.ChunkSize, "chunk-size", chunkcopy.DefaultChunkSize, "bytes copied per checkpointed step")
	f.DurationVar(&req.ChunkDelay, "chunk-delay", 0, "pause after each chunk, to slow the copy down for demos")
	f.StringVar(&key, "idempotency-key", "", "submitting the same key again returns the existing task")
	_ = cmd.MarkFlagRequired("src")
	_ = cmd.MarkFlagRequired("dst")
	return cmd
}

// submitTask stores a task with the given request and prints its ID.
func submitTask(cmd *cobra.Command, opts *rootOptions, taskType string, req any, key string) error {
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
		Type: taskType, Request: request, IdempotencyKey: key,
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
}
