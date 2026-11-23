package cli

import (
	"log/slog"
	"time"

	"github.com/spf13/cobra"

	"github.com/ymertaliskan0/durable-tasks/internal/tasks"
	"github.com/ymertaliskan0/durable-tasks/internal/tasks/sleeptask"
)

// newWorkerCmd builds `durable-tasks worker`, which executes tasks until it
// receives SIGINT or SIGTERM, then releases its running tasks and exits.
//
// Parameters:
//   - opts: the shared flag values; read when the command runs.
func newWorkerCmd(opts *rootOptions) *cobra.Command {
	cfg := tasks.DefaultConfig()
	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Run a worker that claims and executes tasks",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			store, err := openStore(ctx, opts)
			if err != nil {
				return err
			}
			defer store.Close()

			reg := tasks.NewRegistry()
			reg.Register(sleeptask.Type, sleeptask.New)

			log := slog.New(slog.NewTextHandler(cmd.ErrOrStderr(), nil))
			w, err := tasks.NewWorker(store, reg, cfg, log)
			if err != nil {
				return err
			}
			log.Info("worker started", "worker", cfg.WorkerID, "concurrency", cfg.Concurrency, "lease", cfg.Lease)
			if err := w.Run(ctx); err != nil {
				return err
			}
			log.Info("worker stopped", "worker", cfg.WorkerID)
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&cfg.WorkerID, "worker-id", cfg.WorkerID, "lease owner name for this worker")
	f.IntVar(&cfg.Concurrency, "concurrency", cfg.Concurrency, "tasks executed at the same time")
	f.DurationVar(&cfg.Lease, "lease", cfg.Lease, "lease duration; heartbeats are sent every lease/3")
	f.DurationVar(&cfg.PollInterval, "poll-interval", cfg.PollInterval, "wait before claiming again when the queue is empty")
	f.DurationVar(&cfg.StalkerInterval, "stalker-interval", cfg.StalkerInterval, "how often to reclaim tasks with expired leases")
	f.IntVar(&cfg.MaxRetries, "max-retries", cfg.MaxRetries, "retriable errors allowed before a task fails")
	f.DurationVar(&cfg.BackoffBase, "backoff-base", cfg.BackoffBase, "delay before the first retry")
	f.DurationVar(&cfg.BackoffMax, "backoff-max", cfg.BackoffMax, "longest delay between retries")
	f.DurationVar(&cfg.ShutdownGrace, "shutdown-grace", 10*time.Second, "time allowed to release each task on shutdown")
	return cmd
}
