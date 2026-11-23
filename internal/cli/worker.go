package cli

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/spf13/cobra"

	"github.com/ymertaliskan0/durable-tasks/internal/metrics"
	"github.com/ymertaliskan0/durable-tasks/internal/tasks"
	"github.com/ymertaliskan0/durable-tasks/internal/tasks/chunkcopy"
	"github.com/ymertaliskan0/durable-tasks/internal/tasks/sleeptask"
)

// newWorkerCmd builds `durable-tasks worker`, which executes tasks until it
// receives SIGINT or SIGTERM, then releases its running tasks and exits.
//
// Parameters:
//   - opts: the shared flag values; read when the command runs.
func newWorkerCmd(opts *rootOptions) *cobra.Command {
	cfg := tasks.DefaultConfig()
	var metricsAddr string
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

			log := slog.New(slog.NewTextHandler(cmd.ErrOrStderr(), nil))

			var workerStore tasks.Store = store
			if metricsAddr != "" {
				m := metrics.New()
				m.WatchQueue(store)
				workerStore = m.Instrument(store)
				stop, err := serveMetrics(ctx, metricsAddr, m)
				if err != nil {
					return err
				}
				defer stop()
				log.Info("serving metrics", "addr", metricsAddr, "path", "/metrics")
			}

			w, err := tasks.NewWorker(workerStore, newRegistry(), cfg, log)
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
	f.StringVar(&metricsAddr, "metrics-addr", "", "serve Prometheus metrics on this address, e.g. :9090 (off when empty)")
	return cmd
}

// newRegistry returns the task types every worker can execute.
func newRegistry() *tasks.Registry {
	reg := tasks.NewRegistry()
	reg.Register(sleeptask.Type, sleeptask.New)
	reg.Register(chunkcopy.Type, chunkcopy.New)
	return reg
}

// serveMetrics serves m on addr until ctx is done or stop is called.
//
// Returns stop, which shuts the server down, or an error if addr cannot be
// listened on.
func serveMetrics(ctx context.Context, addr string, m *metrics.Metrics) (stop func(), err error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}, nil
}
