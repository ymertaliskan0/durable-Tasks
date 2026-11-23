package cli

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/ymertaliskan0/durable-tasks/internal/nemesis"
)

// newNemesisCmd builds `durable-tasks nemesis`, the crash test: it runs
// worker processes, kills and freezes them at random while they copy files,
// then verifies every copy. It exits with an error if any invariant failed.
func newNemesisCmd(opts *rootOptions) *cobra.Command {
	cfg := nemesis.DefaultConfig()
	cmd := &cobra.Command{
		Use:   "nemesis",
		Short: "Crash-test the scheduler: SIGKILL and SIGSTOP workers mid-task, then verify every task",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			bin, err := os.Executable()
			if err != nil {
				return err
			}
			cfg.Binary, cfg.DSN, cfg.Log = bin, opts.dsn, cmd.ErrOrStderr()
			rep, err := nemesis.Run(cmd.Context(), cfg)
			if err != nil {
				return err
			}
			if err := nemesis.WriteReport(cmd.OutOrStdout(), rep); err != nil {
				return err
			}
			if !rep.Result.Passed() {
				return nemesis.ErrFailed
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&cfg.Dir, "dir", os.TempDir()+"/durable-tasks-nemesis", "where files, copies and worker logs go")
	f.IntVar(&cfg.Workers, "workers", cfg.Workers, "worker processes")
	f.IntVar(&cfg.Tasks, "tasks", cfg.Tasks, "copy tasks")
	f.Int64Var(&cfg.FileSize, "file-size", cfg.FileSize, "bytes per file")
	f.Int64Var(&cfg.ChunkSize, "chunk-size", cfg.ChunkSize, "bytes per checkpointed step")
	f.DurationVar(&cfg.ChunkDelay, "chunk-delay", cfg.ChunkDelay, "pause after each chunk, so tasks are still running when the chaos hits")
	f.DurationVar(&cfg.Lease, "lease", cfg.Lease, "worker lease")
	f.DurationVar(&cfg.Duration, "duration", cfg.Duration, "longest chaos phase (it ends early once every task has ended)")
	f.DurationVar(&cfg.Interval, "interval", cfg.Interval, "average time between chaos events")
	f.DurationVar(&cfg.PauseFor, "pause-for", cfg.PauseFor, "how long a SIGSTOPped worker stays frozen")
	f.DurationVar(&cfg.Settle, "settle", cfg.Settle, "how long to wait for tasks to end after the chaos")
	f.Uint64Var(&cfg.Seed, "seed", cfg.Seed, "random seed, to reproduce a run")
	return cmd
}

// newBenchCmd builds `durable-tasks bench`, which measures how many trivial
// tasks per second the scheduler can execute.
func newBenchCmd(opts *rootOptions) *cobra.Command {
	// 64 slots plus 8 stalker connections stays under PostgreSQL's default
	// limit of 100 connections.
	cfg := nemesis.BenchConfig{Tasks: 10000, Workers: 8, Concurrency: 8}
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Measure scheduler throughput and per-task overhead with trivial tasks",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg.DSN = opts.dsn
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "bench: running %d tasks on %d workers x %d slots...\n", cfg.Tasks, cfg.Workers, cfg.Concurrency)
			rep, err := nemesis.Bench(cmd.Context(), cfg)
			if err != nil {
				return err
			}
			return nemesis.WriteBenchReport(cmd.OutOrStdout(), rep)
		},
	}
	f := cmd.Flags()
	f.IntVar(&cfg.Tasks, "tasks", cfg.Tasks, "tasks to execute")
	f.IntVar(&cfg.Workers, "workers", cfg.Workers, "in-process workers")
	f.IntVar(&cfg.Concurrency, "concurrency", cfg.Concurrency, "task slots per worker")
	f.DurationVar(&cfg.Timeout, "timeout", 10*time.Minute, "give up after this long")
	return cmd
}
