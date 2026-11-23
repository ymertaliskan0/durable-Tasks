// Package nemesis is the crash-test harness. It runs real worker processes,
// kills (SIGKILL) and freezes (SIGSTOP, later SIGCONT) them at random while
// they copy files, and then checks that every task finished with a correct
// copy: nothing lost, nothing stuck, nothing corrupted.
package nemesis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/ymertaliskan0/durable-tasks/internal/storage"
	"github.com/ymertaliskan0/durable-tasks/internal/tasks/chunkcopy"
)

// Config describes one crash-test run.
type Config struct {
	Binary     string        // the durable-tasks executable to start workers from
	DSN        string        // database the workers and the harness share
	Dir        string        // where source files, copies and worker logs go
	Workers    int           // worker processes kept running
	Tasks      int           // copy tasks submitted
	FileSize   int64         // bytes per source file
	ChunkSize  int64         // bytes per checkpointed copy step
	ChunkDelay time.Duration // pause after each chunk, so tasks outlive the chaos
	Lease      time.Duration // worker lease; the stalker runs every Lease/2
	Duration   time.Duration // longest chaos phase; it also ends when all tasks have ended
	Interval   time.Duration // average time between chaos events
	PauseFor   time.Duration // how long a frozen worker stays frozen; > Lease creates zombies
	Settle     time.Duration // how long to wait for all tasks to end after the chaos
	Seed       uint64        // makes file contents and the chaos schedule reproducible
	Log        io.Writer     // progress messages; nil discards them
	// SharedDB runs in DSN's database itself instead of a fresh scratch
	// database. Tests use it; on a real database it would also execute
	// whatever tasks are already queued there.
	SharedDB bool
}

// DefaultConfig returns a run that takes about a minute: 100 copies of 32
// chunks each, on 8 task slots, with a chaos event every 750ms on average.
func DefaultConfig() Config {
	return Config{
		Workers:    4,
		Tasks:      100,
		FileSize:   2 << 20,
		ChunkSize:  64 << 10, // 32 chunks per file
		ChunkDelay: 100 * time.Millisecond,
		Lease:      2 * time.Second,
		Duration:   90 * time.Second,
		Interval:   750 * time.Millisecond,
		PauseFor:   5 * time.Second,
		Settle:     2 * time.Minute,
		Seed:       uint64(time.Now().UnixNano()),
	}
}

// Report summarises a crash-test run.
type Report struct {
	Config  Config
	Kills   int           // SIGKILLs sent
	Pauses  int           // SIGSTOPs sent
	Chaos   time.Duration // length of the chaos phase
	Elapsed time.Duration // from the first submit until every task ended
	Result  Result
}

// Run executes one crash test.
//
// Returns the report, or an error if the test could not be carried out (for
// example, the database is unreachable). A run whose invariants failed is
// not an error: check Report.Result.Passed.
func Run(ctx context.Context, cfg Config) (Report, error) {
	if cfg.Log == nil {
		cfg.Log = io.Discard
	}
	rep := Report{Config: cfg}

	if !cfg.SharedDB {
		dsn, drop, err := scratchDB(ctx, cfg.DSN, "nemesis")
		if err != nil {
			return rep, err
		}
		cfg.DSN = dsn
		defer func() {
			if rep.Result.Passed() && rep.Result.Tasks > 0 {
				drop()
			} else {
				logf(cfg.Log, "keeping database %s for inspection", dsn)
			}
		}()
	}

	store, err := storage.Open(ctx, cfg.DSN)
	if err != nil {
		return rep, err
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		return rep, err
	}

	runDir := filepath.Join(cfg.Dir, "run-"+time.Now().Format("20060102-150405"))
	logf(cfg.Log, "preparing %d files of %s in %s", cfg.Tasks, size(cfg.FileSize), runDir)
	jobs, err := prepare(runDir, cfg)
	if err != nil {
		return rep, err
	}
	defer func() {
		if rep.Result.Passed() && rep.Result.Tasks > 0 {
			_ = os.RemoveAll(runDir)
		} else {
			logf(cfg.Log, "keeping files and worker logs in %s", runDir)
		}
	}()

	start := time.Now()
	runID := uuid.NewString()
	for i := range jobs {
		req := fmt.Sprintf(`{"src":%q,"dst":%q,"chunk_size":%d,"chunk_delay":%d}`,
			jobs[i].Src, jobs[i].Dst, cfg.ChunkSize, cfg.ChunkDelay)
		id, _, err := store.CreateTask(ctx, storage.NewTask{
			Type: chunkcopy.Type, Request: []byte(req), IdempotencyKey: fmt.Sprintf("nemesis-%s-%d", runID, i),
		})
		if err != nil {
			return rep, err
		}
		jobs[i].ID = id
	}

	fleet := &fleet{cfg: cfg, logDir: filepath.Join(runDir, "logs")}
	defer fleet.stopAll()
	for i := range cfg.Workers {
		if err := fleet.start(i); err != nil {
			return rep, err
		}
	}
	logf(cfg.Log, "started %d workers; chaos for up to %s", cfg.Workers, cfg.Duration)

	rng := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15))
	chaosStart := time.Now()
	if err := fleet.chaos(ctx, rng, func() (bool, error) { return allEnded(ctx, store, jobs) }); err != nil {
		return rep, err
	}
	rep.Kills, rep.Pauses, rep.Chaos = fleet.kills, fleet.pauses, time.Since(chaosStart)
	logf(cfg.Log, "chaos over: %d kills, %d pauses; waiting up to %s for tasks to end", rep.Kills, rep.Pauses, cfg.Settle)

	fleet.resumeAll()
	deadline := time.Now().Add(cfg.Settle)
	for {
		done, err := allEnded(ctx, store, jobs)
		if err != nil {
			return rep, err
		}
		if done || time.Now().After(deadline) {
			break
		}
		if err := sleep(ctx, 200*time.Millisecond); err != nil {
			return rep, err
		}
	}
	rep.Elapsed = time.Since(start)
	fleet.stopAll()

	rep.Result, err = check(ctx, store, jobs)
	return rep, err
}

// prepare writes the source files and returns one job per file.
func prepare(dir string, cfg Config) ([]job, error) {
	for _, sub := range []string{"src", "dst", "logs"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, err
		}
	}
	buf := make([]byte, cfg.FileSize)
	jobs := make([]job, cfg.Tasks)
	for i := range jobs {
		var seed [32]byte
		copy(seed[:], fmt.Sprintf("%d-%d", cfg.Seed, i))
		_, _ = rand.NewChaCha8(seed).Read(buf)
		src := filepath.Join(dir, "src", fmt.Sprintf("file-%03d", i))
		if err := os.WriteFile(src, buf, 0o644); err != nil {
			return nil, err
		}
		abs, err := filepath.Abs(src)
		if err != nil {
			return nil, err
		}
		jobs[i] = job{Src: abs, Dst: strings.Replace(abs, string(filepath.Separator)+"src"+string(filepath.Separator), string(filepath.Separator)+"dst"+string(filepath.Separator), 1)}
	}
	return jobs, nil
}

// allEnded reports whether every job's task has reached a terminal status.
func allEnded(ctx context.Context, store *storage.Postgres, jobs []job) (bool, error) {
	for _, j := range jobs {
		ts, err := store.GetTask(ctx, j.ID)
		if err != nil {
			return false, err
		}
		if !ts.Status.Ended() {
			return false, nil
		}
	}
	return true, nil
}

// fleet manages the worker processes.
type fleet struct {
	cfg    Config
	logDir string
	procs  []*proc
	kills  int
	pauses int
}

type proc struct {
	cmd         *exec.Cmd
	exited      chan struct{}
	incarnation int
	resumeAt    time.Time // zero unless the process is frozen
	log         *os.File
}

// start launches worker slot i, replacing whatever ran there before.
func (f *fleet) start(i int) error {
	for len(f.procs) <= i {
		f.procs = append(f.procs, nil)
	}
	inc := 0
	if old := f.procs[i]; old != nil {
		inc = old.incarnation + 1
	}
	logFile, err := os.OpenFile(filepath.Join(f.logDir, fmt.Sprintf("worker-%d.log", i)), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(f.cfg.Binary, "worker",
		"--dsn", f.cfg.DSN,
		"--worker-id", fmt.Sprintf("nemesis-%d.%d", i, inc),
		"--concurrency", "2",
		"--lease", f.cfg.Lease.String(),
		"--stalker-interval", (f.cfg.Lease / 2).String(),
		"--poll-interval", "50ms",
		"--backoff-base", "50ms",
		"--backoff-max", "1s",
		"--shutdown-grace", "5s",
	)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("nemesis: start worker %d: %w", i, err)
	}
	p := &proc{cmd: cmd, exited: make(chan struct{}), incarnation: inc, log: logFile}
	go func() {
		_ = cmd.Wait()
		_ = logFile.Close()
		close(p.exited)
	}()
	f.procs[i] = p
	return nil
}

// chaos kills and freezes random workers until Duration has passed, ctx is
// done, or done reports that every task has ended.
func (f *fleet) chaos(ctx context.Context, rng *rand.Rand, done func() (bool, error)) error {
	end := time.Now().Add(f.cfg.Duration)
	next := time.Now().Add(f.cfg.Interval)
	lastCheck := time.Now()
	for time.Now().Before(end) {
		if err := sleep(ctx, 50*time.Millisecond); err != nil {
			return err
		}
		f.resumeDue(time.Now())

		if time.Since(lastCheck) > 500*time.Millisecond {
			lastCheck = time.Now()
			finished, err := done()
			if err != nil {
				return err
			}
			if finished {
				return nil
			}
		}

		if time.Now().Before(next) {
			continue
		}
		// Next event after 0.5x-1.5x the interval.
		next = time.Now().Add(time.Duration((0.5 + rng.Float64()) * float64(f.cfg.Interval)))
		i := rng.IntN(len(f.procs))
		p := f.procs[i]
		if rng.IntN(2) == 0 {
			_ = p.cmd.Process.Kill() // SIGKILL: no cleanup, no release
			<-p.exited
			f.kills++
			if err := f.start(i); err != nil {
				return err
			}
		} else if p.resumeAt.IsZero() {
			_ = p.cmd.Process.Signal(syscall.SIGSTOP) // frozen mid-task: a future zombie
			p.resumeAt = time.Now().Add(f.cfg.PauseFor)
			f.pauses++
		}
	}
	return nil
}

// resumeDue wakes frozen workers whose pause is over.
func (f *fleet) resumeDue(now time.Time) {
	for _, p := range f.procs {
		if !p.resumeAt.IsZero() && now.After(p.resumeAt) {
			_ = p.cmd.Process.Signal(syscall.SIGCONT)
			p.resumeAt = time.Time{}
		}
	}
}

// resumeAll wakes every frozen worker.
func (f *fleet) resumeAll() { f.resumeDue(time.Now().Add(100 * 365 * 24 * time.Hour)) }

// stopAll shuts every worker down gracefully (SIGTERM), killing any that
// take longer than 10 seconds. It is safe to call more than once.
func (f *fleet) stopAll() {
	f.resumeAll()
	for _, p := range f.procs {
		select {
		case <-p.exited:
			continue
		default:
		}
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
	}
	for _, p := range f.procs {
		select {
		case <-p.exited:
		case <-time.After(10 * time.Second):
			_ = p.cmd.Process.Kill()
			<-p.exited
		}
	}
}

// WriteReport prints a human-readable summary of rep to w.
func WriteReport(w io.Writer, rep Report) error {
	c, r := rep.Config, rep.Result
	verdict := "PASS"
	if !r.Passed() {
		verdict = "FAIL"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "durable-tasks crash test: %s\n", verdict)
	fmt.Fprintf(&b, "  setup:  %d worker processes x 2 slots, %d copy tasks x %s (%s total), lease %s\n",
		c.Workers, c.Tasks, size(c.FileSize), size(c.FileSize*int64(c.Tasks)), c.Lease)
	fmt.Fprintf(&b, "  chaos:  %d SIGKILL + %d SIGSTOP (frozen %s each) over %s\n",
		rep.Kills, rep.Pauses, c.PauseFor, rep.Chaos.Round(100*time.Millisecond))
	fmt.Fprintf(&b, "  result: %d/%d finished with verified copies; %d tasks recovered after an interruption (max generation %d); %d retries; all done in %s\n",
		r.Finished, r.Tasks, r.Recovered, r.MaxGeneration, r.Retries, rep.Elapsed.Round(100*time.Millisecond))
	for _, p := range r.Problems {
		fmt.Fprintf(&b, "  PROBLEM: %s\n", p)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func size(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	default:
		return fmt.Sprintf("%d KiB", n>>10)
	}
}

func logf(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, "nemesis: "+format+"\n", args...)
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// ErrFailed is returned by callers that turn a failed Report into an error.
var ErrFailed = errors.New("crash test failed: invariants violated")
