package nemesis

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ymertaliskan0/durable-tasks/internal/pgtest"
)

func TestMain(m *testing.M) { pgtest.Main(m) }

// buildBinary compiles the durable-tasks command, which the harness starts
// as worker processes.
func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "durable-tasks")
	out, err := exec.Command("go", "build", "-o", bin, "../../cmd/durable-tasks").CombinedOutput()
	if err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

// A short but real crash test: worker processes are SIGKILLed and frozen
// past their lease while they copy files.
func TestRun_SurvivesChaos(t *testing.T) {
	dsn := pgtest.NewDB(t)
	var log strings.Builder
	cfg := Config{
		Binary:     buildBinary(t),
		DSN:        dsn,
		Dir:        t.TempDir(),
		Workers:    3,
		Tasks:      8,
		FileSize:   256 << 10,
		ChunkSize:  16 << 10,
		ChunkDelay: 60 * time.Millisecond, // 16 chunks: about 1s per task
		Lease:      time.Second,
		Duration:   8 * time.Second,
		Interval:   300 * time.Millisecond,
		PauseFor:   1500 * time.Millisecond, // longer than the lease: frozen workers become zombies
		Settle:     60 * time.Second,
		Seed:       42,
		Log:        &log,
	}
	rep, err := Run(t.Context(), cfg)
	if err != nil {
		t.Fatalf("Run: %v\nlog:\n%s", err, log.String())
	}
	var out strings.Builder
	_ = WriteReport(&out, rep)
	t.Log("\n" + out.String())

	if !rep.Result.Passed() {
		t.Fatalf("invariants violated:\n%s\nlog:\n%s", out.String(), log.String())
	}
	if rep.Kills+rep.Pauses == 0 {
		t.Error("no chaos happened; the test proved nothing")
	}
	if rep.Result.Finished != cfg.Tasks {
		t.Errorf("finished %d of %d tasks", rep.Result.Finished, cfg.Tasks)
	}
}

func TestBench(t *testing.T) {
	rep, err := Bench(t.Context(), BenchConfig{DSN: pgtest.NewDB(t), Tasks: 200, Workers: 2, Concurrency: 4})
	if err != nil {
		t.Fatal(err)
	}
	if rep.PerSecond <= 0 || rep.P50 <= 0 || rep.P99 < rep.P50 {
		t.Errorf("implausible report: %+v", rep)
	}
	var out strings.Builder
	_ = WriteBenchReport(&out, rep)
	t.Log(out.String())
}
