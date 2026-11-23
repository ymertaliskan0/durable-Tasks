// Command durable-tasks is the worker and admin CLI for the durable-tasks
// scheduler. All logic lives in internal/cli.
package main

import (
	"os"

	"github.com/ymertaliskan0/durable-tasks/internal/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		os.Exit(1) // cobra has already printed the error
	}
}
