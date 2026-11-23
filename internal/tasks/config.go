package tasks

import (
	"errors"
	"fmt"
	"os"
	"time"
)

// Config tunes a Worker. Start from DefaultConfig and override fields.
type Config struct {
	WorkerID        string        // identifies this worker as the lease owner
	Concurrency     int           // how many tasks run at the same time
	Lease           time.Duration // how long a claim lasts without a heartbeat; heartbeats go every Lease/3
	PollInterval    time.Duration // how long to wait before claiming again when the queue is empty
	StalkerInterval time.Duration // how often to reclaim tasks whose lease expired
	MaxRetries      int           // retriable errors allowed before a task fails
	BackoffBase     time.Duration // delay before the first retry
	BackoffMax      time.Duration // longest delay between retries
	ShutdownGrace   time.Duration // time allowed for each final write during shutdown
}

// DefaultConfig returns production defaults. WorkerID is "hostname-pid".
func DefaultConfig() Config {
	host, err := os.Hostname()
	if err != nil {
		host = "worker"
	}
	return Config{
		WorkerID:        fmt.Sprintf("%s-%d", host, os.Getpid()),
		Concurrency:     4,
		Lease:           30 * time.Second,
		PollInterval:    500 * time.Millisecond,
		StalkerInterval: 5 * time.Second,
		MaxRetries:      5,
		BackoffBase:     time.Second,
		BackoffMax:      time.Minute,
		ShutdownGrace:   10 * time.Second,
	}
}

func (c Config) validate() error {
	switch {
	case c.WorkerID == "":
		return errors.New("tasks: config: WorkerID is empty")
	case c.Concurrency < 1:
		return errors.New("tasks: config: Concurrency must be at least 1")
	case c.Lease <= 0, c.PollInterval <= 0, c.StalkerInterval <= 0, c.ShutdownGrace <= 0:
		return errors.New("tasks: config: Lease, PollInterval, StalkerInterval and ShutdownGrace must be positive")
	case c.MaxRetries < 0:
		return errors.New("tasks: config: MaxRetries must not be negative")
	case c.BackoffBase <= 0, c.BackoffMax < c.BackoffBase:
		return errors.New("tasks: config: need 0 < BackoffBase <= BackoffMax")
	}
	return nil
}
