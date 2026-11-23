# durable-tasks

A fault-tolerant distributed task scheduler in Go, modelled on the task
framework (`cloud/tasks`) in Nebius's open-source
[Network Block Store](https://github.com/ydb-platform/nbs). Tasks
checkpoint their progress in PostgreSQL and survive worker crashes. Stale
workers are fenced off by a generation number.

> Work in progress. The design baseline is in [docs/design.md](docs/design.md).

## Quick start (so far)

```bash
make up        # Postgres 17 in Docker
make migrate   # build the CLI and apply the schema
make test      # all tests with -race (needs Docker for the integration tests)
make test-short # unit tests only
```
