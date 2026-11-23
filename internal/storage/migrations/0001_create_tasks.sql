-- 0001: the tasks table. See docs/design.md §4 for the reasoning behind each
-- column and index.

CREATE TABLE tasks (
    id                    uuid        PRIMARY KEY,
    task_type             text        NOT NULL,
    request               bytea       NOT NULL,
    state                 bytea,
    status                text        NOT NULL CHECK (status IN (
                              'ready_to_run', 'running', 'finished', 'failed',
                              'ready_to_cancel', 'cancelling', 'cancelled')),
    generation            bigint      NOT NULL DEFAULT 0,
    retriable_error_count integer     NOT NULL DEFAULT 0,
    last_error            text,
    idempotency_key       text        UNIQUE, -- NULLs never conflict
    lease_owner           text,
    lease_expires_at      timestamptz,
    run_after             timestamptz NOT NULL DEFAULT now(),
    created_at            timestamptz NOT NULL DEFAULT now(),
    modified_at           timestamptz NOT NULL DEFAULT now(),

    -- A task holds a lease if and only if it is in flight.
    CONSTRAINT lease_iff_inflight CHECK (
        (status IN ('running', 'cancelling'))
        = (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    )
);

-- Claim path: only claimable rows, oldest run_after first.
CREATE INDEX tasks_claimable ON tasks (run_after)
    WHERE status IN ('ready_to_run', 'ready_to_cancel');

-- Stalker path: only in-flight rows, earliest lease expiry first.
CREATE INDEX tasks_inflight ON tasks (lease_expires_at)
    WHERE status IN ('running', 'cancelling');
