-- 0002: why a task is on the cancel path (docs/design.md §5, decision D2).
--
--   'requested' - a user asked to cancel; Cancel() cleanup ends in 'cancelled'
--   'failed'    - Run() failed for good; Cancel() cleanup ends in 'failed'
--
-- last_error cannot carry this information, because retriable errors also set
-- it on tasks that are never cancelled.

ALTER TABLE tasks
    ADD COLUMN cancel_reason text CHECK (cancel_reason IN ('requested', 'failed'));

-- A task on the cancel path must know why it is there. The reason is kept
-- after the task ends, as history.
ALTER TABLE tasks
    ADD CONSTRAINT cancel_path_has_reason CHECK (
        status NOT IN ('ready_to_cancel', 'cancelling') OR cancel_reason IS NOT NULL
    );
