ALTER TABLE notification_tasks
    ADD COLUMN IF NOT EXISTS concurrency_limit INTEGER NOT NULL DEFAULT 1 CHECK (concurrency_limit > 0);
