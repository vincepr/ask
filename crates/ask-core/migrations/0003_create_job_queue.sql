CREATE TABLE IF NOT EXISTS job_queue (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type    TEXT    NOT NULL,
    payload     TEXT    NOT NULL,
    heartbeat   INTEGER,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_job_queue_unique
    ON job_queue (job_type, payload);
