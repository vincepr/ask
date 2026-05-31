ALTER TABLE job_queue RENAME TO job_queue_old;

CREATE TABLE job_queue (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type    TEXT    NOT NULL,
    payload     TEXT    NOT NULL,
    claimed_at  INTEGER,
    created_at  INTEGER NOT NULL
);

INSERT INTO job_queue (id, job_type, payload, claimed_at, created_at)
SELECT id, job_type, payload, heartbeat, created_at
FROM job_queue_old;

DROP TABLE job_queue_old;

CREATE UNIQUE INDEX idx_job_queue_unique
    ON job_queue (job_type, payload);
