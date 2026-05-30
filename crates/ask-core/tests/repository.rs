use ask_core::models::JobQueueEntry;
use ask_core::repository::{claim_job, complete_job, enqueue_job};
use ask_core::types::JobType;
use rusqlite::{Connection, params};

const STALE_JOB_AGE_SECS: i64 = 86_400;

fn setup_db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory database must open");
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS job_queue (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type    TEXT    NOT NULL,
            payload     TEXT    NOT NULL,
            claimed_at  INTEGER,
            created_at  INTEGER NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_job_queue_unique
            ON job_queue (job_type, payload);",
    )
    .expect("job_queue schema must be created");
    conn
}

fn enqueue(conn: &Connection, payload: &str, now: i64) -> anyhow::Result<()> {
    enqueue_job(conn, &JobType::IngestFolder, payload, now)
}

fn count(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM job_queue", [], |row| row.get(0))
        .expect("job count query must succeed")
}

fn get_job(conn: &Connection, id: i64) -> JobQueueEntry {
    conn.query_row(
        "SELECT id, job_type, payload, claimed_at, created_at FROM job_queue WHERE id = ?1",
        params![id],
        |row| {
            Ok(JobQueueEntry {
                id: row.get(0)?,
                job_type: JobType::try_from_str(&row.get::<_, String>(1)?)
                    .expect("job_type must decode"),
                payload: row.get(2)?,
                claimed_at: row.get(3)?,
                created_at: row.get(4)?,
            })
        },
    )
    .expect("job lookup must succeed")
}

#[test]
fn enqueue_creates_job() {
    let conn = setup_db();
    enqueue(&conn, r#"{"root_path":"/tmp"}"#, 1000).unwrap();
    assert_eq!(count(&conn), 1);
}

#[test]
fn enqueue_duplicate_payload_rejected() {
    let conn = setup_db();
    enqueue(&conn, r#"{"root_path":"/tmp"}"#, 1000).unwrap();
    let err = enqueue(&conn, r#"{"root_path":"/tmp"}"#, 1001).unwrap_err();
    assert!(err.to_string().contains("already queued or in progress"));
    assert_eq!(count(&conn), 1);
}

#[test]
fn enqueue_different_payload_allowed() {
    let conn = setup_db();
    enqueue(&conn, r#"{"root_path":"/tmp"}"#, 1000).unwrap();
    enqueue(&conn, r#"{"root_path":"/other"}"#, 1001).unwrap();
    assert_eq!(count(&conn), 2);
}

#[test]
fn enqueue_same_type_different_payload_allowed() {
    let conn = setup_db();
    enqueue(&conn, r#"{"root_path":"/a"}"#, 1000).unwrap();
    enqueue(&conn, r#"{"root_path":"/b"}"#, 1001).unwrap();
    assert_eq!(count(&conn), 2);
}

#[test]
fn enqueue_replaces_stale_job() {
    let conn = setup_db();
    let now = 100_000i64;
    enqueue(&conn, r#"{"root_path":"/tmp"}"#, now).unwrap();

    conn.execute(
        "UPDATE job_queue SET claimed_at = ?1",
        params![now - STALE_JOB_AGE_SECS - 1],
    )
    .unwrap();

    let later = now + STALE_JOB_AGE_SECS + 10;
    enqueue(&conn, r#"{"root_path":"/tmp"}"#, later).unwrap();
    assert_eq!(count(&conn), 1);

    let job = get_job(&conn, 1);
    assert_eq!(job.claimed_at, None);
    assert_eq!(job.created_at, later);
}

#[test]
fn enqueue_rejects_active_job() {
    let conn = setup_db();
    let now = 100_000i64;
    enqueue(&conn, r#"{"root_path":"/tmp"}"#, now).unwrap();

    conn.execute("UPDATE job_queue SET claimed_at = ?1", params![now])
        .unwrap();

    let err = enqueue(&conn, r#"{"root_path":"/tmp"}"#, now + 10).unwrap_err();
    assert!(err.to_string().contains("already queued or in progress"));
    assert_eq!(count(&conn), 1);
}

#[test]
fn enqueue_active_becomes_stale_and_replaced() {
    let conn = setup_db();
    let now = 100_000i64;
    enqueue(&conn, r#"{"root_path":"/tmp"}"#, now).unwrap();

    conn.execute("UPDATE job_queue SET claimed_at = ?1", params![now])
        .unwrap();

    let later = now + STALE_JOB_AGE_SECS + 10;
    enqueue(&conn, r#"{"root_path":"/tmp"}"#, later).unwrap();
    assert_eq!(count(&conn), 1);

    let job = get_job(&conn, 1);
    assert_eq!(job.claimed_at, None);
    assert_eq!(job.created_at, later);
}

#[test]
fn claim_returns_none_when_empty() {
    let mut conn = setup_db();
    let result = claim_job(&mut conn, 1000).unwrap();
    assert!(result.is_none());
}

#[test]
fn claim_returns_oldest_pending_job() {
    let mut conn = setup_db();
    enqueue(&conn, r#"{"root_path":"/a"}"#, 100).unwrap();
    enqueue(&conn, r#"{"root_path":"/b"}"#, 200).unwrap();

    let entry = claim_job(&mut conn, 1000).unwrap().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&entry.payload).unwrap();
    assert_eq!(parsed["root_path"], "/a");
    assert_eq!(entry.claimed_at, Some(1000));
    assert_eq!(count(&conn), 2);
}

#[test]
fn claim_does_not_return_claimed_job() {
    let mut conn = setup_db();
    enqueue(&conn, r#"{"root_path":"/tmp"}"#, 100).unwrap();
    claim_job(&mut conn, 200).unwrap().unwrap();

    let result = claim_job(&mut conn, 300).unwrap();
    assert!(result.is_none());
}

#[test]
fn claim_reclaims_stale_job() {
    let mut conn = setup_db();
    let now = 100_000i64;
    enqueue(&conn, r#"{"root_path":"/tmp"}"#, now).unwrap();
    conn.execute(
        "UPDATE job_queue SET claimed_at = ?1 WHERE id = 1",
        params![now - STALE_JOB_AGE_SECS - 1],
    )
    .unwrap();

    let entry = claim_job(&mut conn, now + STALE_JOB_AGE_SECS + 10)
        .unwrap()
        .unwrap();

    assert_eq!(entry.id, 1);
    assert_eq!(entry.claimed_at, Some(now + STALE_JOB_AGE_SECS + 10));
}

#[test]
fn claim_rejects_unknown_job_type() {
    let mut conn = setup_db();
    conn.execute(
        "INSERT INTO job_queue (job_type, payload, claimed_at, created_at)
         VALUES (?1, ?2, NULL, ?3)",
        params!["not_a_real_job_type", r#"{"root_path":"/tmp"}"#, 100],
    )
    .unwrap();

    let err = claim_job(&mut conn, 200).unwrap_err();
    let err_text = format!("{err:#}");

    assert!(
        err_text.contains("unknown job_type"),
        "unexpected error: {err:#}"
    );
}

#[test]
fn claim_picks_next_after_first_completed() {
    let mut conn = setup_db();
    enqueue(&conn, r#"{"root_path":"/a"}"#, 100).unwrap();
    enqueue(&conn, r#"{"root_path":"/b"}"#, 200).unwrap();

    let first = claim_job(&mut conn, 300).unwrap().unwrap();
    complete_job(&conn, first.id).unwrap();

    let second = claim_job(&mut conn, 400).unwrap().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&second.payload).unwrap();
    assert_eq!(parsed["root_path"], "/b");
}

#[test]
fn claim_concurrent_safety_single_winner() {
    let dir = std::env::temp_dir().join("ask_core_claim_test.db");
    let _ = std::fs::remove_file(&dir);

    let shared = Connection::open(&dir).unwrap();
    shared
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS job_queue (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                job_type    TEXT    NOT NULL,
                payload     TEXT    NOT NULL,
                claimed_at  INTEGER,
                created_at  INTEGER NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_job_queue_unique
                ON job_queue (job_type, payload);",
        )
        .unwrap();
    enqueue_job(
        &shared,
        &JobType::IngestFolder,
        r#"{"root_path":"/tmp"}"#,
        100,
    )
    .unwrap();
    drop(shared);

    let mut c1 = Connection::open(&dir).unwrap();
    let mut c2 = Connection::open(&dir).unwrap();

    let r1 = claim_job(&mut c1, 500).unwrap();
    let r2 = claim_job(&mut c2, 500).unwrap();

    let claimed = [&r1, &r2].iter().filter(|result| result.is_some()).count();
    assert_eq!(claimed, 1, "exactly one concurrent claim should succeed");

    let _ = std::fs::remove_file(&dir);
}

#[test]
fn complete_removes_job() {
    let mut conn = setup_db();
    enqueue(&conn, r#"{"root_path":"/tmp"}"#, 100).unwrap();
    assert_eq!(count(&conn), 1);

    let entry = claim_job(&mut conn, 200).unwrap().unwrap();
    complete_job(&conn, entry.id).unwrap();
    assert_eq!(count(&conn), 0);
}
