use ask_core::models::{Document, EmbedDocumentPayload, EmbeddedChunk, JobQueueEntry};
use ask_core::repository::{
    claim_job, complete_job, enqueue_job, find_document_by_id, find_document_by_path,
    insert_pending_embeddings, replace_embeddings_for_document_model, seed_embed_jobs,
    upsert_document, upsert_document_and_replace_pending_embeddings,
};
use ask_core::types::{ChunkType, DocCategory, EmbedState, JobType};
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

fn setup_documents_db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory database must open");
    conn.execute_batch(
        "CREATE TABLE documents (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            filepath         TEXT NOT NULL,
            file_type        TEXT NOT NULL,
            doc_category     TEXT NOT NULL,
            file_modified_at INTEGER NOT NULL,
            file_size        INTEGER NOT NULL,
            updated_at       INTEGER NOT NULL
        );
        CREATE TABLE document_embeddings (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id     INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
            model_id        INTEGER NOT NULL,
            chunk_type      TEXT    NOT NULL,
            chunk_start     INTEGER NOT NULL,
            chunk_end       INTEGER NOT NULL,
            state           TEXT    NOT NULL,
            embedding       BLOB,
            created_at      INTEGER NOT NULL
         );
         CREATE UNIQUE INDEX idx_embeddings_unique
            ON document_embeddings (document_id, model_id, chunk_type, chunk_start);
         CREATE TABLE job_queue (
             id          INTEGER PRIMARY KEY AUTOINCREMENT,
             job_type    TEXT    NOT NULL,
             payload     TEXT    NOT NULL,
             claimed_at  INTEGER,
             created_at  INTEGER NOT NULL
         );
         CREATE UNIQUE INDEX idx_job_queue_unique
             ON job_queue (job_type, payload);",
    )
    .expect("document schema must be created");
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
                job_type: row.get(1)?,
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
        err_text.contains("invalid JobType value"),
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

#[test]
fn upsert_document_reuses_existing_row_for_unchanged_file() {
    let mut conn = setup_documents_db();
    let doc = Document {
        id: 0,
        filepath: "/tmp/a.txt".to_string(),
        file_type: "txt".to_string(),
        doc_category: DocCategory::Resource,
        file_modified_at: 100,
        file_size: 10,
        updated_at: 100,
    };

    let (first_id, first_changed) = upsert_document(&mut conn, &doc).unwrap();
    let (second_id, second_changed) = upsert_document(&mut conn, &doc).unwrap();

    assert_eq!(first_id, second_id);
    assert!(first_changed);
    assert!(!second_changed);

    let row_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |row| row.get(0))
        .unwrap();
    assert_eq!(row_count, 1, "unchanged upserts must not create duplicates");
}

#[test]
fn upsert_document_updates_existing_row_and_marks_embeddings_stale() {
    let mut conn = setup_documents_db();
    let original = Document {
        id: 0,
        filepath: "/tmp/a.txt".to_string(),
        file_type: "txt".to_string(),
        doc_category: DocCategory::Resource,
        file_modified_at: 100,
        file_size: 10,
        updated_at: 100,
    };
    let (doc_id, _) = upsert_document(&mut conn, &original).unwrap();

    conn.execute(
        "INSERT INTO document_embeddings
            (document_id, model_id, chunk_type, chunk_start, chunk_end, state, embedding, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, X'01', ?7)",
        params![doc_id, 1, ChunkType::Filename, 0, 0, EmbedState::Embedded, 100],
    )
    .unwrap();

    let updated = Document {
        updated_at: 200,
        file_modified_at: 200,
        file_size: 20,
        ..original
    };
    let (updated_id, changed) = upsert_document(&mut conn, &updated).unwrap();

    assert_eq!(updated_id, doc_id);
    assert!(changed);

    let stored = find_document_by_path(&conn, "/tmp/a.txt").unwrap().unwrap();
    assert_eq!(stored.id, doc_id);
    assert_eq!(stored.file_modified_at, 200);
    assert_eq!(stored.file_size, 20);
    assert_eq!(stored.updated_at, 200);

    let state: EmbedState = conn
        .query_row(
            "SELECT state FROM document_embeddings WHERE document_id = ?1",
            [doc_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, EmbedState::Stale);
}

#[test]
fn find_document_by_path_prefers_latest_row_when_duplicates_exist() {
    let conn = setup_documents_db();
    conn.execute(
        "INSERT INTO documents
            (filepath, file_type, doc_category, file_modified_at, file_size, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6),
                (?1, ?2, ?3, ?7, ?8, ?9)",
        params![
            "/tmp/a.txt",
            "txt",
            DocCategory::Resource,
            100,
            10,
            100,
            200,
            20,
            200
        ],
    )
    .unwrap();

    let doc = find_document_by_path(&conn, "/tmp/a.txt").unwrap().unwrap();
    assert_eq!(doc.file_modified_at, 200);
    assert_eq!(doc.file_size, 20);
    assert_eq!(doc.updated_at, 200);
}

#[test]
fn find_document_by_id_returns_matching_document() {
    let mut conn = setup_documents_db();
    let doc = Document {
        id: 0,
        filepath: "/tmp/by-id.txt".to_string(),
        file_type: "txt".to_string(),
        doc_category: DocCategory::Resource,
        file_modified_at: 100,
        file_size: 10,
        updated_at: 100,
    };

    let (doc_id, _) = upsert_document(&mut conn, &doc).unwrap();
    let loaded = find_document_by_id(&conn, doc_id).unwrap().unwrap();

    assert_eq!(loaded.filepath, doc.filepath);
    assert_eq!(loaded.id, doc_id);
}

#[test]
fn insert_pending_embeddings_replaces_existing_row() {
    let mut conn = setup_documents_db();
    let doc = Document {
        id: 0,
        filepath: "/tmp/a.txt".to_string(),
        file_type: "txt".to_string(),
        doc_category: DocCategory::Resource,
        file_modified_at: 100,
        file_size: 10,
        updated_at: 100,
    };
    let (doc_id, _) = upsert_document(&mut conn, &doc).unwrap();

    conn.execute(
        "INSERT INTO document_embeddings
            (document_id, model_id, chunk_type, chunk_start, chunk_end, state, embedding, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, X'01', ?7)",
        params![doc_id, 1, ChunkType::Content, 0, 4, EmbedState::Embedded, 100],
    )
    .unwrap();

    insert_pending_embeddings(&conn, doc_id, 1, &[(ChunkType::Content, 0, 8)], 200).unwrap();

    let (chunk_end, state, embedding_is_null, created_at): (i64, EmbedState, i64, i64) = conn
        .query_row(
            "SELECT chunk_end, state, embedding IS NULL, created_at
             FROM document_embeddings
             WHERE document_id = ?1 AND model_id = ?2 AND chunk_type = ?3 AND chunk_start = ?4",
            params![doc_id, 1, ChunkType::Content, 0],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(chunk_end, 8);
    assert_eq!(state, EmbedState::Pending);
    assert_eq!(embedding_is_null, 1);
    assert_eq!(created_at, 200);
}

#[test]
fn transactional_document_ingest_replaces_pending_embeddings() {
    let mut conn = setup_documents_db();
    let doc = Document {
        id: 0,
        filepath: "/tmp/a.txt".to_string(),
        file_type: "txt".to_string(),
        doc_category: DocCategory::Resource,
        file_modified_at: 100,
        file_size: 10,
        updated_at: 100,
    };

    let (doc_id, changed) = upsert_document_and_replace_pending_embeddings(
        &mut conn,
        &doc,
        7,
        &[(ChunkType::Filename, 0, 0), (ChunkType::Content, 0, 8)],
        100,
    )
    .unwrap();

    assert!(changed);

    let queued: Vec<(ChunkType, i64, i64, EmbedState)> = conn
        .prepare(
            "SELECT chunk_type, chunk_start, chunk_end, state
             FROM document_embeddings
             WHERE document_id = ?1 AND model_id = ?2
             ORDER BY chunk_type, chunk_start",
        )
        .unwrap()
        .query_map(params![doc_id, 7], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();

    assert_eq!(
        queued,
        vec![
            (ChunkType::Content, 0, 8, EmbedState::Pending),
            (ChunkType::Filename, 0, 0, EmbedState::Pending),
        ]
    );
}

#[test]
fn transactional_document_ingest_keeps_existing_pending_rows_when_unchanged() {
    let mut conn = setup_documents_db();
    let doc = Document {
        id: 0,
        filepath: "/tmp/a.txt".to_string(),
        file_type: "txt".to_string(),
        doc_category: DocCategory::Resource,
        file_modified_at: 100,
        file_size: 10,
        updated_at: 100,
    };

    let (doc_id, _) =
        upsert_document_and_replace_pending_embeddings(&mut conn, &doc, 7, &[], 100).unwrap();
    conn.execute(
        "INSERT INTO document_embeddings
            (document_id, model_id, chunk_type, chunk_start, chunk_end, state, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            doc_id,
            7,
            ChunkType::Filename,
            0,
            0,
            EmbedState::Pending,
            100
        ],
    )
    .unwrap();

    let (_, changed) =
        upsert_document_and_replace_pending_embeddings(&mut conn, &doc, 7, &[], 200).unwrap();

    assert!(!changed);

    let embedding_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE document_id = ?1 AND model_id = ?2",
            params![doc_id, 7],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(embedding_count, 1);
}

#[test]
fn replace_embeddings_for_document_model_replaces_only_target_pair_atomically() {
    let mut conn = setup_documents_db();
    let doc = Document {
        id: 0,
        filepath: "/tmp/replace.txt".to_string(),
        file_type: "txt".to_string(),
        doc_category: DocCategory::Resource,
        file_modified_at: 100,
        file_size: 10,
        updated_at: 100,
    };
    let other_doc = Document {
        filepath: "/tmp/other.txt".to_string(),
        ..doc.clone()
    };

    let (doc_id, _) = upsert_document(&mut conn, &doc).unwrap();
    let (other_doc_id, _) = upsert_document(&mut conn, &other_doc).unwrap();

    conn.execute_batch(
        "INSERT INTO document_embeddings
            (document_id, model_id, chunk_type, chunk_start, chunk_end, state, embedding, created_at)
         VALUES
            (1, 7, 'filename', 0, 0, 'pending', NULL, 100),
            (1, 7, 'content', 0, 5, 'stale', NULL, 100),
            (1, 8, 'filename', 0, 0, 'embedded', X'01', 100),
            (2, 7, 'filename', 0, 0, 'embedded', X'02', 100)",
    )
    .unwrap();

    replace_embeddings_for_document_model(
        &mut conn,
        doc_id,
        7,
        &[
            EmbeddedChunk {
                chunk_type: ChunkType::Filename,
                chunk_start: 0,
                chunk_end: 0,
                embedding: vec![1, 2, 3, 4],
            },
            EmbeddedChunk {
                chunk_type: ChunkType::Content,
                chunk_start: 0,
                chunk_end: 8,
                embedding: vec![5, 6, 7, 8],
            },
        ],
        200,
    )
    .unwrap();

    let target_rows: Vec<(ChunkType, i64, i64, EmbedState, Vec<u8>, i64)> = conn
        .prepare(
            "SELECT chunk_type, chunk_start, chunk_end, state, embedding, created_at
             FROM document_embeddings
             WHERE document_id = ?1 AND model_id = ?2
             ORDER BY chunk_type, chunk_start",
        )
        .unwrap()
        .query_map(params![doc_id, 7], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();

    assert_eq!(
        target_rows,
        vec![
            (
                ChunkType::Content,
                0,
                8,
                EmbedState::Embedded,
                vec![5, 6, 7, 8],
                200
            ),
            (
                ChunkType::Filename,
                0,
                0,
                EmbedState::Embedded,
                vec![1, 2, 3, 4],
                200
            ),
        ]
    );

    let untouched_other_model: Vec<u8> = conn
        .query_row(
            "SELECT embedding FROM document_embeddings WHERE document_id = ?1 AND model_id = ?2",
            params![doc_id, 8],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(untouched_other_model, vec![1]);

    let untouched_other_document: Vec<u8> = conn
        .query_row(
            "SELECT embedding FROM document_embeddings WHERE document_id = ?1 AND model_id = ?2",
            params![other_doc_id, 7],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(untouched_other_document, vec![2]);
}

#[test]
fn seed_embed_jobs_enqueues_one_job_per_distinct_document_model_pair() {
    let mut conn = setup_documents_db();
    let doc = Document {
        id: 0,
        filepath: "/tmp/a.txt".to_string(),
        file_type: "txt".to_string(),
        doc_category: DocCategory::Resource,
        file_modified_at: 100,
        file_size: 10,
        updated_at: 100,
    };

    let (doc_id, _) = upsert_document(&mut conn, &doc).unwrap();
    conn.execute(
        "INSERT INTO document_embeddings
            (document_id, model_id, chunk_type, chunk_start, chunk_end, state, created_at)
         VALUES
            (?1, 7, ?2, 0, 0, ?3, 100),
            (?1, 7, ?4, 0, 8, ?3, 100),
            (?1, 9, ?2, 0, 0, ?5, 100)",
        params![
            doc_id,
            ChunkType::Filename,
            EmbedState::Pending,
            ChunkType::Content,
            EmbedState::Stale,
        ],
    )
    .unwrap();

    let seeded = seed_embed_jobs(&conn, 200).unwrap();
    assert_eq!(seeded, 2);

    let queued: Vec<(JobType, EmbedDocumentPayload)> = conn
        .prepare("SELECT job_type, payload FROM job_queue ORDER BY payload")
        .unwrap()
        .query_map([], |row| {
            let job_type = row.get(0)?;
            let payload: String = row.get(1)?;
            let payload = serde_json::from_str(&payload)
                .expect("EmbedDocumentPayload JSON must decode in tests");
            Ok((job_type, payload))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();

    assert_eq!(
        queued,
        vec![
            (
                JobType::EmbedDocument,
                EmbedDocumentPayload {
                    document_id: doc_id,
                    model_id: 7,
                },
            ),
            (
                JobType::EmbedDocument,
                EmbedDocumentPayload {
                    document_id: doc_id,
                    model_id: 9,
                },
            ),
        ]
    );
}

#[test]
fn seed_embed_jobs_deduplicates_repeated_calls() {
    let mut conn = setup_documents_db();
    let doc = Document {
        id: 0,
        filepath: "/tmp/a.txt".to_string(),
        file_type: "txt".to_string(),
        doc_category: DocCategory::Resource,
        file_modified_at: 100,
        file_size: 10,
        updated_at: 100,
    };

    let (doc_id, _) = upsert_document(&mut conn, &doc).unwrap();
    conn.execute(
        "INSERT INTO document_embeddings
            (document_id, model_id, chunk_type, chunk_start, chunk_end, state, created_at)
         VALUES (?1, ?2, ?3, 0, 0, ?4, 100)",
        params![doc_id, 7, ChunkType::Filename, EmbedState::Pending],
    )
    .unwrap();

    assert_eq!(seed_embed_jobs(&conn, 200).unwrap(), 1);
    assert_eq!(seed_embed_jobs(&conn, 201).unwrap(), 0);

    let job_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM job_queue WHERE job_type = ?1",
            [JobType::EmbedDocument],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(job_count, 1);
}

#[test]
fn find_document_by_path_rejects_unknown_doc_category() {
    let conn = setup_documents_db();
    conn.execute(
        "INSERT INTO documents
            (filepath, file_type, doc_category, file_modified_at, file_size, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params!["/tmp/a.txt", "txt", "not_a_real_category", 100, 10, 100],
    )
    .expect("document insert must succeed");

    let err = find_document_by_path(&conn, "/tmp/a.txt")
        .expect_err("invalid doc_category must fail to decode");
    let err_text = format!("{err:#}");

    assert!(
        err_text.contains("invalid DocCategory value"),
        "unexpected error: {err:#}"
    );
}
