use ask_core::migrations::apply_pending_migrations;
use rusqlite::Connection;

#[test]
fn applies_migrations_only_once() {
    let mut connection = Connection::open_in_memory().expect("in-memory database must open");

    let first_run =
        apply_pending_migrations(&mut connection).expect("first migration run must succeed");
    let second_run =
        apply_pending_migrations(&mut connection).expect("second migration run must succeed");

    let applied_total: i64 = connection
        .query_row("SELECT COUNT(*) FROM migrations", [], |row| row.get(0))
        .expect("migration count query must succeed");

    assert_eq!(first_run, 7);
    assert_eq!(second_run, 0);
    assert_eq!(applied_total, 7);
}

#[test]
fn stores_required_actions_as_null_when_not_needed() {
    let mut connection = Connection::open_in_memory().expect("in-memory database must open");

    apply_pending_migrations(&mut connection).expect("migration run must succeed");

    let required_actions: Option<String> = connection
        .query_row(
            "SELECT required_actions FROM migrations WHERE version = 1",
            [],
            |row| row.get(0),
        )
        .expect("required_actions query must succeed");

    assert_eq!(required_actions, None);
}

#[test]
fn embedding_model_identity_allows_same_name_across_different_chunking() {
    let mut connection = Connection::open_in_memory().expect("in-memory database must open");
    apply_pending_migrations(&mut connection).expect("migration run must succeed");

    connection
        .execute(
            "INSERT INTO embedding_models (name, dimensions, chunk_size, chunk_overlap, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            ("shared-model", 1024_i64, 512_i64, 0_i64, 1_i64),
        )
        .expect("first embedding model insert must succeed");

    connection
        .execute(
            "INSERT INTO embedding_models (name, dimensions, chunk_size, chunk_overlap, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            ("shared-model", 1024_i64, 256_i64, 0_i64, 2_i64),
        )
        .expect("second embedding model insert must succeed");
}

#[test]
fn embedding_model_identity_rejects_exact_duplicates() {
    let mut connection = Connection::open_in_memory().expect("in-memory database must open");
    apply_pending_migrations(&mut connection).expect("migration run must succeed");

    connection
        .execute(
            "INSERT INTO embedding_models (name, dimensions, chunk_size, chunk_overlap, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            ("shared-model", 1024_i64, 512_i64, 0_i64, 1_i64),
        )
        .expect("first embedding model insert must succeed");

    let error = connection
        .execute(
            "INSERT INTO embedding_models (name, dimensions, chunk_size, chunk_overlap, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            ("shared-model", 1024_i64, 512_i64, 0_i64, 2_i64),
        )
        .expect_err("duplicate embedding identity must fail");

    assert!(
        error.to_string().contains("UNIQUE"),
        "unexpected error: {error}"
    );
}

#[test]
fn filepath_search_migration_backfills_existing_documents() {
    let mut connection = Connection::open_in_memory().expect("in-memory database must open");
    connection
        .execute_batch(
            "CREATE TABLE migrations (
                version INTEGER PRIMARY KEY NOT NULL,
                required_actions TEXT NULL
            );",
        )
        .unwrap();

    connection
        .execute_batch(include_str!(
            "../migrations/0001_bootstrap_migration_system.sql"
        ))
        .unwrap();
    connection
        .execute(
            "INSERT INTO migrations (version, required_actions) VALUES (1, NULL)",
            [],
        )
        .unwrap();
    connection
        .execute_batch(include_str!("../migrations/0002_create_domain_tables.sql"))
        .unwrap();
    connection
        .execute(
            "INSERT INTO migrations (version, required_actions) VALUES (2, NULL)",
            [],
        )
        .unwrap();
    connection
        .execute_batch(include_str!("../migrations/0003_create_job_queue.sql"))
        .unwrap();
    connection
        .execute(
            "INSERT INTO migrations (version, required_actions) VALUES (3, NULL)",
            [],
        )
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../migrations/0004_simplify_job_queue_claim.sql"
        ))
        .unwrap();
    connection
        .execute(
            "INSERT INTO migrations (version, required_actions) VALUES (4, NULL)",
            [],
        )
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../migrations/0005_create_embedding_search_state.sql"
        ))
        .unwrap();
    connection
        .execute(
            "INSERT INTO migrations (version, required_actions) VALUES (5, NULL)",
            [],
        )
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../migrations/0006_rebuild_embedding_model_identity.sql"
        ))
        .unwrap();
    connection
        .execute(
            "INSERT INTO migrations (version, required_actions) VALUES (6, NULL)",
            [],
        )
        .unwrap();

    connection
        .execute(
            "INSERT INTO documents
                (filepath, file_type, doc_category, file_modified_at, file_size, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            (
                "C:\\Work\\src\\MyImplementation.cs",
                "cs",
                "resource",
                1_i64,
                10_i64,
                1_i64,
            ),
        )
        .unwrap();

    let applied = apply_pending_migrations(&mut connection).expect("migration run must succeed");
    assert_eq!(applied, 1);

    let row: (String, String) = connection
        .query_row(
            "SELECT normalized_filepath, normalized_basename
             FROM document_filepath_search
             WHERE document_id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();

    assert_eq!(row.0, "c:/work/src/myimplementation.cs");
    assert_eq!(row.1, "myimplementation.cs");
}
