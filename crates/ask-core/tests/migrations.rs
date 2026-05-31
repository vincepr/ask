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

    assert_eq!(first_run, 6);
    assert_eq!(second_run, 0);
    assert_eq!(applied_total, 6);
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
