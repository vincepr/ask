use ask_core::types::{ChunkType, DocCategory, EmbedState, JobType};
use rusqlite::Connection;

#[test]
fn sql_round_trip_decodes_every_persisted_enum() {
    let conn = Connection::open_in_memory().expect("in-memory database must open");

    let doc_category: DocCategory = conn
        .query_row("SELECT ?1", [DocCategory::KnowledgeFile], |row| row.get(0))
        .expect("doc category round-trip must succeed");
    let embed_state: EmbedState = conn
        .query_row("SELECT ?1", [EmbedState::Embedded], |row| row.get(0))
        .expect("embed state round-trip must succeed");
    let chunk_type: ChunkType = conn
        .query_row("SELECT ?1", [ChunkType::Filename], |row| row.get(0))
        .expect("chunk type round-trip must succeed");
    let job_type: JobType = conn
        .query_row("SELECT ?1", [JobType::EmbedDocument], |row| row.get(0))
        .expect("job type round-trip must succeed");

    assert_eq!(doc_category, DocCategory::KnowledgeFile);
    assert_eq!(embed_state, EmbedState::Embedded);
    assert_eq!(chunk_type, ChunkType::Filename);
    assert_eq!(job_type, JobType::EmbedDocument);
}

#[test]
fn sql_decode_rejects_unknown_enum_values() {
    let conn = Connection::open_in_memory().expect("in-memory database must open");

    let err = conn
        .query_row("SELECT 'unknown_state'", [], |row| {
            row.get::<_, EmbedState>(0)
        })
        .expect_err("invalid embed state must fail to decode");

    assert!(
        err.to_string().contains("invalid EmbedState value"),
        "unexpected error: {err}"
    );
}
