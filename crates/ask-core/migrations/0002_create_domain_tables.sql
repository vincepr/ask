CREATE TABLE IF NOT EXISTS documents (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    filepath        TEXT    NOT NULL,
    file_type       TEXT    NOT NULL,
    doc_category    TEXT    NOT NULL,
    file_modified_at INTEGER NOT NULL,
    file_size       INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS embedding_models (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL UNIQUE,
    dimensions      INTEGER NOT NULL,
    chunk_size      INTEGER NOT NULL,
    chunk_overlap   INTEGER NOT NULL,
    created_at      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS document_embeddings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id     INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    model_id        INTEGER NOT NULL REFERENCES embedding_models(id) ON DELETE CASCADE,
    chunk_type      TEXT    NOT NULL,
    chunk_start     INTEGER NOT NULL,
    chunk_end       INTEGER NOT NULL,
    state           TEXT    NOT NULL,
    embedding       BLOB,
    created_at      INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_embeddings_unique
    ON document_embeddings (document_id, model_id, chunk_type, chunk_start);

CREATE INDEX IF NOT EXISTS idx_embeddings_document
    ON document_embeddings (document_id);

CREATE INDEX IF NOT EXISTS idx_embeddings_model_state
    ON document_embeddings (model_id, state);
