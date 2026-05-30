/// SQL to create the documents table.
pub const CREATE_DOCUMENTS: &str = "
CREATE TABLE IF NOT EXISTS documents (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    filepath        TEXT    NOT NULL,
    file_type       TEXT    NOT NULL,
    doc_category    TEXT    NOT NULL,
    file_modified_at INTEGER NOT NULL,
    file_size       INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);
";

/// SQL to create the embedding_models table.
pub const CREATE_EMBEDDING_MODELS: &str = "
CREATE TABLE IF NOT EXISTS embedding_models (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL UNIQUE,
    dimensions      INTEGER NOT NULL,
    chunk_size      INTEGER NOT NULL,
    chunk_overlap   INTEGER NOT NULL,
    created_at      INTEGER NOT NULL
);
";

/// SQL to create the document_embeddings table.
pub const CREATE_DOCUMENT_EMBEDDINGS: &str = "
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
";

/// Unique index on document_embeddings — one row per (doc, model, chunk, filename).
pub const CREATE_EMBEDDINGS_UNIQUE_IDX: &str = "
CREATE UNIQUE INDEX IF NOT EXISTS idx_embeddings_unique
    ON document_embeddings (document_id, model_id, chunk_type, chunk_start);
";

/// Index for querying embeddings by document.
pub const CREATE_EMBEDDINGS_DOC_IDX: &str = "
CREATE INDEX IF NOT EXISTS idx_embeddings_document
    ON document_embeddings (document_id);
";

/// Index for querying embeddings by model + state (scan loop).
pub const CREATE_EMBEDDINGS_MODEL_STATE_IDX: &str = "
CREATE INDEX IF NOT EXISTS idx_embeddings_model_state
    ON document_embeddings (model_id, state);
";

/// All DDL statements in one batch.
pub const CREATE_ALL: &str = "
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
";
