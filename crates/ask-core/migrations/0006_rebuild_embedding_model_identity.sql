PRAGMA foreign_keys = OFF;

ALTER TABLE embedding_models RENAME TO embedding_models_old;
ALTER TABLE document_embeddings RENAME TO document_embeddings_old;
ALTER TABLE embedding_search_state RENAME TO embedding_search_state_old;

DROP INDEX IF EXISTS idx_embeddings_unique;
DROP INDEX IF EXISTS idx_embeddings_document;
DROP INDEX IF EXISTS idx_embeddings_model_state;

CREATE TABLE embedding_models (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL,
    dimensions      INTEGER NOT NULL,
    chunk_size      INTEGER NOT NULL,
    chunk_overlap   INTEGER NOT NULL,
    created_at      INTEGER NOT NULL
);

CREATE UNIQUE INDEX idx_embedding_models_identity_unique
    ON embedding_models (name, dimensions, chunk_size, chunk_overlap);

CREATE TABLE document_embeddings (
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

CREATE UNIQUE INDEX idx_embeddings_unique
    ON document_embeddings (document_id, model_id, chunk_type, chunk_start);

CREATE INDEX idx_embeddings_document
    ON document_embeddings (document_id);

CREATE INDEX idx_embeddings_model_state
    ON document_embeddings (model_id, state);

CREATE TABLE embedding_search_state (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    active_model_id INTEGER NOT NULL REFERENCES embedding_models(id) ON DELETE CASCADE,
    dimensions INTEGER NOT NULL CHECK (dimensions > 0),
    updated_at INTEGER NOT NULL
);

INSERT INTO embedding_models (id, name, dimensions, chunk_size, chunk_overlap, created_at)
SELECT id, name, dimensions, chunk_size, chunk_overlap, created_at
FROM embedding_models_old;

INSERT INTO document_embeddings
    (id, document_id, model_id, chunk_type, chunk_start, chunk_end, state, embedding, created_at)
SELECT
    id,
    document_id,
    model_id,
    chunk_type,
    chunk_start,
    chunk_end,
    state,
    embedding,
    created_at
FROM document_embeddings_old;

INSERT INTO embedding_search_state (singleton_id, active_model_id, dimensions, updated_at)
SELECT singleton_id, active_model_id, dimensions, updated_at
FROM embedding_search_state_old;

DROP TABLE embedding_search_state_old;
DROP TABLE document_embeddings_old;
DROP TABLE embedding_models_old;

PRAGMA foreign_keys = ON;
