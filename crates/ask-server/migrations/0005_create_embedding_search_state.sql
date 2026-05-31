CREATE TABLE embedding_search_state (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    active_model_id INTEGER NOT NULL REFERENCES embedding_models(id) ON DELETE CASCADE,
    dimensions INTEGER NOT NULL CHECK (dimensions > 0),
    updated_at INTEGER NOT NULL
);
