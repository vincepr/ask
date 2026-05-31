CREATE TABLE document_filepath_search (
    document_id          INTEGER PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,
    normalized_filepath  TEXT NOT NULL,
    normalized_basename  TEXT NOT NULL
);

CREATE VIRTUAL TABLE document_filepath_search_fts
USING fts5(
    normalized_filepath,
    normalized_basename,
    tokenize = 'trigram',
    content = 'document_filepath_search',
    content_rowid = 'document_id'
);

WITH normalized AS (
    SELECT
        id AS document_id,
        lower(replace(filepath, '\', '/')) AS normalized_filepath
    FROM documents
),
split(document_id, normalized_filepath, segment, remainder) AS (
    SELECT document_id, normalized_filepath, NULL, normalized_filepath
    FROM normalized
    UNION ALL
    SELECT
        document_id,
        normalized_filepath,
        CASE
            WHEN instr(remainder, '/') = 0 THEN remainder
            ELSE substr(remainder, 1, instr(remainder, '/') - 1)
        END,
        CASE
            WHEN instr(remainder, '/') = 0 THEN ''
            ELSE substr(remainder, instr(remainder, '/') + 1)
        END
    FROM split
    WHERE remainder <> ''
)
INSERT INTO document_filepath_search (document_id, normalized_filepath, normalized_basename)
SELECT
    document_id,
    normalized_filepath,
    segment
FROM split
WHERE remainder = ''
  AND segment IS NOT NULL;

INSERT INTO document_filepath_search_fts(document_filepath_search_fts)
VALUES ('rebuild');
