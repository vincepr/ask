› Why did it requre a migration? Wasnt the example i showed with postgres actually working without a migration?

Good catch — the migration in my sketch was not conceptually required for “fuzzy search”; it was required for the FTS5 index table approach. I’ll separate “no schema change” options from “indexed/searchable” options.

Nachgedacht für 17s

You’re right to question that.

The Postgres example did not require a per-table migration. It used pg_trgm directly against existing columns after enabling the extension:

CREATE EXTENSION pg_trgm;

The article explicitly says pg_trgm “only needs to be activated once” and then you can use the % operator pretty much anywhere you would use LIKE. It also notes that this easy version is slower than using proper indexes/search infrastructure.

So the distinction is:

Postgres pg_trgm path

CREATE EXTENSION pg_trgm;

SELECT *
FROM files
WHERE path % 'foo/bar'
ORDER BY similarity(path, 'foo/bar') DESC
LIMIT 20;

This works on the existing files.path column. No shadow table. No copying data. Optional later migration:

CREATE INDEX files_path_trgm_idx
ON files
USING gin (path gin_trgm_ops);

Postgres’s docs describe those GIN/GiST trigram indexes as the thing that makes similarity searches fast, but the functions/operators themselves can be used without creating that index.

SQLite FTS5 trigram path

CREATE VIRTUAL TABLE file_paths_fts
USING fts5(path, tokenize='trigram');

That is why I mentioned a migration: SQLite’s built-in trigram search lives inside an FTS5 virtual table, which is effectively an index table. SQLite’s FTS5 docs show FTS tables are created with CREATE VIRTUAL TABLE, and the trigram behavior is configured through the tokenizer option.

So my earlier answer jumped straight to the “indexed SQLite-native” version. But you absolutely can do a no-migration version in SQLite too.

For example, if your file list is not huge:

SELECT
  id,
  path
FROM files
WHERE lower(path) LIKE '%' || lower(:q) || '%'
ORDER BY
  CASE
    WHEN lower(path) = lower(:q) THEN 0
    WHEN lower(path) LIKE lower(:q) || '%' THEN 1
    WHEN lower(path) LIKE '%' || lower(:q) || '%' THEN 2
    ELSE 3
  END,
  length(path)
LIMIT 20;

That gives you substring search over existing rows with zero schema changes.

For actual fuzzy scoring with no migration, do it as an app-defined SQLite function:

SELECT
  id,
  path,
  trigram_similarity(lower(path), lower(:q)) AS score
FROM files
WHERE
  lower(path) LIKE '%' || lower(:q) || '%'
  OR trigram_similarity(lower(path), lower(:q)) > 0.25
ORDER BY score DESC
LIMIT 20;

Then define trigram_similarity() in your app language. That gives you the Postgres-ish feel:

ORDER BY similarity DESC
LIMIT 20

The tradeoff is that this is a table scan unless you add an indexable prefilter. LIMIT 20 does not prevent SQLite from needing to inspect lots of rows to know which 20 are best.

So I’d frame the options like this:

Approach	Migration?	Fast?	Fuzzy?	Notes
LIKE '%query%'	No	No, unless small DB	No	Easiest
App-defined trigram_similarity(path, q)	No	No	Yes-ish	Closest to Postgres no-index style
SQLite FTS5 trigram table	Yes	Yes	Sort of	Best SQLite-native indexed option
SQLite loadable fuzzy extension	No schema migration, but extension dependency	Usually no unless paired with candidate search	Yes	Good for reranking

For your “existing full filepaths, quick and dirty, ordered limit” case, I would start with no migration:

SELECT
  id,
  path,
  trigram_similarity(lower(path), lower(:q)) AS score
FROM files
ORDER BY
  CASE
    WHEN lower(path) = lower(:q) THEN 0
    WHEN lower(path) LIKE '%' || lower(:q) || '%' THEN 1
    ELSE 2
  END,
  score DESC,
  length(path)
LIMIT 20;

Then only add the FTS5 virtual table if the scan becomes too slow.




