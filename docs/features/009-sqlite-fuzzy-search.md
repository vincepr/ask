# Implement a quick fuzzy search directly over the file paths:
- link that blogpost in the comments as motivation https://rdegges.com/2013/easy-fuzzy-text-searching-with-postgresql/ (link important if we ever switch to postgres)
- The search endpoint gets an optional parameter to use that fuzzy filepath mode instead
- Can be used if api is down. If api is down (no embeddings can be created for the search -> return error description with that parameter)
- Write a bunch of tests. Questions: for example: is MyImplementation.cs always over MyImplementationTets.cs in the score, if searched for "MyImplementation"? (not super important but would be nice if can be done performant)

## Can i implement a fuzzysearch directly in sqlite, similiar to: https://rdegges.com/2013/easy-fuzzy-text-searching-with-postgresql/

Yes. You can do this directly in SQLite, and there are already a few good paths people have written about.

The closest built-in equivalent to `pg_trgm` is **SQLite FTS5 with the trigram tokenizer**. SQLite’s docs describe the trigram tokenizer as indexing each contiguous 3-character sequence and enabling general substring matching, not just word-token matching. It also supports indexed `LIKE`/`GLOB` in some configurations. ([SQLite][1]) SQLite added FTS5 trigram support in SQLite **3.34.0** in December 2020. ([SQLite][2])

The linked Postgres article is essentially: “turn on `pg_trgm`, use `%` as a fuzzy/similarity-ish operator, and optionally accept that it is good enough for small-to-mid datasets.” ([rdegges.com][3]) In SQLite, the nearest “quick and dirty” version is:

```sql
CREATE VIRTUAL TABLE file_paths_fts
USING fts5(path, tokenize='trigram');

INSERT INTO file_paths_fts(rowid, path)
SELECT id, lower(replace(path, '\', '/'))
FROM files;
```

Then query:

```sql
SELECT
  f.id,
  f.path,
  bm25(file_paths_fts) AS score
FROM file_paths_fts
JOIN files f ON f.id = file_paths_fts.rowid
WHERE file_paths_fts MATCH :match
ORDER BY score
LIMIT :limit;
```

For `:match`, start with a quoted normalized query from your app code:

```python
def fts5_quote(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'

match = fts5_quote(query.lower().replace("\\", "/"))
```

SQLite’s `bm25()` returns lower scores for better matches, so `ORDER BY score` is the correct direction. ([SQLite][1])

For file paths, I would probably index a few columns, not just the whole path:

```sql
CREATE VIRTUAL TABLE file_paths_fts
USING fts5(
  path,
  basename,
  dirname,
  tokenize='trigram'
);
```

Then weight basename higher:

```sql
SELECT
  f.id,
  f.path,
  bm25(file_paths_fts, 1.0, 5.0, 0.5) AS score
FROM file_paths_fts
JOIN files f ON f.id = file_paths_fts.rowid
WHERE file_paths_fts MATCH :match
ORDER BY
  CASE
    WHEN lower(f.path) = :q THEN 0
    WHEN lower(f.path) LIKE '%' || :q || '%' THEN 1
    ELSE 2
  END,
  score
LIMIT 20;
```

That gives you a practical order:

1. exact full-path-ish matches
2. literal substring matches
3. trigram-ish matches ranked by FTS

The caveat: SQLite FTS5 trigram is not exactly the same as Postgres `pg_trgm` similarity. It is very good for “I remember part of the path/name” searches like `invoice`, `src/auth`, `migration`, `foo_test`. It is weaker for true typos like `retreive` vs `retrieve`, especially if the typo changes many 3-character chunks.

For more typo tolerance, I would do a two-pass approach:

```python
def trigrams(s: str) -> list[str]:
    s = "  " + s.lower().replace("\\", "/") + "  "
    return sorted({s[i:i+3] for i in range(len(s) - 2)})

match = " OR ".join(fts5_quote(g) for g in trigrams(query))
```

Then run the same FTS query and re-rank the top 100–500 candidates in your application with a better scorer, for example:

```python
score =
    3.0 * basename_similarity(query, basename)
  + 1.5 * path_substring_bonus(query, path)
  + 1.0 * trigram_jaccard(query, path)
  - 0.2 * path_depth_penalty(path)
```

That hybrid tends to feel much better for file paths than relying on database ranking alone.

There are also existing pieces worth knowing about:

* **SQLite’s official `spellfix1` extension** can search a vocabulary for close misspellings and exposes edit-distance-ish scoring, but it is a loadable extension and not part of every standard SQLite build. ([SQLite][4])
* **sqlean-fuzzy** provides SQLite functions for Levenshtein, Damerau-Levenshtein, Jaro-Winkler, and related fuzzy matching helpers. Good for re-ranking candidates, not ideal for full-table scanning. ([GitHub][5])
* **sqlite-better-trigram** is a custom FTS5 tokenizer that improves some behavior around short words and word boundaries compared with SQLite’s built-in trigram tokenizer. ([GitHub][6])
* Dominik Tarnowski wrote a practical “SQLite Fuzzy Search” post combining FTS5 trigram search with `spellfix1` for misspellings. ([tdom.dev][7])
* Simon Willison’s `sqlite-fts5-trigram` repo is now mostly historical; its README says it became obsolete when SQLite shipped trigram support in 3.34.0. ([GitHub][8])

My recommendation for your use case:

Use **FTS5 trigram as the candidate generator**, then optionally **re-rank in app code**. For filepaths this is probably the sweet spot: simple, SQLite-native, fast enough, and no extra search service.

The minimal version I would actually ship first:

```sql
CREATE VIRTUAL TABLE file_paths_fts
USING fts5(path, tokenize='trigram');

INSERT INTO file_paths_fts(rowid, path)
SELECT id, lower(replace(path, '\', '/'))
FROM files;
```

Then:

```sql
SELECT f.id, f.path
FROM file_paths_fts
JOIN files f ON f.id = file_paths_fts.rowid
WHERE file_paths_fts MATCH :match
ORDER BY bm25(file_paths_fts)
LIMIT 25;
```

And later, only if it feels too strict for typos, switch `:match` from a quoted phrase to an `OR` of query trigrams and re-rank the returned candidates yourself.

[1]: https://sqlite.org/fts5.html "SQLite FTS5 Extension"
[2]: https://sqlite.org/changes.html "Release History Of SQLite"
[3]: https://www.rdegges.com/2013/easy-fuzzy-text-searching-with-postgresql/ "Randall Degges - Easy Fuzzy Text Searching With PostgreSQL"
[4]: https://sqlite.org/spellfix1.html "The Spellfix1 Virtual Table"
[5]: https://github.com/nalgeon/sqlean/blob/main/docs/fuzzy.md "sqlean/docs/fuzzy.md at main · nalgeon/sqlean · GitHub"
[6]: https://github.com/streetwriters/sqlite-better-trigram "GitHub - streetwriters/sqlite-better-trigram: A (better) trigram tokenizer for SQLite3 FTS5 that also handles words less than 3 characters in length. · GitHub"
[7]: https://tdom.dev/sqlite-fuzzy-search.html "SQLite Fuzzy Search"
[8]: https://github.com/simonw/sqlite-fts5-trigram/?utm_source=chatgpt.com "simonw/sqlite-fts5-trigram"
