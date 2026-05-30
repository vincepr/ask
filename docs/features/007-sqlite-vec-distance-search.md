# 007: sqlite-vec (Alex Garcia) + rusqlite

## What It Is

[`sqlite-vec`](https://github.com/asg017/sqlite-vec) is a pure-C, zero-dependency
SQLite extension for vector search. It provides `vec0` virtual tables with
`MATCH` for KNN queries. It is the successor to `sqlite-vss`.

- **License**: MIT/Apache-2.0 (fully open source)
- **Maintainer**: Alex Garcia (Mozilla Builders project, ~7.3k GitHub stars)
- **Rust crate**: [`sqlite-vec`](https://crates.io/crates/sqlite-vec) compiles + statically links the C source via `cc`
- **Works with**: rusqlite (via `sqlite3_auto_extension`)

## Vector Search SQL

```sql
CREATE VIRTUAL TABLE vec_docs USING vec0(
  embedding float[768]
);

INSERT INTO vec_docs(rowid, embedding) VALUES (?, ?);

SELECT rowid, distance
FROM vec_docs
WHERE embedding MATCH ?
ORDER BY distance
LIMIT 20;
```

Distance metrics: `cosine`, `l2` (euclidean).

## Integration with Current Stack

Works with rusqlite as-is. Register once per connection:

```rust
use sqlite_vec::sqlite3_vec_init;
use rusqlite::ffi::sqlite3_auto_extension;

unsafe {
    sqlite3_auto_extension(Some(
        std::mem::transmute::<_, unsafe extern "C" fn()>(sqlite3_vec_init as *const ()),
    ));
}
```

After registration, ALL rusqlite connections have `vec0` available — including
those from `r2d2`. No changes needed to the pooling code.

## Performance

- Brute-force (full scan) only — no ANN indexes yet (planned)
- ~68ms for 100k vectors at 768-dim (per benchmarks)
- Quantization support (binary, scalar) for faster scans
- Can handle hundreds of thousands of vectors

## Pros

- **Stays on SQLite** — zero infrastructure change
- Works with r2d2 pool unchanged
- Open source, permissive license
- Active development, Mozilla-backed
- Cross-platform (Linux, macOS, Windows, WASM, mobile)
- Pure C, no heavy dependencies

## Cons

- **Pre-v1** (breaking changes possible)
- Brute-force only (no ANN index yet)
- Requires separate virtual table — vectors live outside your main table schema
- MATCH syntax is non-standard SQL
- Insert/update requires manual `serialize_f32` calls
- Small community relative to pgvector

## Recommendation

**Best option if staying on SQLite.** Lowest migration cost — add the crate,
register the init function, write new `vec0` tables. Keep everything else as-is.
