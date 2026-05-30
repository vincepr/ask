# 008: Switch to PostgreSQL

## Context

The server currently uses SQLite via `rusqlite` + `r2d2` for all data storage.
This was a reasonable starting point for an embedded database, but as the
application grows toward production use with vector search, several pain points
have accumulated.

## Problem

### 1. Blocking DB calls in an async application

Every database call requires `tokio::task::spawn_blocking` to avoid blocking the
async runtime. This adds boilerplate to every handler:

```rust
tokio::task::spawn_blocking(move || {
    let conn = pool.get().map_err(|e| ...)?;
    repository::some_function(&conn, args).map_err(|e| ...)
})
.await
.map_err(panic_error)?
.map_err(db_error)?;
```

The `?` chaining is awkward (`Result<Result<T, E>, JoinError>`), and every
handler must remember to wrap DB work. This is error-prone and verbose.

### 2. Manual connection pooling

The pool (`SqliteConnectionManager`) is hand-rolled with `r2d2`. While it
works, it's additional code to maintain with no special benefit — the
`ManageConnection` trait impl, the `DbPool` type alias, and the `create_pool`
function are all infrastructure that every project must reinvent for r2d2.

### 3. No native vector search

SQLite has no built-in vector type or cosine distance operator. Embeddings are
stored as opaque `BLOB` columns. Vector search requires either:

- Loading all vectors into Rust and computing cosine in-process — does not
  scale past ~50k vectors
- Adding a SQLite extension like `sqlite-vec` — pre-v1, brute-force only,
  non-standard `MATCH` syntax
- Adding the official `Vec1` extension — real ANN, but requires compiling C
  and per-connection loading

All three add complexity that a PostgreSQL-native solution avoids.

## Proposed Solution

Replace the entire SQLite data layer with PostgreSQL + `sqlx` + `pgvector`.

### Stack change

| Component | Before | After |
|---|---|---|
| Database | SQLite (embedded file) | PostgreSQL (server) |
| Driver | `rusqlite` (sync) | `sqlx` (async-native) |
| Pooling | `r2d2` (manual `ManageConnection` impl) | Built into `sqlx::PgPool` |
| Vector search | `BLOB` + in-process cosine / SQLite extension | `pgvector` extension, `<=>` operator |
| Migration system | Custom SQL files + `ask-server/src/migrations.rs` | `sqlx::migrate!` or `Refinery` |
| Async pattern | `spawn_blocking` wrapper per call | Direct `.await` on `sqlx::query` |

### What `sqlx` gives us

`sqlx` is an async-native SQL driver with a built-in connection pool. A query
looks like this:

```rust
let doc: Document = sqlx::query_as(
    "SELECT id, filepath, file_type, doc_category, file_modified_at, file_size, updated_at
     FROM documents WHERE id = $1"
)
.bind(doc_id)
.fetch_one(&pool)
.await?;
```

No `spawn_blocking`. No `r2d2`. No manual connection management. The pool is
`PgPool` — created once, cloned cheaply, passed via Axum `State`:

```rust
let pool = PgPool::connect(&database_url).await?;
```

### What `pgvector` gives us

`pgvector` adds a `vector(n)` column type and distance operators. After
`CREATE EXTENSION vector;`:

```sql
CREATE TABLE document_embeddings (
    id          SERIAL PRIMARY KEY,
    document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    model_id    INTEGER NOT NULL REFERENCES embedding_models(id) ON DELETE CASCADE,
    chunk_type  TEXT NOT NULL,
    chunk_start INTEGER NOT NULL,
    chunk_end   INTEGER NOT NULL,
    state       TEXT NOT NULL DEFAULT 'pending',
    embedding   vector(768),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_embeddings_hnsw ON document_embeddings
    USING hnsw (embedding vector_cosine_ops)
    WHERE state = 'embedded';
```

KNN query:

```sql
SELECT document_id, 1 - (embedding <=> $1) AS similarity
FROM document_embeddings
WHERE state = 'embedded'
ORDER BY embedding <=> $1
LIMIT 20;
```

Distance operators: `<=>` (cosine), `<->` (L2), `<#>` (inner product).

```rust
use sqlx::postgres::PgPool;
use pgvector::Vector;

let query_vec = Vector::from(vec![0.1f32, 0.2, ...]);

let results: Vec<(i64, f64)> = sqlx::query_as(
    "SELECT document_id, 1 - (embedding <=> $1) AS similarity
     FROM document_embeddings
     WHERE state = 'embedded'
     ORDER BY embedding <=> $1
     LIMIT 20"
)
.bind(&query_vec)
.bind(&query_vec)
.fetch_all(&pool)
.await?;
```

#### sqlx vector awareness

`sqlx` does **not** natively understand the `vector` column type. It sees it as
a custom PostgreSQL type and passes the bytes. The `pgvector` Rust crate
provides `ToSql`/`FromSql` implementations that register the type with `sqlx`:

```rust
// Register once at startup
pgvector::register_type(&pool).await?;

// Then use Vector as a bind parameter / return type
let vec = Vector::from(slice);
sqlx::query("INSERT INTO docs (embedding) VALUES ($1)")
    .bind(&vec)
    .execute(&pool)
    .await?;
```

This is a one-time registration call, similar to how `sqlite-vec` requires
`sqlite3_auto_extension`. After that, `Vector` works transparently with
`sqlx::query` — bind it, fetch it, use `<=>` in SQL.

## Migration Path

This is a significant change. An incremental approach reduces risk:

### Phase 1: Extract repository trait

Define a `Repository` trait behind which both SQLite and PostgreSQL
implementations can live. This is pure refactoring — no new dependencies.

```rust
#[async_trait]
pub trait Repository: Send + Sync {
    async fn insert_document(&self, doc: &Document) -> Result<i64>;
    async fn mark_documents_stale(&self, doc_ids: &[i64]) -> Result<()>;
    async fn find_document_by_path(&self, path: &str) -> Result<Option<Document>>;
    async fn enqueue_job(&self, job_type: &JobType, payload: &str, now: i64) -> Result<()>;
    async fn claim_job(&self, now: i64) -> Result<Option<JobQueueEntry>>;
    async fn complete_job(&self, job_id: i64) -> Result<()>;
    async fn update_heartbeat(&self, job_id: i64, now: i64) -> Result<()>;
    // ... etc
}
```

The existing `repository.rs` module becomes a `SqliteRepository` impl. HTTP
handlers and `dispatch_job` take `&dyn Repository` instead of `&DbPool`.

### Phase 2: Implement `PgRepository`

Create a parallel `PgRepository` that implements the same trait using
`sqlx::PgPool`. Vector operations use `pgvector::Vector` and the `<=>`
operator. All other queries translate directly — PostgreSQL supports the same
SQL DDL concepts (tables, indexes, transactions, UPSERT).

### Phase 3: Flip the switch

Add a config option (`database.url`) that selects the backend. At startup,
choose `SqliteRepository` or `PgRepository` based on config. Run both in
parallel during a transition period if needed.

### Phase 4: Remove SQLite code (optional)

Once PostgreSQL is proven in production, delete `SqliteConnectionManager`,
`open_database`, `create_pool`, and all `spawn_blocking` wrappers. Remove the
`r2d2`, `rusqlite` dependencies.

## What We Lose

| Asset | Impact |
|---|---|
| **Single-file database** | SQLite's killer feature — a single `.db` file | PostgreSQL requires a server process |
| **Zero-config startup** | SQLite is `open()` | PostgreSQL needs a running server, URL, credentials |
| **Portability** | SQLite runs everywhere | PostgreSQL needs Docker / native install |
| **Test simplicity** | `:memory:` databases are instant | Tests need either `testcontainers` or a shared PG instance |
| **Current migrations** | Custom `migrations.rs` with `execute_batch` | Must rewrite as `sqlx::migrate!` SQL files |

## What We Gain

| Benefit | Detail |
|---|---|
| **No `spawn_blocking`** | All DB calls are `async fn` — direct `.await` in handlers |
| **No manual pool** | `sqlx::PgPool` is production-grade, built-in |
| **Native vector search** | `pgvector` with HNSW/IVFFlat indexes, `<=>` operator |
| **ANN at scale** | 50M+ vectors, sub-10ms queries |
| **Production ecosystem** | Backup, replication, tooling, hosted options |
| **`ON CONFLICT` instead of UPSERT hacks** | Real PostgreSQL upsert syntax |

## Required Sub-tasks

- [ ] Add `sqlx` with `postgres` feature and `pgvector` crate to dependencies
- [ ] Extract `Repository` trait in `ask-core`, refactor existing code to use it
- [ ] Implement `PgRepository` with `sqlx::PgPool`
- [ ] Add `database.url` config field alongside existing `database.path`
- [ ] Wire `PgRepository` into Axum `State` and `dispatch_job`
- [ ] Rewrite migration SQL files as PostgreSQL-compatible `sqlx::migrate!` source
- [ ] Update integration tests — either use `testcontainers` or a separate PG database
- [ ] Remove `r2d2`, `rusqlite` dependencies and all `spawn_blocking` DB wrappers
- [ ] Update `config.rs` tests
- [ ] Document PostgreSQL setup in README / `.env.example`

## Interaction with Other Features

**001 (Re-embedding)**: PostgreSQL makes re-embedding simpler — query `WHERE state = 'stale'` with `ORDER BY embedding <=>` for semantic re-embed ordering. No BLOB deserialization needed.

**002 (File watch)**: The orphan cleanup sweep becomes a simple `SELECT filepath FROM documents` with application-level fs check, same as before. No change in logic.
