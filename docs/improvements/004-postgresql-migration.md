# 004: PostgreSQL Migration Assessment

## Context

The current server uses:

- SQLite
- `rusqlite`
- `r2d2`
- `spawn_blocking` around database work

Moving to PostgreSQL plus `sqlx` and `pgvector` is technically feasible, but it
is a strategic migration, not a small feature.

## What Was Wrong in the Earlier Draft

1. The draft mixed real benefits with inaccurate details.
2. For `sqlx`, the `pgvector` Rust crate already supports `Vector` directly; the
   draft's custom `register_type(&pool)` call was incorrect.
3. `ON CONFLICT` is not a PostgreSQL-only advantage; SQLite already supports it.
4. Ordering stale rows by vector distance is unrelated to re-embedding.
5. Maintaining SQLite and PostgreSQL backends behind a shared trait would be a
   large extra project on top of the migration itself.

## Feasibility

Feasible, but high-cost.

This is a good option if the project wants:

- async-native database access
- mature hosted database operations
- `pgvector` for indexed vector search

It is **not** required for the first usable search release if feature 008 is
chosen instead.

## Recommended Scope

Treat PostgreSQL as a **cutover** project, not as a dual-backend abstraction
exercise.

The repository layer is still small and SQLite-specific today. Rewriting it
once for PostgreSQL is likely simpler than carrying two storage backends plus a
trait hierarchy through a long transition.

## Practical Migration Plan

1. Add PostgreSQL configuration and bootstrapping.
2. Rewrite migrations for PostgreSQL and enable `CREATE EXTENSION vector`.
3. Rewrite `ask-core::repository` against `sqlx::PgPool`.
4. Replace `spawn_blocking` database wrappers in HTTP handlers and worker code.
5. Represent embeddings with `pgvector::Vector`.
6. Add a one-time data migration path from SQLite to PostgreSQL.
7. Move tests to a PostgreSQL-backed setup such as `testcontainers` or a
   dedicated test database.

## Required Sub-tasks

- [ ] Add `sqlx` with PostgreSQL support
- [ ] Add `pgvector` with `sqlx` support
- [ ] Add PostgreSQL config fields and startup wiring
- [ ] Port schema and migrations to PostgreSQL
- [ ] Rewrite repository functions and callers
- [ ] Remove per-call `spawn_blocking` DB wrappers after the rewrite
- [ ] Add data-migration tooling for existing SQLite data
- [ ] Rewrite integration tests for PostgreSQL
- [ ] Document operational setup in README and `.env.example`

## Constraints

- PostgreSQL adds real operational overhead compared with a single SQLite file
- test setup becomes heavier
- this should be treated as an alternative to feature 008 in the near term, not
  something pursued in parallel with it

## Acceptance Criteria

1. The server runs fully on PostgreSQL without SQLite in the request path.
2. Vector search uses `pgvector` rather than application-side BLOB scanning.
3. HTTP handlers and workers perform database calls without `spawn_blocking`.
4. Existing SQLite data can be migrated with a documented path.


## HUMAN REMARK
- After switching to postgres write a new docs/feature:
- support a out of the box fuzzy search for alternative searching. (when the api is unavialiable aswell as toggleable via a mode!)
- https://rdegges.com/2013/easy-fuzzy-text-searching-with-postgresql/