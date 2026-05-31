# Embedding Model Contract Implementation Plan

## Goal

Make the embedding model contract minimal, explicit, and immutable.

The current design is ambiguous because `embedding_models.name` is used as both:

- the application's lookup key
- the `model` value sent to the embedding provider

That ambiguity causes two failures:

- an invalid request model like `default` can be sent to the provider
- an existing DB row can be reused even when the real embedding identity has
  changed

This feature should fix that without adding a richer provider-capabilities layer
 or provider-specific abstractions.

## Design Decision

Use one model string and give it exactly one meaning:

- `ASK_SERVER_EMBEDDING_MODEL` is the exact string sent in the embedding
  request body as `model`

Do not support local aliases such as `default`.

The persisted embedding identity should be the immutable tuple:

- `model`
- `dimensions`
- `chunk_size`
- `chunk_overlap`

If any of those values change, that is a new embedding generation and requires
new persisted rows.

The following are not part of embedding identity:

- `base_url`
- `auth_token`

Those are transport details. Changing them should not require re-embedding.

## Why This Is the Minimal Correct Design

This keeps the system slim:

- no provider capability registry
- no health endpoint metadata
- no special-case support for empty model handling
- no logical-model-key versus provider-model-id split

But it still preserves correctness:

- the app always sends the configured model string
- stored embeddings are only reused when the full embedding identity matches
- chunk-setting changes cannot silently reuse old embeddings

## Scope

This feature should only solve contract identity and model-row reuse.

It should not include:

- provider readiness logic
- request batching
- retry redesign
- search endpoint changes
- document path changes

Those later features should build on this simpler contract.

## Current Problems in Code

Startup currently reuses an existing model row by `name` only in:

- [ask-server.rs](/home/vince/ask/crates/ask-server/src/bin/ask-server.rs:35)

The current config default still allows a synthetic model name:

- [config.rs](/home/vince/ask/crates/ask-server/src/config.rs:19)

The HTTP client currently sends the persisted model name directly in requests:

- [embeddings.rs](/home/vince/ask/crates/ask-server/src/embeddings.rs:69)

The table currently stores only:

- `name`
- `dimensions`
- `chunk_size`
- `chunk_overlap`

with uniqueness on `name` alone:

- [0002_create_domain_tables.sql](/home/vince/ask/crates/ask-server/migrations/0002_create_domain_tables.sql:11)

That uniqueness rule is the core bug.

## Target Behavior

At startup:

1. Load the configured embedding identity from env:
   - model
   - dimensions
   - chunk size
   - chunk overlap
2. Look up an existing `embedding_models` row by exact identity match
3. If found, reuse it
4. If not found, insert a new row
5. If a new row was inserted, trigger normal backfill for that row

At request time:

- send the configured model string as the request `model`

At restart after config changes:

- if only `base_url` or `auth_token` changed, reuse the same model row
- if `model`, `dimensions`, `chunk_size`, or `chunk_overlap` changed, create a
  new model row and re-embed against it

## Schema Plan

Keep the table concept but change its semantics.

### Desired table meaning

Each row in `embedding_models` represents one immutable embedding identity.

### Desired columns

Keep:

- `id`
- `name`
- `dimensions`
- `chunk_size`
- `chunk_overlap`
- `created_at`

But change the meaning of `name`:

- `name` must be the exact request model string

### Desired uniqueness

Replace unique-on-name with uniqueness on the full identity:

- `(name, dimensions, chunk_size, chunk_overlap)`

This avoids silent reuse when chunking or dimensions change.

## Migration Plan

Add a migration that changes uniqueness semantics.

Recommended approach:

1. create a replacement `embedding_models_v2` table with the same columns
2. add a composite unique constraint on:
   - `name`
   - `dimensions`
   - `chunk_size`
   - `chunk_overlap`
3. copy data from old `embedding_models`
4. drop the old table
5. rename `embedding_models_v2` to `embedding_models`
6. recreate any dependent indexes if needed

Important constraint:

- do not try to preserve the old `name` uniqueness rule
- do not rewrite old rows to guessed provider values

Legacy rows with `name='default'` may remain in old databases. They simply
should not match the new configured identity unless the user explicitly
configures `ASK_SERVER_EMBEDDING_MODEL=default`, which they should not.

That is acceptable. The new startup logic should insert a new correct row.

## Config Plan

In [config.rs](/home/vince/ask/crates/ask-server/src/config.rs):

### Change the meaning of `ASK_SERVER_EMBEDDING_MODEL`

It must now mean:

- exact request model string for the embedding server

### Remove the synthetic default

Do not default this to `default`.

Preferred approach:

- require `ASK_SERVER_EMBEDDING_MODEL` to be set explicitly

If you want a default for convenience, it should be a real model string valid
for the expected local setup, not a placeholder alias.

Given the repo's goal of correctness over magic, requiring the env var is
cleaner.

### Keep existing transport config

Keep:

- `ASK_SERVER_EMBEDDING_BASE_URL`
- `ASK_SERVER_EMBEDDING_AUTH_TOKEN`

unchanged

### Optional future config

Later features may add:

- `ASK_SERVER_EMBEDDING_MAX_BATCH_SIZE`

but not in this feature.

## Rust Type Plan

The repo may keep using `EmbeddingModel` as the persisted row type, but the code
should introduce a small explicit config-side identity type to avoid passing
loose fields around.

Suggested shape:

```rust
pub struct EmbeddingIdentity {
    pub name: String,
    pub dimensions: i64,
    pub chunk_size: i64,
    pub chunk_overlap: i64,
}
```

This type should be:

- built from config
- used for exact-match lookup
- used for insertion

It should not include:

- base URL
- auth token

## Repository Plan

In [repository.rs](/home/vince/ask/crates/ask-core/src/repository.rs):

### Add exact-match lookup

Add a function like:

```rust
pub fn find_model_by_identity(
    conn: &Connection,
    identity: &EmbeddingIdentity,
) -> Result<Option<EmbeddingModel>>
```

This lookup should match on:

- `name`
- `dimensions`
- `chunk_size`
- `chunk_overlap`

### Stop using name-only lookup for startup correctness

`find_model_by_name()` may remain temporarily if other code still uses it, but
startup should stop depending on it.

### Add identity-based insert helper

Either:

- keep `insert_model()` but build the row from `EmbeddingIdentity`

or:

- add `insert_model_from_identity()`

The important part is that insertion semantics follow the exact identity.

## Startup Plan

In [ask-server.rs](/home/vince/ask/crates/ask-server/src/bin/ask-server.rs):

Replace this behavior:

- `find_model_by_name()`
- otherwise insert

With this behavior:

1. build `EmbeddingIdentity` from config
2. `find_model_by_identity()`
3. reuse exact match if present
4. otherwise insert a new row
5. call `backfill_pending_for_model()` only for newly inserted rows

This makes config drift explicit and safe.

## HTTP Request Plan

In [embeddings.rs](/home/vince/ask/crates/ask-server/src/embeddings.rs):

Keep the request format the same, but make sure:

- `request.model` is always the exact configured model string stored in the
  persisted row

Once the config default is removed and startup uses exact identity rows, the
current request code becomes acceptable again because `model.name` will now mean
the right thing.

## Backward Compatibility Plan

Old databases may contain:

- legacy rows with `name='default'`
- rows whose uniqueness was based on name only

Do not mutate those rows in place.

Preferred behavior:

- leave legacy rows as historical data
- insert a new correct row for the configured identity
- backfill embeddings under the new row

This is simpler and safer than trying to patch old rows automatically.

## Failure Semantics

This feature should make failures clearer.

### Misconfigured model string

If the configured model string is invalid for the provider:

- startup may still succeed
- the first provider request should fail clearly

That is acceptable for this feature.

Do not add provider probing or metadata fetches yet.

### Wrong dimensions

If configured dimensions do not match provider output:

- the embed call should fail
- no old compatible row should be silently reused

That is the key correctness guarantee.

## Detailed Step-by-Step Task List

1. Update `Config` so `ASK_SERVER_EMBEDDING_MODEL` is required or defaults only
   to a real valid model string, never `default`.
2. Introduce `EmbeddingIdentity` in Rust.
3. Add a new migration changing `embedding_models` uniqueness from `name` to
   `(name, dimensions, chunk_size, chunk_overlap)`.
4. Add repository lookup by exact identity.
5. Update startup to resolve models by exact identity instead of name only.
6. Insert a new model row when any identity field changes.
7. Reuse normal backfill only for newly inserted identities.
8. Keep transport settings out of model identity logic.
9. Confirm HTTP embedding requests now send the actual configured model string.

## Test Plan

### Config tests

- loading fails when `ASK_SERVER_EMBEDDING_MODEL` is missing, if the required
  approach is chosen
- loading succeeds with a real configured model string
- transport config changes do not affect identity construction

### Repository tests

- exact same `(name, dimensions, chunk_size, chunk_overlap)` reuses the same row
- same `name` with different dimensions inserts a new row
- same `name` with different chunk size inserts a new row
- same `name` with different chunk overlap inserts a new row

### Startup/integration tests

- fresh DB creates one model row and backfills work
- restart with same identity reuses the same row
- restart with changed dimensions creates a new row
- restart with changed chunk size creates a new row
- restart with changed chunk overlap creates a new row

### Regression tests

- provider request does not send `model=default`
- an old legacy row named `default` does not block insertion of a new correct
  row for the configured model string

## Non-Goals

Do not include any of the following here:

- dynamic provider metadata discovery
- health checks
- retry classification
- request batching
- queue redesign

Those are follow-up features and should remain separate.

## Acceptance Criteria

This feature is done when:

- the configured embedding model string is always a real request model string
- startup reuses persisted rows only on exact identity match
- config changes to model or chunking create a new model row
- transport-only changes do not create a new model row
- existing embeddings cannot be silently reused under an incompatible contract
