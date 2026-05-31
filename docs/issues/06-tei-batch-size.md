# TEI Batch Size, Concurrency, and Model Registry

## Batch / Concurrency Configuration

The TEI container is configured with `--max-concurrent-requests 64`, but the
ONNX backend for Qwen3-Embedding-0.6B caps actual concurrency at 8:

```
Backend does not support a batch size > 8
forcing max_batch_requests=8
```

Additionally, `--max-batch-tokens 2048` is far below the model's 32768-token
maximum input length. TEI will silently truncate longer sequences.

## Embedding Model Registry

The `embedding_models` table contains a row with `name='default'` — not the
actual model identifier. Relevant details:

**Schema** (from `0002_create_domain_tables.sql`):
```sql
CREATE TABLE embedding_models (
    name TEXT NOT NULL UNIQUE,
    dimensions INTEGER NOT NULL,
    chunk_size INTEGER NOT NULL,
    chunk_overlap INTEGER NOT NULL,
    ...
);
```

The UNIQUE constraint is on `name` alone. At startup (`ask-server.rs:35-57`):
- If a row with `name` matching `ASK_SERVER_EMBEDDING_MODEL` exists, it is used
  as-is
- If not, a new row is inserted with the current config values

This means:
- The model name `"default"` is hardcoded (`config.rs:19`) — not the actual
  TEI model name (`Qwen3-Embedding-0.6B`)
- Changing `dimensions`, `chunk_size`, or `chunk_overlap` in env vars after the
  first startup has no effect — the existing row is always reused
- The `model` field in the embedding HTTP request
  (`embeddings.rs:69-72`) sends this meaningless name to TEI

No startup validation exists: the server never queries TEI to confirm the
actual model name, output dimensions, or supported features.

## Questions

**Batch / Concurrency:**
- Should `--max-concurrent-requests` track the actual backend limit, or is it
  harmless to over-provision and let the provider queue internally?
- Is the 2048-token truncation acceptable for the expected document types
  (code, configs), or should `--max-batch-tokens` be raised to match the model's
  full window?
- Should `ASK_SERVER_EMBEDDING_CHUNK_SIZE` be tuned relative to
  `--max-batch-tokens`, or are these independent concerns?

**Model Registry:**
- What is the purpose of the `embedding_models` table? Does it describe the
  provider's capabilities, or is it purely for the application's chunking
  parameters?
- Should the model name reflect the actual provider model (e.g., discovered
  from TEI `/info`), or is an opaque identifier fine as long as chunking
  parameters are correct?
- When parameters change, should a new row be created (with a unique constraint
  on `(name, dimensions, chunk_size, chunk_overlap)`), or should the row be
  updated in-place? Each choice has different implications for existing
  embeddings and backfill semantics.
- Do we need the table at all for single-model deployments? Could chunking
  parameters live in config and be passed directly to the worker, removing the
  indirection and the stale-row problem?
- If multiple models are a future concern, what minimal schema supports that
  without over-engineering today?
