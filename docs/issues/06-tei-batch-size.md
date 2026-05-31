# TEI Provider Integration: Request Batch Limit and Model Name Mismatch

## Logs

```
ask-tei-qwen3-embedding  | 2026-05-31T13:35:35.770646Z ERROR openai_embed:
  text_embeddings_router::http::server:
  router/src/http/server.rs:1233:
  batch size 53 > maximum allowed batch size 32
ask-server               | 2026-05-31T13:35:35.771984Z ERROR job failed;
  leaving claim in place until stale
  job_id=632 job_type=embed_document
  error=failed to embed document 35 with model 1:
  embedding provider returned 422 Unprocessable Entity:
  {"message":"batch size 53 > maximum allowed batch size 32",
   "code":422,"type":"Validation"}
ask-server               | 2026-05-31T13:35:35.777460Z ERROR worker tick failed
  error=job 632 (embed_document):
  failed to embed document 35 with model 1:
  embedding provider returned 422 Unprocessable Entity:
  {"message":"batch size 53 > maximum allowed batch size 32",
   "code":422,"type":"Validation"}

ask-server               | 2026-05-31T13:35:40.851327Z INFO claimed job
  job_id=863 job_type=embed_document
ask-server               | 2026-05-31T13:35:40.851372Z INFO processing
  embed_document job job_id=863 document_id=41 model_id=1
ask-tei-qwen3-embedding  | 2026-05-31T13:35:40.855250Z WARN openai_embed:
  text_embeddings_router::http::server:
  router/src/http/server.rs:1161:
  The provided `model=default` has not been found, the `model` parameter
  should be provided either empty or with
  `model=onnx-community/Qwen3-Embedding-0.6B-ONNX` instead.
ask-tei-qwen3-embedding  | 2026-05-31T13:35:40.855293Z ERROR openai_embed:
  text_embeddings_router::http::server:
  router/src/http/server.rs:1233:
  batch size 106 > maximum allowed batch size 32
ask-server               | 2026-05-31T13:35:40.856260Z ERROR job failed;
  leaving claim in place until stale
  job_id=863 job_type=embed_document
  error=failed to embed document 41 with model 1:
  embedding provider returned 422 Unprocessable Entity:
  {"message":"batch size 106 > maximum allowed batch size 32",
   "code":422,"type":"Validation"}
ask-server               | 2026-05-31T13:35:40.856491Z ERROR worker tick failed
  error=job 863 (embed_document):
  failed to embed document 41 with model 1:
  embedding provider returned 422 Unprocessable Entity:
  {"message":"batch size 106 > maximum allowed batch size 32",
   "code":422,"type":"Validation"}
```

## What Happened

Two separate `embed_document` jobs failed for the same underlying reason:

- document 35 produced 53 embedding inputs
- document 41 produced 106 embedding inputs

The worker prepares every chunk for a document, collects every chunk string into
one `Vec<String>`, and sends that vector in one call to
`embedding_client.embed()` at `crates/ask-server/src/worker.rs:236-242`.

The HTTP client then sends the `embedding_models.name` field as the request
model identifier and forwards the entire input slice in one JSON request body at
`crates/ask-server/src/embeddings.rs:69-72`.

This yields two distinct provider-side problems in the same request path:

- TEI enforces a maximum of 32 inputs per embeddings request, so requests with
  53 or 106 inputs fail with `422 Unprocessable Entity`.
- The application sends `model=default`, but TEI expects either an empty model
  field or the concrete model id
  `onnx-community/Qwen3-Embedding-0.6B-ONNX`.

The batch-size failure is fatal for the job. The model-name mismatch is only a
warning today, but it proves the app is not sending a provider-valid model
identifier.

## Context

### TEI Request Limits

The TEI container is configured with `--max-concurrent-requests 64`, but the
ONNX backend for Qwen3-Embedding-0.6B caps actual concurrency at 8:

```
Backend does not support a batch size > 8
forcing max_batch_requests=8
```

That concurrency cap is separate from the failing limit in the logs above. The
observed failures come from TEI's per-request input-count limit of 32, not from
the number of requests in flight.

Additionally, `--max-batch-tokens 2048` is far below the model's 32768-token
maximum input length. TEI will silently truncate longer sequences.

### Embedding Model Registry

The `embedding_models` table stores `name` as a unique application identifier:

```sql
CREATE TABLE embedding_models (
    name TEXT NOT NULL UNIQUE,
    dimensions INTEGER NOT NULL,
    chunk_size INTEGER NOT NULL,
    chunk_overlap INTEGER NOT NULL,
    ...
);
```

At startup in `crates/ask-server/src/bin/ask-server.rs:37-57`:

- if a row exists whose `name` matches `ASK_SERVER_EMBEDDING_MODEL`, it is reused
- otherwise a new row is inserted from current config

This means:

- the default configured model name is `"default"`, not the real TEI model id
- changing dimensions or chunking env vars after first startup does not update
  the existing row
- the HTTP client forwards that same application-level name to TEI as if it were
  a provider model identifier

No startup validation checks TEI for the actual model id, dimensions, or
provider limits.

## Why This Matters

- Any document that produces more than 32 embedding chunks is a permanent
  failure under the current code path.
- Retrying those jobs without code changes will fail identically after the stale
  claim timeout.
- The system currently conflates an internal model registry key with the
  provider's real model identifier.
- The request path has no source of truth for provider limits such as maximum
  request batch size.

## Questions

- Should batch splitting happen in the worker, in `HttpEmbeddingClient`, or in a
  provider-aware abstraction that can apply limits consistently?
- Where should the max-request batch size come from: static config, TEI
  discovery metadata, or provider-specific defaults?
- Should the application store both an internal model key and a provider model
  id instead of reusing one `name` field for both concerns?
- If the configured chunking or dimensions change, should that create a new
  model row or mutate the existing row and trigger re-embedding?
- Should startup validate TEI metadata early and fail fast when the configured
  model id is not provider-valid?

---
_This document captures problems observed during exploration. Update or close when the corresponding implementation resolves the underlying concern._
