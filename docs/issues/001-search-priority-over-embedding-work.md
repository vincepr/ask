# Search Priority Over Embedding Work

## Problem

Search requests can become slow or unusable while embedding work is active.
Search is the interactive path, so it should not be starved by background
embedding jobs.

## Goal

Make `/search` responsive even when the embedding queue is busy.

## Scope

- Prioritize request-time search work over background embedding work.
- Evaluate whether longer search timeouts are sufficient before adding queue or
  scheduler complexity.
- Keep the worker model simple unless measurements show it is the bottleneck.

## Investigation

- Measure search latency while embedding jobs are running.
- Check whether SQLite connection-pool contention is the limiting factor.
- Check whether embedding-provider calls from search and workers compete for the
  same provider capacity.
- Check whether worker count, pool size, or request timeout configuration gives
  enough control without new abstractions.

## Acceptance Criteria

- Search requests remain predictably usable during active ingestion and
  embedding.
- The solution does not add a job scheduler unless simpler configuration changes
  are insufficient.
