# The Database Boundary Is Too Permissive

## Problem

The schema accepts invalid values and the Rust side silently coerces some of them into valid states.

## Evidence

- `crates/ask-server/migrations/0002_create_domain_tables.sql:1-39` stores enum-like fields such as
  `doc_category`, `chunk_type`, and `state` as unconstrained `TEXT`.
- The same migration stores numeric fields such as `dimensions`, `chunk_size`, `chunk_overlap`,
  `chunk_start`, and `chunk_end` without positivity or range checks.
- `crates/ask-core/src/repository.rs:45-46` and `crates/ask-core/src/repository.rs:106-107`
  downgrade an unknown `doc_category` to `DocCategory::Resource`.
- `crates/ask-core/src/types.rs:21-27`, `57-63`, `91-97`, and `121-125` already expose closed enums,
  but the schema does not enforce those domains.

## Why This Is Risky

- Corrupt rows are accepted and then misinterpreted as normal data.
- A bad migration or manual repair can quietly change behavior instead of failing fast.
- Future repository code will need more defensive branches because the stored state is not trustworthy.

## Simplest Stable Fix

- Add `CHECK` constraints for every enum-like text field.
- Add `CHECK` constraints for positive numeric fields and for `chunk_end >= chunk_start`.
- Decode enum values strictly and return a typed repository error instead of silently substituting
  `Resource`.
- Centralize row decoding so every query uses the same invariant checks.

## Human review:
- How does rust handle this enums coming from the db? -> as there really seems nothing premade in rusqlite
- ideally implement rusqlite::types::FromSql and ToSql for the enum seems reasonable