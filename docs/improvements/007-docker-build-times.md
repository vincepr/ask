# 007: Docker Build Times

## Current State

A `docker compose build ask-server` currently takes ~267 seconds from scratch. Most of that is `cargo build --locked --release -p ask-server --bin ask-server` in the builder stage.

When the project started, builds were fast — a few seconds to a minute. Build times have regressed significantly as dependencies accumulated (rusqlite with bundled C SQLite, sqlite-vec with bundled C, axum, tower, serde, r2d2, tracing, etc.).

The common workflow — edit code, `docker compose up --build`, wait, repeat — is now cumbersome.

## Known Contributing Factors (Not Exhaustive)

- Full dependency tree must be compiled on every build; `target/` is ephemeral per Docker layer and incremental compilation is lost.
- `rusqlite` with the `bundled` feature compiles SQLite from C source.
- `sqlite-vec` compiles its own C source.
- The workspace layout (two crates) adds a small overhead for dependency resolution.
- `cargo build --release` is inherently slower than `--debug` due to optimization passes.
- It is unclear whether all current dependencies are still needed, or whether some could be swapped out without losing functionality.

## Investigation Needed

The goal is not a single prescribed fix. The following areas should be explored to determine where the biggest gains lie:

1. **Dependency audit** — Are all dependencies in `Cargo.toml` still necessary? Could any heavy crates (e.g. a full HTTP framework, a large serialization library) be replaced with lighter alternatives or features trimmed? Could sqlite-vec or rusqlite's bundled build be dropped in favor of system packages?

2. **Docker layer caching** — Can the Dockerfile be restructured so that dependency compilation is cached separately from source compilation, avoiding a full rebuild on every code change?

3. **Alternate build strategies** — Would a cargo-chef or sccache approach help? Could we use a persistent build cache volume?

4. **Debug vs release** — Should the development Docker build use `--debug` for faster iteration, reserving `--release` for CI/deployment?

5. **Incremental compilation** — Can incremental compilation artifacts be persisted across Docker builds?

## Acceptance Criteria

- `docker compose up --build` after a small code change completes in significantly less time than today.
- The solution does not introduce fragility or platform-specific breakage (given the existing glibc/musl constraints with sqlite-vec).
