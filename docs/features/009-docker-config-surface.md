# Docker Config Surface

## Goal

Keep the Docker configuration explicit and unsurprising.

## Decision

- `docker-compose.yml` is the source of truth for environment variables injected
  into containers.
- Docker-only overrides must stay visible in `docker-compose.yml`.
- `.env.example` remains focused on local non-Docker runs.
- Do not add extra Docker-specific env files or config layers.

## Scope

Most of the original problem is already fixed.

The remaining work is only to keep the Compose env contract small, explicit, and
 verified when Docker-specific settings change.

## Implementation Plan

1. Keep required container env vars explicit in `docker-compose.yml`.
2. Keep Docker-only overrides there when they differ from local defaults.
3. Avoid moving Docker-required behavior back into implicit code defaults.
4. Add or keep a minimal smoke-path for validating the Compose config surface
   when Docker settings are changed.

## Non-Goals

- No config-system redesign.
- No `.env.docker` file.
- No new abstraction around Docker vs local config.

## Acceptance Criteria

- A developer can read `docker-compose.yml` and see the effective container env.
- Docker-specific required settings are not hidden in app defaults.
- Local `.env.example` stays small and separate from container wiring.
