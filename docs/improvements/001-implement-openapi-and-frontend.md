# 001: Implement OpenAPI and Minimal Frontend

## Summary

Expose an OpenAPI JSON document from the Rust `axum` server and provide a minimal, no-build API reference UI running on port `80`.

This feature intentionally avoids generated TypeScript clients. The frontend is documentation/testing only.

## Goals

- Expose OpenAPI spec at `/api-docs/openapi.json`.
- Serve Swagger UI from Rust for in-app documentation browsing.
- Provide an ultra-light static Scalar HTML page for request exploration.
- Keep implementation minimally invasive to the current codebase.

## Non-Goals

- Do not generate any TypeScript client.
- Do not add Axios/OpenAPI TS tooling (`@hey-api/openapi-ts`, `@hey-api/client-axios`).
- Do not introduce a frontend build system.

## Technical Approach

### 1. Rust OpenAPI + Swagger UI

Use `utoipa` and `utoipa-swagger-ui` in `ask-server`:

- Define an `ApiDoc` type with `#[derive(OpenApi)]`.
- Register HTTP paths and schemas already present in the service.
- Mount:
  - OpenAPI JSON at `/api-docs/openapi.json`
  - Swagger UI route (e.g. `/swagger-ui`)

Follow the standard `utoipa-swagger-ui` pattern:

```rust
.url("/api-docs/openapi.json", ApiDoc::openapi())
```

### 2. Minimal Scalar Frontend (Static HTML)

Add a single static file `api.html`:

```html
<!doctype html>
<html>
  <head>
    <title>API Reference</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
  </head>

  <body>
    <div id="app"></div>

    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
    <script>
      Scalar.createApiReference('#app', {
        url: 'http://localhost:3000/api-docs/openapi.json'
      })
    </script>
  </body>
</html>
```

Serve this file on port `80` (for example via a tiny static container or existing reverse-proxy/static service), while the Rust API continues on its configured API port.

## Delivery Plan

1. Add `utoipa` and `utoipa-swagger-ui` dependencies to `crates/ask-server/Cargo.toml`.
2. Implement `ApiDoc` and annotate/register API paths + schemas.
3. Mount OpenAPI JSON and Swagger UI routes in the `axum` router.
4. Add `api.html` as a static asset.
5. Update `docker-compose.yml` (or equivalent runtime wiring) so:
   - API remains reachable (e.g. `:3000`)
   - static frontend is reachable on `:80`
6. Verify endpoints and UI manually.

## Acceptance Criteria

- `GET /api-docs/openapi.json` returns a valid OpenAPI JSON document.
- Swagger UI loads and points to `/api-docs/openapi.json`.
- Scalar page loads on port `80` and renders API reference from the OpenAPI URL.
- API requests can be executed from the Scalar UI.
- No TypeScript client generation scripts, packages, or output directories are introduced.

## Verification

- `cargo build --quiet`
- `cargo test`
- Open:
  - `http://localhost:3000/api-docs/openapi.json`
  - `http://localhost:3000/swagger-ui/` (or configured Swagger route)
  - `http://localhost/api.html` (port 80)

## Notes

- Preferred stack for this feature:
  - Rust + `axum`
  - `utoipa` / `utoipa-axum` (if needed)
  - `utoipa-swagger-ui`
  - Scalar static HTML for lightweight API exploration
- Swagger UI is better when everything is served directly by Rust.
- Scalar is better when a single drop-in HTML file is desired.
