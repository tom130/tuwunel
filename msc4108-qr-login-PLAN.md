# MSC4108 QR Login Implementation Plan

## 0. How to Read This Plan

This is the single authoritative specification for adding **MSC4108 QR-code login** to Tuwunel, **end-to-end**: both the rendezvous server (the homeserver's transport responsibility) **and** the OAuth 2.0 authorization-server stack (discovery, dynamic client registration, the RFC 8628 device-authorization grant, a browser consent UI, and the token endpoint) that QR login depends on for token issuance.

It is self-contained. It supersedes and consolidates:

- The MSC4108 proposal text (PR [#4108](https://github.com/matrix-org/matrix-spec-proposals/pull/4108), commit `ec24672`)
- The MSC2965/2966/2967/2964/3861 "next-generation auth" proposals
- MSC4341 device-authorization grant (PR [#4341](https://github.com/matrix-org/matrix-spec-proposals/pull/4341))
- RFC 8628 (Device Authorization Grant), RFC 7591 (Dynamic Client Registration), RFC 7009 (Token Revocation), RFC 8414 (AS Metadata)
- Synapse's Rust rendezvous reference (`rust/src/rendezvous/{mod,session}.rs`)
- The `matrix-rust-sdk` QR-login client (`crates/matrix-sdk/src/authentication/oauth/qrcode/`)

Those documents remain available for reference but **this plan is the source of truth**. Where they conflict, this plan wins.

**Audience:** AI coding agents executing tasks autonomously, and human reviewers approving the plan.

### 0.1 Non-Negotiable Scope Doctrine

This plan describes the **complete target implementation**: rendezvous server **and** the self-contained OAuth 2.0 authorization server. There is no "we'll do the OAuth half later" escape hatch — the user has explicitly chosen full end-to-end scope. Every task, test, and constraint here is in scope. Genuinely excluded items appear in §4 with a technical rationale. Tasks are numbered/phased for **dependency ordering**, not scope reduction. Phase 0 (rendezvous) is independently shippable, but Phases 1–7 are equally required for the stated goal of working QR login.

### 0.2 Normative Language

This plan uses RFC 2119 / RFC 8174 keywords:

- **MUST** / **MUST NOT**: Absolute requirement. Violation is a plan-conformance bug.
- **SHOULD** / **SHOULD NOT**: Strong recommendation; deviation requires a documented justification in a code comment.
- **MAY**: Truly optional.

Pseudocode and type definitions are **normative** unless explicitly labeled "illustrative."

### 0.3 Glossary

| Term | Definition (as used in THIS plan) |
| --- | --- |
| **Rendezvous server** | The homeserver's opaque store-and-forward HTTP session store (MSC4108 transport). Implements **no** cryptography. |
| **Secure channel** | The ECIES handshake between the two client devices, carried as opaque payloads through the rendezvous server. tuwunel never sees plaintext. |
| **AS** | Authorization Server — the OAuth 2.0 role that issues tokens. tuwunel becomes its own AS. |
| **RS** | Resource Server — the role that validates tokens on C-S API calls. tuwunel is already this; OAuth-issued tokens reuse the existing token store. |
| **New device** | The device scanning the QR and acquiring a token via the device-code grant. |
| **Existing device** | The already-logged-in device that displays the QR and shares secrets. |
| **Device-code grant** | RFC 8628 OAuth flow: `device_authorization_endpoint` → user approval → poll `token_endpoint`. |
| **`m.login.*` messages** | MSC4108 protocol messages (`m.login.protocols`, `m.login.secrets`, …) exchanged **between clients** over the secure channel. **Opaque to tuwunel.** |
| **next-gen auth** | The MSC3861 family (MSC2964/2965/2966/2967 + MSC4341). |

---

## 1. Overview

A user logs into a new device by scanning a QR code shown on an existing device. Mechanically, two independent halves must both exist on the homeserver:

1. **Rendezvous server** — a tiny HTTP store-and-forward endpoint that relays opaque encrypted blobs between the two devices. tuwunel performs **no** crypto here.
2. **OAuth 2.0 authorization server** — discovery (`auth_metadata` + `m.authentication`), dynamic client registration, the RFC 8628 device-authorization grant with an interactive browser **consent UI**, and a token endpoint that mints a **native tuwunel device + access/refresh token**.

tuwunel today is **only an OAuth client** (outbound SSO to upstream IdPs). It is **not** an authorization server and has none of the device-grant machinery. This plan builds that machinery as a **self-contained** AS+RS — tuwunel issues and validates its own tokens, so no external MAS and **no token-introspection endpoint** are required.

## 2. Current State Analysis

### Key Discoveries (verified, with file:line)

**Routing & patterns**
- All routes are registered in one function: `src/api/router.rs:24` `build(...)`. Two styles: `.ruma_route(&client::handler)` (typed, auto-path) and raw `.route("/path", get(handler))`.
- The MSC4143 RTC endpoint (`src/api/router.rs:159-166`) is the canonical **custom unauthenticated/manually-authenticated route** precedent, handler in `src/api/client/rtc.rs` (full file read; manual `Bearer`/query token extraction).
- Custom HTML/JSON routes exist: `/_tuwunel/server_version` (`router.rs:205`), `/client/server.json` (`router.rs:207`, handler `well_known.rs:127`).
- `ruma_route` mechanism: `src/api/router/handler.rs` (iterates `Req::METADATA.history.all_paths()`).

**Versions / features**
- `src/api/client/versions.rs:34` `VERSIONS: [&str; 17]` (includes `v1.15`); `src/api/client/versions.rs:54` `UNSTABLE_FEATURES: [&str; 19]` (last entry `org.matrix.msc4143`). New flags append here and bump the array length.

**Well-known**
- `well_known_client` (`src/api/client/well_known.rs:15`) returns `discover_homeserver::Response` with only `base_url` + `rtc_foci`. It does **not** set `m.authentication`. (Whether the `matrix-construct/ruma` fork's `discover_homeserver::Response` exposes an `authentication` field is unverified — see Task 4.)

**Auth & tokens (the reusable RS core)**
- Token lookup: `services.users.find_from_token(token) -> (OwnedUserId, OwnedDeviceId, Option<SystemTime>)` (`src/service/users/device.rs:128`). This is how **all** tokens are validated; OAuth-issued tokens will be validated the same way for free.
- Issuance primitives: `generate_access_token` (`device.rs:234`), `generate_refresh_token` (`device.rs:297`, `"refresh_"`-prefixed), `create_device` (`device.rs:31`), `set_access_token(user, device, token, expires_in, refresh_token)` (`device.rs:164`), `set_refresh_token` (`device.rs:246`), `remove_access_token` (`device.rs:209`). `TOKEN_LENGTH = 32` (`device.rs:26`).
- Single-use, self-removing, TTL token precedent: `create_login_token`/`find_from_login_token` (`src/service/users/mod.rs:350`/`:366`), map `logintoken_expiresatuserid`. Model for `device_code`/`user_code`.
- The **browser→token handoff** precedent (consent UI model): `sso_callback_route` (`src/api/client/session/sso.rs:449-465`) mints a single-use login token and redirects; cookie `tuwunel_grant_session` (`sso.rs:66`); stored browser/session state in `src/service/oauth/sessions.rs` (`Session` with nonces, `authorize_expires_at`, PKCE verifier). HTML-page precedent: `sso_custom_providers_page` config.

**Services & storage**
- Service pattern: a dir under `src/service/`, `Service` struct with `build(args) -> Arc<Self>` + `name()`; registered as a field in `src/service/services.rs:22-70`, constructed at `:82-130`, and listed in the `services()` iterator at `:143-187` (for lifecycle/`interrupt`/`clear_cache`).
- In-memory ephemeral state precedent: `tokio::sync::Mutex<...>` fields on services (e.g. `services.rs:67`). DB maps declared in `src/database/maps.rs` (e.g. `oauthid_session`).

**CORS**
- Global CORS layer: `src/router/layers.rs:100` applies `cors_layer(server)` (`:146`). Allowed headers: `ACCEPT, AUTHORIZATION, CONTENT_TYPE, ORIGIN, x-requested-with` (`:157-163`). **Does not** currently allow `If-Match`/`If-None-Match` or expose `ETag` — Task 1 must fix this for rendezvous.

**Existing endpoints the client flow needs (already present — no work)**
- `whoami_route` (`router.rs:44`), `get_device_route`/`get_devices_route` (`router.rs:174-175`), `upload_keys_route` (`router.rs:80`), `get_keys_route` (`router.rs:81`).

**Greenfield**
- No `rendezvous`/`msc4108`/`msc3886`/`qr`/`auth_metadata`/`device_code`/`m.authentication` code exists anywhere (verified by repo-wide grep).

**Ruma fork**
- Dependency is `git = "https://github.com/matrix-construct/ruma"` (`Cargo.toml:328`). The fork has `crates/ruma-client-api/src/rendezvous/create_rendezvous_session.rs` but **not** the session GET/PUT/DELETE types. No `unstable-msc4108` feature is enabled in this workspace. → We use **custom axum handlers** for all new endpoints (see §7.1).

## 3. Desired End State

A tuwunel server with `next_gen_auth` enabled can complete the full `matrix-rust-sdk` QR-login sequence (validated against the researched call order):

1. `POST …/org.matrix.msc4108/rendezvous` (create) → `201` + `{"url": …}` + `ETag`/`Expires`/`Last-Modified`.
2. `PUT`/`GET` session URL with `If-Match`/`If-None-Match` relaying opaque blobs (`202`/`200`/`304`/`412 M_CONCURRENT_WRITE`).
3. `GET /_matrix/client/v1/auth_metadata` → discovery doc advertising the device grant.
4. `POST {registration_endpoint}` → `client_id`.
5. `POST {device_authorization_endpoint}` → `device_code`/`user_code`/`verification_uri[_complete]`/`interval`/`expires_in`.
6. User opens `verification_uri_complete`, authenticates (password/SSO), sees device + scopes + `user_code`, approves.
7. `POST {token_endpoint}` (`grant_type=urn:ietf:params:oauth:grant-type:device_code`) polling → `access_token` + `refresh_token` bound to the scope's `device_id`.
8. New device's token validates via existing `find_from_token`; `whoami`, `GET /devices/{device_id}`, `keys/upload` all succeed.

### Acceptance Criteria (Testable)

- [x] Rendezvous create/get/put/delete honor ETag concurrency and TTL: `cargo test rendezvous`
- [x] Rendezvous rejects >4096-byte payloads with `413 M_TOO_LARGE` and non-`text/plain` with `400 M_INVALID_PARAM`: `cargo test rendezvous`
- [x] `GET /_matrix/client/v1/auth_metadata` returns a doc with `device_authorization_endpoint` and `urn:ietf:params:oauth:grant-type:device_code` in `grant_types_supported`: `cargo test auth_metadata`
- [x] Dynamic registration returns a `client_id` for a public client (`token_endpoint_auth_method: none`): `cargo test oauth_register`
- [x] Device-grant lifecycle pending→approved→token issues a **real** tuwunel device+token validated by `find_from_token`, with `device_id` taken from the scope: `cargo test device_grant`
- [x] Token endpoint returns `authorization_pending`/`slow_down`/`access_denied`/`expired_token` correctly: `cargo test device_grant_errors`
- [x] Consent page denies unauthenticated approval and is CSRF-protected: `cargo test consent`
- [x] `refresh_token` grant and revocation (`/revoke`) work: `cargo test oauth_refresh oauth_revoke`
- [x] All new endpoints are gated off by default; with `next_gen_auth` disabled they return `404 M_UNRECOGNIZED`: `cargo test gating`
- [x] `org.matrix.msc4108`, `org.matrix.msc2965`, `org.matrix.msc2967` advertised in `/_matrix/client/versions`: `cargo test versions`
- [x] Project compiles and lints: `cargo build` + `cargo clippy`

## 4. Exclusions (What We Are NOT Building)

- **Token introspection endpoint (RFC 7662 / MSC3861 delegated mode)**: Not built. tuwunel is its own RS and validates tokens internally via `find_from_token`. Introspection is only needed when a *separate* RS must validate AS-issued tokens. **Rationale:** self-contained AS+RS (ADR-001).
- **External MAS delegation**: Not built. tuwunel does not delegate to an external auth server. **Rationale:** user chose the self-contained device-grant; delegated mode is a different architecture.
- **Full browser `authorization_code` + PKCE login flow for arbitrary OAuth clients**: The AS advertises `authorization_endpoint`/`response_types: ["code"]` for RFC 8414 conformance, but the **only** fully wired grant is the device-code grant (plus `refresh_token`). **Rationale:** MSC4108 QR login uses *only* the device grant; the redirect flow is a large separate feature (Element Web web-login) not required for QR. The `authorization_endpoint` returns `501`/`M_UNRECOGNIZED` until/unless a later plan implements it. *(This is an explicit, documented limitation, not silent truncation.)*
- **The ECIES secure channel, QR byte format, and `m.login.*` message semantics**: Not built. These are **client-to-client** and opaque to the homeserver. tuwunel only relays bytes.
- **MSC4388 (`io.element.msc4388`) sequence-token rendezvous**: Not built. We target the 2024 `org.matrix.msc4108` ETag protocol (ADR-005). MSC4388 MAY be a future addition sharing the same store.
- **Server-held HTTP long-polling for rendezvous GET**: Not built. Clients long-poll by retry (1s) per the SDK; the server responds immediately with `304`/`200`.

## 5. Non-Negotiable Constraints

These apply to **every** task. Violating any is a plan-conformance bug regardless of which task you are in.

- **C1 — Default-off & isolation**: All new rendezvous + AS endpoints MUST be gated behind config (`next_gen_auth`/`rendezvous`), **defaulting to disabled**. With the gate off they MUST behave as if absent (`404 M_UNRECOGNIZED`). Existing deployments MUST be byte-for-byte unaffected.
- **C2 — Rendezvous is opaque**: The rendezvous server MUST NOT parse, validate, or interpret payload contents. It stores/forwards bytes. It implements **no** cryptography.
- **C3 — Rendezvous is unauthenticated**: Rendezvous endpoints MUST use no access-token auth (`NoAccessToken`) and MUST be rate-limited. They MUST emit the CORS headers in §7.3 so browser clients (Element Web) can use them cross-origin.
- **C4 — Legacy auth preserved**: Existing `m.login.password`/`m.login.token`/SSO and the outbound-SSO `oauth` service MUST continue to work unchanged. The new AS is **additive**.
- **C5 — Device binding integrity**: Tokens issued by the device-code grant MUST be bound to the `device_id` parsed from the request scope (the base64 Curve25519 key). The created Matrix device MUST use exactly that `device_id`, so the existing device's `GET /devices/{device_id}` poll succeeds. A mismatch silently breaks E2EE bring-up.
- **C6 — Interactive, informed consent**: Token issuance via the device grant MUST require (a) interactive user authentication at `verification_uri`, and (b) an explicit approval action that displays the requesting `device_id`, the requested scopes, and the `user_code`. The approval form MUST be CSRF-protected. Approval MUST NOT be inferable from a GET.
- **C7 — Namespace duality**: Scope and metadata handling MUST accept **both** stable (`urn:matrix:client:*`) and unstable (`urn:matrix:org.matrix.msc2967.client:*`) names, because `matrix-rust-sdk` emits the unstable forms.
- **C8 — Single-use secrets**: `device_code`, `user_code`, and consent browser-sessions MUST be single-use and expire. Approved grants MUST be consumed exactly once at the token endpoint.
- **C9 — No new auth bypass**: Token issuance MUST go through the existing `users` service primitives (`create_device`/`set_access_token`/`set_refresh_token`). No parallel token store.

## 6. Downstream Gates

These MUST pass before ANY task is considered complete:

```bash
cargo build           # Compilation (workspace)
cargo clippy --workspace --all-targets   # Linting
cargo test            # Unit/integration tests
cargo fmt --check     # Formatting (repo uses nightly rustfmt; match existing style)
```

A task is complete only when ALL gates pass. No exceptions.

## 7. Design Decisions

### 7.1 Endpoint implementation: custom axum handlers (not ruma types)

| Approach | Pros | Cons |
| --- | --- | --- |
| **Custom axum handlers (chosen)** | Full control of status codes/headers (ETag, 304, 412); matches `rtc.rs` precedent; no dependency on fork's ruma feature set; session GET/PUT/DELETE not in fork anyway | Manual request parsing; not in ruma's typed registry |
| Ruma `IncomingRequest` types | Typed; auto path registration | Fork only ships `create_rendezvous_session`; OAuth/device-grant/consent types absent; header-level control (ETag/304) fights ruma's response model |

**Decision:** Custom axum handlers for **all** new endpoints. **Rationale:** the protocol needs byte-level header/status control the typed layer resists, and the fork lacks most types.

### 7.2 Rendezvous storage: in-memory service

| Approach | Pros | Cons |
| --- | --- | --- |
| **In-memory `Mutex<map>` service (chosen)** | Matches Synapse reference exactly; zero DB churn for 60 s blobs; trivial eviction | Lost on restart (acceptable — clients re-scan) |
| DB-backed map | Survives restart; multi-process | Heavy for ephemeral data; diverges from reference |

**Decision:** In-memory. Sessions are ephemeral by nature (ADR-004).

### 7.3 Rendezvous wire contract (normative, 2024 `org.matrix.msc4108`)

Ported from Synapse `rust/src/rendezvous/`. Defaults: `capacity = 100`, `max_bytes = 4096`, `ttl = 60s`, `eviction_interval = 60s`.

- **`POST /_matrix/client/unstable/org.matrix.msc4108/rendezvous`**
  - Require `Content-Type: text/plain` (else `400 M_INVALID_PARAM`). Require body ≤ `max_bytes` (else `413 M_TOO_LARGE`).
  - Allocate id; store `{hash=sha256(body), data, content_type, last_modified=now, expires=now+ttl}`. If `len() >= capacity*2`, evict expired then oldest until `< capacity`.
  - Respond `201` `Content-Type: application/json` body `{"url": "<public_base>/_matrix/client/unstable/org.matrix.msc4108/rendezvous/<id>"}` + headers `ETag`, `Expires`, `Last-Modified` + CORS + `Cache-Control: no-store, no-transform` + `Pragma: no-cache`.
- **`GET …/rendezvous/{id}`**: `404` if missing/expired; if `If-None-Match` == current `ETag` → `304` (with `ETag`/`Expires`/`Last-Modified`); else `200` with body + `Content-Type` + `ETag`/`Expires`/`Last-Modified`.
- **`PUT …/rendezvous/{id}`**: require `If-Match`; `404` if missing; size check (`413`); if `If-Match` != current `ETag` → `412` with body `{"errcode":"M_UNKNOWN","org.matrix.msc4108.errcode":"M_CONCURRENT_WRITE","error":"…"}`; else replace payload, refresh `expires`, recompute `ETag`, respond `202` with **empty body but `Content-Type: text/plain`** (Cloudflare ETag-stripping workaround) + `ETag`/`Expires`/`Last-Modified`.
- **`DELETE …/rendezvous/{id}`**: `204`; `404` if absent.
- **ETag** = `format!("\"{}\"", URL_SAFE_NO_PAD.encode(sha256(body)))`.
- **CORS (all rendezvous responses)**: `Access-Control-Allow-Origin: *`, `Access-Control-Allow-Methods: GET,PUT,POST,DELETE`, `Access-Control-Allow-Headers: Content-Type,If-Match,If-None-Match`, `Access-Control-Expose-Headers: ETag`.

### 7.4 AS topology & token model — ADR-001 / ADR-002 (see §7.8)

tuwunel is its own AS **and** RS. The device-code grant ends by calling the existing `create_device` + `set_access_token`/`set_refresh_token`. The issued `access_token` **is** a normal tuwunel token; no introspection. The `device_id` comes from the scope (C5).

### 7.5 AS endpoint paths (chosen, normative)

Hosted under a tuwunel namespace and advertised verbatim by `auth_metadata` (RFC 8414 permits arbitrary absolute endpoint URLs; `issuer` = homeserver base URL):

| Purpose | Path |
| --- | --- |
| issuer | `<public_base>/` |
| `auth_metadata` (stable) | `GET /_matrix/client/v1/auth_metadata` |
| `auth_metadata` (unstable alias) | `GET /_matrix/client/unstable/org.matrix.msc2965/auth_metadata` |
| `registration_endpoint` | `POST /_tuwunel/oauth/register` |
| `device_authorization_endpoint` | `POST /_tuwunel/oauth/device` |
| `token_endpoint` | `POST /_tuwunel/oauth/token` |
| `revocation_endpoint` | `POST /_tuwunel/oauth/revoke` |
| `verification_uri` (consent page) | `GET`/`POST /_tuwunel/oauth/link` |
| `authorization_endpoint` (advertised, not wired — §4) | `GET /_tuwunel/oauth/authorize` |

### 7.6 Scopes (normative)

Requested scope is space-separated. The handler MUST locate **exactly one** device token of the form `urn:matrix:client:device:<id>` **or** `urn:matrix:org.matrix.msc2967.client:device:<id>` and treat `<id>` as the Matrix `device_id` (validate chars `[A-Za-z0-9._~-]`, reject empty/duplicate). It MUST accept the API scope `urn:matrix:client:api:*` or `urn:matrix:org.matrix.msc2967.client:api:*`. The issued token's returned `scope` MUST echo the granted scopes.

### 7.7 Consent UI — ADR-003

Interactive browser flow at `/_tuwunel/oauth/link` (C6). Authenticates via tuwunel password **and** existing SSO, establishes a short-lived stored browser session (cookie id → server-side record, modeled on `oauth::sessions` + the `tuwunel_grant_session` cookie), then renders an approve/deny consent screen.

### 7.8 ADRs (consequential decisions)

#### ADR-001: tuwunel as self-contained AS + RS
- **Problem:** QR login needs an OAuth AS that issues device-grant tokens. tuwunel has none. Delegating to external MAS is one option; being its own AS is another.
- **Options:** (a) Delegate to MAS (MSC3861 introspection). (b) tuwunel is its own AS+RS.
- **Decision:** (b). User explicitly chose the in-tuwunel device grant with no external MAS.
- **Consequences:** No introspection endpoint; OAuth tokens are native tuwunel tokens validated by `find_from_token`. tuwunel must implement discovery/registration/device-grant/consent/token endpoints. Rules out, for now, acting as a delegated RS for a separate AS.
- **Tests (stop-ship):** device-grant token validates via `find_from_token`; `whoami` returns the granting user; `GET /devices/{device_id}` finds the created device.

#### ADR-002: OAuth tokens are native tuwunel tokens (no introspection)
- **Problem:** How are AS-issued tokens validated on C-S calls?
- **Decision:** Reuse `create_device`/`set_access_token`; the existing `Ruma<T>` extractor validates them unchanged.
- **Consequences:** Zero changes to request auth. No JWT/signing/JWKS needed (`jwks_uri` MAY be omitted from metadata).
- **Tests:** a C-S call (e.g. `whoami`) with the granted token succeeds; logout/revoke invalidates it.

#### ADR-003: Interactive consent reusing existing auth + cookie-session
- **Problem:** The device grant needs a human approval in a browser; tuwunel has no web auth/consent UI.
- **Decision:** Build a minimal HTML login+consent at `/_tuwunel/oauth/link` reusing password + SSO auth, with a server-side browser session keyed by a cookie (modeled on `oauth::sessions`/`sso.rs`).
- **Consequences:** New HTML surface + CSRF + cookie handling; SSO round-trip must return to the consent page. Largest single piece of work.
- **Tests:** unauthenticated approve rejected; CSRF token enforced; approve binds the correct `user_id`; deny yields `access_denied`.

#### ADR-004: Rendezvous in-memory & ephemeral — see §7.2. **Tests:** restart drops sessions; eviction past capacity; TTL expiry → `404`.

#### ADR-005: Target 2024 `org.matrix.msc4108` (ETag) over MSC4388
- **Decision:** Implement the ETag protocol; advertise `org.matrix.msc4108`.
- **Consequences:** Interop with current Element/rust-sdk; MSC4388 deferred.
- **Tests:** the §7.3 contract; `org.matrix.msc4108` present in versions.

## 8. Failure Modes & Recovery

| Failure Mode | Impact | Mitigation |
| --- | --- | --- |
| Concurrent rendezvous writes | Lost update | ETag `If-Match` → `412 M_CONCURRENT_WRITE`; client GETs + retries |
| Payload > 4096 B / wrong content-type | Abuse / malformed | `413 M_TOO_LARGE` / `400 M_INVALID_PARAM` |
| Clock skew on `Expires` | Session appears expired immediately | Use generous TTL; emit relative-correct `Expires`; document NTP requirement |
| Rendezvous capacity exhaustion (DoS) | Memory growth | Hard cap (100) + eviction at `len≥cap*2` + rate limiting (C3) |
| `device_id` collision | `device_already_exists` from existing device | Token endpoint MUST reject reuse of an existing device id for a *different* user; surface `M_*`; client maps to failure |
| Token-endpoint polling abuse | Load | Enforce `interval`; return `slow_down` when polled faster |
| `device_code` expiry | Stuck client | `expired_token`; client restarts QR flow |
| User denies at consent | — | Mark grant denied → token endpoint returns `access_denied` |
| CSRF on consent approve | Unauthorized token issuance | Per-session CSRF token in form; reject mismatches (C6) |
| Approved grant replayed | Duplicate device/token | Single-use: delete grant on first successful token exchange (C8) |
| `next_gen_auth` disabled but routes hit | Confusion | Gate returns `404 M_UNRECOGNIZED` (C1) |
| Scope missing/duplicate device token | Cannot bind device | Reject device-authorization request with `invalid_scope` |

## 9. Implementation Approach

Build bottom-up so each layer is independently testable: Phase 0 (rendezvous, standalone) → Phase 1 (discovery surface) → Phase 2 (client registry) → Phase 3 (device-auth request) → Phase 4 (consent UI) → Phase 5 (token issuance, the keystone) → Phase 6 (lifecycle) → Phase 7 (wiring/config/conformance). Two new services: `service/rendezvous` and `service/oauth_provider` (kept distinct from the existing outbound-SSO `service/oauth`). All routes gated by config (C1).

---

## 10. Task List

> Every task MUST satisfy the §6 gates and the §5 constraints. "Specification Updates" lists the docs/specs each task must keep in sync (tuwunel uses an mdBook under `book/`/docs + generated `tuwunel-example.toml`; there is no `specs/` dir, so doc updates target those).

### Task 1: Rendezvous service + endpoints (`org.matrix.msc4108`)

**Priority**: 1 — **Depends on**: None

#### What This Task Accomplishes
The complete opaque store-and-forward rendezvous server (§7.3), gated by config.

#### Files to Create
- `src/service/rendezvous/mod.rs` — `Service` with `Mutex<BTreeMap<SessionId, Session>>`, methods `create`, `get`, `update`, `delete`, plus an eviction sweep. `Session { hash:[u8;32], data:Vec<u8>, content_type:String, last_modified:SystemTime, expires:SystemTime }`. `SessionId` = `utils::random_string(16)` (ULID MAY be used if the `ulid` crate is added; default avoids a new dep — eviction sorts by `last_modified`).
- `src/api/client/rendezvous.rs` — four custom axum handlers (`create/get/put/delete`) implementing §7.3 exactly, returning `404 M_UNRECOGNIZED` when `!config.next_gen_auth` (or a dedicated `rendezvous` gate).

#### Files to Modify
- `src/service/mod.rs` + `src/service/services.rs` — register `rendezvous` service (field at `services.rs:22-70`, build at `:82-130`, add to `services()` iterator `:143-187`).
- `src/api/client/mod.rs` — `pub(super) mod rendezvous;` + re-export.
- `src/api/router.rs` — register the 4 routes (gated) near `router.rs:166`.
- `src/router/layers.rs:146` `cors_layer` — add `If-Match`, `If-None-Match` to allowed headers and expose `ETag` (or attach a dedicated `CorsLayer` to the rendezvous routes). MUST NOT loosen CORS for other routes beyond exposing `ETag`.
- `src/core/config/mod.rs` — add `next_gen_auth: bool` (default false) + optional `rendezvous_ttl_secs`/`rendezvous_max_sessions`/`rendezvous_max_bytes` with `default_*` fns (model: `default_login_token_ttl:3305`).

#### Tests to Add
- `tests` for: create→201+headers; get 200/304 via `If-None-Match`; put 202 + ETag advance; put 412 `M_CONCURRENT_WRITE`; 413/400 validation; delete 204/404; TTL expiry→404; eviction past capacity; gate-off→404.

#### Gate Commands
```bash
cargo build && cargo clippy && cargo test rendezvous
```

#### Specification Updates
- `book/`/docs: new "QR login / rendezvous" page stub; `tuwunel-example.toml` regenerated with new config keys.

---

### Task 2: Advertise `org.matrix.msc4108` (+ scaffold feature flags)

**Priority**: 2 — **Depends on**: Task 1

#### Files to Modify
- `src/api/client/versions.rs:54` — append `"org.matrix.msc4108"` and bump `[&str; 19]`→`20`.

#### Tests
- versions response contains `org.matrix.msc4108`.

#### Gate Commands
```bash
cargo build && cargo test versions
```

---

### Task 3: `oauth_provider` service skeleton + client registry (MSC2966 / RFC 7591)

**Priority**: 3 — **Depends on**: Task 1

#### What This Task Accomplishes
A new AS service holding the registered-client store, and the dynamic client registration endpoint.

#### Files to Create
- `src/service/oauth_provider/mod.rs` — `Service` (registered like Task 1) holding `clients` + (later) `grants` + `consent_sessions`.
- `src/service/oauth_provider/clients.rs` — persisted client registry. DB map `oauthclient_metadata` (declare in `src/database/maps.rs`): `client_id -> ClientMetadata` (CBOR). `register(metadata) -> client_id` (`client_id = utils::random_string(…)`); `get(client_id)`.
- `src/api/client/oauth/register.rs` — `POST /_tuwunel/oauth/register` handler: parse JSON `ClientMetadata` (`client_name`, `client_uri` (REQUIRED, https), `redirect_uris` (optional for device-only), `application_type` default `web`, `token_endpoint_auth_method` MUST support `none`, `grant_types`, `response_types`, localized `name#lang`). Ignore unknown grant/response types. Respond `201` `{client_id, client_id_issued_at, ...echoed}`. Gated by `next_gen_auth`.

#### Files to Modify
- `src/database/maps.rs` — add `oauthclient_metadata`.
- `src/service/{mod.rs,services.rs}`, `src/api/client/mod.rs`, `src/api/router.rs` — register service, module, route (gated).

#### Tests
- register public client (`token_endpoint_auth_method: none`, `grant_types:[device_code,refresh_token]`) → 201 + `client_id`; missing/invalid `client_uri` → 400; gate-off → 404.

#### Gate Commands
```bash
cargo build && cargo clippy && cargo test oauth_register
```

#### Specification Updates
- docs: AS endpoints section; `tuwunel-example.toml` (if new keys).

---

### Task 4: Discovery — `auth_metadata` + `m.authentication` + feature flags (MSC2965)

**Priority**: 4 — **Depends on**: Task 3

#### What This Task Accomplishes
The discovery surface that tells clients the device grant exists.

#### Files to Create
- `src/api/client/oauth/metadata.rs` — `GET /_matrix/client/v1/auth_metadata` (+ unstable `org.matrix.msc2965` alias). Returns JSON: `issuer` (public base), `authorization_endpoint`, `token_endpoint`, `registration_endpoint`, `device_authorization_endpoint`, `revocation_endpoint`, `response_types_supported:["code"]`, `grant_types_supported:["authorization_code","refresh_token","urn:ietf:params:oauth:grant-type:device_code"]`, `response_modes_supported:["query","fragment"]`, `code_challenge_methods_supported:["S256"]`, `prompt_values_supported:["create"]` (optional), `account_management_uri` (optional). `404 M_UNRECOGNIZED` if `!next_gen_auth`. Endpoint URLs from §7.5 built off `config.well_known.client`/server name.

#### Files to Modify
- `src/api/client/well_known.rs:15` `well_known_client` — add `m.authentication: {issuer, account?}`. **First verify** whether the `matrix-construct/ruma` `discover_homeserver::Response` exposes an `authentication` field. If yes, set it. If no, convert this handler to a **custom JSON handler** (like `syncv3_client_server_json`) that merges `m.homeserver`, `org.matrix.msc4143.rtc_foci`, and `m.authentication`. The conversion MUST preserve current behavior (404 when `well_known.client` unset; existing `rtc_foci` shape).
- `src/api/client/versions.rs:54` — append `"org.matrix.msc2965"`, `"org.matrix.msc2966"`, `"org.matrix.msc2967"` and bump the length.
- `src/api/router.rs` — register `auth_metadata` (both paths, gated).

#### Tests
- `auth_metadata` shape incl. device grant + 404 when gated off; well-known includes `m.authentication` when enabled and is unchanged when disabled; versions include the new flags.

#### Gate Commands
```bash
cargo build && cargo clippy && cargo test auth_metadata well_known versions
```

---

### Task 5: Scope parsing/validation module (MSC2967, dual namespace)

**Priority**: 5 — **Depends on**: Task 3

#### Files to Create
- `src/service/oauth_provider/scope.rs` — `parse_scope(&str) -> Result<GrantedScope>` extracting exactly one `device:<id>` (stable or unstable, C7), validating the API scope, validating `device_id` charset. `to_string()` re-emits granted scopes. Pure function (heavily unit-tested).

#### Tests
- stable + unstable forms; missing device token → error; duplicate device token → error; bad device-id chars → error; round-trip.

#### Gate Commands
```bash
cargo build && cargo test oauth_scope
```

---

### Task 6: Device authorization endpoint (MSC4341 / RFC 8628)

**Priority**: 6 — **Depends on**: Tasks 3, 5

#### What This Task Accomplishes
`device_authorization_endpoint` + the pending-grant store.

#### Files to Create
- `src/service/oauth_provider/grants.rs` — in-memory `Mutex<HashMap<DeviceCode, PendingGrant>>` (+ a `user_code -> device_code` index). `PendingGrant { client_id, granted_scope, device_id, user_code, created, expires, interval, last_polled, status: Pending|Approved{user_id}|Denied }`. Methods: `create`, `by_device_code`, `by_user_code`, `approve(user_code, user_id)`, `deny`, `consume(device_code)`, eviction sweep. Single-use + TTL (C8).
- `src/api/client/oauth/device.rs` — `POST /_tuwunel/oauth/device` (form-encoded `client_id`, `scope`): validate client exists; `parse_scope`; gen `device_code` (`random_string(32)`), `user_code` (e.g. `XXXX-XXXX` from an unambiguous alphabet); build `verification_uri`/`verification_uri_complete` (with `user_code`); `expires_in` + `interval` from config. Store grant. Respond JSON. Gated.

#### Files to Modify
- `src/service/oauth_provider/mod.rs` — wire `grants`.
- `src/core/config/mod.rs` — `device_grant_expires_secs` (default e.g. 600), `device_grant_interval_secs` (default 5).
- `src/api/router.rs` — register route (gated).

#### Tests
- valid request → device_code/user_code/uris/interval/expires_in; unknown client → `invalid_client`; bad scope → `invalid_scope`; gate-off → 404.

#### Gate Commands
```bash
cargo build && cargo clippy && cargo test device_authorization
```

---

### Task 7: Consent UI — login + approve/deny (`/_tuwunel/oauth/link`) (ADR-003, C6)

**Priority**: 7 — **Depends on**: Task 6

#### What This Task Accomplishes
The interactive browser approval flow that authorizes a pending grant.

#### Files to Create
- `src/service/oauth_provider/consent.rs` — server-side browser-session store (cookie id → `{user_id, csrf_token, expires}`), modeled on `service/oauth/sessions.rs`. Config `consent_session_ttl_secs`.
- `src/api/client/oauth/consent.rs` —
  - `GET /_tuwunel/oauth/link?user_code=…[&…]`: resolve grant by `user_code` (404/expired page if absent). If no valid consent cookie → render **login page** (password fields + SSO buttons enumerated from existing IdP config). If authenticated → render **consent page** showing requesting `device_id`, granted scopes (human-readable), the `user_code`, and Approve/Deny buttons carrying a CSRF token.
  - `POST /_tuwunel/oauth/link`: dispatch on form action — (a) **login**: authenticate via tuwunel password (reuse `users`/login verification) or kick off SSO (reuse `sso_login_route` round-trip returning to `link`), set consent cookie; (b) **approve**: verify CSRF + cookie session → `grants.approve(user_code, user_id)` → success page; (c) **deny**: `grants.deny` → denied page.
- HTML templates via `include_str!` (no template-engine dependency); minimal CSS. Precedent: `sso_custom_providers_page`.

#### Files to Modify
- `src/api/router.rs` — register `GET`/`POST /_tuwunel/oauth/link` (gated). SSO return path wiring if needed.
- `src/service/oauth_provider/mod.rs` — wire `consent`.

#### Tests
- unauthenticated POST approve → rejected; CSRF mismatch → rejected; password login → consent → approve binds correct `user_id`; deny sets denied; expired/invalid `user_code` → error page. (SSO path MAY be covered by an integration stub.)

#### Gate Commands
```bash
cargo build && cargo clippy && cargo test consent
```

#### Specification Updates
- docs: "Approving a QR sign-in" user guide page.

---

### Task 8: Token endpoint — device-code grant (the keystone) (RFC 8628 + C5/C9)

**Priority**: 8 — **Depends on**: Tasks 6, 7

#### What This Task Accomplishes
`POST /_tuwunel/oauth/token` for `grant_type=urn:ietf:params:oauth:grant-type:device_code`, minting a **native tuwunel device + token**.

#### Files to Create
- `src/api/client/oauth/token.rs` — form-encoded handler. For the device-code grant: require `device_code` + `client_id`; look up grant. Return RFC 8628 errors as `400` `{"error":...}`: `authorization_pending` (status Pending), `slow_down` (polled faster than `interval`; also bump interval), `access_denied` (Denied), `expired_token` (past `expires`), `invalid_grant` (unknown/consumed). On `Approved{user_id}`:
  1. `device_id = grant.device_id` (C5). If a device with that id exists for a **different** user → reject (failure maps to `device_already_exists`).
  2. `access = users.generate_access_token(true)`, `refresh = users.generate_refresh_token()`.
  3. `users.create_device(user_id, device_id, …)` (if absent) then `users.set_access_token(user, device, access, expires_in, Some(refresh))` + `set_refresh_token`.
  4. `grants.consume(device_code)` (single-use, C8).
  5. Respond `{access_token, token_type:"Bearer", expires_in, refresh_token, scope}`.
  Gated.

#### Files to Modify
- `src/api/router.rs` — register `POST /_tuwunel/oauth/token` (gated).

#### Tests
- pending→`authorization_pending`; fast poll→`slow_down`; denied→`access_denied`; expired→`expired_token`; **approved→real token that `find_from_token` resolves to (user_id, device_id)**; device id matches scope; second exchange of same `device_code`→`invalid_grant`; `whoami` with the token returns the user.

#### Gate Commands
```bash
cargo build && cargo clippy && cargo test device_grant token_endpoint
```

---

### Task 9: Lifecycle — `refresh_token` grant + revocation (MSC4254 / RFC 7009)

**Priority**: 9 — **Depends on**: Task 8

#### Files to Modify/Create
- `src/api/client/oauth/token.rs` — add `grant_type=refresh_token`: validate `refresh_token` via existing store, rotate via `users` refresh primitives (reuse `src/api/client/session/refresh.rs` logic), respond new tokens.
- `src/api/client/oauth/revoke.rs` (new) — `POST /_tuwunel/oauth/revoke` (RFC 7009): accept `token` (+ optional `token_type_hint`); remove access/refresh token via `users.remove_access_token`/`remove_refresh_token`. Always `200`.
- `src/api/router.rs` — register `/revoke` (gated).

#### Tests
- refresh rotates and old access invalidated per policy; revoke makes `find_from_token` fail; revoking unknown token still `200`.

#### Gate Commands
```bash
cargo build && cargo clippy && cargo test oauth_refresh oauth_revoke
```

---

### Task 10: End-to-end wiring, gating, config docs, conformance test

**Priority**: 10 — **Depends on**: Tasks 1–9

#### What This Task Accomplishes
Confirms the whole researched `matrix-rust-sdk` sequence works and everything is gated/documented.

#### Files to Modify
- `src/api/router.rs` — final review: all new routes present and gated; OPTIONS/CORS correct for rendezvous.
- `tuwunel-example.toml` — regenerate (config doc-comments drive it); document `next_gen_auth` and sub-keys.
- `book/`/docs — "QR login" admin + user pages; note the `authorization_code` flow limitation (§4).

#### Tests (integration / conformance)
- A single test driving the full sequence against an in-process router: rendezvous create/put/get relay → `auth_metadata` → register → device authorization → (simulate consent approve) → token poll → token validates → `GET /devices/{device_id}` finds device. Asserts ordering/headers per §3.
- Gating test: with `next_gen_auth=false`, every new endpoint returns `404 M_UNRECOGNIZED` and rendezvous is absent; existing routes unaffected.

#### Gate Commands
```bash
cargo build && cargo clippy --workspace --all-targets && cargo test && cargo fmt --check
```

#### Specification Updates
- All docs above; CHANGELOG entry; `tuwunel-example.toml` committed.

---

## 11. Tests To Create

### Unit Tests
| What It Verifies | Task |
| --- | --- |
| Rendezvous ETag/304/412/413/400/204/TTL/eviction | 1 |
| versions advertise msc4108/2965/2966/2967 | 2,4 |
| Dynamic client registration (public client, validation) | 3 |
| `auth_metadata` shape + gating; `m.authentication` in well-known | 4 |
| Scope parse (dual namespace, device-id extraction/validation) | 5 |
| Device-authorization request (codes/uris/interval) | 6 |
| Consent: auth required, CSRF, approve binds user, deny | 7 |
| Token device-grant: pending/slow_down/denied/expired/success; device binding; single-use | 8 |
| Refresh + revoke | 9 |

### Integration/E2E Tests
| What It Verifies | Task |
| --- | --- |
| Full QR-login HTTP sequence end-to-end (rendezvous→discovery→register→device→consent→token→whoami/devices) | 10 |
| Global gating off → all new endpoints 404; legacy auth unaffected | 10 |

## 12. Subjective Quality Gates (Optional)
| Criterion | Evaluation Prompt | Pass |
| --- | --- | --- |
| Consent page is clear | "Does this consent screen clearly state which device and permissions are being granted, and how to verify the code? YES/NO" | YES |

(Prefer the concrete tests above; this is only for the consent copy.)

## 13. Migration Notes
No data migration. New DB map `oauthclient_metadata` is additive. All features default off (C1); enabling `next_gen_auth` is opt-in and reversible (disabling restores prior behavior; in-memory rendezvous/grant state is ephemeral).

## 14. References
- MSC4108 (PR #4108, `ec24672`); MSC4341 (PR #4341); MSC2965/2966/2967/2964/3861 (raw markdown on `matrix-org/matrix-spec-proposals` `main`)
- RFC 8628, RFC 7591, RFC 7009, RFC 8414, RFC 7662
- Synapse rendezvous: `element-hq/synapse` `rust/src/rendezvous/{mod,session}.rs`, `synapse/rest/client/rendezvous.py`
- Client flow: `matrix-org/matrix-rust-sdk` `crates/matrix-sdk/src/authentication/oauth/qrcode/`
- Local precedents: `src/api/client/rtc.rs`, `src/api/client/session/sso.rs`, `src/service/oauth/{mod,sessions}.rs`, `src/service/users/{mod,device}.rs`, `src/router/layers.rs:146`, `src/api/router.rs:24`
- Sibling plan: `msc4143-rtc-transports-PLAN.md`

---

## 15. Implementation Roadmap (Phased)

### Phase 0: Rendezvous server (independently shippable)
- [x] Task 1 (service + endpoints + CORS + config), Task 2 (advertise)
- **Exit criteria**: `cargo test rendezvous versions` green; the §7.3 contract holds; gate-off → absent.

### Phase 1: AS foundation
- [x] Task 3 (client registry), Task 4 (discovery), Task 5 (scope)
- **Exit criteria**: `cargo test oauth_register auth_metadata oauth_scope well_known` green; `auth_metadata` advertises the device grant.

### Phase 2: Device grant + consent (the core)
- [x] Task 6 (device-authorization), Task 7 (consent UI), Task 8 (token endpoint)
- **Exit criteria**: `cargo test device_authorization consent device_grant` green; an approved grant yields a token resolvable by `find_from_token` with the scope's device id.

### Phase 3: Lifecycle + conformance
- [x] Task 9 (refresh/revoke), Task 10 (wiring/docs/E2E)
- **Exit criteria**: full §6 gate suite green incl. the end-to-end conformance test and the gating test.

---

## 16. Operational Playbook

### 16.1 Merge Gates
- Any PR touching rendezvous or AS endpoints MUST include the relevant unit tests (§11), pass all §6 gates, and keep `next_gen_auth` defaulting **off**.
- Any PR changing `cors_layer` MUST assert non-rendezvous routes are unchanged except for exposing `ETag`.

### 16.2 Decision Discipline
- §7 decisions and ADRs are locked. Changing one (e.g. introducing introspection, or MSC4388) requires updating this plan first.

### 16.3 Deliverables Order
1. Phase 0 (rendezvous) — shippable alone, validates patterns.
2. Phase 1 (discovery/registry/scope).
3. Phase 2 (device grant + consent + token) — keystone.
4. Phase 3 (lifecycle + E2E + docs).

### 16.4 Immediate Next Steps
1. Implement Task 1 (`service/rendezvous` + handlers + CORS + `next_gen_auth` config) and its tests.
2. Implement Task 2.
3. Verify §7.3 contract against a captured `matrix-rust-sdk` rendezvous exchange.

---

## Appendix A: Risk Register
1. **Consent UI scope creep** — Likelihood: High; Impact: schedule. Mitigation: minimal `include_str!` HTML; reuse existing auth; defer styling.
2. **ruma fork `discover_homeserver` lacks `authentication`** — Likelihood: Medium; Impact: Task 4 rework. Mitigation: custom JSON well-known handler fallback (already specified).
3. **`authorization_code` flow expected by some client** — Likelihood: Low (QR uses device grant only); Impact: that client can't web-login. Mitigation: documented exclusion (§4); advertised-but-`501`.
4. **CORS/proxy mangling ETag** (Cloudflare) — Likelihood: Medium; Impact: PUT 412 loops. Mitigation: `Content-Type: text/plain` on 202 + `Cache-Control: no-store` (§7.3); document proxy guidance.
5. **Clock skew expiring sessions** — Likelihood: Low; Impact: failed logins. Mitigation: generous TTL; NTP note in docs.

## Appendix B: Performance Budgets
| Metric | Budget | How Measured |
| --- | --- | --- |
| Rendezvous GET/PUT latency | < 5 ms p99 (in-memory) | unit bench |
| Rendezvous memory | ≤ `capacity * max_bytes` ≈ 400 KiB | cap + eviction |
| Token-endpoint poll handling | O(1) map lookup | code review + test |

---

# MASTER TODO INVENTORY

## A) Rendezvous (Phase 0)
- [x] `src/service/rendezvous/mod.rs` (in-memory store + eviction)
- [x] `src/api/client/rendezvous.rs` (POST/GET/PUT/DELETE per §7.3)
- [x] CORS: allow `If-Match`/`If-None-Match`, expose `ETag` (`src/router/layers.rs:146`)
- [x] Register service + module + routes (gated)
- [x] Config `next_gen_auth` (+ rendezvous tunables), default off
- [x] `org.matrix.msc4108` in `versions.rs`
- [x] Tests: contract, TTL, eviction, gating

## B) AS Foundation (Phase 1)
- [x] `src/service/oauth_provider/mod.rs` skeleton + service registration
- [x] `clients.rs` + DB map `oauthclient_metadata` + `POST /_tuwunel/oauth/register`
- [x] `metadata.rs` + `GET /_matrix/client/v1/auth_metadata` (+ unstable alias)
- [x] `m.authentication` in well-known (ruma field or custom JSON fallback)
- [x] `org.matrix.msc2965/2966/2967` in `versions.rs`
- [x] `scope.rs` (dual-namespace parse/validate)
- [x] Tests: register, auth_metadata, well_known, scope

## C) Device Grant + Consent (Phase 2)
- [x] `grants.rs` (pending-grant store, single-use, TTL)
- [x] `POST /_tuwunel/oauth/device`
- [x] `consent.rs` browser-session store + CSRF
- [x] `GET`/`POST /_tuwunel/oauth/link` (login: password + SSO; approve/deny) + HTML templates
- [x] `POST /_tuwunel/oauth/token` device-code grant → native device+token (C5/C9)
- [x] Config: `device_grant_expires_secs`, `device_grant_interval_secs`, `consent_session_ttl_secs`
- [x] Tests: device-auth, consent (auth/CSRF/approve/deny), token (all RFC8628 states + binding + single-use)

## D) Lifecycle + Conformance (Phase 3)
- [x] `refresh_token` grant at token endpoint
- [x] `POST /_tuwunel/oauth/revoke` (RFC 7009)
- [x] End-to-end conformance test (full sequence)
- [x] Gating test (all new endpoints 404 when disabled; legacy unaffected)
- [x] Docs: admin + user QR-login pages; `authorization_code` limitation note
- [x] `tuwunel-example.toml` regenerated; CHANGELOG entry

## E) Verification
- [x] `cargo build`
- [x] `cargo clippy --workspace --all-targets`
- [x] `cargo test`
- [x] `cargo fmt --check`
