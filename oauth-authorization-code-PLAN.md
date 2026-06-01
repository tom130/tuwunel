# OAuth 2.0 Authorization Code + PKCE Login Flow Implementation Plan

## 0. How to Read This Plan

This is the single authoritative specification for adding the **OAuth 2.0 `authorization_code` + PKCE interactive login flow** to Tuwunel's self-contained authorization server (AS). It completes the one piece deliberately deferred by `msc4108-qr-login-PLAN.md` §4 ("Full browser `authorization_code` + PKCE login flow … is not implemented"). With this flow in place, `matrix-rust-sdk` / Element X can establish an **OAuth-backed session** against the homeserver, which is the precondition for both (a) primary OAuth login and (b) MSC4108 QR / Apple-Watch device pairing — because the rust-sdk gate `is_login_with_qr_code_supported()` requires `auth_api() == OAuth`, and that is only set once a client drives an OAuth login/registration.

It is self-contained. It supersedes and consolidates:

- `msc4108-qr-login-PLAN.md` (the sibling plan; this plan implements its §4 deferred item and updates its exclusion + docs)
- The live diagnosis of `matrix.datadialect.cz` on 2026-06-01 (advertised `authorization_endpoint` → `404`)
- The `matrix-rust-sdk` OAuth client implementation (`crates/matrix-sdk/src/authentication/oauth/{mod,auth_code_builder,registration}.rs`)
- MSC2964 (auth-code grant), MSC2965 (metadata discovery), MSC2966 (dynamic registration), MSC2967 (scopes)
- RFC 6749 §4.1 (authorization code grant), RFC 7636 (PKCE S256), RFC 8252 (native-app redirect URIs), RFC 7591 (dynamic client registration), RFC 9207 (issuer identification — informational only)

Where those conflict, **this plan wins**.

**Audience:** AI coding agents executing tasks autonomously, and human reviewers approving the plan.

### 0.1 Non-Negotiable Scope Doctrine

This plan describes the **complete target implementation** of the `authorization_code` + PKCE flow for the tuwunel AS, sufficient for `matrix-rust-sdk`/Element X to complete a primary OAuth login (and thereby enable QR/watch pairing). Every task, test, and constraint here is in scope. Genuinely excluded items appear in §4 with a technical rationale. Tasks are numbered/phased for **dependency ordering**, not scope reduction.

### 0.2 Normative Language

This plan uses RFC 2119 / RFC 8174 keywords:

- **MUST** / **MUST NOT**: Absolute requirement. Violation is a plan-conformance bug.
- **SHOULD** / **SHOULD NOT**: Strong recommendation; deviation requires a documented justification in a code comment.
- **MAY**: Truly optional.

Pseudocode and type definitions are **normative** unless explicitly labeled "illustrative."

### 0.3 Glossary

| Term | Definition (as used in THIS plan) |
| --- | --- |
| **AS** | Authorization Server — the OAuth 2.0 role that issues tokens. tuwunel is its own AS (per `msc4108-qr-login-PLAN.md` ADR-001). |
| **auth-code flow** | RFC 6749 §4.1 authorization code grant: `GET authorization_endpoint` → user auth + consent → redirect with `code` → `POST token_endpoint` exchanges `code` (+ PKCE verifier) for tokens. |
| **PKCE** | RFC 7636 Proof Key for Code Exchange. `code_challenge = BASE64URL(SHA256(code_verifier))`, method `S256`. |
| **Pending authorization** | A server-side record of a *validated* authorization request, created at `GET /authorize`, carried by an opaque `auth_id` across the login/SSO/consent steps, consumed when a code is minted. |
| **Authorization code** | A single-use, short-TTL opaque string returned to the client, bound to `client_id`/`redirect_uri`/scope/`device_id`/`user_id`/`code_challenge`. |
| **Browser session** | The existing `oauth_provider::consent::BrowserSession` (cookie `tuwunel_oauth_consent` → `{user_id, csrf_token, expires}`) used to carry interactive-auth state. Reused verbatim. |
| **Public client** | An OAuth client with `token_endpoint_auth_method = "none"` (no `client_secret`). rust-sdk is always a public client. |

---

## 1. Overview

`matrix-rust-sdk` (and thus Element X and the user's watch-pairing app) performs a **pure OAuth 2.0 authorization_code + PKCE** login (not OIDC — verified: no `id_token`, no `nonce`, no JWKS, no RFC 9207 `iss` checking). The Matrix identity is carried entirely in MSC2967 **scope strings**; after the token exchange the SDK calls `GET /_matrix/client/v3/account/whoami` to learn the `user_id`, and takes the `device_id` from the scope.

tuwunel already implements the device-code grant, dynamic client registration (which already accepts `authorization_code` + `redirect_uris`), discovery metadata (which **already advertises** `authorization_endpoint` and the `authorization_code` grant), token issuance, refresh, revocation, and an interactive password+SSO consent UI. The **only** missing piece is the `authorization_endpoint` itself (it 404s — the route is unregistered and unimplemented) and the `authorization_code` branch of the token endpoint.

This plan builds:

1. A server-side **pending-authorization** store and a single-use **authorization-code** store (both in-memory, modeled on `oauth_provider::grants`).
2. PKCE-S256 verification and RFC 8252 `redirect_uri` matching helpers.
3. `GET`/`POST /_tuwunel/oauth/authorize` — validate the request, authenticate the user (reusing the existing password + SSO consent infrastructure), obtain explicit consent, mint a code, and 302-redirect to the client's `redirect_uri` with `code` + `state`.
4. The `authorization_code` branch of `POST /_tuwunel/oauth/token` (PKCE verification → reuse `issue_native_token`).
5. Wiring, config, docs, and an end-to-end conformance test.

## 2. Current State Analysis

### Key Discoveries (verified, with file:line)

**The gap (live + code):**
- `GET /_tuwunel/oauth/authorize` returns **404** on the live server; the route is **not** registered in `src/api/router.rs` (the oauth routes registered are `register`, `auth_metadata`, `device`, `link`, `token`, `revoke` — `router.rs:177-198`). No `authorize` handler exists (`grep` confirms only `metadata.rs:31` references the string, as the advertised URL).
- `docs/qr_login.md:56-61` and `msc4108-qr-login-PLAN.md:128` document the auth-code flow as **not implemented** ("returns `501`/`M_UNRECOGNIZED`"). This plan reverses that; both docs MUST be updated (Task 5).

**Already present — reused, not rebuilt:**
- **Metadata** (`src/api/client/oauth/metadata.rs:26-45`) already advertises `authorization_endpoint` = `<base>/_tuwunel/oauth/authorize`, `response_types_supported:["code"]`, `grant_types_supported` incl. `authorization_code`, `code_challenge_methods_supported:["S256"]`, `response_modes_supported:["query","fragment"]`. **No metadata change required.** (Live `GET /_matrix/client/v1/auth_metadata` confirmed returning this with HTTP 200.)
- **Client registry** (`src/service/oauth_provider/clients.rs`): `ClientMetadata` stores `redirect_uris` (`clients.rs:51`); registration already whitelists `authorization_code` as a supported grant and `code` as a response type (`clients.rs:16,165,167`); `redirect_uris` are validated as URLs but **not** forced to https (`clients.rs:128-130`, `validate_url` at `:205`) — so custom-scheme and loopback redirect URIs already register. `Clients::get(client_id) -> RegisteredClient` (`clients.rs:98`).
- **Scope parsing** (`src/service/oauth_provider/scope.rs`): `parse_scope(&str) -> Result<GrantedScope{device_id, device_scope, api_scope}>` already accepts BOTH the stable `urn:matrix:client:*` and the unstable `urn:matrix:org.matrix.msc2967.client:*` forms (`scope.rs:5-8,17-54`). rust-sdk emits the **unstable** form. Reused verbatim.
- **Interactive auth + consent** (`src/api/client/oauth/consent.rs`): password login via `crate::client::session::authenticate_password_login` (`consent.rs:205`, defined `src/api/client/session/mod.rs:41`), SSO round-trip via `…/login/sso/redirect/{idp}?redirectUrl=…` returning a `loginToken` (`consent.rs:352-380`, mirrors `session/sso.rs:442-461`), `loginToken`→`user_id` via `services.users.find_from_login_token` (`consent.rs:90-93`), `BrowserSession` cookie `tuwunel_oauth_consent` + CSRF + `SameSite=Lax` (`consent.rs:20,334-350`). Consent session store `oauth_provider::consent::Consent` (`src/service/oauth_provider/consent.rs`): `create(user_id)->CreatedSession`, `get(id)`, `validate(id,csrf)`; TTL `consent_session_ttl_secs`.
- **Token issuance** (`src/api/client/oauth/token.rs`): `oauth_token_route` dispatches on `grant_type` (`token.rs:71-103`) — add an `authorization_code` arm. `issue_native_token(services, ApprovedDeviceGrant{user_id, device_id, scope}) -> TokenIssue` (`token.rs:233-276`) creates/updates the device and mints native access+refresh tokens (C9-compliant). `device_id_owned_by_other_user` collision guard (`token.rs:278-302`). `token_success_response` (`token.rs:304-316`) emits exactly the JSON rust-sdk expects.
- **Token validation**: AS-issued tokens are native tokens validated by `services.users.find_from_token` (`src/service/users/device.rs:128`); `whoami` works unchanged.

**Service & store patterns:**
- In-memory ephemeral store pattern: `oauth_provider::grants::Grants` — `Mutex<State{HashMap,…}>`, `create_at/by_*_at/consume_at/evict_expired`, deterministic `_at(now)` methods for unit tests, `new_for_testing(...)` (`src/service/oauth_provider/grants.rs:16-130,273-307`). The two new stores MUST follow this exact shape.
- Sub-store wiring: `oauth_provider::Service` fields `clients`/`consent`/`grants` built in `build()` (`src/service/oauth_provider/mod.rs:10-23`). Add the new store field here.
- Config: doc-commented fields on `Config` with `#[serde(default = "default_*")]` (`src/core/config/mod.rs`; e.g. `next_gen_auth:1025`, `consent_session_ttl_secs`, `device_grant_*`). `tuwunel-example.toml` is generated from these doc-comments.
- Routing & gating: every AS handler begins `if !services.server.config.next_gen_auth { return Err(<unrecognized 404>); }` (e.g. `token.rs:65-67`, `metadata.rs:19-21`). New routes registered in `src/api/router.rs` near `:177-198`.

**rust-sdk wire expectations (verified against `crates/matrix-sdk/src/authentication/oauth/`):**
- Authorization request (GET): `response_type=code`, `client_id`, `redirect_uri`, `scope` (unstable msc2967 api + device forms; SDK generates the `device_id`), `state` (random), `code_challenge`, `code_challenge_method=S256`; optional `prompt` (`create` only for sign-up), optional `login_hint=mxid:<id>`. No `nonce`, no `response_mode`.
- Authorization response: `code` + echoed `state` in the **query string**. `state` MUST be returned unchanged. `iss` NOT required.
- Token exchange (POST, form-encoded): `grant_type=authorization_code`, `code`, `redirect_uri`, `client_id` (in body — **no** `client_secret`/Basic), `code_verifier`.
- Token response: `access_token` (required), `token_type:"Bearer"` (required), `refresh_token` (expected), `expires_in` (recommended), `scope` (optional). **`id_token` MUST NOT be required** and is not parsed.
- `redirect_uri` is opaque (custom scheme / loopback / https app-link), declared at registration, exact-match at authorize and token.

## 3. Desired End State

With `next_gen_auth` enabled, a `matrix-rust-sdk` client completes a full primary OAuth login:

1. `POST /_tuwunel/oauth/register` (already works) → `client_id` (public client, `redirect_uris` declared).
2. `GET /_tuwunel/oauth/authorize?response_type=code&client_id=…&redirect_uri=…&scope=…&state=…&code_challenge=…&code_challenge_method=S256` → interactive password/SSO login → consent screen → **302** to `redirect_uri?code=…&state=…`.
3. `POST /_tuwunel/oauth/token` (`grant_type=authorization_code`, `code`, `redirect_uri`, `client_id`, `code_verifier`) → `{access_token, token_type:"Bearer", expires_in, refresh_token, scope}`.
4. The access token validates via `find_from_token`; `GET /_matrix/client/v3/account/whoami` returns the user; the created device's id equals the scope's `device_id`.
5. Consequently, in `matrix-rust-sdk`, `auth_api()` becomes `OAuth` and (with `org.matrix.msc4108` already advertised) `is_login_with_qr_code_supported()` returns `true`.

### Acceptance Criteria (Testable)

- [x] `GET /_tuwunel/oauth/authorize` with a valid request for an authenticated browser session renders a consent page; on approve it 302-redirects to the registered `redirect_uri` with `code` + unchanged `state`: `cargo test oauth_authorize`
- [x] Unknown `client_id` or non-matching `redirect_uri` → an **HTML error page** (MUST NOT redirect to the supplied URI): `cargo test oauth_authorize_redirect_validation`
- [x] Recoverable errors after redirect-URI validation (e.g. `response_type != code`, missing/`!=S256` `code_challenge`, invalid `scope`, user **deny**) → 302 to `redirect_uri?error=…&state=…` with the correct RFC 6749 error code: `cargo test oauth_authorize_error_redirect`
- [x] Approve requires an authenticated `BrowserSession` and a valid CSRF token; unauthenticated/CSRF-mismatch approve is rejected: `cargo test oauth_authorize_consent`
- [x] `POST /_tuwunel/oauth/token` `grant_type=authorization_code` with the correct `code_verifier` issues a real tuwunel device+token bound to the scope's `device_id`, validated by `find_from_token`; wrong `code_verifier` → `invalid_grant`: `cargo test authorization_code_grant`
- [x] Authorization codes are single-use (second exchange → `invalid_grant`) and expire (`auth_code_ttl_secs`): `cargo test authorization_code_single_use`
- [x] PKCE: `BASE64URL(SHA256(code_verifier)) == code_challenge` is enforced; non-S256 method rejected: `cargo test pkce`
- [x] `redirect_uri` exact match incl. RFC 8252 loopback (any port on `127.0.0.1`/`[::1]`): `cargo test redirect_uri_match`
- [x] All new endpoints gated off by default: with `next_gen_auth` disabled they return `404 M_UNRECOGNIZED`: `cargo test oauth_authorize_gating`
- [x] End-to-end: register → authorize (approve) → token → `whoami` returns the user and the scope's device id: `cargo test authorization_code_conformance`
- [x] Project compiles, lints, formats: `cargo build && cargo clippy --workspace --all-targets && cargo fmt --check`

## 4. Exclusions (What We Are NOT Building)

- **In-browser account registration (`prompt=create`)**: Not built. `/authorize` authenticates **existing** accounts only (password + SSO). `prompt=create` is treated as a normal login (the login page MAY show a "use an existing account" note). **Rationale:** the goal (watch pairing + existing-user login) needs login only; browser signup adds a token-gated registration form and abuse surface that is a separate feature. *(Explicit, documented limitation — `prompt=create` does not error, it logs in.)*
- **OIDC / `id_token` / JWT signing / JWKS**: Not built. `matrix-rust-sdk` uses pure OAuth 2.0 and never parses an `id_token` (verified). tuwunel issues opaque native tokens. **Rationale:** unnecessary for every Matrix client of record; avoids a signing-key subsystem.
- **`nonce`, RFC 9207 `iss` in the redirect, `response_mode=fragment`/`form_post`**: Not implemented as inputs/outputs. We always use `response_mode=query`. **Rationale:** rust-sdk neither sends `nonce`/`response_mode` nor requires `iss`; query mode is parsed by the SDK. (`iss` MAY be added later; harmless.)
- **Confidential clients (`client_secret`, HTTP Basic at the token endpoint)**: Not built. Only public clients (`token_endpoint_auth_method=none`) are supported, matching registration (`clients.rs:135-137`). **Rationale:** rust-sdk is always a public client.
- **Consent "remember"/silent re-approval**: Not built. Consent is shown on every authorization (C6). **Rationale:** informed-consent invariant; optimization out of scope.

## 5. Non-Negotiable Constraints

Apply to **every** task. Violating any is a plan-conformance bug.

- **C1 — Default-off & isolation**: The `authorize` routes and the `authorization_code` token branch MUST be gated behind `next_gen_auth`, defaulting disabled. Gate-off MUST return `404 M_UNRECOGNIZED`. Deployments with `next_gen_auth=false` MUST be byte-for-byte unaffected.
- **C2 — Open-redirect prevention**: The handler MUST NOT issue an HTTP redirect to a `redirect_uri` until that URI has been validated as an **exact match** of a value registered for the given `client_id`. Validation failures of `client_id`/`redirect_uri` MUST render a local HTML error page, never a redirect. Only *after* the redirect URI is validated may recoverable errors be delivered as `redirect_uri?error=…&state=…`.
- **C3 — PKCE mandatory (S256)**: An authorization request without `code_challenge`, or with `code_challenge_method` other than `S256`, MUST be rejected. The token endpoint MUST verify `BASE64URL_NOPAD(SHA256(code_verifier)) == code_challenge` and reject mismatch with `invalid_grant`. No `plain` method.
- **C4 — Single-use, bound, expiring codes**: An authorization `code` MUST be single-use (consumed atomically at first token exchange), short-TTL (`auth_code_ttl_secs`), and bound to `client_id`, `redirect_uri`, granted scope, `device_id`, `user_id`, and `code_challenge`. Mismatch on `client_id`/`redirect_uri` at exchange MUST fail (`invalid_grant`).
- **C5 — Informed, CSRF-protected consent**: Code issuance MUST require (a) interactive user authentication establishing a `BrowserSession`, and (b) an explicit approve action carrying a server-issued CSRF token that matches the session. The approval form MUST display the requesting client, the target `redirect_uri`, the scope, and the `device_id`. Approval MUST NOT be inferable from a bare GET.
- **C6 — Device binding integrity**: The created Matrix device MUST use exactly the `device_id` parsed from the request scope (C6 mirrors `msc4108-qr-login-PLAN.md` C5). The token's `whoami`/device lookups MUST resolve consistently.
- **C7 — Namespace duality**: Scope handling MUST accept both stable (`urn:matrix:client:*`) and unstable (`urn:matrix:org.matrix.msc2967.client:*`) forms (reuse `parse_scope`). rust-sdk emits the unstable form.
- **C8 — No new auth bypass / no parallel token store**: Token issuance MUST go through the existing `users` primitives (`create_device`/`set_access_token`/`set_refresh_token`) via `issue_native_token`. Interactive auth MUST reuse `authenticate_password_login` + the existing SSO round-trip + `consent::Consent`. No new password verification or token store.
- **C9 — Legacy + existing AS preserved**: Existing `m.login.*`/SSO, the outbound-SSO `oauth` service, and the device-code grant / `/link` consent flow MUST continue to work unchanged. This flow is additive; shared code (e.g. `issue_native_token`, consent templates) MUST be extended without altering existing behavior.
- **C10 — `state` fidelity**: The `state` parameter MUST be echoed back verbatim (byte-for-byte) on both success and error redirects. The server MUST treat it as opaque.

## 6. Downstream Gates

These MUST pass before ANY task is considered complete:

```bash
cargo build                               # Compilation (workspace)
cargo clippy --workspace --all-targets    # Linting
cargo test                                # Unit/integration tests
cargo fmt --check                         # Formatting (nightly rustfmt; match existing style)
```

A task is complete only when ALL gates pass. No exceptions.

## 7. Design Decisions

### 7.1 Pure OAuth 2.0 auth-code + PKCE (no OIDC) — ADR-101

| Approach | Pros | Cons |
| --- | --- | --- |
| **Pure OAuth2 + PKCE, opaque native tokens (chosen)** | Matches rust-sdk exactly (no `id_token` parsed); reuses `issue_native_token`; zero new crypto/signing | None for the Matrix client of record |
| OIDC with signed `id_token` + JWKS | "Standards-complete" OIDC | rust-sdk ignores `id_token`; adds a signing-key subsystem, JWKS endpoint, key rotation — large, unused surface |

**Decision:** Pure OAuth2 + PKCE. **Rationale:** verified that `matrix-rust-sdk` never requests or parses an `id_token`; identity flows via scope + `whoami`. See ADR-101 (§7.8).

### 7.2 Request carriage across login/SSO/consent: server-side pending store — ADR-102

| Approach | Pros | Cons |
| --- | --- | --- |
| **Server-side pending-authorization store keyed by opaque `auth_id` (chosen)** | Validate once at GET; browser carries only `auth_id`+CSRF → no parameter tampering between GET and approve; short URLs; matches `grants`/`oauth::sessions` patterns | One new in-memory store + TTL/eviction |
| Stateless: re-carry all params via query + hidden fields | No new store | Re-validate every hit; long SSO `redirectUrl`s; approve must trust browser-resubmitted `client_id`/`redirect_uri`/`code_challenge` (tampering risk) |
| Signed self-contained request token (HMAC) | Stateless + tamper-evident | New signing-key handling; more crypto surface than the in-memory store |

**Decision:** Server-side pending store. **Rationale:** security (params fixed at validation time, C2/C3/C4) and consistency with existing patterns.

### 7.3 redirect_uri matching: exact + RFC 8252 loopback — ADR-103

The authorization-request `redirect_uri` MUST exactly equal a registered value, **except** loopback redirect URIs per RFC 8252 §7.3: for a registered `http://127.0.0.1[:port]/…` or `http://[::1][:port]/…` or `http://localhost[:port]/…`, the runtime port MAY differ (the SDK/OS picks an ephemeral port). Matching for loopback compares scheme + host + path, ignoring the port. All other URIs (custom scheme, https app-link) use byte-exact match. The token exchange MUST re-match the same `redirect_uri` against the code's bound value with the identical rule.

### 7.4 Reuse, not rebuild

The interactive auth + consent surface is reused from the device-code/`/link` flow: `authenticate_password_login`, the SSO `redirectUrl`→`loginToken` round-trip, `consent::Consent` `BrowserSession` (cookie `tuwunel_oauth_consent`, CSRF). New HTML templates (`authorize_login.html`, `authorize_consent.html`) mirror the existing ones but POST to `/_tuwunel/oauth/authorize` and carry `auth_id` instead of `user_code`. `consent_message.html` is reused for error/expired pages. Token issuance reuses `issue_native_token` (with a parameterized initial device display name — "OAuth login" instead of hard-coded "QR login").

### 7.5 Endpoint paths (normative)

| Purpose | Path |
| --- | --- |
| `authorization_endpoint` (GET: render; POST: login/approve/deny) | `/_tuwunel/oauth/authorize` |
| token (`authorization_code` branch added) | `POST /_tuwunel/oauth/token` (existing) |

Already advertised by `auth_metadata` (no metadata change).

### 7.6 Error mapping (normative, RFC 6749 §4.1.2.1 / §5.2)

- Pre-redirect-validation (HTML error page, **no** redirect): unknown/missing `client_id`; missing/❌-match `redirect_uri`; malformed request that prevents identifying the client or redirect URI; expired/unknown `auth_id` at POST.
- Post-validation **redirect** `redirect_uri?error=<code>&state=<echo>`: `unsupported_response_type` (`response_type != code`); `invalid_request` (missing `code_challenge`); `invalid_request` (`code_challenge_method != S256`) — MAY use `invalid_request`; `invalid_scope` (`parse_scope` fails); `access_denied` (user denies).
- Token endpoint (`400 {"error":…}`): `invalid_grant` (unknown/expired/consumed code, PKCE mismatch, `client_id`/`redirect_uri` mismatch); `invalid_request` (missing fields); `invalid_client` (unknown `client_id`); `unsupported_grant_type` (default arm — already present).

### 7.7 Authorization code & PKCE format (normative)

- `code` = `utils::random_string(32)` (opaque). `auth_id` = `utils::random_string(32)`.
- PKCE S256 verify = `URL_SAFE_NO_PAD.encode(sha256(code_verifier.as_bytes())) == code_challenge`. Use the same base64/sha256 utilities already used in the workspace (e.g. `base64::engine::general_purpose::URL_SAFE_NO_PAD`, `tuwunel_core::utils::hash::sha256`); constant-time compare SHOULD be used for the challenge equality.
- `auth_code_ttl_secs` default **60**; pending-authorization TTL reuses `consent_session_ttl_secs` (default 600).

### 7.8 ADRs (consequential)

#### ADR-101: Pure OAuth2 (no OIDC `id_token`)
- **Problem:** Does the AS need to issue a signed OIDC `id_token`?
- **Options:** (a) Pure OAuth2 opaque tokens. (b) OIDC with `id_token`+JWKS.
- **Decision:** (a). **Rationale:** `matrix-rust-sdk` `finish_authorization`/`set_session_tokens` reads only `access_token`/`token_type`/`refresh_token`/`expires_in`; it does not parse `id_token`. Identity via scope + `whoami`.
- **Consequences:** No signing keys, no JWKS endpoint, no `jwks_uri`. Token response identical to the device-grant response. Rules out OIDC-only clients (none of record).
- **Tests (stop-ship):** auth-code exchange returns a token with no `id_token`; `whoami` with that token returns the granting user; created device id == scope device id.

#### ADR-102: Server-side pending-authorization store — see §7.2.
- **Tests (stop-ship):** params captured at GET are used at approve even if a tampered POST supplies different `client_id`/`redirect_uri`/`code_challenge`; pending entry is single-use and TTL-evicted.

#### ADR-103: Exact + RFC 8252 loopback redirect matching — see §7.3.
- **Tests (stop-ship):** exact match passes; differing path/scheme/host fails; loopback with a different port passes; non-loopback differing port fails; the token exchange applies the identical rule.

## 8. Failure Modes & Recovery

| Failure Mode | Impact | Mitigation |
| --- | --- | --- |
| Open redirect (attacker-supplied `redirect_uri`) | Token/code exfiltration, phishing | C2: validate exact/loopback match before any redirect; else HTML error page |
| PKCE downgrade/missing | Code interception attack | C3: require `code_challenge` + `S256`; verify at token endpoint |
| Authorization code replay | Duplicate device/token | C4: atomic single-use `consume`; bound to client/redirect/challenge |
| `code`/`auth_id` brute force | Unauthorized issuance | 32-char random secrets; short TTL; global rate limiting on the routes |
| Pending-store growth from unauthenticated GETs | Memory DoS | TTL + capacity cap + eviction sweep (model `grants::evict_expired`); only create after request validates |
| CSRF on approve | Unauthorized issuance | C5: per-session CSRF token in the form; reject mismatch |
| `state` altered/dropped | SDK `InvalidState` → login fails | C10: echo `state` verbatim on success and error |
| Parameter tampering between GET and approve | Issuance with attacker params | ADR-102: approve reads the server-side pending record, not resubmitted params |
| `device_id` collision with another user | `device_already_exists` | Reuse `device_id_owned_by_other_user` guard (`token.rs:278`) → `invalid_grant`/`access_denied` page |
| Clock skew on code/pending TTL | Premature expiry | Generous TTL (60s code / 600s pending); document NTP (shared with sibling plan) |
| `next_gen_auth` disabled but route hit | Confusion | C1: `404 M_UNRECOGNIZED` |
| Loopback port-exact match would break native apps | Login fails for ephemeral-port clients | ADR-103 loopback port-agnostic match |

## 9. Implementation Approach

Bottom-up, each layer independently testable: Phase 0 (stores + PKCE/redirect helpers, pure unit tests) → Phase 1 (`/authorize` GET/POST + consent, reusing existing auth) → Phase 2 (token `authorization_code` branch, the keystone) → Phase 3 (wiring, config, docs, E2E conformance + gating). All new surface gated by `next_gen_auth` (C1). No new service is required — the new stores live as fields on the existing `oauth_provider::Service`.

---

## 10. Task List

> Every task MUST satisfy the §6 gates and the §5 constraints. "Specification Updates" lists docs to keep in sync (tuwunel uses `docs/`/`book/` + generated `tuwunel-example.toml`; there is no `specs/` dir).

### Task 1: Authorization stores — pending requests + single-use codes

**Priority**: 1 — **Depends on**: None

#### What This Task Accomplishes
The two in-memory stores backing the flow, modeled exactly on `oauth_provider::grants`.

#### Files to Create
- `src/service/oauth_provider/authorize.rs` — one module, two `Mutex`-guarded maps with deterministic `_at(now)` methods and `new_for_testing`:
  - `PendingAuthorizations` — `HashMap<AuthId, PendingAuthorization>`. `PendingAuthorization { client_id, redirect_uri, granted_scope: GrantedScope, device_id, code_challenge, state, created, expires }`. Methods: `create(...) -> auth_id`, `get_at`, `consume_at`, `evict_expired`, capacity cap. TTL = `consent_session_ttl_secs`.
  - `AuthorizationCodes` — `HashMap<Code, AuthorizationCode>`. `AuthorizationCode { client_id, redirect_uri, granted_scope, device_id, user_id: OwnedUserId, code_challenge, created, expires }`. Methods: `create(...) -> code`, `consume_at(code) -> AuthorizationCode` (single-use, removes), `evict_expired`. TTL = `auth_code_ttl_secs`.

#### Files to Modify
- `src/service/oauth_provider/mod.rs` — add `pub authorize: Arc<authorize::...>` (or a single struct holding both maps) field + build wiring (`mod.rs:10-23`).
- `src/core/config/mod.rs` — add `auth_code_ttl_secs: u64` (`#[serde(default = "default_auth_code_ttl_secs")]`, default 60) with doc-comment; add the `default_auth_code_ttl_secs` fn.

#### Tests to Add (unit, in-module, deterministic `_at`)
- pending: create→get; consume is single-use; TTL eviction; capacity cap.
- codes: create→consume single-use (second consume errors); TTL eviction; fields preserved (device_id/user_id/code_challenge/redirect_uri/client_id).

#### Gate Commands
```bash
cargo build && cargo clippy && cargo test oauth_provider::authorize
```

#### Specification Updates
- `tuwunel-example.toml` regenerated (new `auth_code_ttl_secs` key).

---

### Task 2: PKCE-S256 + redirect_uri matching helpers

**Priority**: 2 — **Depends on**: None

#### What This Task Accomplishes
Pure, heavily unit-tested helpers used by Tasks 3 and 4.

#### Files to Create
- `src/service/oauth_provider/pkce.rs` — `verify_s256(code_verifier: &str, code_challenge: &str) -> bool` = `URL_SAFE_NO_PAD.encode(sha256(code_verifier)) == code_challenge` (SHOULD use constant-time equality). `const S256: &str = "S256";`
- `src/service/oauth_provider/redirect.rs` — `redirect_uri_matches(registered: &str, requested: &str) -> bool` implementing ADR-103 (exact match; loopback host `127.0.0.1`/`::1`/`localhost` compares scheme+host+path ignoring port). Parse via `url::Url`.

#### Files to Modify
- `src/service/oauth_provider/mod.rs` — `pub mod pkce; pub mod redirect;`

#### Tests to Add
- pkce: known RFC 7636 test vector verifies; wrong verifier fails; padded/uppercase base64 fails.
- redirect: exact pass; differing path/host/scheme fail; loopback differing port pass (all three loopback hosts); non-loopback differing port fail; custom-scheme exact pass.

#### Gate Commands
```bash
cargo build && cargo clippy && cargo test oauth_provider::pkce oauth_provider::redirect
```

---

### Task 3: `GET`/`POST /_tuwunel/oauth/authorize` — auth + consent + code issuance

**Priority**: 3 — **Depends on**: Tasks 1, 2

#### What This Task Accomplishes
The authorization endpoint: validate the request, authenticate the user (reusing existing password+SSO consent infra), show consent, and on approve mint a code and 302-redirect.

#### Files to Create
- `src/api/client/oauth/authorize.rs`:
  - `GET` handler `get_oauth_authorize_route(State, CookieJar, Uri)`:
    1. Gate on `next_gen_auth` (else 404).
    2. Parse query: `response_type, client_id, redirect_uri, scope, state, code_challenge, code_challenge_method, prompt?, login_hint?`.
    3. Look up `client_id` (`clients.get`); if absent → HTML error page (C2). Validate `redirect_uri` via `redirect_uri_matches` against `client.metadata.redirect_uris`; if none match → HTML error page (C2).
    4. Now redirect-safe. Validate `response_type=="code"` (else error redirect), `code_challenge` present + `code_challenge_method=="S256"` (C3, else error redirect), `parse_scope(scope)` (else `invalid_scope` redirect).
    5. Resolve auth state: if `loginToken` present (SSO return), `find_from_login_token`→`consent.create(user_id)` (set cookie); else read `consent_session_from_cookie`. (Reuse helpers from `consent.rs`, refactored to shared fns — see Files to Modify.)
    6. Create a `PendingAuthorization` (Task 1) capturing the validated request → `auth_id`.
    7. If authenticated → render `authorize_consent.html` (client name, `redirect_uri` host, human scope, `device_id`, CSRF, hidden `auth_id`). Else render `authorize_login.html` (password form + SSO links whose `redirectUrl` returns to `/_tuwunel/oauth/authorize?auth_id=<id>`), carrying `auth_id`.
  - `POST` handler `post_oauth_authorize_route(State, CookieJar, Bytes)`: form `{action: login|approve|deny, auth_id, csrf?, username?, password?}`.
    - `login`: `authenticate_password_login` → `consent.create` → re-render consent page with cookie (mirror `password_login_response` in `consent.rs`).
    - `approve`: require valid `BrowserSession` (cookie) + CSRF match; load `PendingAuthorization` by `auth_id` (404 page if expired); `consume` it; `AuthorizationCodes.create(... user_id from session ...)` → **302** to `redirect_uri?code=…&state=…` (state from the pending record, C10).
    - `deny`: load pending (for `redirect_uri`+`state`) → **302** `redirect_uri?error=access_denied&state=…`.

#### Files to Modify
- `src/api/client/oauth/consent.rs` — extract reusable helpers (`consent_session_from_cookie`, `append_consent_cookie`, `public_base_url`, `escape_html`, SSO-link building) into a shared module `src/api/client/oauth/web.rs` (or `pub(crate)` in `consent.rs`) so `authorize.rs` reuses them **without** changing `/link` behavior (C9). Keep `consent.rs` behavior identical.
- `src/api/client/oauth/mod.rs` — `pub(crate) mod authorize;` + re-export; add `web` module if created.
- `src/api/router.rs` — register `GET`/`POST /_tuwunel/oauth/authorize` (gated) near `:177-198`.
- Create templates `src/api/client/oauth/templates/authorize_login.html` and `authorize_consent.html` (mirror existing consent templates; POST to `/_tuwunel/oauth/authorize`, hidden `auth_id`).

#### Tests to Add
- redirect-validation: unknown client / non-matching redirect → HTML error page (no `Location` header).
- error-redirect: bad `response_type`/missing `code_challenge`/non-S256/bad scope/deny → 302 to `redirect_uri?error=…&state=…` (state echoed).
- consent: unauthenticated approve rejected; CSRF mismatch rejected; password login → approve → 302 with `code`+`state`; `auth_id` expired → error page.
- gating: `next_gen_auth=false` → 404 on GET and POST.
- (SSO path MAY be covered by a unit stub mirroring `consent.rs` tests.)

#### Gate Commands
```bash
cargo build && cargo clippy && cargo test oauth_authorize
```

#### Specification Updates
- `docs/qr_login.md` — replace the "Current limitations" paragraph (`:56-61`) to state the auth-code+PKCE login flow is implemented (login-only; `prompt=create` excluded).

---

### Task 4: Token endpoint — `authorization_code` grant (keystone)

**Priority**: 4 — **Depends on**: Tasks 1, 2, 3

#### What This Task Accomplishes
Adds the `authorization_code` branch to `oauth_token_route`, completing the exchange.

#### Files to Modify
- `src/api/client/oauth/token.rs`:
  - Add `const AUTHORIZATION_CODE_GRANT_TYPE: &str = "authorization_code";` and a match arm in `oauth_token_route` (`:71-103`).
  - Parse `{code, redirect_uri, client_id, code_verifier}` (form). Errors → `invalid_request`.
  - `clients.get(client_id)` → `invalid_client` if absent.
  - `AuthorizationCodes.consume(code)` (atomic single-use, C4) → `invalid_grant` if missing/expired.
  - Verify `code.client_id == client_id`, `redirect_uri_matches(code.redirect_uri, redirect_uri)`, and `pkce::verify_s256(code_verifier, code.code_challenge)` → any failure `invalid_grant`.
  - Guard `device_id_owned_by_other_user` → `device_already_exists` (reuse `token.rs:278`).
  - Issue via `issue_native_token(services, ApprovedDeviceGrant{ user_id: code.user_id, device_id: code.device_id, scope: code.granted_scope.to_string() })`. Parameterize the initial device display name in `issue_native_token` (e.g. add an arg or set "OAuth login"); `token.rs:264` currently hard-codes "QR login".
  - Respond via `token_success_response` (already emits the exact fields rust-sdk reads).

#### Tests to Add
- approved code + correct verifier → real token resolvable by `find_from_token` to (user_id, scope device_id); response has `access_token`/`token_type:Bearer`/`refresh_token`/`expires_in`/`scope` and **no** `id_token`.
- wrong `code_verifier` → `invalid_grant`; second exchange of same code → `invalid_grant`; mismatched `client_id`/`redirect_uri` → `invalid_grant`; unknown client → `invalid_client`.

#### Gate Commands
```bash
cargo build && cargo clippy && cargo test authorization_code_grant token_endpoint
```

---

### Task 5: Wiring, config docs, end-to-end conformance + gating

**Priority**: 5 — **Depends on**: Tasks 1–4

#### What This Task Accomplishes
Confirms the full auth-code sequence works in-process and everything is gated/documented; removes the now-obsolete "not implemented" notes.

#### Files to Modify
- `src/api/router.rs` — final review: `authorize` GET/POST registered and gated.
- `tuwunel-example.toml` — regenerate; document `auth_code_ttl_secs`.
- `docs/qr_login.md` — full pass: document the auth-code+PKCE login flow and that it is what enables Element X / rust-sdk QR (watch) pairing (existing-session OAuth requirement).
- `msc4108-qr-login-PLAN.md` §4 — strike the "Full browser `authorization_code` + PKCE login flow … is not implemented" exclusion (now implemented by this plan); add a back-reference.
- `RELEASE.md`/CHANGELOG — entry.

#### Tests to Add (integration / conformance, in-process router like `token.rs::qr_login_router_conformance_sequence`)
- Full sequence: `POST /register` (with `redirect_uris`, `authorization_code` grant) → `GET /authorize` (simulate authenticated `BrowserSession` + approve, OR drive POST approve with a seeded `consent` session) → capture `code`+`state` from the `Location` header → `POST /token` (`authorization_code`, with the matching `code_verifier`) → assert token validates via `find_from_token` to the user and the scope's device id → `GET /_matrix/client/v3/account/whoami` returns the user.
- Gating: with `next_gen_auth=false`, `GET`/`POST /_tuwunel/oauth/authorize` and the `authorization_code` token branch return `404 M_UNRECOGNIZED`; legacy/device-code flows unaffected.

#### Gate Commands
```bash
cargo build && cargo clippy --workspace --all-targets && cargo test && cargo fmt --check
```

#### Specification Updates
- All docs above committed; `tuwunel-example.toml` committed.

---

## 11. Tests To Create

### Unit Tests
| What It Verifies | Task |
| --- | --- |
| Pending-auth + auth-code stores: create/consume single-use/TTL/capacity/field preservation | 1 |
| PKCE S256 verify (RFC 7636 vector); redirect_uri exact + RFC 8252 loopback matching | 2 |
| `/authorize`: redirect-validation error page; error redirect (response_type/PKCE/scope/deny); consent auth+CSRF; approve→code; gating | 3 |
| Token `authorization_code`: success+binding; bad verifier; replay; client/redirect mismatch; unknown client; no `id_token` | 4 |

### Integration/E2E Tests
| What It Verifies | Task |
| --- | --- |
| Full auth-code sequence register→authorize(approve)→token→whoami; device id == scope | 5 |
| Gating off → authorize + auth-code branch 404; device-code/legacy unaffected | 5 |

## 12. Subjective Quality Gates (Optional)
| Criterion | Evaluation Prompt | Pass |
| --- | --- | --- |
| Consent page clarity | "Does this screen clearly show which app is signing in, the device, the permissions, and where it will redirect? YES/NO" | YES |

## 13. Migration Notes
No data migration. No new DB map (both stores are in-memory and ephemeral). New config `auth_code_ttl_secs` is additive with a default. All new surface defaults off (C1); enabling/disabling `next_gen_auth` is reversible.

## 14. References
- `matrix-rust-sdk` `crates/matrix-sdk/src/authentication/oauth/{mod,auth_code_builder,registration}.rs`; `bindings/matrix-sdk-ffi/src/{client,authentication}.rs`
- `oauth2` crate (public client → `AuthType::RequestBody`, no `client_secret`)
- MSC2964/2965/2966/2967; RFC 6749 §4.1, RFC 7636, RFC 8252 §7.3, RFC 7591, RFC 7009, RFC 9207
- Local precedents: `src/api/client/oauth/{token,consent,device,register,metadata}.rs`, `src/service/oauth_provider/{mod,clients,grants,consent,scope}.rs`, `src/api/client/session/{mod,sso}.rs`, `src/api/router.rs:177-198`
- Sibling plan: `msc4108-qr-login-PLAN.md` (this plan implements its §4 deferral)

---

## 15. Implementation Roadmap (Phased)

### Phase 0: Stores + helpers (independently testable, pure)
- [x] Task 1 (pending + code stores + config), Task 2 (PKCE + redirect helpers)
- **Exit criteria**: `cargo test oauth_provider::authorize oauth_provider::pkce oauth_provider::redirect` green.

### Phase 1: Authorization endpoint + consent
- [x] Task 3 (`/authorize` GET/POST, templates, consent reuse)
- **Exit criteria**: `cargo test oauth_authorize` green; redirect-validation, error-redirect, consent/CSRF, and gating tests pass.

### Phase 2: Token exchange (keystone)
- [x] Task 4 (`authorization_code` branch)
- **Exit criteria**: `cargo test authorization_code_grant` green; approved code yields a `find_from_token`-resolvable token bound to the scope device id; PKCE/replay/mismatch rejected.

### Phase 3: Wiring + conformance + docs
- [x] Task 5 (router/config/docs/E2E + gating)
- **Exit criteria**: full §6 gate suite green incl. the end-to-end conformance test and the gating test; docs updated.

---

## 16. Operational Playbook

### 16.1 Merge Gates
- Any PR touching the authorize/token endpoints MUST include the relevant unit tests (§11), pass all §6 gates, and keep `next_gen_auth` defaulting **off**.
- Any PR refactoring shared consent helpers MUST assert the `/link` (device-code) flow behavior is unchanged (C9).

### 16.2 Decision Discipline
- §7 decisions/ADRs are locked. Changing one (e.g. adding OIDC `id_token`, confidential clients, or `prompt=create` registration) requires updating this plan first.

### 16.3 Deliverables Order
1. Phase 0 (stores + helpers) — pure, fast feedback.
2. Phase 1 (authorize + consent).
3. Phase 2 (token branch) — keystone.
4. Phase 3 (wiring + E2E + docs).

### 16.4 Immediate Next Steps
1. Implement Task 1 (`oauth_provider/authorize.rs` stores + `auth_code_ttl_secs`) and its unit tests.
2. Implement Task 2 (`pkce.rs` + `redirect.rs`) and its unit tests.
3. Proceed to Task 3.

---

## Appendix A: Risk Register
1. **Shared-helper refactor regresses `/link`** — Likelihood: Medium; Impact: device-code consent breaks. Mitigation: extract pure helpers only; keep `/link` tests green (C9).
2. **Open redirect via lax `redirect_uri` match** — Likelihood: Medium; Impact: security. Mitigation: C2 + ADR-103 exact/loopback match, validated before any redirect; dedicated tests.
3. **PKCE bypass** — Likelihood: Low; Impact: code interception. Mitigation: C3 mandatory S256 verify; tests for missing/wrong verifier.
4. **Pending-store DoS via unauthenticated GET** — Likelihood: Low; Impact: memory. Mitigation: TTL + cap + eviction; global rate limiting.
5. **Native-app loopback ephemeral port** — Likelihood: Medium; Impact: login fails. Mitigation: ADR-103 loopback port-agnostic match.

## Appendix B: Performance Budgets
| Metric | Budget | How Measured |
| --- | --- | --- |
| `/authorize` GET/POST handler (excl. password hash) | < 5 ms p99 | in-memory store lookups; code review |
| Token `authorization_code` exchange | O(1) map consume + 1 SHA-256 | code review + test |

---

# MASTER TODO INVENTORY

## A) Stores + Helpers (Phase 0)
- [x] `src/service/oauth_provider/authorize.rs` — `PendingAuthorizations` + `AuthorizationCodes` (in-memory, single-use, TTL, cap, `_at`/`new_for_testing`)
- [x] Wire stores into `oauth_provider::Service` (`mod.rs`)
- [x] Config `auth_code_ttl_secs` (default 60) + `default_*` fn; pending TTL reuses `consent_session_ttl_secs`
- [x] `src/service/oauth_provider/pkce.rs` (`verify_s256`, `S256`)
- [x] `src/service/oauth_provider/redirect.rs` (`redirect_uri_matches`, exact + RFC 8252 loopback)
- [x] Tests: stores (create/consume/TTL/cap), PKCE vector, redirect matching

## B) Authorization Endpoint (Phase 1)
- [x] Extract shared consent helpers to `src/api/client/oauth/web.rs` (no `/link` behavior change)
- [x] `src/api/client/oauth/authorize.rs` — GET (validate → auth → consent/login render) + POST (login/approve/deny)
- [x] Templates `authorize_login.html`, `authorize_consent.html`
- [x] Register `GET`/`POST /_tuwunel/oauth/authorize` (gated)
- [x] Tests: redirect-validation error page; error redirects; consent auth+CSRF; approve→code; gating off→404

## C) Token Exchange (Phase 2)
- [x] `authorization_code` arm in `oauth_token_route` (parse, client/redirect/PKCE checks, single-use consume)
- [x] Parameterize initial device display name in `issue_native_token` ("OAuth login")
- [x] Reuse `device_id_owned_by_other_user` guard
- [x] Tests: success+binding; bad verifier; replay; client/redirect mismatch; unknown client; no `id_token`

## D) Wiring + Conformance + Docs (Phase 3)
- [x] Router review: authorize routes present + gated
- [x] End-to-end conformance test (register→authorize→token→whoami; device id == scope)
- [x] Gating test (authorize + auth-code branch 404 when disabled; device-code/legacy unaffected)
- [x] `docs/qr_login.md` updated (auth-code flow implemented; enables Element X/rust-sdk QR pairing)
- [x] `msc4108-qr-login-PLAN.md` §4 exclusion struck + back-reference
- [x] `tuwunel-example.toml` regenerated; CHANGELOG/`RELEASE.md` entry

## E) Verification
- [x] `cargo build`
- [x] `cargo clippy --workspace --all-targets`
- [x] `cargo test`
- [x] `cargo fmt --check`
