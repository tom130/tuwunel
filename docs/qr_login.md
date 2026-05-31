# QR Login

Tuwunel has experimental support for Matrix QR-code login. The MSC4108
rendezvous transport and the OAuth authorization-server endpoints used by this
flow are gated behind `next_gen_auth` and are disabled by default.

When enabled, the rendezvous endpoint stores short-lived opaque `text/plain`
payloads for client-to-client QR login handshakes. Tuwunel does not inspect,
decrypt, or interpret those payloads.

Relevant configuration:

```toml
[global]
next_gen_auth = true
rendezvous_ttl_secs = 60
rendezvous_max_sessions = 100
rendezvous_max_bytes = 4096
rendezvous_rate_limit_per_minute = 600
device_grant_expires_secs = 600
device_grant_interval_secs = 5
consent_session_ttl_secs = 600
```

The authorization-server endpoints are available when `next_gen_auth` is
enabled:

- `GET /_matrix/client/v1/auth_metadata` advertises the authorization-server
  endpoints, including the OAuth device-authorization grant.
- `GET /.well-known/matrix/client` includes `m.authentication` when
  `next_gen_auth` is enabled.
- `POST /_tuwunel/oauth/register` registers a public OAuth client using dynamic
  client registration. It requires an HTTPS `client_uri`, supports
  `token_endpoint_auth_method = "none"`, and returns a generated `client_id`.
- `POST /_tuwunel/oauth/device` starts the device-authorization grant and
  returns `device_code`, `user_code`, and verification URI fields for polling
  clients.
- `GET /_tuwunel/oauth/link?user_code=...` renders the browser approval flow.
  Users sign in with their homeserver password or an existing SSO provider, then
  explicitly approve or deny the pending QR sign-in.
- `POST /_tuwunel/oauth/token` exchanges an approved device code for native
  Matrix access and refresh tokens. The Matrix device id is taken from the
  requested device scope. The same endpoint also supports the OAuth
  `refresh_token` grant for rotating QR-login tokens.
- `POST /_tuwunel/oauth/revoke` revokes an access or refresh token and returns
  success for unknown tokens, matching RFC 7009.

## Approving a QR sign-in

The verification URI displays the pending `user_code`, the requesting Matrix
`device_id`, and the requested Matrix client API scope. Approval is submitted by
`POST /_tuwunel/oauth/link` with a CSRF token from a short-lived server-side
consent session. A GET request can show the page, but cannot approve a grant.

## Current limitations

The discovery metadata advertises `authorization_endpoint` and
`response_types = ["code"]` for compatibility with Matrix OAuth discovery, but
the browser `authorization_code` + PKCE flow for arbitrary OAuth clients is not
implemented by this QR-login work. QR login uses the device-code grant and
refresh-token grant only.
