# matrix.datadialect.cz — Tuwunel 1.8.2 deployment migration

Validated on 2026-08-04 against Tuwunel `adf1c0bd` plus the two fork commits in `update-1.8.2`. This document contains no secret values.

## Live deployment audit

The deployment is managed by Flux from `oci-flux/apps/tuwunel-datadialect.yaml`. The live HelmRelease and ConfigMap match Git revision `6c7e26c3a2a90a0e06b56f81086efbb871e43c20`.

- Workload: `matrix-datadialect/tuwunel-datadialect`
- Image before the upgrade: `ghcr.io/tom130/tuwunel@sha256:09b3203f81b9156b312b4c7732ed6845addd33ca7306c59414827aa9d1c35dfb`
- Reported version before the upgrade: `1.5.0 (b66e191)`
- Database PVC: `matrix-datadialect/tuwunel-datadialect-data`, 30 GiB, approximately 2.5 GiB used
- The registration and TURN secrets are Kubernetes Secret references; neither is copied here.
- The only deployed fork-only configuration key is `TUWUNEL_NEXT_GEN_AUTH=true`.
- `well_known.client`, `well_known.server`, and the MSC4143 LiveKit transport are already configured.

## Exact key map

| Fork 1.5 key | Upstream 1.8.2 key | Migration decision |
|---|---|---|
| `next_gen_auth` | `oidc_native_auth` and `rendezvous_enabled` | Set both to `true`. The fork used one switch for native OIDC and MSC4108; upstream separates them. |
| `rendezvous_ttl_secs` | `rendezvous_session_ttl` | Rename. Set `60` to preserve the fork default; upstream otherwise defaults to `600`. |
| `rendezvous_max_bytes` | `rendezvous_session_max_bytes` | Rename. Preserve `4096`. |
| `rendezvous_max_sessions` | `rendezvous_max_sessions` | Unchanged. Preserve `100`. |
| `rendezvous_rate_limit_per_minute` | `rendezvous_rc_per_second` and `rendezvous_rc_burst_count` | Convert the fork default `600/min` to `10/sec`; use upstream's `20` request burst. The new limiter is per client IP. |
| `device_grant_expires_secs` | removed | Upstream constant `DEVICE_GRANT_LIFETIME` is 30 minutes; the fork default was 10 minutes. Accepted behavioral difference. |
| `device_grant_interval_secs` | removed | Upstream constant remains 5 seconds, matching the fork default. |
| `consent_session_ttl_secs` | removed | Upstream `AUTH_REQUEST_LIFETIME` is 10 minutes, matching the fork default. |
| `auth_code_ttl_secs` | removed | Upstream `AUTH_CODE_LIFETIME` is 10 minutes; the fork default was 1 minute. Accepted behavioral difference. |
| `well_known.rtc_transports` | `well_known.rtc_transports` | Unchanged. The existing LiveKit entry is retained. |
| `device_key_update_encrypted_rooms_only` | same | Unchanged; the deployment does not override its default. |

New OIDC controls were reviewed:

- `oidc_require_pkce=true` is retained at its secure upstream default.
- `oidc_registration_access_token` remains empty so Element X can use dynamic client registration without a pre-shared secret.
- `oidc_registration_allowed_redirect_hosts=[]` remains unrestricted. A hostname allowlist would reject native-app redirect URIs without an ordinary HTTPS host, so it must not be tightened without testing every deployed client.
- `oidc_rc_per_second=0` and `oidc_rc_burst_count=0` retain the upstream default (optional OIDC throttling disabled). The device-code entry endpoint still has its mandatory built-in throttle.
- `rendezvous_authenticated_only=true` retains the upstream default for MSC4388. MSC4108 creation remains unauthenticated as required by that flow.

## Secret-free migrated fragment

The GitOps manifest uses environment variables for global keys and a mounted TOML file for RTC transport metadata. Replace `TUWUNEL_NEXT_GEN_AUTH` with:

```yaml
TUWUNEL_OIDC_NATIVE_AUTH: "true"
TUWUNEL_RENDEZVOUS_ENABLED: "true"
TUWUNEL_RENDEZVOUS_SESSION_TTL: "60"
TUWUNEL_RENDEZVOUS_SESSION_MAX_BYTES: "4096"
TUWUNEL_RENDEZVOUS_MAX_SESSIONS: "100"
TUWUNEL_RENDEZVOUS_RC_PER_SECOND: "10"
TUWUNEL_RENDEZVOUS_RC_BURST_COUNT: "20"
```

Retain the existing public TOML fragment:

```toml
[[global.well_known.rtc_transports]]
type = "livekit"
livekit_service_url = "https://lk-jwt.datadialect.cz"
```

The remaining deployment variables, including `TUWUNEL_WELL_KNOWN__CLIENT=https://matrix.datadialect.cz`, stay unchanged. The registration and TURN values continue to come from their existing Secret references.

## Local configuration proof

The 1.8.2 debug binary was started on `127.0.0.1:8008` with the fragment above, a scratch RocksDB database, and the existing public well-known values. Startup logged `Initializing OIDC server for next-gen auth (MSC2965)` and no `unknown to tuwunel` warning.

The first run revealed that upstream omitted the fork's `m.authentication` well-known field even though OIDC initialized. A red-first regression test and a minimal JSON response port now preserve that discovery field. After rebuilding, all of these assertions passed:

- `/_matrix/client/v1/auth_metadata`: issuer `https://matrix.datadialect.cz/`
- `/_matrix/client/versions`: `org.matrix.msc3861`, `org.matrix.msc4108`, and `org.matrix.msc4143` are `true`
- `/.well-known/matrix/client`: `m.authentication.issuer` and the configured `org.matrix.msc4143.rtc_foci` are present
- Both stable `/_matrix/client/v1/rtc/transports` and unstable MSC4143 transport routes return HTTP 200

## Production upgrade runbook

### Preconditions

1. Confirm the Tuwunel branch still contains no more than two commits above `upstream/main` and fork CI is green.
2. Record the new immutable GHCR digest and confirm it reports Tuwunel 1.8.2.
3. Confirm both Git backup refs still exist on `origin`.
4. Keep the migrated `oci-flux` commit ready locally, but do not push it while production still runs 1.5.0: removing `NEXT_GEN_AUTH` would disable the old OAuth implementation.
5. Create a dated local backup directory with at least 5 GiB free. The intended location is `/Users/tom130/_BACKUPS/tuwunel/<UTC timestamp>/`.

### Mandatory offline database backup

The four old column families are dropped during the first 1.8.2 open. A binary-only rollback is unsafe; a verified pre-upgrade database copy is mandatory.

1. Record the current pod image digest and reported federation version.
2. Scale `deployment/tuwunel-datadialect` to zero and wait until its pod is gone. Verify it remains at zero before and after the copy.
3. Create a temporary `busybox:1.37` pod named `tuwunel-offline-backup` that mounts PVC `tuwunel-datadialect-data` read-only at `/data`. The local-storage PV node affinity selects the correct node.
4. Stream `tar -C /data -cf - .` from that pod into a local `database.tar.zst` archive. Do not edit any RocksDB files.
5. Generate `database.tar.zst.sha256`, run `zstd -t database.tar.zst`, and record the source byte count and file count in `backup-evidence.txt`.
6. Delete the temporary backup pod. Keep the archive and checksum until the upgrade has been stable for at least one week.

### Deploy

1. Push the verified two-commit Tuwunel history to fork `main`; wait for fork CI to publish `ghcr.io/tom130/tuwunel:main` and record its digest.
2. Push the reviewed `oci-flux` migration commit to `main`.
3. Reconcile the Flux Git source and `apps` Kustomization. The environment-key change rolls the Deployment and `pullPolicy: Always` fetches the new image.
4. Wait for rollout availability. Follow the app logs and confirm version 1.8.2, schema version 17, the expected old-column drops, and OIDC initialization. Unknown-config warnings fail the rollout.
5. If the Deployment remains scaled to zero because the Helm release did not reconcile it, restore one replica only after the GitOps manifest and image digest are confirmed.

### Verify

1. Run the live auth-metadata, versions, well-known, stable/unstable RTC, federation-version, and password-login checks.
2. Complete dynamic OIDC registration plus authorization-code/PKCE login, and an MSC4108 QR/device-grant pairing.
3. Verify the registered appservice's device management, device assertion, cross-signing upload, plaintext-room device-list update, and one real bridge message.
4. Confirm Element Call sees the LiveKit focus and can establish a call.
5. Record the post-deploy pod image digest, all HTTP results, bridge health, and the backup archive checksum in the execution plan amendment.

### Roll back

1. Scale Tuwunel to zero. Preserve a separate copy of the failed post-upgrade database for diagnosis.
2. Create a writable recovery pod mounting the PVC. Validate that the target mount is exactly `/data`, then empty only that mount and extract the verified pre-upgrade archive into it.
3. Revert the GitOps config to `TUWUNEL_NEXT_GEN_AUTH=true` and pin the image to the recorded pre-upgrade digest `sha256:09b3203f81b9156b312b4c7732ed6845addd33ca7306c59414827aa9d1c35dfb`.
4. Reconcile Flux, restore one replica, and verify version 1.5.0, login, federation, bridge connectivity, and database health.
5. Never start the 1.5.0 binary against the database after it has been opened by 1.8.2 without restoring this backup first.
