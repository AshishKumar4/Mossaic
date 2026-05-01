# Mossaic operations

Concise operator reference. For the API surface see [`docs/integration-guide.md`](./integration-guide.md); for SDK consumer DX see [`sdk/README.md`](../sdk/README.md).

---

## Required secrets

| Secret | Purpose |
|---|---|
| `JWT_SECRET` | HS256 signing for session JWTs, VFS Bearer tokens, multipart upload tokens, download tokens, share tokens, signed preview-variant tokens, and the `listFiles` HMAC pagination cursor. **No dev fallback in source** &mdash; without it every `/api/vfs/*` route returns `503 EMOSSAIC_UNAVAILABLE`. |
| `JWT_SECRET_PREVIOUS` *(optional)* | Set during a graceful rotation window (see below). All token verifiers honor this slot &mdash; tokens minted under the old secret remain valid through the rotation window. |

```bash
wrangler secret put JWT_SECRET
```

## Pre-deploy checklist

The CI gate is `pnpm ci:check` which chains:

1. `pnpm typecheck` &mdash; full project tsc, exit 0.
2. `pnpm build:sdk` &mdash; production tsup build with strict DTS generation. This catches structural mismatches (e.g. a new RPC added to the worker but not declared on the SDK's `UserDOClient` interface) that the integration test harness's path-aliased TS resolution skips.
3. `pnpm lint:no-phase-tags` &mdash; refuses any `Phase NN` narration in production code. Tests use `Phase NN` as stable test IDs (excluded). Documentation under `docs/`, `local/`, `lean/`, and `README.md` is also out of scope. The single intentional location for `Phase NN` is `docs/scaling-roadmap.md`.

Also before each deploy:

- [ ] `pnpm test` &mdash; 929 cases pass (unit + integration + cli + browser e2e).
- [ ] `pnpm verify:proofs` &mdash; 226 Lean theorems, 0 `axiom`, 0 `sorry`; no xref drift.
- [ ] `JWT_SECRET` set on the target environment (`wrangler secret list`).
- [ ] `wrangler deploy --dry-run` &mdash; bundle clean, migrations match.
- [ ] Smoke against the deployed worker:
  ```bash
  curl -s -o /dev/null -w "%{http_code}\n" https://<worker>/api/health   # \u2192 200
  curl -s -o /dev/null -w "%{http_code}\n" \
    -H "Authorization: Bearer bogus" https://<worker>/api/vfs/exists \
    -X POST -H "Content-Type: application/json" -d '{"path":"/"}'        # \u2192 401
  ```

---

## Structured logging via the worker logger

Production code emits structured logs through `worker/core/lib/logger.ts`:

```ts
import { logInfo, logWarn, logError } from "@core/lib/logger";

logError("rate limit exceeded", { tenantId, requestId }, err, {
  event: "rate_limit_exceeded",
  bucket: "vfs",
});
```

Output is JSON-stringified single-line `console.error` / `console.warn` / `console.log`. Workers Logs and Logpush v2 parse this as structured fields without further configuration.

Common shape:

```json
{
  "ts": 1735776000123,
  "level": "error",
  "msg": "alarm handler failure",
  "requestId": "5fb...",
  "tenantId": "default::acme-corp",
  "event": "alarm_handler_failed",
  "errCode": "EAGAIN",
  "errMsg": "rate limit exceeded for tenant: 100 ops/sec, 200 burst",
  "errStack": "VFSError: ...\n    at ...",
  "sweepKind": "stale_tmp_sweep"
}
```

### Recommended Logpush queries

The `event:` field is the load-bearing discriminator. Filter Logpush sinks (R2 / S3 / Splunk / queues) by it:

| Filter | What you'll see |
|---|---|
| `event = "alarm_handler_failed"` | Alarm-handler exceptions per tenant. Surfaces what bare `catch {}` would otherwise swallow. |
| `event = "shard_capacity_soft_cap_exceeded"` | A ShardDO crossed the 9 GiB soft cap. Operator's signal that pool growth is keeping pace (or not). |
| `event = "inline_tier_cap_first_crossing"` | A tenant first crossed the 1 GiB inline-tier cap. New writes spill to chunked tier. |
| `event = "pool_growth_forced_by_full_shards"` | All shards in a tenant's pool were over the soft cap; an extra shard was forced. Rare. |
| `event = "rate_limit_exceeded"` | Per-tenant token-bucket pressure. Default 100 ops/sec, 200 burst. |
| `errCode = "EAGAIN"` | Same as above, surfaced via the typed-error path. |
| `errCode = "EMOSSAIC_UNAVAILABLE"` | `JWT_SECRET` missing or empty. Set the secret. |

### Quick `wrangler tail` filters

```bash
wrangler tail --format json | jq 'select(.outcome != "ok")'
wrangler tail --format json | jq 'select(.logs[]?.message[]? | fromjson? | .level == "error")'
wrangler tail --format json | jq 'select(.logs[]?.message[]? | fromjson? | .event == "alarm_handler_failed")'
wrangler tail --format json | jq 'select(.logs[]?.message[]? | fromjson? | .errCode == "EAGAIN")'
```

---

## Request-ID correlation

Every `/api/*` request gets a `crypto.randomUUID()` correlation id assigned at the Worker edge by `requestIdMiddleware`. The id is mirrored onto the response as `X-Mossaic-Request-Id` and made available to route handlers via `c.var.requestId`. Every `logInfo` / `logWarn` / `logError` call inside the request scope carries the same id in its `requestId` field.

Operators tracing a user-reported failure can:

1. Get the id from the response header (`X-Mossaic-Request-Id`) in the user's HAR file or curl trace.
2. Filter Logpush by that id to see every log line emitted under that request, across the Worker, the UserDO, and any fan-out into ShardDOs.

The middleware honors caller-supplied ids when valid (regex-gated) so an upstream proxy or SDK client can thread its own correlation id end-to-end:

```bash
curl -H "X-Mossaic-Request-Id: my-trace-abc123" https://mossaic/api/health
```

---

## Audit log

Every destructive operation writes a row to a per-tenant `audit_log` table inside the UserDO. Operators answer "did tenant X delete this file?" with one SQL query.

### Schema

```sql
CREATE TABLE audit_log (
  id          TEXT PRIMARY KEY,    -- ULID
  ts          INTEGER NOT NULL,    -- ms epoch
  op          TEXT NOT NULL,       -- 'unlink' | 'purge' | 'archive' | ... (see below)
  actor       TEXT NOT NULL,       -- userId, 'operator', or 'system'
  target      TEXT NOT NULL,       -- path / fileId / share-token jti
  payload     TEXT,                -- small JSON blob (\u22481 KB)
  request_id  TEXT                 -- correlation id when minted via /api/*
);
CREATE INDEX idx_audit_log_op_ts ON audit_log(op, ts DESC);
CREATE INDEX idx_audit_log_ts    ON audit_log(ts DESC);
```

### Op classes recorded

`unlink`, `purge`, `archive`, `unarchive`, `rename`, `removeRecursive`, `restoreVersion`, `dropVersions`, `adminSetVersioning`, `adminDedupePaths`, `adminReapTombstonedHeads`, `adminPreGenerateStandardVariants`, `adminWipeAccountData`, `shareLinkMint`, `accountDelete`. The full union lives in `worker/core/objects/user/vfs/audit-log.ts:AuditLogOp`.

### Common operator queries

The audit log lives on the per-tenant UserDO. To query, use the operator-side admin RPC tooling against the tenant's DO instance.

```sql
-- Last 50 destructive ops by a tenant
SELECT ts, op, actor, target, payload
  FROM audit_log
 WHERE actor = 'tenant-X-userId'
 ORDER BY ts DESC
 LIMIT 50;

-- All purges in the last 24h
SELECT ts, actor, target, payload
  FROM audit_log
 WHERE op = 'purge' AND ts > strftime('%s','now','-1 day') * 1000
 ORDER BY ts DESC;

-- All admin actions on this tenant
SELECT ts, op, actor, target, payload
  FROM audit_log
 WHERE op LIKE 'admin%' OR op IN ('accountDelete', 'shareLinkMint')
 ORDER BY ts DESC;

-- A specific request's audit trail (cross-reference with Logpush)
SELECT ts, op, target, payload
  FROM audit_log
 WHERE request_id = '5fb1...';
```

### Retention

Per-tenant cap defaults to **10,000 rows**; oldest rows trim down to a 9,800-row floor when the cap is exceeded. Trim runs from the existing UserDO alarm; no separate scheduling. Operators can override per-tenant via `vfs_meta.key='audit_log_max_rows'`:

```sql
INSERT OR REPLACE INTO vfs_meta (key, value) VALUES ('audit_log_max_rows', '50000');
```

Insertion is best-effort &mdash; a SQL failure during `insertAuditLog` is swallowed and logged via `logError(event="audit_log_insert_failed")`. Audit-log infrastructure must not block the destructive op it's recording.

---

## Alarm-handler failure alerting

The UserDO alarm handles four sweeps in sequence: stale tmp-row reaper, expired multipart sessions, shard capacity poll, and audit-log retention trim. Bare `catch {}` is forbidden; every alarm exception goes through `recordAlarmFailure`:

1. Logs `event=alarm_handler_failed` via `logError` with `sweepKind` (`stale_tmp_sweep` / `multipart_sweep` / `shard_capacity_poll` / `audit_log_reap`) and the underlying error code/message/stack.
2. Increments a persistent `vfs_meta.alarm_failures` counter (UPSERT pattern with INTEGER cast).
3. Continues processing &mdash; alarms have at-least-once retry semantics; throwing replays the alarm without progress on the remaining work.

### Alerting

Watch two signals:

```bash
# Logpush stream
event = "alarm_handler_failed"

# vfs_meta counter (per-tenant)
SELECT value FROM vfs_meta WHERE key = 'alarm_failures';
```

A steadily-rising counter on a single tenant signals a permanent failure in their alarm path (e.g. corrupted tmp row that blocks every subsequent stale-sweep). The counter + structured log surface the failure without operator paging on every transient ShardDO RPC error.

---

## Monitoring SLOs

From prod traffic on `mossaic.ashishkumarsingh.com`:

| Operation | Target | Notes |
|---|---|---|
| `/api/health` | p99 < 50 ms | regional cache |
| `/api/vfs/stat` | p99 < 80 ms | one DO RPC |
| `/api/vfs/readFile` (\u2264 1 MB) | p99 < 250 ms | single chunk fan-in |
| `/api/vfs/writeFile` (1 MB) | p99 < 350 ms | tmp insert + commitRename |
| `/api/vfs/multipart/finalize` | p99 < 600 ms | shard fan-in + commitRename |
| `GET /api/vfs/preview-variant/:token` (cache hit) | p99 < 30 ms | Workers Cache + CDN edge; bypasses the Worker entirely after first warmup |
| `GET /api/gallery/image/:fileId` (range request) | p99 < 200 ms for first 1 MB; 304 for `If-None-Match` |  |

---

## Common failure modes

### `503 EMOSSAIC_UNAVAILABLE`

`JWT_SECRET` missing or empty. `wrangler secret put JWT_SECRET`; \u2264 60 s propagation.

### `401 Invalid or expired token` after rotation

If you rotated `JWT_SECRET` without staging `JWT_SECRET_PREVIOUS` first, every outstanding token is invalidated. SPA users are logged out; CLI profiles need `mossaic auth setup --secret <newvalue>`. Use the graceful path (below) for routine rotations.

### `409 EBUSY` on writes for a single tenant

UNIQUE-INDEX corruption during a partial migration. Run:

```bash
# operator-only RPC \u2014 drives admin.dedupePaths(userId, scope)
mossaic admin dedupe --tenant <userId>
```

### `410 Gone` on a preview-variant URL

The token references a variant whose underlying `chunk_hash` no longer matches (re-render produced different bytes). SPA recovery is to re-mint via `vfs.previewInfo(path, opts)` and retry. The 410 error message includes "re-mint token" to make this clear.

### Storm of `EAGAIN`

Per-tenant rate limit (default 100 ops/s, burst 200). Either back off the client or raise the limit on the UserDO row directly. Watch Logpush by `event = "rate_limit_exceeded"` to spot patterns.

### Steadily-rising `vfs_meta.alarm_failures` counter

A permanent error in the alarm path of a specific tenant. Logpush by `event = "alarm_handler_failed"` to see the underlying error and the failing `sweepKind`. Common causes: corrupted tmp row blocking every stale-sweep tick, ShardDO scope drift after a migration.

---

## Graceful `JWT_SECRET` rotation (zero-downtime)

All token verifiers &mdash; `verifyJWT`, `verifyVFSToken`, `verifyVFSMultipartToken`, `verifyVFSDownloadToken`, `verifyShareToken`, `verifyPreviewToken` &mdash; accept tokens signed with **either** `env.JWT_SECRET` (current) or `env.JWT_SECRET_PREVIOUS` (rotation-window-only). Signing always uses `env.JWT_SECRET`. This lets you rotate without invalidating any outstanding session JWT, VFS-Bearer token, in-flight multipart upload session, pre-minted download URL, share link, or signed preview URL.

1. **Generate the new secret**:
   ```bash
   openssl rand -base64 48
   ```
2. **Stage the previous-secret slot**:
   ```bash
   wrangler secret put JWT_SECRET_PREVIOUS  # paste the CURRENT JWT_SECRET value
   ```
3. **Promote the new secret**:
   ```bash
   wrangler secret put JWT_SECRET           # paste the NEW value
   ```
4. **Wait for the drain window** &mdash; the longest of:
   - `JWT_EXPIRATION_MS` (\u2248 30 days for session JWTs; see `shared/constants.ts`)
   - 15 minutes for VFS-Bearer tokens
   - 1 hour for multipart upload tokens
   - 1 hour for download tokens
   - 24 hours for preview-variant tokens
   - 90 days for share tokens (`SHARE_TOKEN_DEFAULT_TTL_MS`)
5. **Drop the previous-secret slot**:
   ```bash
   wrangler secret delete JWT_SECRET_PREVIOUS
   ```

### Hard cutover (emergency only)

After a confirmed leak: skip the multi-secret window &mdash; `wrangler secret put JWT_SECRET <new>` and notify users that re-auth is required. Every outstanding token returns 401 immediately.

---

## Sign-off

Fill out per deploy and store in your incident-tracking system:

```
Deploy:        <wrangler deployment id>
Date / time:   <UTC>
Operator:      <name>
Commit SHA:    <git rev-parse HEAD>
Pre-deploy:    [\u2713] tests, [\u2713] ci:check, [\u2713] verify:proofs, [\u2713] dry-run, [\u2713] JWT_SECRET set
Smoke:         [\u2713] /api/health 200, [\u2713] /api/vfs/exists 401 on bogus token
Rollback ref:  <previous wrangler deployment id> + <git tag>
```
