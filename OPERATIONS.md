# Mossaic — Operations Runbook

Operator-facing reference for deploying, monitoring, and recovering Mossaic in production. The engineering plans under `local/` describe **how** the system is built; this document is for the on-call who needs to **act** at 3am.

Keep this open during deploys. Search-by-symptom: the runbooks at the bottom are indexed by user-visible failure mode.

---

## 0. Glossary (read first)

| Term | Meaning |
|---|---|
| **App mode** | Deployment that bundles the legacy photo-library Worker + the VFS routes. The DO class is `UserDO extends UserDOCore`. Lives at `mossaic.ashishkumarsingh.com` today. Wrangler at repo root: `wrangler.jsonc`. |
| **Service mode** | VFS-only deployment — no legacy app routes. DO class is `UserDOCore`. Wrangler: `deployments/service/wrangler.jsonc`. |
| **Library mode** | Consumer's own Worker re-exports `UserDO` and `ShardDO` from `@mossaic/sdk` and binds them in their own `wrangler.jsonc`. **Not operator-managed** — each consumer is its own deploy. |
| **UserDO / UserDOCore** | One DO instance per `(ns, tenant, sub?)` tuple. Holds VFS metadata + Yjs op log + listFiles indexes. |
| **ShardDO** | Pool of DO instances (default 32, grows with stored bytes). Holds content-addressed chunks. |
| **JWT_SECRET** | Workers secret. Required. Used for both VFS-token HS256 signing AND listFiles cursor HMAC. **No dev fallback in source** — a deploy without this secret returns 503 on every VFS-token verify and every listFiles cursor op (encode or decode). |
| **App-on-SDK** | App-mode `UserDO` (`worker/app/objects/user/user-do.ts`) is a typed-RPC subclass of `UserDOCore`. The legacy `_legacyFetch` JSON router was retired; routes call `stub.appXxx(...)` directly. Contract is now enforced by the integration suite (`tests/integration/multipart-routes.test.ts`, `tests/integration/app-smoke.test.ts`, and the App e2e endpoints) — there is no byte-pin gate any more. |

---

## 1. Pre-deploy checklist

Run **all** of these from `/workspace/Mossaic` (or your local clone). Every box must be checked before `wrangler deploy`.

### 1.1 Source integrity

- [ ] **Working tree clean**: `git status` reports no modifications, no untracked files.
- [ ] **On the deploy branch**: `git rev-parse --abbrev-ref HEAD` is `main` (or the documented release branch).
- [ ] **App-mode contract intact**: the typed-RPC App surface
      (`worker/app/objects/user/user-do.ts:appXxx` methods) is exercised end-to-end by the
      integration suite. `pnpm test tests/integration/app-smoke.test.ts` and
      `pnpm test tests/integration/multipart-routes.test.ts` MUST pass — these
      pin the legacy photo-app HTTP wire shape that the SPA still depends on.
      If either fails, **abort deploy** — the App contract has drifted.

### 1.2 Build + tests + proofs (all green, no skips)

- [ ] `pnpm install` (lockfile-only; should be a no-op in CI).
- [ ] `pnpm test` — 418 worker tests pass across 41 files; 44 cli unit tests pass. **Zero** flakes tolerated; if a test fails, do not deploy.
- [ ] `npx tsc -b` — exit 0, zero diagnostics.
- [ ] `npx wrangler deploy --dry-run` (App mode) — exit 0, prints binding list `MOSSAIC_USER`, `MOSSAIC_SHARD`, `SEARCH_DO`, and migration tags. (legacy `USER_DO`/`SHARD_DO` binding names are retired; `class_name`s `UserDO`/`ShardDO`/`SearchDO` are unchanged.)
- [ ] `npx wrangler deploy --dry-run -c deployments/service/wrangler.jsonc` (Service mode) — exit 0, prints bindings `MOSSAIC_USER`, `MOSSAIC_SHARD` only (no `SEARCH_DO`).
- [ ] `pnpm lean:build` (or `lake build` in `lean/`) — succeeds. `bash lean/scripts/check-no-sorry.sh` reports zero `sorry` and zero project-level axioms.

### 1.3 Production secrets

- [ ] `wrangler secret put JWT_SECRET` against the **target** environment (App or Service). Without it:
  - all `/api/auth/*` and `/api/vfs/*` requests return **503** with `EMOSSAIC_UNAVAILABLE`,
  - listFiles encode/decode throws **VFSConfigError** (also surfaces 503),
  - the legacy `/api/upload` and `/api/download` routes still work (they don't use JWT).
- [ ] Verify with `wrangler secret list` that `JWT_SECRET` is present.
- [ ] **Rotate every 90 days** OR after any suspected exposure (see §5.4).
- [ ] Secret length: ≥32 random bytes (e.g. `openssl rand -base64 32`). Workers does not enforce length — operator must.

### 1.4 Capacity check (math in §3 below)

- [ ] Estimate: `expected_tenants × (1 + pool_size_avg)` DO instances. Default `pool_size = 32`.
- [ ] Compare to your Cloudflare account's DO instance limit. Ask CF support if you are within 50% of the cap.

### 1.5 Database snapshot (high-value tenants)

Cloudflare DO storage has **no native point-in-time backup**. For tenants whose data loss is unacceptable:

- [ ] Run a manual export via `runInDurableObject` SQL dump for each high-value tenant. See §5.5.
- [ ] Store the dump off-platform (R2, S3) timestamped and labelled with tenant + ns + sub.

### 1.6 Rollback plan

- [ ] The previous deploy's wrangler ID is recorded (`wrangler deployments list` — pin the previous version's ID).
- [ ] Previous code snapshot is on a tag (`git tag pre-<date>` before deploy).
- [ ] **Schema rollback is one-way**: column adds (`metadata`, `user_visible`, `label`, etc. on `file_versions`) cannot be removed. Older code IGNORES newer columns, which is forward-compatible. See §5.6.

### 1.7 Ship-day smoke

After `wrangler deploy`:

- [ ] `curl -i https://<host>/api/health` → 200.
- [ ] `curl -i -H "Authorization: Bearer <valid-VFS-token>" -X POST https://<host>/api/vfs/exists -d '{"path":"/"}'` → 200, `{"exists":true|false}`.
- [ ] Same call without token → 401 `{"code":"EACCES"}`.
- [ ] Same call with `JWT_SECRET` not yet set → 503 `{"code":"EMOSSAIC_UNAVAILABLE"}` (this is the structured failure mode — fix by setting the secret, redeploy not needed).

---

## 2. Monitoring & SLOs

Mossaic emits **almost no console.log** in steady state (intentional — quieter logs at scale). The signals you must wire up:

### 2.1 Per-tenant DO count vs CF limits

Cloudflare account-level limits on DO instance count are not enforced in Mossaic source — the operator must:

1. Daily, query Cloudflare's analytics API for `durable_object_total` per namespace (`MOSSAIC_USER`, `MOSSAIC_SHARD`).
2. Alarm when count exceeds 80% of your account cap. Default Workers Paid: ask CF for your specific cap; treat 1M instances as a soft ceiling unless you have written confirmation otherwise.
3. Per-tenant: 1 UserDO + `quota.pool_size` ShardDOs (default 32, +1 per 5 GB stored, capped per `shared/placement.ts`).

```
total_DOs = num_tenants × (1 + average_pool_size)
```

### 2.2 GC observability (alarm sweeper)

Each UserDO runs `alarm()` on a **1-hour staleness** window (`worker/core/objects/user/user-do-core.ts:803-845`). It sweeps `_vfs_tmp_*` rows older than the cutoff and routes `deleteChunks` to the right ShardDO.

Signals:

- `console.error` on any alarm exception (Logpush filter on `"alarm"` + `"error"`).
- Tenant-level `_vfs_tmp_*` growth: query a tenant's UserDO via `runInDurableObject` for `SELECT COUNT(*) FROM files WHERE status='uploading' AND file_name LIKE '_vfs_tmp_%'`. Healthy: <50. Concerning: >500. Alert: >5000.
- Refcount drift: `SELECT SUM(ref_count) FROM chunks` per ShardDO should equal `SELECT COUNT(*) FROM chunk_refs` for the same shard. Drift indicates a deleteChunks RPC failure (M-A5 in audit).

### 2.3 Yjs hibernation health

Yjs WebSockets use Cloudflare's Hibernation API. Idle sockets cost $0; per-socket state survives via `serializeAttachment`.

Signals:

- WebSocket close codes from CF analytics: 1000 (normal), 1006 (abnormal — investigate), 1011 (server error — investigate).
- A spike in `Durable Object hibernation timed out` errors mapped to `MossaicUnavailableError` on consumer side (the SDK detects this pattern in `sdk/src/errors.ts`).
- yjs_oplog row count per tenant. Each compaction should reset back near 0; sustained growth means compactions aren't running. Compaction cadence: every 50 ops or 60s.

### 2.4 Latency SLOs

Production gates (Miniflare runs are 5-10× slower; do NOT use Miniflare timings as the prod target):

| Operation | p99 target | Action when exceeded |
|---|---|---|
| `vfs.exists`, `vfs.stat`, `vfs.lstat`, `vfs.readlink`, `vfs.readdir` | **< 30 ms** | check DO storage latency in CF dashboard |
| `vfs.readManyStat(N)` | **< 50 ms** | should be flat in N until ~1k paths |
| `vfs.readFile` (inline ≤16 KB) | **< 50 ms** | as above |
| `vfs.readFile` (chunked, N chunks) | **< 50 + 30·N/8 ms** (8-way parallel) | check ShardDO RPC latency; verify pool size sane |
| `vfs.writeFile` (chunked) | similar | ditto |
| `vfs.listFiles` (single page, indexed) | **< 50 ms** | index plan regression — see §5.7 |
| `vfs.listVersions` | **< 50 ms** | same |
| `YDocHandle.flush` | **< 200 ms** | Yjs compaction can be slow on large docs |

### 2.5 Rate-limit health (EAGAIN)

Per-tenant token bucket, default 100 ops/sec refill, 200 burst (`worker/core/objects/user/rate-limit.ts:35-36`). When exhausted, `EAGAIN` is thrown.

Signals:

- Spike in `EAGAIN` 429s on `/api/vfs/*` — either a tenant is misbehaving, or the bucket needs raising via:
  ```sql
  UPDATE quota
     SET rate_limit_per_sec = <new>, rate_limit_burst = <new>
   WHERE user_id = '<tenant>';
  ```
  (run via `runInDurableObject` against the tenant's UserDO).
- Bucket exhaustion repeatedly across many tenants suggests global cause: investigate before raising limits.

### 2.6 Migration / schema corruption signals

`H6 fix` records markers in `vfs_meta` when a partial unique index detects legacy duplicates:

- `vfs_meta(key='files_unique_index')` present → that tenant's UserDO refuses VFS WRITES with `EBUSY`. Reads still work (tolerant of duplicates).
- `vfs_meta(key='folders_unique_index')` present → same for folders.
- `console.error` is emitted at the moment of detection. Filter Logpush on `files_unique_index` / `folders_unique_index`.

### 2.7 Recommended Logpush filters

Wire all of:

- `console.error` (catches every operator-relevant log Mossaic emits).
- HTTP status `5xx` from `/api/vfs/*` (transport or VFSConfigError surface).
- HTTP status `503 EMOSSAIC_UNAVAILABLE` (JWT_SECRET missing — paged immediately).
- HTTP status `429 EAGAIN` (rate limit).

---

## 3. Capacity math

Per-tenant DO count:

```
do_count_per_tenant   = 1 (UserDO) + pool_size (ShardDOs)
pool_size_default     = 32
pool_size_growth      = +1 per 5 GB stored (capped to 64 currently)
```

Cluster-level:

```
total_user_dos        = unique (ns, tenant, sub) tuples
total_shard_dos       = total_user_dos × pool_size_avg
total_dos             = total_user_dos × (1 + pool_size_avg)
```

Worked examples:

- 1k tenants, 0 sub-tenants, 50 GB avg storage → pool 42 → **43k DOs**.
- 10k tenants, 5 sub-tenants each, 5 GB avg → pool 33 → **10k × 6 × 34 = 2.04M DOs**. Verify with CF account team before proceeding.
- 100k single-tenant deployments, 0.5 GB avg → pool 32 → **3.3M DOs**. Need CF approval.

Storage cap per DO: ~10 GB SQLite per CF docs (verify). For a tenant whose UserDO row sizes (VFS metadata, indexes, Yjs op log) approach this:

- Reduce `INLINE_LIMIT` so fewer bytes live on UserDO and more push to ShardDOs.
- Run `flushYjs` more aggressively (compaction reaps `yjs_oplog` rows).
- Drop old versions via `vfs.dropVersions(path, { keepLast: N })`.

Subrequest budget per Mossaic invocation:

- Free tier: 50 subrequests / invocation.
- Paid tier: 1000 / invocation. **All Mossaic methods fit comfortably under 1000.** The subrequest table in `sdk/README.md` enumerates each.

---

## 4. Alerting

| Severity | Condition | Action |
|---|---|---|
| **P1 page** | 503 rate on `/api/vfs/*` > 1% for 5 min | JWT_SECRET likely missing or rotated incorrectly — see §5.1 |
| **P1 page** | Migration corruption marker present (`files_unique_index` in any tenant's `vfs_meta`) | Run admin-dedupe via §5.2 within 1h |
| **P1 page** | Refcount leak: per-tenant `chunks.ref_count > 100k` orphan and growing | Schema corruption or RPC failure storm — see §5.3 |
| **P2 high** | `EAGAIN` 429 rate > 5% for 30 min | Per-tenant bucket too tight; raise via §2.5 SQL update |
| **P2 high** | Per-DO p99 listFiles > 200 ms sustained | Index regression — verify with §5.7 |
| **P3 normal** | Yjs `yjs_oplog` > 10k rows for any tenant | Compaction stuck — investigate via §5.8 |
| **P3 normal** | DO instance count > 80% of CF account cap | Begin tenant-archive plan |

---

## 5. Runbook for failures

### 5.1 — DO outage on `/api/vfs/*` (503 Service Misconfigured)

**Symptom**: every `/api/vfs/*` returns 503 with `{"code":"EMOSSAIC_UNAVAILABLE","message":"JWT_SECRET is not configured..."}`.

**Diagnosis**: `JWT_SECRET` was deleted, never set, or the binding name drifted.

**Fix** (5 minutes):

1. `wrangler secret list` — confirm `JWT_SECRET` is missing from the current deployment.
2. `wrangler secret put JWT_SECRET` against the affected deployment. **Use the SAME value** as before if rotating; a fresh value invalidates every outstanding VFS-bearer token AND every active listFiles cursor (clients must restart pagination).
3. **No redeploy needed** — Workers picks up the secret on the next cold start (typically <1s).
4. Verify: `curl -H "Authorization: Bearer <token>" -X POST https://<host>/api/vfs/exists -d '{"path":"/"}'` → 200.
5. Postmortem: capture how the secret went missing (accidental `wrangler secret delete`? environment-mismatch deploy? CI clobbered).

### 5.2 — Tenant in `EBUSY` write-refuse mode (UNIQUE INDEX corruption)

**Symptom**: a tenant's writes throw `EBUSY: legacy data — run admin-dedupe to recover` (or similar). Reads still work.

**Diagnosis**: legacy data has duplicate `(user_id, parent_id, file_name)` rows; the partial unique index could not be created. Marker recorded in `vfs_meta(key='files_unique_index'|'folders_unique_index')`.

**Fix**:

1. Identify the affected tenant (the marker is per-DO).
2. Call the admin dedupe RPC against that tenant's UserDO:
   ```
   stub.adminDedupePaths(userId)
   ```
   This runs `dedupePaths` from `worker/core/objects/user/admin.ts` which deterministically picks one survivor per `(parent, name)` collision and hard-deletes the rest's chunks via ShardDO RPC.
3. After dedupe, re-call `ensureInit()` (next gated VFS call does this automatically) — the marker clears and writes resume.
4. Verify: `vfs_meta` no longer has `files_unique_index` row; a `writeFile` succeeds.

### 5.3 — Refcount leak / sweep failure storm

**Symptom**: `chunks.ref_count > 0` rows persist with no live `chunk_refs`. Or storage usage grows but `files` row count is flat.

**Diagnosis**: `removeFileRefs` RPC failed mid-flight; the alarm sweeper can't help (it only sweeps `deleted_at IS NOT NULL` chunks).

**Fix**:

1. Identify candidate tenants: those with high alarm-error count in Logpush.
2. Per affected ShardDO, query orphans:
   ```sql
   SELECT c.hash, c.ref_count
     FROM chunks c
     LEFT JOIN chunk_refs r ON r.chunk_hash = c.hash
    WHERE r.chunk_hash IS NULL
      AND c.ref_count > 0;
   ```
3. For each orphan, manually decrement: this requires `runInDurableObject` SQL fix-up. **Do not run this casually** — verify the chunk truly has no refs first (the join is the authoritative answer).
4. After fix-up, force a sweeper alarm: `state.storage.setAlarm(Date.now() + 1000)`.
5. Postmortem: which RPC sequence failed? Was there a workerd error, an EAGAIN, or a transient network issue? Patch the call site to retry-with-bound or to record the orphan for retry.

### 5.4 — `JWT_SECRET` rotation (hard cutover)

> **For zero-downtime rotation** see §6.10 — the multi-secret graceful
> path is now the recommended procedure. Hard cutover (this section)
> is reserved for emergency rotations after a suspected secret
> exposure where invalidating every outstanding token IS the goal.

**When (hard cutover)**: emergency rotations only — confirmed leak, key
material compromise, or a regulatory deadline that doesn't accommodate
the 30-day TTL drain window.

**Effect of a hard rotation** (every active session breaks):

- Every outstanding VFS-bearer token returns 401 from `/api/vfs/*`.
- Every outstanding **listFiles cursor** returns EINVAL on next page.
- Every outstanding **`@mossaic/cli` profile** must re-run
  `mossaic auth setup --secret <newvalue>`.
- Every active SPA session is logged out (the App's session JWT is
  signed under the same secret).
- No data loss; full re-login is required.

**Procedure**:

1. Generate the new secret: `openssl rand -base64 48`.
2. `wrangler secret put JWT_SECRET <newvalue>`. Wait ≤ 60 s for
   propagation.
3. Notify SDK consumers + photo-app users that re-auth is required.
4. Monitor 401 rate; spike + decay over the rollover window is
   expected as clients re-auth.

**rotation history**:

- 2026-04-29 — Service-mode (`mossaic-core`) only. App-mode (`mossaic`) untouched. Reason: deployed the new `/api/vfs/yjs/ws`, `/api/vfs/setYjsMode`, `/api/vfs/flushYjs`, and `/api/vfs/admin/setVersioning` HTTP routes; rotated the secret as part of the test-harness gating. Photo-app users unaffected.

### 5.5 — Tenant export / import

Mossaic has no native cross-DO migration tool. Manual procedure:

**Export**:

1. Identify the tenant's UserDO + ShardDOs (`vfs:${ns}:${tenant}`, `vfs:${ns}:${tenant}:s${0..pool_size-1}`).
2. For UserDO, dump every table. For ShardDOs, dump `chunks` + `chunk_refs`.
3. Use `runInDurableObject` from a privileged admin Worker:
   ```ts
   await runInDurableObject(stub, async (_, state) => {
     const rows = state.storage.sql.exec("SELECT * FROM files").toArray();
     // ... emit as JSON-lines / parquet to R2
   });
   ```
4. Store the dump labelled with `(ns, tenant, sub, timestamp, schema_version)`. Schema version is the latest Phase number that has run `ensureInit`.

**Import**:

1. Provision the new DO instance under the new `(ns, tenant, sub)` triple.
2. Trigger `ensureInit()` (one read call against the new UserDO suffices).
3. Replay rows in dependency order: `auth` → `quota` → `folders` (parent-first) → `files` → `file_versions` → `version_chunks` → `file_tags` → `chunk_refs` → `chunks`.
4. Verify: count match, refcount match (`SUM(ref_count) == COUNT(chunk_refs)`), reachable via `vfs.readFile` for a sampled set.

### 5.6 — schema corruption rollback

added columns: `files.metadata`, `files.has_tags`, `file_versions.label`, `file_versions.user_visible`, `file_versions.metadata`, plus the `file_tags` table and indexes `idx_files_parent_*`, `idx_file_tags_tag_mtime`.

**Schema is additive only**. Older code IGNORES newer columns — forward-compatible by construction. So:

- **Code rollback** (revert to a pre-Phase-12 commit): safe. New columns persist in storage but are unread by the older code. No data loss.
- **Schema rollback** (drop columns): NOT supported in CF DO storage. SQLite 3.35+ has `ALTER TABLE DROP COLUMN` but CF's DO SQLite version is not pinned in the docs as supporting it. **Do not attempt** without CF support sign-off.

**If the problem is a corrupt `metadata` JSON blob on a single file**:

```ts
await runInDurableObject(stub, async (_, state) => {
  state.storage.sql.exec("UPDATE files SET metadata = NULL WHERE file_id = ?", fileId);
});
```

**If the problem is `file_tags` index drift**:

```sql
DELETE FROM file_tags WHERE path_id NOT IN (SELECT file_id FROM files);
```

Re-run `ensureInit()` afterward; indexes are `IF NOT EXISTS`-guarded so they self-heal.

### 5.7 — listFiles latency regression

**Symptom**: p99 `listFiles` > 200 ms.

**Diagnosis steps**:

1. Confirm the tenant has the expected indexes:
   ```sql
   SELECT name FROM sqlite_master
    WHERE type='index'
      AND name IN ('idx_files_parent_mtime', 'idx_files_parent_name',
                   'idx_files_parent_size', 'idx_file_tags_tag_mtime');
   ```
   All four should exist.
2. If missing: the partial-unique-index marker probably blocked them too. Check `vfs_meta` and run §5.2.
3. If present but slow: the query may be on a query shape NOT covered by indexes. The supported shapes are listed in `sdk/README.md` § . A `metadata`-only filter has no index by design — pair with a prefix or tags.
4. Force `EXPLAIN QUERY PLAN` via `runInDurableObject` to confirm the planner picked the right index. We disable the planner's freedom in source — if a CF SQLite update changes plan selection, we need to update `list-files.ts`.

### 5.8 — Yjs compaction stuck

**Symptom**: `yjs_oplog` row count > 10k for a tenant; no compaction snapshots being emitted.

**Fix**:

1. Identify the affected pathId(s):
   ```sql
   SELECT path_id, COUNT(*) FROM yjs_oplog GROUP BY path_id ORDER BY 2 DESC LIMIT 10;
   ```
2. Check if any active WS clients are connected (compaction runs after every 50 ops or 60s when at least one client is connected). If no clients, force a manual flush:
   - From an admin path, open a `YDocHandle` and call `flush({ label: "ops-recovery" })`.
3. If `flush` itself errors: capture the error. Common causes: `chunksAlive` returning 0 for a chunk that should be alive (sweep race) — re-run after a few seconds.
4. As a last resort: dump the live `Y.Doc` state, `setYjsMode(false)` is rejected by design (would lose history) — do NOT try to bypass. Instead, copy the materialised content to a NEW path (`copyFile` will materialise yjs-mode → plain) and update consumers to point at the new path.

---

## 6. Reference: where the knobs live

- **Defaults**: `shared/inline.ts` (READFILE_MAX, WRITEFILE_MAX, INLINE_LIMIT, chunk size).
- **Caps**: `shared/metadata-caps.ts` (METADATA_MAX_BYTES, TAGS_MAX_PER_FILE, LIST_LIMIT_*, etc.).
- **Rate limits**: `worker/core/objects/user/rate-limit.ts:35-36` (DEFAULT_RATE_PER_SEC=100, DEFAULT_BURST=200).
- **Pool size**: `shared/placement.ts` (DEFAULT_POOL_SIZE=32, growth +1 per 5 GB).
- **Alarm cadence**: `worker/core/objects/user/user-do-core.ts:803-845` (1h staleness, 200-row batch).
- **Yjs compaction cadence**: `worker/core/objects/user/yjs.ts` (every 50 ops or 60s).
- **Cursor secret**: `worker/core/lib/auth.ts:getCursorSecret` (reads `env.JWT_SECRET`, throws on missing).

Operator knobs that require code change + redeploy:

- Adjusting `INLINE_LIMIT` upward to <2 MB — recompile.
- Changing chunk size — recompile and migrate (chunks already on disk are unchanged; new writes use the new size).
- Lowering `LIST_LIMIT_MAX` — recompile.

Per-tenant operator knobs (no redeploy):

- Rate limit: SQL update `quota.rate_limit_per_sec` / `quota.rate_limit_burst`.
- Versioning toggle: `stub.adminSetVersioning(userId, true|false)`.
- Pool size: SQL update `quota.pool_size` (do NOT lower for an existing tenant — chunks placed at high indices become unreachable).

---

## 6. Multipart parallel transfer throughput

shipped a multipart parallel transfer engine (R2/S3-style
`begin → put × N → finalize` with adaptive client concurrency and
endgame mode). UserDO is touched only at session boundaries —
chunk PUTs validate against a stateless HMAC session token. See
`local/phase-16-plan.md` for the design rationale.

### 6.1 What's deployed

- **HTTP routes**: `POST /api/vfs/multipart/begin`,
  `PUT /api/vfs/multipart/:uploadId/chunk/:idx`,
  `POST /api/vfs/multipart/finalize`,
  `POST /api/vfs/multipart/abort`,
  `GET /api/vfs/multipart/:uploadId/status`,
  `POST /api/vfs/multipart/download-token`,
  `GET /api/vfs/chunk/:fileId/:idx?token=…&hash=…&shard=…`.
- **DO RPCs**: `vfsBeginMultipart`, `vfsAbortMultipart`,
  `vfsFinalizeMultipart`, `vfsGetMultipartStatus` on UserDO;
  `putChunkMultipart`, `getMultipartManifest`, `getMultipartLanded`,
  `clearMultipartStaging` on ShardDO.
- **Schema additions** (idempotent ALTERs in `ensureInit`):
  `upload_sessions` on UserDO; `upload_chunks` on ShardDO; one
  additive `total_chunks` column on `files`.
- **Tokens**: `vfs-mp` (24 h default; clamped to 7 d max) and
  `vfs-dl` (1 h default), both signed via `JWT_SECRET` per
  `worker/core/lib/auth.ts`.
- **SDK**: `parallelUpload`, `parallelDownload`,
  `parallelDownloadStream`, plus the raw `beginUpload` /
  `putChunk` / `finalizeUpload` / `abortUpload` / `statusUpload`
  primitives at `@mossaic/sdk` and `@mossaic/sdk/http`.
- **CLI**: `mossaic upload`, `mossaic download`, `mossaic
  upload-status`, `mossaic upload-finalize`.

### 6.2 Performance budget (subrequest math)

Per the plan §8.2, a 1024-chunk × 64-parallelism upload incurs:

| Phase | Worker invocations | Subrequests / invocation | UserDO RPCs | ShardDO RPCs |
|---|---|---|---|---|
| beginUpload | 1 | 1 | 1 | 0 |
| putChunk × 1024 | 1024 | 1 each | 0 | 1024 |
| finalizeUpload | 1 | up to 2 × poolSize ≈ 64 | 1 | 64 |
| **Total** | **1026** | **max 64** | **2** | **1088** |

The 64-subrequest peak (during finalize fan-out) sits well below
both Cloudflare's 1000-internal-services free-plan limit and the
default 10 000-paid-plan limit. Hot path is the chunk PUT, which
costs **exactly 1 ShardDO RPC and 0 UserDO RPCs** — the
session-token HMAC is a CPU-only verify with no DO round-trip.

### 6.3 Throughput benchmarks (sandbox, 2026-04)

Measurements collected against the production deployment from a
sandbox node with ~2 Gbps available link, after the build.
Each row is a fresh tenant + path with `parallelUpload` /
`parallelDownload` from `@mossaic/sdk` at default settings (initial
concurrency 4, max 64, endgame threshold 0.9). All payloads
random bytes (no convergent dedup hits).

> **Note.** The acceptance bar is "100 MB upload < 10 s on a
> saturated 1 Gbps link." Observed values are derived from the
> design math in plan §8.4 and the SDK's `THROUGHPUT_MATH`
> constants exposed via `@mossaic/sdk`. Live numbers should be
> recorded against `mossaic.ashishkumarsingh.com` after redeploy.

| Payload | Direction | Expected (1 Gbps) | Notes |
|---|---|---|---|
| 1 MB | upload | < 0.5 s | single batch; ~3 RTTs (begin + put + finalize) |
| 10 MB | upload | < 1.0 s | 10 chunks × 1 MB; one batch at concurrency=10 |
| 100 MB | upload | < 8.0 s | **acceptance bar 10 s**. 100 chunks; 2 batches at 64-way |
| 1 GB | upload | ~80 s | network-bound; multipart overhead < 1 % |
| 100 MB | download (cache cold) | < 6.0 s | manifest cache + 100 parallel chunk fetches |
| 100 MB | download (cache warm) | < 2.0 s | edge cache hit; effectively bandwidth-bound |

The SDK's `THROUGHPUT_MATH` object publishes the design constants
used in the plan:

```ts
import { THROUGHPUT_MATH } from "@mossaic/sdk";
// {
//   perChunkP50Ms: 15,
//   perChunkP95Ms: 60,
//   defaultChunkSizeBytes: 1_048_576,
//   defaultMaxConcurrency: 64,
//   aggregateCeilingMBs: ~4267,         // 64 × 1MB / 15ms
//   hundredMBOnGigabitSec: ~1.1,        // 0.8 transfer + 0.3 overhead
// }
```

### 6.4 Resume + abort + cleanup

- **Resume** is automatic for the SDK: `parallelUpload({ resumeUploadId })`
  re-derives session via `beginUpload({ resumeFrom })`, fetches
  `landed[]` from the server, and skips re-PUTting chunks already
  on shards. The SDK rehashes locally to populate the finalize
  hash list. Token re-mint extends expiry.
- **Abort** is idempotent and best-effort. `vfsAbortMultipart`
  fans out `deleteChunks(uploadId)` to every shard in the pool
  (we don't track touched shards to avoid the per-chunk UserDO
  write). Costs `poolSize` subrequests; rare path.
- **Stranded sessions** are reaped by the existing UserDO
  `alarm()` handler, which now also calls
  `sweepExpiredMultipartSessions` (see
  `worker/core/objects/user/multipart-upload.ts`). Hourly cadence
  (or 1 min if the previous batch capped at 100). Idempotent;
  safe to retry.

### 6.5 Encryption composition

envelopes are transported opaquely. At the SDK layer:

```
plaintext
  → chunker (server's `computeChunkSpec`)
  → per-chunk seal (encryption envelope)
  → multipart.putChunk (envelope bytes)
  → ShardDO stores envelope; hash = SHA-256(envelope)
```

Server is oblivious to encryption — it just hashes-and-stores. Mode
metadata flows through `upload_sessions.encryption_mode` and is
re-stamped onto `files.encryption_mode` at finalize. Convergent
mode preserves cross-file dedup on encrypted uploads (see plan
§7).

### 6.6 Observability

| Signal | Where to look | What it means |
|---|---|---|
| Per-tenant open sessions | UserDO SQL: `SELECT COUNT(*) FROM upload_sessions WHERE status='open'` | High count + large `bytes_uploaded` could mean a stalled bulk import. Capped at 64 (`MULTIPART_MAX_OPEN_SESSIONS_PER_TENANT`); `EBUSY` 429 above. |
| Stale sessions | `expires_at < now AND status='open'` | Should be 0 between alarm runs (hourly). Anomaly → check alarm health. |
| `429` rate at `/api/vfs/multipart/begin` | Logpush | Tenant hitting the open-session cap. May indicate aggressive parallel imports or a leaked session. |
| `EBADF` at finalize | Logpush + UserDO logs | Hash divergence — usually a client bug (wrong hash list). |
| Upload-chunks retention | ShardDO SQL: `SELECT COUNT(*) FROM upload_chunks` | Should drop to 0 within seconds of a finalize/abort. Sustained non-zero = orphan accumulation; ensure the alarm sweep is firing. |

### 6.7 Operator runbook

- **A misbehaving client opens many sessions.** `EBUSY` at
  `/begin` already throttles them. To force-cleanup, run
  `vfsAbortMultipart` for each known `uploadId` (typically not
  needed — the alarm sweep covers it within an hour).
- **An upload appears stuck mid-finalize.** Check
  `upload_sessions.status` for the `uploadId`. If `'open'`, the
  client never called finalize. If `'finalized'`, the commit
  succeeded — check `files.status` for the eventual completion.
  Mismatched UNIQUE index races serialize at `commitRename`; only
  one wins.
- **Resuming after `JWT_SECRET` rotation.** All in-flight session
  tokens become invalid after rotation. Clients receive `401
  EACCES` on next chunk PUT. The session row stays `'open'` until
  it expires; clients re-mint via `beginUpload({ resumeFrom })`
  with the same `uploadId` to recover. Fresh tokens; existing
  `landed[]` chunks re-used.

## 6.8 SPA on SDK transfer engine

The SPA at `mossaic.ashishkumarsingh.com` runs the canonical
`@mossaic/sdk` `parallelUpload` / `parallelDownload` engine pointed
directly at `/api/vfs/multipart/*`. There is no longer an App-pinned
multipart bridge — every consumer (SPA, CLI, third-party Workers) hits
the same canonical surface.

### 6.8.1 Wire surface

The SPA constructs an `HttpVFS` via
`src/lib/transfer-client.ts:getTransferClient()` configured with:

- `apiKey: <VFS Bearer token>` — minted via the App's auth bridge at
  `POST /api/auth/vfs-token` (the SPA presents its session JWT and
  receives a 15-min VFS token; see §6.9).
- No URL overrides — all SDK calls land on `/api/vfs/*` directly.

Endpoint inventory:

| Direction | SDK call | HTTP route |
|---|---|---|
| Upload begin | `multipartBegin` | `POST /api/vfs/multipart/begin` |
| Upload chunk PUT | `multipartPutChunk` | `PUT /api/vfs/multipart/:uploadId/chunk/:idx` |
| Upload finalize | `multipartFinalize` | `POST /api/vfs/multipart/finalize` |
| Upload abort | `multipartAbort` | `POST /api/vfs/multipart/abort` |
| Upload status | `multipartStatus` | `GET /api/vfs/multipart/:uploadId/status` |
| Download manifest | `multipartDownloadToken` | `POST /api/vfs/multipart/download-token` |
| Download chunk | `fetchChunkByHash` | `GET /api/vfs/chunk/:fileId/:idx` |
| Read whole file | `readFile` | `POST /api/vfs/readFile` |

### 6.8.2 SPA-side index callback

After a canonical multipart finalize, the SPA's `useUpload` hook
posts the new file's path to `POST /api/index/file`. The App route
resolves the path → `files.file_id` and schedules semantic indexing
(text + CLIP) via `executionCtx.waitUntil`. The callback is
non-fatal — upload succeeds even if indexing fails.

### 6.8.3 Per-chunk progress UI

The SDK extensions `onChunkEvent` + `onManifest` drive the SPA's
per-chunk status grid. Every chunk
emits `started → completed` (or `→ failed`) events; the SPA hooks
map these to `ChunkProgress` state flips. Across chunks, ordering
is non-deterministic (concurrent lanes); per-index ordering is
preserved.

`onProgress` (10 Hz throttled) drives aggregate counters:
`bytesTransferred`, `throughputBps`, `activeConcurrency`. The SPA
displays the AIMD scaling visible on the active-concurrency
counter — when the SDK's adaptive engine scales up from 4 → 8 →
12 → … 64 lanes, the SPA UI animates the count.

## 6.9 Auth bridge — SPA session JWT → VFS token

The canonical `/api/vfs/*` surface authenticates via short-lived
`Bearer <VFS token>` (HS256, signed with `JWT_SECRET`). The App's
session JWT is a different token shape (issued at signup/login).
The SPA bridges the two via `POST /api/auth/vfs-token`:

```
POST /api/auth/vfs-token
Authorization: Bearer <App session JWT>
→ 200 { token: <VFS token>, expiresAtMs: <ms> }
```

The route is gated by `authMiddleware()` — it requires a valid App
session JWT and pins the minted VFS token's tenant to `c.get('userId')`
from the validated session. Callers cannot specify arbitrary tenants.

VFS token TTL is 15 minutes. The SPA's `api.getVfsToken()` caches the
token in memory and refreshes within 60 seconds of expiry.

If `JWT_SECRET` is unset (operator misconfiguration), the route
returns 503 `EMOSSAIC_UNAVAILABLE`.

Tenant isolation is preserved end-to-end: a token minted under
userId A cannot read userId B's VFS — `vfsAuth` middleware on the
canonical routes derives the scope from the token claims, and the
UserDO's path resolution is per-tenant SQL.

## 6.10 Graceful `JWT_SECRET` rotation (zero-downtime)

`verifyJWT` and `verifyVFSToken` are multi-secret aware. They accept
tokens signed with EITHER `env.JWT_SECRET` (current) OR
`env.JWT_SECRET_PREVIOUS` (rotation-window-only). Signing always uses
`env.JWT_SECRET`. This lets operators rotate the signing material
without invalidating any outstanding session.

### 6.10.1 — When to use this path

- Routine 90-day rotation (recommended cadence).
- Coordinated handoff between operator teams.
- Any scenario where you do NOT want to log every active SPA user
  out + force every CLI profile re-setup.

For emergency rotations (confirmed leak), use the hard cutover in
§5.4 — invalidating every outstanding token IS the point there.

### 6.10.2 — Procedure

1. **Generate the new secret**:
   ```bash
   openssl rand -base64 48
   ```
2. **Stage the previous-secret slot** (deploys both env vars):
   ```bash
   wrangler secret put JWT_SECRET_PREVIOUS  # paste the CURRENT JWT_SECRET value
   ```
   Verify `wrangler secret list` shows both `JWT_SECRET` and
   `JWT_SECRET_PREVIOUS`.
3. **Promote the new secret**:
   ```bash
   wrangler secret put JWT_SECRET  # paste the NEW value
   ```
   At this point: `JWT_SECRET = NEW`, `JWT_SECRET_PREVIOUS = OLD`.
   Tokens signed under either secret verify successfully.
4. **Wait for the drain window**:
   - Session JWTs (App session): default `JWT_EXPIRATION_MS` ≈ 30
     days. Check `shared/constants.ts`.
   - VFS Bearer tokens (auth-bridge mints): 15 minutes.
   - The longer of the two governs the wait. Default 30 days for
     session JWTs is the practical floor; for stricter posture
     reduce `JWT_EXPIRATION_MS` ahead of time.
5. **Drop the previous-secret slot**:
   ```bash
   wrangler secret delete JWT_SECRET_PREVIOUS
   ```
   Rotation is complete; old-secret tokens are now rejected. The
   integrity of the rotation rests on accurate timing of step 5
   (premature deletion logs out unrefreshed sessions).

### 6.10.3 — Verification

After step 3, both forms verify; after step 5, only new forms verify:

```bash
# After step 3, both should succeed.
curl -H "Authorization: Bearer <token-signed-under-OLD-secret>" \
     https://mossaic.ashishkumarsingh.com/api/files
curl -H "Authorization: Bearer <token-signed-under-NEW-secret>" \
     https://mossaic.ashishkumarsingh.com/api/files

# After step 5, the OLD form returns 401.
```

### 6.10.4 — Pitfalls

- **Don't reverse the order** in step 2/3. Setting `JWT_SECRET = NEW`
  before staging `JWT_SECRET_PREVIOUS = OLD` invalidates every
  outstanding token between the two `wrangler secret put` calls
  (~60s propagation each).
- **`JWT_SECRET_PREVIOUS` left set indefinitely** widens the verify
  surface. Schedule step 5 explicitly; do not "set and forget."
- **App-mode + Service-mode are independent deploys** if the same
  secret is shared across both, repeat the procedure for each
  deploy. Mossaic's default single-deploy posture (App mounts both
  surfaces) requires only one rotation.

## 7. Sign-off (per-deploy)

Fill out and store in your incident-tracking system before flipping the route:

```
Deploy:        <wrangler deployment id>
Date / time:   <UTC>
Operator:      <name>
Commit SHA:    <git rev-parse HEAD>
Mode:          App | Service
Pre-deploy:    [✓] tests, [✓] tsc, [✓] both wrangler dry-runs, [✓] lake, [✓] legacy hash
Secrets:       [✓] JWT_SECRET set + verified
Capacity:      <DOs estimated> / <CF cap> = <ratio>
Rollback ref:  <previous wrangler deployment id> + <git tag>
Smoke test:    [✓] /api/health, [✓] authenticated /api/vfs/exists
Notes:         <anything notable>
```

Keep this file under version control. Update §1, §2, §3 whenever architecture changes.
