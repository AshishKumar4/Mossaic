# Mossaic scaling roadmap

This is the only document that cites phase numbers. Every other
document describes shipped behaviour. Phase entries here trace
historical decisions that operators may need to reconstruct from
`git log`; the alternative is letting that trail fade into commit
archaeology and discovering load-bearing context months later.

## Architectural status (as of HEAD)

Per-tenant scaling is bounded by UserDO SQLite size (~10 GiB
practical ceiling at workerd's per-DO storage limit). Inside
that ceiling, ShardDOs scale horizontally:

- **Pool growth.** `quota.pool_size` grows by 1 ShardDO per 5 GiB
  of stored bytes. Monotonic by design; never shrinks.
  `recordWriteUsage` is the load-bearing function.
- **Cap-aware placement.** `placeChunk` skips shards over the
  soft cap (9 GiB) via the `shard_storage_cache` table; falls
  through to the next-best rendezvous score. On all-full,
  returns a `POOL_FULL` sentinel and the caller force-bumps
  `pool_size` to acquire fresh capacity.
- **Inline-tier graceful migration.** Per-tenant cumulative
  inline bytes (`quota.inline_bytes_used`) capped at 1 GiB.
  Writes past the cap spill to the chunked tier and live on
  ShardDOs.
- **Versioned accounting consolidation.** `commitVersion` is the
  single accounting chokepoint for all versioned writes \u2014
  storage_used, file_count, and inline_bytes_used update through
  one path. Versioning-on tenants get pool growth (the bug that
  motivated the consolidation: pre-fix, ver-on writes never
  bumped `storage_used`, so pool_size stayed at 32 forever).

The scalability ceiling beyond ~100 GiB user data per tenant is
the metadata layer (`files` / `file_versions` / `file_chunks`
all live on the UserDO). Lifting that ceiling is intentionally
deferred \u2014 see Phase 32b below.

---

## Phase ledger (newest first)

### Phase 47 \u2014 HTTP Range support + pagination audit + CI gate hardening (LANDED)

- HTTP Range support for `gallery/image/:fileId` and
  `shared/:token/image/:fileId`. `serveBytesWithRange()`
  helper at `worker/core/lib/http-range.ts` parses
  `Range: bytes=N-M`, returns 206 with `Content-Range` +
  `Accept-Ranges: bytes`, 416 on out-of-bounds. Range
  requests bypass the Workers Cache wrapper since the cached
  full response is the upstream of any range slice.
- Pagination audit pass over `listFiles` / `listChildren`
  cursor encoding; covered by deterministic-ordering tests.
- CI gate hardening. New `ci:check` script chains
  `typecheck` \u2192 `build:sdk` \u2192 `lint:no-phase-tags` so a
  missing-tag-leak or DTS-strict-mode mismatch fails the
  build, not the deploy.

### Phase 46 \u2014 listChildren batched listing + folder revision counter + listFiles pagination fix (LANDED)

- `vfs.listChildren(path, opts?)` returns a single-RPC
  enumeration of one folder's direct children. Replaces
  `readdir + lstat\xd7N` for SPA file-tree views; one DO
  invocation regardless of child count. Discriminated-union
  entries (`{kind: "file"|"dir"|"symlink", path, stat, ...}`),
  optional metadata + tags + contentHash via opts.
- Folder revision counter: each folder row carries a
  monotonic `revision` column; `bumpFolderRevision` fires on
  every direct-child mutation. Routes that emit ETag for
  directory listings derive the etag from
  `(folder_id, revision)` so the SPA can `If-None-Match` a
  whole subtree without re-downloading children.
- listFiles pagination fix: a tie-break bug at the
  `(orderbyValue, file_id)` boundary occasionally repeated or
  skipped a row at page boundaries; fixed by tightening the
  cursor predicate to strict `<`/`>` instead of `<=`/`>=`.
  Pinned by `tests/integration/list-files-pagination.test.ts`.

### Phase 45 \u2014 signed preview-variant URLs (LANDED)

- `vfs.previewUrl(path, opts?)` mints an HMAC-signed URL the
  browser fetches directly: `GET /api/vfs/preview-variant/<token>`.
  Bytes are content-addressed by the `contentHash` claim in
  the token, so the response carries
  `Cache-Control: public, max-age=31536000, immutable` without
  `Vary: Authorization`. CDN edge tier caches across all
  clients; subsequent loads bypass the Worker entirely.
- `vfs.previewInfo(path, opts?)` returns the same URL plus
  the metadata bundle (mimeType, width, height, etag,
  rendererKind, versionId, cacheControl, contentHash,
  expiresAtMs). One mint RPC, all the data the SPA needs.
- `vfs.previewInfoMany(paths, opts?)` batched mint (cap 256
  paths). Per-path failures land as `{ok: false, code, message}`
  entries; one ENOENT in a 50-photo grid doesn't 4xx the
  whole batch.
- HMAC token at `worker/core/lib/preview-token.ts`. HS256 JWT
  with `scope: "vfs-pv"` (RFC 8725 \xa72.8 scope-binding rejects
  cross-purpose replay). Multi-secret rotation aware. Default
  TTL 24h; clamped to [60s, 30d].
- Backward compat: `vfs.readPreview` and `POST /readPreview`
  preserved verbatim. SDK consumers migrate at their own pace.

### Phase 44 \u2014 Phase NN tag purge (LANDED)

- Phase NN narration drift in production code reverted. The
  Phase 24 hygiene sweep had cleaned 307 markers to 0; Phase
  46-era growth reintroduced 60+. Phase 44 stripped the new
  narration, then Phase 47 added the `lint:no-phase-tags`
  CI gate so future phases can't regress silently.
- This roadmap is the SOLE document where `Phase NN` appears.
  Tests use `Phase NN` as stable test IDs (excluded from the
  gate). Scope: production code only.

### Phase 43 \u2014 Lean catch-up (LANDED)

- Theorem count 172 \u2192 226. Coverage extensions for Cache
  (preview-variant cache-key bust completeness),
  ShareToken (HMAC scope-binding), RPC (typed-RPC
  null-safety), Yjs (compaction monotonicity). Zero
  `axiom`, zero `sorry`.

### Phase 42 \u2014 observability infrastructure (LANDED, closes Phase 34 carryover)

- `audit_log` table emits one row per destructive operation:
  `unlink`, `purge`, `archive`, `unarchive`, `rename`,
  `removeRecursive`, `restoreVersion`, `dropVersions`, the
  4 `admin*` RPCs, `appWipeAccountData`, `accountDelete`,
  `shareLinkMint`. Per-tenant retention (cap 10K rows, trim
  to 9.8K floor) via the existing UserDO alarm.
- Structured logger at `worker/core/lib/logger.ts`:
  `logInfo` / `logWarn` / `logError` emit JSON-stringified
  single-line `console.*` output. Workers Logs + Logpush
  parse this as structured fields without further config.
- `requestIdMiddleware` mints `crypto.randomUUID()` per
  `/api/*` request, mirrors onto `X-Mossaic-Request-Id`
  response header. Honors caller-supplied valid id (regex-
  gated) so a wrapping proxy can thread its own correlation
  id.
- Alarm-handler bare `catch {}` sites visible: every
  alarm-handler exception goes through `recordAlarmFailure`
  \u2014 `logError(event=alarm_handler_failed)` plus a persistent
  `vfs_meta.alarm_failures` counter. Alarms continue (at-
  least-once retry); throwing would replay without progress.

### Phase 41 \u2014 four targeted Phase 40 audit bug fixes (LANDED)

- `errToResponse` `EAGAIN` mapping now consistent across
  all routes (was missing from a multipart-routes
  re-implementation). 429 returns 429 instead of collapsing
  to 500.
- `Vary: Authorization` on cached responses (preview /
  manifest / chunk routes) so an intermediary CDN keys
  cached entries by Bearer token \u2014 no cross-tenant replay
  through a coincidental URL collision.
- Encrypted Yjs files refuse `flush()` with `ENOTSUP` (the
  server can't materialize the doc to snapshot it).
- Transfer concurrency cap fixed for `parallelDownload`
  (was bypassing the AIMD controller in one branch).

### Phase 39 \u2014 RPC efficiency + binding cleanup + image-passthrough fallback (LANDED)

- `getChunksBatch` typed RPC: per-shard batched chunk reads
  in one DO turn. Replaces the legacy `fetch(http://internal/chunk/<hash>)`
  shape across the four straggler call sites.
- Image-passthrough renderer: `image/*` MIMEs serve verbatim
  bytes when the Cloudflare Images binding is unavailable,
  preserving variant cache shape (kind = `"image-passthrough"`).
  Bridge for environments without IMAGES (local dev,
  service-mode without binding).
- Subrequest budget bumped to 100,000; image binding fallback
  no longer multiplies subrequests.

### Phase 38 \u2014 Yjs arbitrary named shared types (LANDED)

- The Yjs runtime now broadcasts the entire `Y.Doc` (every
  named `Y.XmlFragment`, `Y.Map`, `Y.Array`, `Y.Text`,
  `Y.XmlElement`) rather than just `Y.Text("content")`.
  Tiptap / ProseMirror / Notion-style apps work end-to-end.
- New `vfs.readYjsSnapshot(path)` returns
  `Y.encodeStateAsUpdate(doc)` bytes for SDK consumers
  bootstrapping a doc without an open WebSocket.
- Encrypted-Yjs refuses snapshot reads (ENOTSUP) \u2014
  server can't materialize ciphertext.

### Phase 37 \u2014 P0/P1 audit fixes (LANDED)

- Share-token HMAC: tokens are now HS256-signed (was
  unsigned `base64(JSON({...}))`). Pre-fix tokens fail
  verification; users re-share to mint new tokens.
- Atomic `restoreVersion` via `restoreChunkRef` shard RPC
  (was a `chunksAlive` preflight + per-chunk `putChunk`
  loop with a TOCTOU window where concurrent
  `dropVersions` could reap a chunk between preflight and
  re-ref).
- Admin gates: every `admin*` RPC routes through the
  per-tenant rate-limit bucket so replay attempts are
  bounded.
- Hard cap on concurrent Yjs WebSocket clients per pathId
  (100; warn at 80) so the synchronous broadcast loop
  doesn't burn DO event-loop time.
- Multipart session abort-attempts cap (5) so a poisoned
  session can't block alarm progress forever.
- Indexer attempts cap (5) so a poison file in the
  search-index reconciler can't burn AI binding budget on
  every alarm tick.

### Phase 36b \u2014 cacheable surfaces complete + staleness audit (LANDED)

Workers Cache extended to every read-heavy surface, all
sharing one helper at `worker/core/lib/edge-cache.ts`:

- `GET /api/gallery/thumbnail/:fileId`
- `GET /api/gallery/image/:fileId`
- `GET /api/shared/:token/image/:fileId`
- `POST /api/vfs/readPreview`
- `POST /api/vfs/readChunk`
- `POST /api/vfs/openManifest`
- `GET /api/vfs/chunk/:fileId/:idx`

Cache-key shape: `https://<surfaceTag>.mossaic.local/<namespace>/<fileId>/<updatedAt>[/<...extras>]`.
Pre-flight `vfsResolveCacheKey` returns
`(fileId, headVersionId, updatedAt, encryption stamp)` in
one SQL JOIN; routes call it before the heavy operation to
build deterministic keys. Cache-key-versioning beats active
invalidation \u2014 races produce orphaned cache entries that
expire per their TTL, never serve stale responses.

### Phase 36 \u2014 versioned accounting consolidation (LANDED)

- `commitVersion` is the single accounting chokepoint for
  versioned writes. 13 callers across 6 files all update
  `(storage_used, file_count, inline_bytes_used)` via the
  `(prevWasLive, nowIsLive)` tuple inside `commitVersion`.
- `dropVersionRows` is the symmetric decrement: accumulates
  bytes / inline-bytes per dropped non-tombstone version +
  tracks file_count delta when path goes ENOENT.
- Non-versioning paths that previously didn't account
  (`vfsCommitWriteStream` non-versioning,
  `copyInline` / `copyChunked` non-versioning) gained
  explicit `recordWriteUsage` calls.
- Multipart finalize stops double-counting under
  versioning ON.
- VER-ON TENANTS NOW GET POOL GROWTH (THE bug that
  motivated this consolidation): a 5 GB versioned upload
  moves `pool_size` 32\u219233. Pre-fix it stayed at 32 forever.

Lean invariant `Mossaic.Vfs.Quota.pool_size_monotonic`
preserved by `recordWriteUsage`'s `MAX(0, col + ?)` clamp +
the `newPool > row.pool_size` guard. Negative deltas can
never shrink `pool_size` regardless of sign or magnitude.

### Phase 32.5 \u2014 quota desync correction (LANDED, superseded by Phase 36)

`recordWriteUsage` was called only on positive deltas;
zero call sites decremented on
`unlink` / `remove-recursive` / `rename-supersede` /
`multipart-abort` / `dropVersionRows`. `quota.storage_used`
grew monotonically forever; gallery / analytics surfaces
reported wildly inflated bytes for any tenant that had ever
deleted anything.

Phase 32.5 threaded negative-delta calls through every
destructive path. Superseded by Phase 36, which made
`commitVersion` the single chokepoint and removed the
caller-by-caller delta plumbing.

### Phase 32 \u2014 capacity story (LANDED)

The original capacity work that the architectural status
(top of doc) summarises:

- **Fix 4**: cap-aware placement. `placeChunk(...)` skips
  shards over the 9 GiB soft cap; falls through to next-best
  score. On all-full, returns `POOL_FULL` and the caller
  force-bumps `pool_size`. `shard_storage_cache` table
  records per-shard byte counts; `monitorShardCapacity`
  refreshes every 30 minutes.
- **Fix 5**: inline-tier graceful migration. Per-tenant
  `quota.inline_bytes_used` capped at 1 GiB. Writes past the
  cap spill to chunked tier even when below `INLINE_LIMIT`
  (16 KiB) so a tenant with millions of tiny files doesn't
  monopolize the UserDO's SQLite quota.

---

## Intentionally deferred

### Phase 32b \u2014 MetaShardDO (single-tenant metadata sharding)

Out of scope per user direction. UserDO holds a tenant's
entire metadata layer (`files` / `file_versions` /
`file_chunks` / `file_tags` / `version_chunks` /
`upload_sessions` / `folders`); at the workerd ~10 GiB
SQLite limit this caps a single tenant at ~100 GiB user
data (pool growth scales BYTES horizontally; the metadata
still concentrates on the UserDO).

The ceiling lift is a multi-day phase on its own \u2014 new
DO class, new wrangler binding, schema migration, fan-out
reads/writes, listFiles parallel-merge with cursor encoding.
Tracked here so the deferral is owned in writing; will be
revisited when a real tenant approaches the ~100 GiB
ceiling.

The chunked-bytes side already scales horizontally via
ShardDO pool growth + cap-aware placement, so the
architectural story holds for the workloads that matter
today.

### Phase 33 \u2014 Workers Cache integration (CLOSED via Phase 36b)

Phase 33 was a research-only plan. Implementation landed
across Phase 36, Phase 36b, Phase 41, and Phase 45 (signed
URLs); the plan's deferred surfaces are all shipped.
