# Mossaic scaling roadmap

This document tracks the architectural work that takes Mossaic from
"horizontally scalable per ShardDO" (Phase 23 pool growth + Phase 32
skip-full placement) toward "horizontally scalable per UserDO" and
beyond. Each entry is a phase boundary with a concrete blocker, an
approach, and the Mossaic invariants the change must preserve.

## Status as of Phase 32 (HEAD `<post-merge>`)

Per-tenant scaling is **bounded by UserDO SQLite size** (~10 GiB
practical ceiling at workerd's per-DO storage limit). Inside that
ceiling, ShardDOs scale horizontally:

- **Pool growth** (Phase 23): `quota.pool_size` grows by 1 ShardDO per
  5 GiB of stored bytes. Monotonic by design; never shrinks.
  `recordWriteUsage` is the load-bearing function.
- **Cap-aware placement** (Phase 32 Fix 4): `placeChunk` skips
  shards over the soft cap (9 GiB) via the `shard_storage_cache`
  table; falls through to next-best rendezvous score; on all-full,
  returns a `POOL_FULL` sentinel and the caller force-bumps
  `pool_size` to acquire fresh capacity.
- **Inline-tier graceful migration** (Phase 32 Fix 5): per-tenant
  cumulative inline bytes (`quota.inline_bytes_used`) capped at
  1 GiB. Writes past the cap spill to the chunked tier and live on
  ShardDOs.

The SCALABILITY CEILING after Phase 32: a single tenant's
`files`/`file_versions`/`file_chunks` metadata fits inside one
UserDO. Empirically that's ~100 GiB of user data before metadata
hits the SQLite limit (depending on file count + version churn).
To scale a single tenant past this, the metadata layer needs to
shard horizontally too \u2014 see Phase 32b.

## Phase 32.5 \u2014 Quota desync correction (deferred from Phase 32)

**Blocker (cosmetic, not scaling):** `recordWriteUsage` is
called only on positive deltas from
`commitInlineTier` / `commitChunkedTier` /
`vfsFinalizeMultipart`. Zero call sites decrement on
unlink / remove-recursive / rename-supersede / multipart-abort /
dropVersionRows. `quota.storage_used` grows monotonically forever;
the App's gallery / analytics surfaces report wildly inflated bytes
for any tenant that has ever deleted anything.

**Why deferred:** Pool growth is monotonic by design (Lean
invariant `Mossaic.Vfs.Quota.pool_size_monotonic`), so
`storage_used` inflation does NOT impact placement / scaling. The
inflation is purely a UX accuracy concern (gallery shows 100 GiB
when actual is 10 GiB).

**Approach:** Phase 32 already makes `recordWriteUsage` accept
negative deltas and clamp at zero (`MAX(0, col + ?)`). Phase 32.5
threads the negative-delta calls through every destructive path:

- `hardDeleteFileRow` reads `(file_size, status)` BEFORE the delete
  cascade; if `status='complete'`, decrements
  `(-file_size, -1)` AFTER the cascade succeeds. (The
  `status='uploading'` branch was never positive-counted in the
  first place \u2014 do not decrement.)
- `dropVersionRows` reads each version's `size` BEFORE delete;
  accumulates `bytesReaped` across LIVE versions; decrements once at
  the end. Tombstones contribute 0.

Inline-bytes accounting (Phase 32 Fix 5) is already balanced \u2014
inline-tier deletes correctly subtract from `inline_bytes_used` so
the cap stays accurate.

## Phase 32b \u2014 MetaShardDO (single-tenant metadata sharding)

**Blocker:** UserDO holds ALL of a tenant's metadata: `files`,
`file_versions`, `file_chunks`, `file_tags`, `version_chunks`,
`upload_sessions`, `folders`. At the workerd ~10 GiB SQLite limit
this caps a single tenant at empirically ~100 GiB of user data
(the exact ratio depends on chunk size + version churn). Pool
growth scales the BYTES horizontally across ShardDOs, but the
METADATA still concentrates on the UserDO.

**Approach:**

- UserDO retains: auth, quota, folders, user-level metadata
  (versioning_enabled, rate_limit, indexed_at), the canonical
  `vfs_meta` table, `shard_storage_cache`.
- New `MetaShardDO` durable object class holds the per-file
  tables:
  `files` / `file_versions` / `file_chunks` / `version_chunks` /
  `file_tags`.
- Sharding key: `floor(murmur3(file_id) % metaShardCount)`,
  starting `metaShardCount = 4`. Pool grows like ShardDO: +1
  MetaShard per 5 GiB of metadata (track via a new
  `quota.metadata_bytes` column).
- Address: `vfs:default:${userId}:m${idx}`. ShardDO addressing
  unchanged.
- **Critical invariant:** every operation on a single file is
  fully owned by ONE MetaShard \u2014 no cross-MetaShard
  transactions. `commitVersion`, `commitRename`,
  `hardDeleteFileRow`, `dropVersionRows` all operate within a
  single MetaShard.
- `vfsListFiles` fans out to all MetaShards in parallel, merges,
  paginates via cursor that encodes `(shardIdx,
  in-shard-cursor)`. List / search becomes O(metaShardCount)
  rather than O(1) but stays within the Workers subrequest
  budget for any sane `metaShardCount`.

**Migration (pick simpler):**

- **Option A** \u2014 Existing UserDO-resident tables stay for
  backward compat; new tenants \u2192 MetaShardDOs. Two parallel
  schemas.
- **Option B (preferred)** \u2014 Treat UserDO as MetaShard #0; new
  chunks sharded normally including #0=UserDO. Trivially
  backward compat: legacy `file_id` values that hash to 0 stay
  exactly where they are; values hashing to 1..N migrate
  lazily on next write (sharding key has changed; the file
  needs to live on a different MetaShard now).

Going with **Option B** because Option A doubles the schema
surface and complicates `listFiles`'s fan-out logic.

**Tests:**

- meta-shard-routing.test.ts (~6 cases):
  - hash deterministic;
  - listFiles fan-out merges;
  - write to correct MetaShard;
  - pool grows on metadata-bytes boundary;
  - cross-MetaShard reads;
  - MetaShard #0 backward compat with legacy UserDO-resident
    rows.

**Why deferred from Phase 32:** This is a multi-day phase on its
own. Adding a new DO class, new wrangler binding, schema
migration, fan-out reads/writes, listFiles parallel-merge with
cursor encoding \u2014 too invasive to land on the same branch as 4
surgical fixes (Fix 2/3/4/5). Phase 32 ships the immediate
wins; Phase 32b ships the architectural ceiling lift.

**Lean follow-up:**
- `meta_shard_routing_determinism`
- `meta_shard_pool_growth`
- `list_completeness_across_metashards`
- `single_metashard_owns_file_invariant`

## Phase 33 \u2014 Workers Cache integration (in-progress, plan-only)

A separate audit-fixes-2 session is producing a plan for caching
read-heavy paths (variant previews, listFiles results, public
shares) via the Workers Cache API. This is orthogonal to
metadata sharding (Phase 32b): caching reduces UserDO load on
the read path without changing the per-DO storage ceiling.

Scope to be defined by that session. No Phase 32 dependencies.
