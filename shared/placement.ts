import { murmurhash3 } from "./hash";
import type { VFSScope } from "./vfs-types";

/**
 * Build the ShardDO name for a given user and shard index.
 *
 * **Legacy template.** Used by the App's photo-library routes
 * (`worker/app/routes/*.ts`) and embedded in the rendezvous-score
 * key for ALL placements (legacy AND canonical) — see §1.4 of the
 * Phase 17.5 plan for why score-string compatibility is forever-pinned.
 */
export function shardDOName(userId: string, shardIndex: number): string {
  return `shard:${userId}:${shardIndex}`;
}

/**
 * Compute placement score for a chunk on a specific shard.
 * Higher score = higher priority for placement (rendezvous hashing).
 *
 * @internal — exported only so test fixtures can validate placement
 *  invariance across legacy/canonical implementations. The score
 *  string template (`shardDOName(userId, idx)`) is **forever pinned**:
 *  changing it orphans every existing chunk. See plan §1.4 + §7.
 */
function placementScore(
  fileId: string,
  chunkIndex: number,
  shardId: string
): number {
  const key = `${fileId}:${chunkIndex}:${shardId}`;
  return murmurhash3(key);
}

/**
 * Determine which ShardDO holds a specific chunk.
 * FULLY DETERMINISTIC: depends only on (userId, fileId, chunkIndex, poolSize).
 * No network calls. No state lookups.
 *
 * **Score-template invariance (Phase 17.5 §1.4):** the score key uses
 * the legacy `shard:${userId}:${idx}` template regardless of whether
 * the eventual DO instance name is legacy or canonical. This decouples
 * the score from the name — both placement implementations end up
 * routing to the same shard *index*, while building different
 * instance *names* on top of that index.
 */
export function placeChunk(
  userId: string,
  fileId: string,
  chunkIndex: number,
  poolSize: number
): number {
  let bestShard = 0;
  let bestScore = -1;

  for (let shard = 0; shard < poolSize; shard++) {
    const shardId = shardDOName(userId, shard);
    const score = placementScore(fileId, chunkIndex, shardId);
    if (score > bestScore) {
      bestScore = score;
      bestShard = shard;
    }
  }

  return bestShard;
}

/**
 * Place all chunks of a file and return a map of chunkIndex -> shardIndex.
 */
export function placeFile(
  userId: string,
  fileId: string,
  chunkCount: number,
  poolSize: number
): Map<number, number> {
  const placement = new Map<number, number>();
  for (let i = 0; i < chunkCount; i++) {
    placement.set(i, placeChunk(userId, fileId, i, poolSize));
  }
  return placement;
}

/**
 * Compute pool size based on total storage used.
 */
export function computePoolSize(storageUsedBytes: number): number {
  const BASE_POOL = 32;
  const BYTES_PER_SHARD = 5 * 1024 * 1024 * 1024; // 5 GB
  const additional = Math.floor(storageUsedBytes / BYTES_PER_SHARD);
  return BASE_POOL + additional;
}

// ── Phase 17.5: Placement abstraction ────────────────────────────────────
//
// The `Placement` interface lets two name templates coexist for the
// rollout window. Production data lives under the legacy
// `shard:${userId}:${idx}` / `user:${userId}` instances; canonical SDK
// consumers use `vfs:${ns}:${tenant}[:${sub}][:s${idx}]`.
//
// Both placement implementations share the SAME `placeChunk` rendezvous
// score (see §1.4 of the Phase 17.5 plan) — the integer they return
// MUST be identical for the same `(fileId, chunkIndex, poolSize)` and
// the same `userId` derivation from scope. Only the resulting DO
// *instance name* differs.
//
// `legacyAppPlacement` is App-internal — NOT re-exported from
// `@mossaic/sdk`. The public SDK API exposes only `Placement` (the
// interface) and `canonicalPlacement` (the default).

/**
 * Pluggable placement strategy. Decides:
 *   1. Which shard index holds chunk `chunkIndex` of file `fileKey`.
 *   2. The DO instance name for a given `(scope, shardIndex)` tuple.
 *   3. The DO instance name for a given `scope` (UserDO).
 *
 * MUST be deterministic. MUST keep the score-string template stable
 * across implementations to preserve every existing chunk's
 * addressability (Phase 17.5 §1.4).
 */
export interface Placement {
  /**
   * Decide which shard index holds chunk `chunkIndex` of file `fileKey`
   * for this scope. `fileKey` is opaque — typically a `fileId` or, for
   * content-addressed convergent placement, a content hash.
   * `poolSize` is the snapshotted pool size for this write session.
   *
   * MUST be deterministic given (scope, fileKey, chunkIndex, poolSize).
   * MUST return an integer in [0, poolSize).
   */
  placeChunk(
    scope: VFSScope,
    fileKey: string,
    chunkIndex: number,
    poolSize: number
  ): number;

  /**
   * Build the ShardDO instance name for a given (scope, shardIndex)
   * tuple. Consumed by `MOSSAIC_SHARD.idFromName(name)`. MUST be
   * injective in `(scope, shardIndex)` to preserve cross-tenant
   * isolation.
   */
  shardDOName(scope: VFSScope, shardIndex: number): string;

  /**
   * Build the UserDO instance name for a scope. MUST be injective
   * in `scope`. Symmetric to `shardDOName`.
   */
  userDOName(scope: VFSScope): string;
}

// ── Internal helpers (mirror worker/core/lib/utils.ts byte-exact) ──────

/**
 * Token validation regex shared with `worker/core/lib/utils.ts`.
 * Mirrors the `VFS_NAME_TOKEN` constant: alphanumerics + `._-`,
 * 1–128 characters. Cross-tenant collision impossible by construction
 * because the separator `:` is excluded from the allowed class.
 */
const VFS_NAME_TOKEN = /^[A-Za-z0-9._-]{1,128}$/;

function validateVfsToken(label: string, value: string): void {
  if (typeof value !== "string" || !VFS_NAME_TOKEN.test(value)) {
    throw new Error(
      `invalid vfs ${label}: ${JSON.stringify(value)}; allowed: [A-Za-z0-9._-], 1-128 chars`
    );
  }
}

/**
 * Inline canonical UserDO name builder. Mirrors
 * `worker/core/lib/utils.ts:vfsUserDOName` byte-exact so the two
 * code paths agree. The Lean proof of `userName_inj` continues to
 * hold for either source — both produce the same string for the
 * same input.
 */
function buildCanonicalUserDOName(scope: VFSScope): string {
  validateVfsToken("namespace", scope.ns);
  validateVfsToken("tenant", scope.tenant);
  if (scope.sub !== undefined) {
    validateVfsToken("sub", scope.sub);
    return `vfs:${scope.ns}:${scope.tenant}:${scope.sub}`;
  }
  return `vfs:${scope.ns}:${scope.tenant}`;
}

/**
 * Inline canonical ShardDO name builder. Mirrors
 * `worker/core/lib/utils.ts:vfsShardDOName` byte-exact.
 */
function buildCanonicalShardDOName(scope: VFSScope, shardIndex: number): string {
  if (
    !Number.isFinite(shardIndex) ||
    !Number.isInteger(shardIndex) ||
    shardIndex < 0
  ) {
    throw new Error(`invalid vfs shardIndex: ${shardIndex}`);
  }
  return `${buildCanonicalUserDOName(scope)}:s${shardIndex}`;
}

/**
 * Derive the legacy `userId` string from a scope. Used by the
 * rendezvous-score key (which is invariant across placements per
 * §1.4) and by `legacyAppPlacement` for direct addressing.
 *
 * Mirrors `worker/core/objects/user/vfs/helpers.ts:userIdFor`:
 *   - sub === undefined → userId = tenant
 *   - sub set           → userId = `${tenant}::${sub}`
 *
 * The `::` separator is the existing convention; it cannot collide
 * with the bare-tenant case because `tenant` matches `[A-Za-z0-9._-]+`
 * and contains no `:`.
 */
function userIdFromScope(scope: VFSScope): string {
  if (scope.sub === undefined) return scope.tenant;
  return `${scope.tenant}::${scope.sub}`;
}

// ── Concrete placements ────────────────────────────────────────────────

/**
 * The default, canonical VFS placement. Used by `createVFS()` consumers.
 *
 * Naming: `vfs:${ns}:${tenant}[:${sub}][:s${idx}]`.
 *
 * Score key: legacy `shard:${userId}:${idx}` template (preserved across
 * placements for chunk-addressability — see §1.4 of the plan).
 *
 * @lean-invariant Mossaic.Vfs.Tenant.shardName_inj_fixed_idx
 *   The canonical name template is the literal subject of the Lean
 *   injectivity proof. Delegating to `buildCanonicalShardDOName`
 *   (which mirrors `vfsShardDOName` byte-exact) preserves the proof.
 */
export const canonicalPlacement: Placement = {
  placeChunk(scope, fileKey, chunkIndex, poolSize) {
    return placeChunk(userIdFromScope(scope), fileKey, chunkIndex, poolSize);
  },
  shardDOName(scope, shardIndex) {
    return buildCanonicalShardDOName(scope, shardIndex);
  },
  userDOName(scope) {
    return buildCanonicalUserDOName(scope);
  },
};

/**
 * The legacy App-side placement. Addresses the `shard:${userId}:${idx}`
 * and `user:${userId}` DO instances that hold the photo-library's
 * existing data. Used by `createAppVFS()` and by App routes via
 * explicit dispatch.
 *
 * Naming: `shard:${userId}:${idx}`, `user:${userId}`.
 *
 * Score key: legacy template (same as canonical — the score is
 * placement-invariant by design).
 *
 * NOT re-exported from `@mossaic/sdk`. App-internal only.
 */
export const legacyAppPlacement: Placement = {
  placeChunk(scope, fileKey, chunkIndex, poolSize) {
    return placeChunk(userIdFromScope(scope), fileKey, chunkIndex, poolSize);
  },
  shardDOName(scope, shardIndex) {
    return shardDOName(userIdFromScope(scope), shardIndex);
  },
  userDOName(scope) {
    return `user:${userIdFromScope(scope)}`;
  },
};
