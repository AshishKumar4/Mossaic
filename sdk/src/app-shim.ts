/**
 * Phase 17 — internal shim for the photo-library App at
 * mossaic.ashishkumarsingh.com.
 *
 * NOT part of the SDK's public API. This file is intentionally not
 * re-exported from `sdk/src/index.ts` — external SDK consumers
 * continue to address the canonical `vfs:${ns}:${tenant}` UserDO
 * namespace via `createVFS`. Only the App's own monorepo paths
 * (`worker/app/routes/*`) reach this shim, by importing the
 * concrete file path.
 *
 * ── Why ────────────────────────────────────────────────────────────
 *
 * The App's UserDO instances are named via the LEGACY `userDOName`
 * pattern (`user:${userId}`); the canonical `createVFS` uses
 * `vfsUserDOName` (`vfs:${ns}:${tenant}`). Production data lives in
 * the legacy form and cannot be remapped. This shim addresses the
 * legacy UserDO instance while reusing the SDK's VFS surface.
 *
 * ── What works through this shim ───────────────────────────────────
 *
 * Operations that touch ONLY UserDO's SQLite tables (no ShardDO
 * fan-out):
 *
 *   - `vfs.mkdir`, `vfs.rmdir`, `vfs.readdir`         — folders table
 *   - `vfs.listFiles`                                 — files index
 *   - `vfs.stat`, `vfs.lstat`, `vfs.exists`           — files row
 *   - `vfs.unlink`                                    — files row + GC
 *   - `vfs.openManifest`                              — files + file_chunks
 *   - `vfs.patchMetadata`                             — files row
 *   - `vfs.rename`                                    — files row
 *
 * ── What does NOT work through this shim ───────────────────────────
 *
 * Operations that fan out to ShardDOs:
 *
 *   - `vfs.writeFile` / `vfs.readFile`
 *   - `vfs.readChunk`
 *   - `vfs.createReadStream` / `vfs.createWriteStream`
 *   - `parallelUpload` / `parallelDownload`
 *
 * The server-side VFS write/read helpers in
 * `worker/core/objects/user/vfs-ops.ts` resolve shard targets via
 * `vfsShardDOName(scope.ns, scope.tenant, scope.sub, idx)`. Through
 * this shim, that resolves to `vfs:default:${userId}:s${idx}` — a
 * DIFFERENT physical DO from the legacy `shard:${userId}:${idx}`
 * where the App's existing chunk bytes live.
 *
 * For the photo-library App's chunk PUT/GET path, the App routes
 * continue to address `shardDOName(userId, idx)` directly. The
 * upload-route migration to `parallelUpload` is therefore deferred
 * to a follow-up phase that includes ShardDO data migration (see
 * `local/phase-17-plan.md` §5.9).
 */

import { VFS, type CreateVFSOptions, type MossaicEnv } from "./vfs";
import { legacyAppPlacement } from "../../shared/placement";

export interface CreateAppVFSOptions
  extends Omit<CreateVFSOptions, "namespace" | "sub" | "placement"> {
  /** App user id; addresses DOs as `user:<userId>`. */
  userId: string;
}

/**
 * Construct a VFS instance bound to the legacy UserDO naming
 * (`user:<userId>`) and legacy ShardDO naming
 * (`shard:<userId>:<idx>`).
 *
 * Phase 17.5: this is now a thin factory that passes
 * `placement: legacyAppPlacement` to the canonical `VFS` constructor.
 * No subclass override needed — the base class's `user()` method
 * routes through the placement abstraction (`sdk/src/vfs.ts`).
 *
 * The returned `VFS` satisfies the same `VFSClient` surface as
 * `createVFS`, with the documented constraint that chunk-touching
 * operations land on the legacy `shard:${userId}:${idx}` instances
 * where the App's existing photo-library data lives.
 */
export function createAppVFS(
  env: MossaicEnv,
  opts: CreateAppVFSOptions
): VFS {
  return new VFS(env, {
    tenant: opts.userId,
    ...opts,
    placement: legacyAppPlacement,
  });
}
