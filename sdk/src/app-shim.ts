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

import { VFS, type CreateVFSOptions, type MossaicEnv, type UserDOClient } from "./vfs";
import type { VFSScope } from "../../shared/vfs-types";
import { userDOName } from "../../worker/core/lib/utils";

export interface CreateAppVFSOptions
  extends Omit<CreateVFSOptions, "namespace" | "sub"> {
  /** App user id; addresses DOs as `user:<userId>`. */
  userId: string;
}

/**
 * App-only VFS subclass. Overrides `user()` to address the legacy
 * `user:<userId>` UserDO instance. `scope()` still returns a
 * canonical-shaped `VFSScope` because the server-side typed RPCs
 * (`vfsListFiles`, `vfsMkdir`, etc.) all expect a `VFSScope`
 * argument; the scope drives only the rate-limiter accounting and
 * (for shard-touching ops) the shard naming. Since this shim is
 * scoped to non-shard operations, the scope's tenant identity is
 * what matters.
 */
class AppVFS extends VFS {
  protected override user(): UserDOClient {
    const id = this.env.MOSSAIC_USER.idFromName(userDOName(this.opts.tenant));
    return this.env.MOSSAIC_USER.get(id) as UserDOClient;
  }

  protected override scope(): VFSScope {
    return { ns: "default", tenant: this.opts.tenant };
  }
}

/**
 * Construct an `AppVFS` instance bound to the legacy UserDO naming
 * (`user:<userId>`). The returned object satisfies the same
 * `VFSClient` surface as `createVFS`, but with the documented
 * shard-addressing caveat.
 */
export function createAppVFS(
  env: MossaicEnv,
  opts: CreateAppVFSOptions
): AppVFS {
  return new AppVFS(env, { tenant: opts.userId, ...opts });
}
