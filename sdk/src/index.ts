/**
 * @mossaic/sdk — Cloudflare-Worker-native VFS over Mossaic.
 *
 * Two consumer-facing things:
 *
 *   1. `createVFS(env, opts)` returns a `VFS` instance that exposes a
 *      thin fs/promises shape. Each method is one DO RPC subrequest in
 *      the consumer's invocation; internal chunk fan-out happens inside
 *      Mossaic's UserDO and is billed against Mossaic's own
 *      per-invocation subrequest budget, not the consumer's.
 *
 *   2. Re-exports of `UserDO`, `ShardDO`, `SearchDO` so consumer Workers
 *      can re-export them in turn — wrangler discovers DO classes from
 *      the consumer's main module's exports, not from package
 *      dependencies. Mirrors the `cloudflare/sandbox-sdk` and
 *      `cloudflare/agents` precedent.
 *
 * Typical consumer:
 *
 *     // src/index.ts
 *     import { UserDO, ShardDO, createVFS } from "@mossaic/sdk";
 *     export { UserDO, ShardDO };
 *     export default {
 *       async fetch(req, env) {
 *         const vfs = createVFS(env, { tenant: "acme" });
 *         await vfs.writeFile("/foo.txt", "hi");
 *         return new Response("ok");
 *       },
 *     };
 *
 *     // wrangler.jsonc — copy from @mossaic/sdk/templates/wrangler.jsonc
 */

export { VFS, type CreateVFSOptions, type MossaicEnv } from "./vfs";
export { VFSStat } from "./stats";
export {
  VFSFsError,
  ENOENT,
  EEXIST,
  EISDIR,
  ENOTDIR,
  EFBIG,
  ELOOP,
  EBUSY,
  EINVAL,
  EACCES,
  EROFS,
  ENOTEMPTY,
  MossaicUnavailableError,
  type VFSErrorCode,
} from "./errors";
export type {
  ReadStreamOptions,
  ReadHandle,
  WriteHandle,
} from "./streams";

// Re-export the DO classes so consumer Workers can re-export them in
// their own entry module — wrangler resolves DO bindings via the
// consumer's main module's exports, not via npm dep graph.
export { UserDO } from "../../worker/objects/user/index";
export { ShardDO } from "../../worker/objects/shard/index";
export { SearchDO } from "../../worker/objects/search/index";

// VFSScope is the wire shape of the multi-tenant scope; consumers
// rarely need it directly but isomorphic-git plugins or HTTP fallback
// adapters may.
export type { VFSScope } from "../../shared/vfs-types";

// Token issuance helpers (operator-side; needs JWT_SECRET in env).
export { issueVFSToken, verifyVFSToken, type VFSTokenPayload } from "./auth";

import { VFS, type CreateVFSOptions, type MossaicEnv } from "./vfs";

/**
 * Construct a VFS client. Each call returns a fresh instance — they
 * are cheap (no I/O happens until a method is invoked) so the typical
 * pattern is to construct one per request.
 *
 * The consumer's wrangler.jsonc must declare a Durable Object binding
 * named `MOSSAIC_USER` pointing at the re-exported `UserDO` class.
 * (See `@mossaic/sdk/templates/wrangler.jsonc` for the template.)
 */
export function createVFS(env: MossaicEnv, opts: CreateVFSOptions): VFS {
  return new VFS(env, opts);
}
