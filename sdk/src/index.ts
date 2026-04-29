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
 *   2. Re-exports of `UserDO` and `ShardDO` so consumer Workers can
 *      re-export them in turn — wrangler discovers DO classes from
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

export {
  VFS,
  type CreateVFSOptions,
  type MossaicEnv,
  type VersionInfo,
  type DropVersionsPolicy,
  type WriteFileOpts,
  type CopyFileOpts,
  type PatchMetadataOpts,
  type ListFilesOpts,
  type ListFilesItem,
  type ListFilesPage,
  type ListVersionsOpts,
  type VersionMarkOpts,
} from "./vfs";

// Phase 12: cap constants — surfaced for client-side pre-validation
// + so consumers know the limits without reading the README.
export {
  METADATA_MAX_BYTES,
  TAGS_MAX_PER_FILE,
  TAG_MAX_LEN,
  TAGS_MAX_PER_LIST_QUERY,
  LIST_LIMIT_MAX,
  LIST_LIMIT_DEFAULT,
} from "../../shared/metadata-caps";
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
  EAGAIN,
  MossaicUnavailableError,
  isLikelyUnavailable,
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
//
// Phase 11: re-export the Core class as `UserDO` so consumer Workers
// can continue to declare `class_name: "UserDO"` in their wrangler
// without any change. The class itself is `UserDOCore` in the
// production tree (worker/core/objects/user/user-do-core.ts);
// aliasing on export preserves the SDK's public API contract.
//
// Phase 11.1: SearchDO is intentionally NOT re-exported. It backed
// the photo-library's CLIP/BGE vector search, which is not part of
// the SDK's pure-VFS contract. Consumers who need semantic search
// should run their own Vectorize index or DO; nothing in the VFS
// surface (UserDO + ShardDO) depends on SearchDO.
export { UserDOCore as UserDO } from "../../worker/core/objects/user/index";
export { ShardDO } from "../../worker/core/objects/shard/index";

// VFSScope is the wire shape of the multi-tenant scope; consumers
// rarely need it directly but isomorphic-git plugins or HTTP fallback
// adapters may.
export type { VFSScope } from "../../shared/vfs-types";

// Token issuance helpers (operator-side; needs JWT_SECRET in env).
export { issueVFSToken, verifyVFSToken, type VFSTokenPayload } from "./auth";

// HTTP fallback for non-Worker consumers (Phase 7).
export {
  createMossaicHttpClient,
  HttpVFS,
  type CreateMossaicHttpClientOptions,
  type VFSClient,
} from "./http";

// isomorphic-git adapter (Phase 8: optional batched-lstat). Re-exported
// from the root for ergonomic single-import use; also available via
// the explicit `@mossaic/sdk/fs` subpath.
export { createIgitFs, type CreateIgitFsOptions } from "./igit";

// Phase 10: yjs-mode bit constant. The runtime adapter (openYDoc,
// YDocHandle) lives at the `@mossaic/sdk/yjs` subpath so the main
// bundle stays free of the optional `yjs` peer dep — only consumers
// that import `@mossaic/sdk/yjs` pay for the runtime.
export { VFS_MODE_YJS_BIT } from "./yjs-mode-bit";

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
