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
  type FileInfoOpts,
  type ListFilesOpts,
  type ListFilesItem,
  type ListFilesPage,
  type ListVersionsOpts,
  type VersionMarkOpts,
} from "./vfs";

// cap constants — surfaced for client-side pre-validation
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
  // encryption-surface error classes.
  EBADF,
  ENOTSUP,
  MossaicUnavailableError,
  isLikelyUnavailable,
  type VFSErrorCode,
} from "./errors";

// encryption types are re-exported from the main entry for
// convenient `import { type EncryptionConfig } from "@mossaic/sdk"`.
// The runtime helpers (encrypt/decrypt) stay on the `/encryption` lazy
// chunk to avoid pulling crypto code into bundles that don't need it.
export type {
  EncryptionConfig,
  EncryptionMode,
  FileEncryption,
  AadTag,
} from "@shared/encryption-types";
export type {
  ReadStreamOptions,
  ReadHandle,
  WriteHandle,
} from "./streams";

// Universal preview pipeline — types only; the renderer
// implementations live in the worker (server-side rendering). SDK
// consumers call `vfs.readPreview()` and receive `ReadPreviewResult`.
export type {
  Variant,
  StandardVariant,
  CustomVariant,
  FitMode,
  PreviewFormat,
  ReadPreviewOpts,
  ReadPreviewResult,
  FileVariant,
} from "../../shared/preview-types";
export { STANDARD_VARIANT_DIMS } from "../../shared/preview-types";

// Re-export the DO classes so consumer Workers can re-export them in
// their own entry module — wrangler resolves DO bindings via the
// consumer's main module's exports, not via npm dep graph.
//
// re-export the Core class as `UserDO` so consumer Workers
// can continue to declare `class_name: "UserDO"` in their wrangler
// without any change. The class itself is `UserDOCore` in the
// production tree (worker/core/objects/user/user-do-core.ts);
// aliasing on export preserves the SDK's public API contract.
//
// SearchDO is intentionally NOT re-exported. It backed
// the photo-library's CLIP/BGE vector search, which is not part of
// the SDK's pure-VFS contract. Consumers who need semantic search
// should run their own Vectorize index or DO; nothing in the VFS
// surface (UserDO + ShardDO) depends on SearchDO.
//
// ALSO re-export under the canonical `MossaicUserDO` /
// `MossaicShardDO` names. Both pairs reach the same underlying
// class; new workspace consumers should prefer the canonical
// names so their wrangler `class_name` strings clearly identify
// the SDK origin. Existing `UserDO` / `ShardDO` re-exports stay
// for back-compat — production wrangler at
// mossaic.ashishkumarsingh.com binds `class_name: "UserDO"` and
// must not be disturbed.
export {
  UserDOCore as UserDO,
  UserDOCore as MossaicUserDO,
} from "../../worker/core/objects/user/index";
export {
  ShardDO,
  ShardDO as MossaicShardDO,
} from "../../worker/core/objects/shard/index";

// VFSScope is the wire shape of the multi-tenant scope; consumers
// rarely need it directly but isomorphic-git plugins or HTTP fallback
// adapters may.
export type { VFSScope } from "../../shared/vfs-types";

// Token issuance helpers (operator-side; needs JWT_SECRET in env).
export { issueVFSToken, verifyVFSToken, type VFSTokenPayload } from "./auth";

// HTTP fallback for non-Worker consumers.
export {
  createMossaicHttpClient,
  HttpVFS,
  type CreateMossaicHttpClientOptions,
  type VFSClient,
} from "./http";

// parallel multipart transfer engine. Built on top of the
// HTTP client; saturates user bandwidth via N-way parallel chunk
// uploads/downloads while keeping UserDO touches limited to session
// boundaries (begin + finalize).
export {
  parallelUpload,
  parallelDownload,
  parallelDownloadStream,
  beginUpload,
  putChunk,
  finalizeUpload,
  abortUpload,
  statusUpload,
  deriveClientChunkSpec,
  THROUGHPUT_MATH,
  type BeginUploadOpts,
  type BeginUploadResult,
  type ParallelUploadOpts,
  type ParallelDownloadOpts,
  type ProgressEvent as TransferProgressEvent,
  type ChunkEvent,
  type ManifestEvent,
  type MossaicHttpClient,
} from "./transfer";

// isomorphic-git adapter (optional batched-lstat). Re-exported
// from the root for ergonomic single-import use; also available via
// the explicit `@mossaic/sdk/fs` subpath.
export { createIgitFs, type CreateIgitFsOptions } from "./igit";

// yjs-mode bit constant. The runtime adapter (openYDoc,
// YDocHandle) lives at the `@mossaic/sdk/yjs` subpath so the main
// bundle stays free of the optional `yjs` peer dep — only consumers
// that import `@mossaic/sdk/yjs` pay for the runtime.
export { VFS_MODE_YJS_BIT } from "../../shared/constants";

// content-hash helpers. Exposed at the root so consumers
// (e.g. browser-side `useUpload`) can hash chunks the same way the
// SDK does — single import surface, no internal `@shared/*` reach-in.
export { hashChunk, computeFileHash } from "../../shared/crypto";

// AIMD controller. Exposed as a building block for consumer-side
// adaptive transfer engines that aren't covered by `parallelUpload`
// (e.g. the photo-library SPA's existing per-chunk PUT path against
// the App's legacy `/api/upload/chunk/*` URL contract).
export { AIMDController } from "../../shared/aimd";

// chunk-spec helper for callers that need to mirror the server's
// adaptive chunk-size decisions on the client.
export { computeChunkSpec } from "../../shared/chunking";

import { VFS, type CreateVFSOptions, type MossaicEnv } from "./vfs";

/**
 * Construct a VFS client. Each call returns a fresh instance — they
 * are cheap (no I/O happens until a method is invoked) so the typical
 * pattern is to construct one per request.
 *
 * The consumer's wrangler.jsonc must declare TWO Durable Object
 * bindings — `MOSSAIC_USER` (pointing at the re-exported `UserDO` class)
 * and `MOSSAIC_SHARD` (pointing at `ShardDO`). The SDK only addresses
 * `MOSSAIC_USER` directly; `MOSSAIC_SHARD` is consumed internally by
 * the bundled UserDO code from its own env. (See
 * `@mossaic/sdk/templates/wrangler.jsonc` for the template.)
 */
export function createVFS(env: MossaicEnv, opts: CreateVFSOptions): VFS {
  return new VFS(env, opts);
}
