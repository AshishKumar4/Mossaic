/**
 * `@mossaic/sdk/http` — a Node-safe SDK entry that exposes the HTTP
 * client surface ONLY.
 *
 * The default `@mossaic/sdk` entry re-exports `UserDO` / `ShardDO`,
 * which transitively imports `cloudflare:workers`. That virtual
 * module is unresolvable in plain Node, so non-Worker consumers
 * (the `@mossaic/cli`, third-party scripts, etc.) need an entry
 * that omits the DO re-exports.
 *
 * This module exposes:
 *   - `createMossaicHttpClient` / `HttpVFS`
 *   - `issueVFSToken` / `verifyVFSToken`
 *   - all error classes + `mapServerError` helper
 *   - all type exports needed for typed client code
 *   - capacity constants
 *
 * It intentionally does NOT export `createVFS`, `UserDO`, `ShardDO`,
 * `createIgitFs`, or the Yjs adapter — those require either a Worker
 * runtime or peer-dep bring-your-own that the SDK's main entry handles.
 */

export {
  createMossaicHttpClient,
  HttpVFS,
  type CreateMossaicHttpClientOptions,
  type VFSClient,
} from "./http";

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
  WriteFileOpts,
  CopyFileOpts,
  PatchMetadataOpts,
  ListFilesOpts,
  ListFilesItem,
  ListFilesPage,
  ListVersionsOpts,
  VersionMarkOpts,
  VersionInfo,
  DropVersionsPolicy,
} from "./vfs";

// cap constants — surfaced for client-side pre-validation.
export {
  METADATA_MAX_BYTES,
  TAGS_MAX_PER_FILE,
  TAG_MAX_LEN,
  TAGS_MAX_PER_LIST_QUERY,
  LIST_LIMIT_MAX,
  LIST_LIMIT_DEFAULT,
} from "../../shared/metadata-caps";

// Yjs-mode bit constant — pure value, no Yjs runtime needed.
export { VFS_MODE_YJS_BIT } from "./yjs-mode-bit";

// Token issuance helpers (operator-side; needs JWT_SECRET in env).
export { issueVFSToken, verifyVFSToken, type VFSTokenPayload } from "./auth";

// Re-export shared scope shape for typed scopes in client code.
export type { VFSScope } from "../../shared/vfs-types";

// parallel multipart transfer engine.
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
  type MossaicHttpClient,
} from "./transfer";
