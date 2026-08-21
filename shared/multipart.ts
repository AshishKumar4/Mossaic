/**
 * Multipart parallel transfer engine.
 *
 * Shared types and constants used across worker (UserDO + ShardDO +
 * routes) and SDK (transfer.ts). Carrying these in a single shared
 * module guarantees the wire shape stays in sync between the two
 * sides — the SDK marshals what the routes expect, the routes marshal
 * what the DO RPCs expect, all from the same source of truth.
 *
 * No runtime imports here — pure types + numeric/string constants so
 * this file is safe to import from both worker and browser bundles.
 */

/** Sentinel for the upload-session HMAC token. Distinct from "vfs" and "vfs-dl" (§4). */
export const VFS_MP_SCOPE = "vfs-mp" as const;

/** Sentinel for the cacheable-chunk download HMAC token. */
export const VFS_DL_SCOPE = "vfs-dl" as const;

/** Sentinel for opaque multipart status continuations. */
export const VFS_MP_STATUS_SCOPE = "vfs-mp-status" as const;

/** Current multipart control-plane protocol. Missing means legacy v1. */
export const MULTIPART_PROTOCOL_VERSION = 2;

/** Negotiated capability for bounded hash, finalize, abort, and status pages. */
export const MULTIPART_PAGED_CONTROL_CAPABILITY = "paged-control-v2";

/** Placement v1 is the original O(poolSize) rendezvous algorithm. */
export const MULTIPART_LEGACY_PLACEMENT_VERSION = 1;

/** Placement used by newly-created multipart protocol v2 sessions. */
export const MULTIPART_PLACEMENT_VERSION = 2;

export type MultipartPlacementVersion =
  | typeof MULTIPART_LEGACY_PLACEMENT_VERSION
  | typeof MULTIPART_PLACEMENT_VERSION;

export function isMultipartPlacementVersion(
  value: unknown
): value is MultipartPlacementVersion {
  return (
    value === MULTIPART_LEGACY_PLACEMENT_VERSION ||
    value === MULTIPART_PLACEMENT_VERSION
  );
}

/** Maximum expected-hash and verification rows processed per call. */
export const MULTIPART_HASH_PAGE_SIZE = 256;

/** Maximum encoded JSON body accepted by the expected-hash page route. */
export const MULTIPART_HASH_PAGE_MAX_BODY_BYTES = 24 * 1024;

/** Maximum shard fences persisted per finalize step. */
export const MULTIPART_FENCE_PAGE_SIZE = 64;

/** Maximum shards inspected by one multipart status/resume invocation. */
export const MULTIPART_STATUS_SHARD_PAGE_SIZE = 64;

/** Maximum landed entries returned by one multipart status/resume invocation. */
export const MULTIPART_STATUS_ENTRY_PAGE_SIZE = 256;

/** Continuations are small JWTs; reject oversized inputs before verification/RPC. */
export const MULTIPART_STATUS_CURSOR_MAX_BYTES = 4 * 1024;

/** Default upload-session TTL — 24h. Configurable per `beginUpload` call. */
export const MULTIPART_DEFAULT_TTL_MS = 24 * 60 * 60 * 1000;

/** Default download-token TTL — 1h. Short by design; downloads are bursty. */
export const DOWNLOAD_TOKEN_DEFAULT_TTL_MS = 60 * 60 * 1000;

/** Hard ceiling on session TTL; prevents a misbehaving caller from minting eternal tokens. */
export const MULTIPART_MAX_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

/** Terminal results remain replayable until every session token could have expired. */
export const MULTIPART_TERMINAL_RETENTION_MS = MULTIPART_MAX_TTL_MS;

/** Terminal fences outlive the longest token by this safety margin. */
export const MULTIPART_FENCE_GC_GRACE_MS = 60 * 60 * 1000;

/** Hard ceiling on the per-chunk size accepted by the multipart PUT route. */
export const MULTIPART_MAX_CHUNK_BYTES = 4 * 1024 * 1024; // 2× MAX_BLOB_SIZE; defensive

/** Per-tenant ceiling on concurrent open multipart sessions. Defends against orphan-session storms. */
export const MULTIPART_MAX_OPEN_SESSIONS_PER_TENANT = 64;

/**
 * Wire shape of a `vfs-mp` session token's *payload* (after JWT verify
 * + scope discrimination). Mirrors the JWT claim names exactly so we
 * can hand a parsed `payload` straight into the verify result.
 */
export interface MultipartSessionTokenPayload {
  scope: typeof VFS_MP_SCOPE;
  uploadId: string;
  /** Random per-session capability used by ShardDO terminal fences. */
  fenceId?: string;
  /** Canonical tenant user id bound into the session capability. */
  userId?: string;
  ns: string;
  tn: string;
  sub?: string;
  poolSize: number;
  /** Missing on tokens minted before placement versioning; interpreted as v1. */
  placementVersion?: MultipartPlacementVersion;
  totalChunks: number;
  chunkSize: number;
  totalSize: number;
  iat: number;
  exp: number;
}

/** Wire shape of a `vfs-dl` download token's payload. */
export interface DownloadTokenPayload {
  scope: typeof VFS_DL_SCOPE;
  fileId: string;
  ns: string;
  tn: string;
  sub?: string;
  iat: number;
  exp: number;
}

/** Signed seek state for a multipart status page. */
export interface MultipartStatusCursorPayload {
  scope: typeof VFS_MP_STATUS_SCOPE;
  uploadId: string;
  userId: string;
  ns: string;
  tn: string;
  sub?: string;
  shardIndex: number;
  afterIndex: number;
  iat: number;
  exp: number;
}

// ── Begin / Finalize / Abort wire types ────────────────────────────────

/** Body of `POST /api/vfs/multipart/begin`. */
export interface MultipartBeginRequest {
  path: string;
  size: number;
  /** Capabilities understood by the client. Unknown values are ignored. */
  capabilities?: readonly string[];
  /** Compatibility advertisement used by protocol-v2 SDK prereleases. */
  protocolVersion?: number;
  chunkSize?: number;
  mode?: number;
  mimeType?: string;
  metadata?: Record<string, unknown> | null;
  tags?: readonly string[];
  version?: { label?: string; userVisible?: boolean };
  encryption?: { mode: "convergent" | "random"; keyId?: string };
  resumeFrom?: string;
  ttlMs?: number;
}

/** Response of `POST /api/vfs/multipart/begin`. */
export interface MultipartBeginResponse {
  uploadId: string;
  chunkSize: number;
  totalChunks: number;
  poolSize: number;
  sessionToken: string;
  putEndpoint: string;
  expiresAtMs: number;
  landed: number[];
  /** Opaque cursor for the next bounded landed page. */
  continuation?: string;
  recommendedConcurrency?: number;
  /** Capabilities selected by the server. Absent on legacy servers. */
  capabilities?: string[];
  /** Compatibility selection returned to protocol-v2 SDK prereleases. */
  protocolVersion?: number;
}

/** Body of `POST /api/vfs/multipart/finalize`. */
export interface MultipartFinalizeRequest {
  uploadId: string;
  chunkHashList: string[];
}

export interface MultipartHashPageRequest {
  uploadId: string;
  startIndex: number;
  hashes: string[];
}

export interface MultipartHashPageResponse {
  staged: number;
  total: number;
}

export interface MultipartFinalizeStepRequest {
  uploadId: string;
}

/** Response of `POST /api/vfs/multipart/finalize`. */
export interface MultipartFinalizeResponse {
  fileId: string;
  /** New version id when versioning was frozen on; empty otherwise. */
  versionId: string;
  size: number;
  chunkCount: number;
  fileHash: string;
  /**
   * Absolute path the finalize committed to. Reconstructed from the
   * session's `(parent_id, leaf)` server-side. Surfaced so the route
   * layer can dispatch follow-on side effects (preview pre-gen)
   * without re-querying.
   */
  path: string;
  /** File's MIME type as recorded on the session. */
  mimeType: string;
  /**
   * `true` when the file was uploaded with per-chunk encryption (the
   * session carries an `encryption_mode`). Pre-gen renderers MUST
   * skip encrypted files — the server can't decrypt envelopes.
   */
  isEncrypted: boolean;
}

export type MultipartFinalizeProgress =
  | {
      done: false;
      phase:
        | "fencing"
        | "verifying"
        | "preparing"
        | "publishing"
        | "cleaning";
      cursor: number;
      total: number;
    }
  | {
      done: true;
      result: MultipartFinalizeResponse;
      /** True only on the first terminal response, for idempotent side effects. */
      fresh: boolean;
    };

/** Body of `POST /api/vfs/multipart/abort`. */
export interface MultipartAbortRequest {
  uploadId: string;
}

export type MultipartAbortProgress =
  | {
      done: false;
      phase: "fencing" | "intents" | "cleanup" | "old_intents" | "local";
      cursor: number;
      total: number;
    }
  | { done: true };

/** Response of `GET /api/vfs/multipart/:uploadId/status`. */
export interface MultipartStatusResponse {
  landed: number[];
  total: number;
  bytesUploaded: number;
  expiresAtMs: number;
}

/** One bounded status page. Absence of `continuation` means the scan is complete. */
export interface MultipartStatusPageResponse extends MultipartStatusResponse {
  continuation?: string;
}

/** Response of `PUT /api/vfs/multipart/:uploadId/chunk/:idx`. */
export interface MultipartPutChunkResponse {
  ok: true;
  hash: string;
  idx: number;
  bytesAccepted: number;
  status: "created" | "deduplicated" | "superseded";
}

/** Body of `POST /api/vfs/multipart/download-token`. */
export interface DownloadTokenRequest {
  path: string;
  ttlMs?: number;
}

/** Response of `POST /api/vfs/multipart/download-token`. */
export interface DownloadTokenResponse {
  token: string;
  expiresAtMs: number;
  // Manifest is included so the SDK saves a round-trip — same data
  // it would otherwise need from `openManifest`. Shape mirrors
  // OpenManifestResult from shared/vfs-types.
  manifest: {
    fileId: string;
    size: number;
    chunkSize: number;
    chunkCount: number;
    chunks: Array<{ index: number; hash: string; size: number }>;
    inlined: boolean;
    /**
     * Surfaced for SPA Blob construction. Optional so the canonical
     * route stays backward-compatible; the App-pinned route always
     * populates it from the legacy `files.mime_type` column.
     */
    mimeType?: string;
  };
}

// ── Internal ShardDO wire shapes (HTTP-internal, used by UserDO finalize) ──

export interface ShardMultipartManifestRow {
  idx: number;
  hash: string;
  size: number;
}

export interface ShardMultipartManifestResponse {
  rows: ShardMultipartManifestRow[];
}

export interface ShardMultipartLandedResponse {
  idx: number[];
  /** Parallel to `idx`; absent only on an older ShardDO during a rolling deploy. */
  sizes?: number[];
}

export interface ShardMultipartClearResponse {
  dropped: number;
}
