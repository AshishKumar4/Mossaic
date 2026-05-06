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

/** Default upload-session TTL — 24h. Configurable per `beginUpload` call. */
export const MULTIPART_DEFAULT_TTL_MS = 24 * 60 * 60 * 1000;

/** Default download-token TTL — 1h. Short by design; downloads are bursty. */
export const DOWNLOAD_TOKEN_DEFAULT_TTL_MS = 60 * 60 * 1000;

/** Hard ceiling on session TTL; prevents a misbehaving caller from minting eternal tokens. */
export const MULTIPART_MAX_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

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
  ns: string;
  tn: string;
  sub?: string;
  poolSize: number;
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

// ── Begin / Finalize / Abort wire types ────────────────────────────────

/** Body of `POST /api/vfs/multipart/begin`. */
export interface MultipartBeginRequest {
  path: string;
  size: number;
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
  recommendedConcurrency?: number;
}

/** Body of `POST /api/vfs/multipart/finalize`. */
export interface MultipartFinalizeRequest {
  uploadId: string;
  chunkHashList: string[];
}

/** Response of `POST /api/vfs/multipart/finalize`. */
export interface MultipartFinalizeResponse {
  fileId: string;
  size: number;
  chunkCount: number;
  fileHash: string;
}

/** Body of `POST /api/vfs/multipart/abort`. */
export interface MultipartAbortRequest {
  uploadId: string;
}

/** Response of `GET /api/vfs/multipart/:uploadId/status`. */
export interface MultipartStatusResponse {
  landed: number[];
  total: number;
  bytesUploaded: number;
  expiresAtMs: number;
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
}

export interface ShardMultipartClearResponse {
  dropped: number;
}
