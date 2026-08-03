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

/** Sentinel for the opaque status/resume continuation. */
export const VFS_MP_STATUS_SCOPE = "vfs-mp-status" as const;

/** Placement v1 is the original O(poolSize) rendezvous algorithm. */
export const MULTIPART_LEGACY_PLACEMENT_VERSION = 1;

/** Placement frozen into every session this server mints. */
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

/**
 * Control plane a client must speak to drive an upload whose finalize cannot
 * fit in one call. Absent means the caller only knows the original one-request
 * finalize, which the server then has to keep within its bounded limits.
 */
export const MULTIPART_PROTOCOL_VERSION = 2;

/** Expected hashes staged, and chunks verified, per call. */
export const MULTIPART_HASH_PAGE_SIZE = 256;

/** Shard fences persisted, and shards fanned out to, per call. */
export const MULTIPART_FENCE_PAGE_SIZE = 64;

/** Shards one status or resume invocation may probe. */
export const MULTIPART_STATUS_SHARD_PAGE_SIZE = 64;

/** Landed entries one status or resume invocation may report. */
export const MULTIPART_STATUS_ENTRY_PAGE_SIZE = 256;

/**
 * Longest continuation this server will look at. A continuation is a small
 * signed token, so anything larger is rejected before it costs a verify.
 */
export const MULTIPART_STATUS_CURSOR_MAX_BYTES = 4 * 1024;

// ── Bounded-operation arithmetic ───────────────────────────────────────
//
// Both sides of the protocol need the same answer to "how much work does
// this shape cost". The server refuses at begin what one request cannot
// finish; the client picks between the one-request form and the paged
// control plane from the dimensions the server already handed it, without
// spending a round-trip to discover the answer. One definition, so the two
// can never disagree about where the boundary is.

/** Shard round-trips finalizing this shape costs, fencing through publication. */
export function multipartFinalizeFanout(
  totalChunks: number,
  poolSize: number
): number {
  const chunkPages = Math.ceil(totalChunks / MULTIPART_HASH_PAGE_SIZE);
  // Fencing walks the whole pool once, and every chunk page walks the shards
  // its own indices are placed on — at most one page's worth of them.
  return poolSize + chunkPages * Math.min(poolSize, MULTIPART_HASH_PAGE_SIZE);
}

/**
 * Shard round-trips one `finalize` invocation may spend.
 *
 * Half of the per-invocation subrequest budget is what a caller who only
 * knows the one-request finalize may commit to at begin; the rest stays for
 * the cleanup drain publication triggers.
 */
export const MULTIPART_ONE_REQUEST_FINALIZE_MAX_FANOUT = 500;

/**
 * Bounded pages one one-request finalize may run before it asks the caller to
 * call again. Begin bounds the pages an upload of its own needs; this also
 * bounds the routing scan over a file the upload displaces, whose size the
 * upload itself says nothing about. Every page it did run is durable, so the
 * next call resumes from the cursor instead of restarting.
 */
export const MULTIPART_ONE_REQUEST_FINALIZE_MAX_PAGES = 512;

/**
 * Pages one one-request abort may run before it hands the rest to the alarm.
 *
 * Only two phases leave the object — fencing walks the pool, and the terminal
 * page drains one bounded batch of the cleanup it staged — so the ceiling here
 * is about the local pages, which is why it can be this generous.
 */
export const MULTIPART_ONE_REQUEST_ABORT_MAX_PAGES = 512;

/**
 * Bounded requests driving a fresh finalize costs: one per staged hash page,
 * then one per page of the machine — fencing the pool, verifying each chunk
 * page against the shards holding it, publishing, and reaping the scratch the
 * publication leaves.
 *
 * A publication that displaces an existing file owes further pages over *that*
 * file's manifest, whose length this upload's dimensions say nothing about, so
 * this is the cost of the fresh path rather than of every path.
 */
export function multipartFinalizeRequestCount(
  chunkCount: number,
  poolSize: number
): number {
  const fencePages = Math.ceil(poolSize / MULTIPART_FENCE_PAGE_SIZE);
  const hashPages = Math.ceil(chunkCount / MULTIPART_HASH_PAGE_SIZE);
  // An empty upload still costs the page that observes it has nothing to
  // verify, and the publication after it.
  if (hashPages === 0) return fencePages + 2;
  const verifyPagesPerChunkPage = Math.ceil(
    Math.min(chunkCount, poolSize, MULTIPART_HASH_PAGE_SIZE) /
      MULTIPART_FENCE_PAGE_SIZE
  );
  return (
    hashPages +
    fencePages +
    hashPages * Math.max(1, verifyPagesPerChunkPage) +
    1 +
    hashPages
  );
}

/**
 * Bounded pages an abort of this shape owes: fencing and staging intents walk
 * the pool, dropping what a finalize staged walks the manifest, discarding its
 * routing walks the pool again, and one page makes the session terminal.
 */
export function multipartAbortPageCount(
  totalChunks: number,
  poolSize: number
): number {
  const poolPages = Math.ceil(poolSize / MULTIPART_FENCE_PAGE_SIZE);
  const cleanupPages = Math.max(
    1,
    Math.ceil(totalChunks / MULTIPART_HASH_PAGE_SIZE)
  );
  return poolPages * 3 + cleanupPages + 1;
}

/**
 * Bounded status pages a session owes. Every page ends either because it
 * filled its entry budget or because it exhausted a page of the pool, so the
 * two limits bound the walk together.
 */
export function multipartStatusPageCount(
  totalChunks: number,
  poolSize: number
): number {
  return (
    Math.floor(totalChunks / MULTIPART_STATUS_ENTRY_PAGE_SIZE) +
    Math.ceil(poolSize / MULTIPART_STATUS_SHARD_PAGE_SIZE)
  );
}

/** Default upload-session TTL — 24h. Configurable per `beginUpload` call. */
export const MULTIPART_DEFAULT_TTL_MS = 24 * 60 * 60 * 1000;

/** Default download-token TTL — 1h. Short by design; downloads are bursty. */
export const DOWNLOAD_TOKEN_DEFAULT_TTL_MS = 60 * 60 * 1000;

/** Hard ceiling on session TTL; prevents a misbehaving caller from minting eternal tokens. */
export const MULTIPART_MAX_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

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

/**
 * Seek state of one status or resume page, signed so the server never has to
 * trust a caller with where its own scan got to.
 *
 * The tenant and upload it was minted for are part of the payload: a
 * continuation presented against any other upload, or by any other tenant, is
 * refused rather than followed.
 */
export interface MultipartStatusCursorPayload {
  scope: typeof VFS_MP_STATUS_SCOPE;
  uploadId: string;
  userId: string;
  ns: string;
  tn: string;
  sub?: string;
  /** Shard the next page resumes on. */
  shardIndex: number;
  /** Highest chunk index that shard already reported. */
  afterIndex: number;
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
  /** Control plane the client can drive. Absent means one-request finalize. */
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
  /** Opaque seek state for the next bounded landed page, when one remains. */
  continuation?: string;
  recommendedConcurrency?: number;
  /** Echoed when the server accepted the client's paged control plane. */
  protocolVersion?: number;
}

/** Body of `POST /api/vfs/multipart/finalize`. */
export interface MultipartFinalizeRequest {
  uploadId: string;
  chunkHashList: string[];
}

/** Body of `POST /api/vfs/multipart/hash-page`. */
export interface MultipartHashPageRequest {
  uploadId: string;
  startIndex: number;
  hashes: string[];
}

/** Response of `POST /api/vfs/multipart/hash-page`. */
export interface MultipartHashPageResponse {
  staged: number;
  total: number;
}

/** Body of `POST /api/vfs/multipart/finalize-step`. */
export interface MultipartFinalizeStepRequest {
  uploadId: string;
}

/** Response of `POST /api/vfs/multipart/finalize`. */
export interface MultipartFinalizeResponse {
  fileId: string;
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

/**
 * One `POST /api/vfs/multipart/finalize-step` outcome.
 *
 * `cursor`/`total` are progress for the caller to display, not a resume
 * token: the server holds the only authoritative cursor. `fresh` is true on
 * the single response that observed the operation reach its terminal state,
 * so a caller can run one-shot side effects without a second guard.
 */
export type MultipartFinalizeProgress =
  | {
      done: false;
      phase: "fencing" | "verifying" | "preparing" | "publishing" | "cleaning";
      cursor: number;
      total: number;
    }
  | { done: true; result: MultipartFinalizeResponse; fresh: boolean };

/** Body of `POST /api/vfs/multipart/abort` and `.../abort-step`. */
export interface MultipartAbortRequest {
  uploadId: string;
}

/**
 * One `POST /api/vfs/multipart/abort-step` outcome.
 *
 * Same contract as `MultipartFinalizeProgress`: `cursor`/`total` are progress
 * to display, not a resume token, and `done: true` is only ever returned by a
 * session whose abort is terminal — a caller that sees it owes nothing more.
 */
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

/**
 * One bounded status page. `continuation` is present exactly while shards
 * remain unread, so a caller with no continuation has the complete set — which
 * is every caller whose session fits inside one page.
 */
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

/**
 * One staged chunk, as the shard reports it. Declared as an alias rather than
 * an interface so it carries the implicit index signature
 * `SqlStorage.exec<T>` requires of the projection that produces it.
 */
export type ShardMultipartManifestRow = {
  idx: number;
  hash: string;
  size: number;
};

export interface ShardMultipartManifestResponse {
  rows: ShardMultipartManifestRow[];
}

export interface ShardMultipartLandedResponse {
  idx: number[];
  /** Staged byte count per entry, positionally aligned with `idx`. */
  sizes: number[];
}

export interface ShardMultipartClearResponse {
  dropped: number;
}
