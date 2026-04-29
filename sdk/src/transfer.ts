/**
 * @mossaic/sdk — multipart parallel transfer engine.
 *
 * BitTorrent-class throughput for VFS uploads & downloads. Splits a
 * source file into adaptive chunks, hashes each, and PUTs them in
 * parallel against the multipart endpoints (`/api/vfs/multipart/*`)
 * — saturating user bandwidth without bypassing UserDO for manifest
 * truth.
 *
 * Design invariants (preserved from server-side plan §1):
 *   1. UserDO touched only at session boundaries (`begin` + `finalize`).
 *   2. Per-chunk PUT does ONE ShardDO RPC; no UserDO involvement.
 *  3. encryption composes per-chunk: each plaintext chunk is
 *      sealed independently and the envelope is what the server hashes.
 *   4. Backward-compatible: existing `writeFile`/`readFile` are
 *      preserved; this module is additive.
 *
 * Two surface levels:
 *   - **Raw**: `beginUpload`, `putChunk`, `finalizeUpload`, `abortUpload`,
 *     `statusUpload`. Caller drives the parallelism.
 *   - **High-level**: `parallelUpload(client, path, source, opts)` and
 *     `parallelDownload(client, path, opts)` — built-in adaptive engine
 *     (probe-and-scale 4→64), endgame mode for tail-latency.
 *
 * Plan reference: `local/phase-16-plan.md` §5, §6.
 */

import type { HttpVFS } from "./http";
import { mapServerError, MossaicUnavailableError } from "./errors";

/**
 * Public client alias. The transfer engine is a method
 * surface on top of `HttpVFS`; the alias makes the binding shape
 * intent clearer at call sites.
 */
export type MossaicHttpClient = HttpVFS;
import type {
  MultipartBeginRequest,
  MultipartBeginResponse,
  MultipartFinalizeResponse,
  MultipartPutChunkResponse,
  MultipartStatusResponse,
  DownloadTokenResponse,
} from "@shared/multipart";
import { hashChunk } from "@shared/crypto";
import { computeChunkSpec } from "@shared/chunking";
import { INLINE_LIMIT } from "@shared/inline";

// ── Public option types ────────────────────────────────────────────────

export interface BeginUploadOpts {
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
  signal?: AbortSignal;
}

export type BeginUploadResult = MultipartBeginResponse;

export interface ParallelUploadOpts extends Omit<BeginUploadOpts, "size" | "signal"> {
  /** Concurrency window for the adaptive engine. Defaults: min=1, max=64, initial=4. */
  concurrency?: { min?: number; max?: number; initial?: number };
  /** Endgame trigger threshold (fraction). Defaults to 0.9. Set to 1 to disable. */
  endgameThreshold?: number;
  /** Maximum number of pending chunks at endgame entry. Defaults to 8. */
  endgameMaxFanout?: number;
  /** Resume an existing uploadId obtained from a prior session. */
  resumeUploadId?: string;
  /** Progress callback (throttled to ~10 Hz). */
  onProgress?: (e: ProgressEvent) => void;
  /**
   * Phase 17.6 — per-chunk lifecycle event callback. Optional.
   * When undefined, the engine is byte-equivalent to the pre-17.6
   * implementation (zero overhead).
   */
  onChunkEvent?: (e: ChunkEvent) => void;
  /** Cancel the in-flight upload. Triggers `abortUpload` on the server. */
  signal?: AbortSignal;
  /**
   * Optional per-chunk transformer. encryption uses this to
   * seal each plaintext chunk into an envelope BEFORE the engine
   * hashes and PUTs it. Called once per chunk index, in arbitrary
   * order; must be deterministic given (idx, plaintext) — the same
   * idx must always produce the same envelope bytes.
   */
  chunkTransform?: (
    plaintext: Uint8Array,
    idx: number
  ) => Uint8Array | Promise<Uint8Array>;
}

export interface ProgressEvent {
  uploaded: number;
  total: number;
  chunksDone: number;
  chunksTotal: number;
  currentParallelism: number;
  endgameActive: boolean;
  rttP50Ms: number;
  rttP95Ms: number;
  errorsRecovered: number;
}

/**
 * Phase 17.6 — per-chunk lifecycle event.
 *
 * Emitted from `parallelUpload` / `parallelDownload` (and the
 * streaming download variant). Consumers (e.g. the photo-library
 * SPA) drive a per-chunk status grid from these events.
 *
 * **Per-index ordering guarantee.** Within a single chunk index:
 *   `started` → (`retrying`)* → (`completed` | `failed`)
 *
 * **Cross-index ordering.** Non-deterministic across indices because
 * concurrent lanes process them in parallel. Within a lane, work is
 * serialized by `await` so per-index ordering holds.
 */
export interface ChunkEvent {
  /** Chunk index, 0-based. */
  index: number;
  /** Lifecycle phase. */
  state: "started" | "completed" | "failed" | "retrying";
  /** SHA-256 hex of the chunk; present on `completed`. */
  hash?: string;
  /** Bytes the server accepted; present on `completed`. */
  bytesAccepted?: number;
  /** Retry attempt number (1-indexed); present on `retrying` / `failed`. */
  attempt?: number;
  /** Error message; present on `failed`. */
  error?: string;
}

/**
 * Phase 17.6 — manifest delivered before any chunk download.
 *
 * Fired exactly once by `parallelDownload` / `parallelDownloadStream`
 * after `multipartDownloadToken` returns. Lets consumers seed their
 * per-chunk progress UI state (and capture `mimeType` for Blob
 * construction) before a single chunk arrives.
 */
export interface ManifestEvent {
  fileId: string;
  mimeType: string;
  size: number;
  chunkCount: number;
  chunks: ReadonlyArray<{ index: number; size: number; hash: string }>;
}

export interface ParallelDownloadOpts {
  concurrency?: { min?: number; max?: number; initial?: number };
  endgameThreshold?: number;
  endgameMaxFanout?: number;
  signal?: AbortSignal;
  onProgress?: (e: ProgressEvent) => void;
  /**
   * Phase 17.6 — per-chunk lifecycle event callback. Optional.
   * When undefined, the engine is byte-equivalent to the pre-17.6
   * implementation (zero overhead).
   */
  onChunkEvent?: (e: ChunkEvent) => void;
  /**
   * Phase 17.6 — fired exactly once after the download token returns,
   * before any `onChunkEvent`. Carries the manifest (mimeType, size,
   * chunk index/hash/size triples) for UI seeding.
   */
  onManifest?: (m: ManifestEvent) => void;
  /** Optional per-chunk transformer (e.g. unseal). */
  chunkTransform?: (
    envelope: Uint8Array,
    idx: number
  ) => Uint8Array | Promise<Uint8Array>;
}

// ── Raw protocol ───────────────────────────────────────────────────────

/**
 * Mint a multipart upload session. One round-trip; caller then
 * spawns parallel `putChunk` calls bounded by their own concurrency
 * budget (typically 32–64) and finalises with `finalizeUpload`.
 */
export async function beginUpload(
  client: MossaicHttpClient,
  path: string,
  opts: BeginUploadOpts
): Promise<BeginUploadResult> {
  const body: MultipartBeginRequest = {
    path,
    size: opts.size,
    ...(opts.chunkSize !== undefined ? { chunkSize: opts.chunkSize } : {}),
    ...(opts.mode !== undefined ? { mode: opts.mode } : {}),
    ...(opts.mimeType !== undefined ? { mimeType: opts.mimeType } : {}),
    ...(opts.metadata !== undefined ? { metadata: opts.metadata } : {}),
    ...(opts.tags !== undefined ? { tags: opts.tags } : {}),
    ...(opts.version !== undefined ? { version: opts.version } : {}),
    ...(opts.encryption !== undefined ? { encryption: opts.encryption } : {}),
    ...(opts.resumeFrom !== undefined ? { resumeFrom: opts.resumeFrom } : {}),
    ...(opts.ttlMs !== undefined ? { ttlMs: opts.ttlMs } : {}),
  };
  try {
    return await client.multipartBegin(body, opts.signal);
  } catch (err) {
    throw mapServerError(err, { path, syscall: "open" });
  }
}

/** PUT a single chunk by index. Idempotent under same (uploadId, idx, hash). */
export async function putChunk(
  client: MossaicHttpClient,
  session: BeginUploadResult,
  idx: number,
  bytes: Uint8Array,
  signal?: AbortSignal
): Promise<MultipartPutChunkResponse> {
  return client.multipartPutChunk(
    session.uploadId,
    idx,
    bytes,
    session.sessionToken,
    signal
  );
}

/** Atomic commit. */
export async function finalizeUpload(
  client: MossaicHttpClient,
  session: BeginUploadResult,
  chunkHashList: readonly string[]
): Promise<MultipartFinalizeResponse> {
  try {
    return await client.multipartFinalize(session.uploadId, chunkHashList);
  } catch (err) {
    throw mapServerError(err, {
      syscall: "open",
    });
  }
}

/** Drop the session — releases chunks via the existing GC. */
export async function abortUpload(
  client: MossaicHttpClient,
  session: BeginUploadResult
): Promise<{ ok: true }> {
  try {
    return await client.multipartAbort(session.uploadId);
  } catch (err) {
    throw mapServerError(err, { syscall: "open" });
  }
}

/** Read landed[] for resume / progress. */
export async function statusUpload(
  client: MossaicHttpClient,
  session: BeginUploadResult
): Promise<MultipartStatusResponse> {
  try {
    return await client.multipartStatus(session.uploadId, session.sessionToken);
  } catch (err) {
    throw mapServerError(err, { syscall: "open" });
  }
}

// ── Adaptive engine ────────────────────────────────────────────────────

interface AdaptiveState {
  active: number;
  target: number;
  min: number;
  max: number;
  rttMs: number[];
  successWindow: number;
  errorBackoffMs: number;
  endgameActive: boolean;
  errorsRecovered: number;
}

function quantile(arr: number[], q: number): number {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.min(
    sorted.length - 1,
    Math.max(0, Math.floor(q * sorted.length))
  );
  return sorted[idx];
}

/**
 * High-level parallel upload. Splits `source` into chunks of the
 * server-authoritative size, runs an adaptive concurrency engine,
 * and triggers endgame mode in the tail. encryption is
 * supported via `opts.chunkTransform` (apply seal per-chunk) — the
 * engine treats the transformed bytes as opaque and the server
 * hashes whatever arrives, exactly as the plan §7 specifies.
 *
 * Tiny-file short-circuit: if `source.byteLength <= INLINE_LIMIT`,
 * we still go through multipart (1 chunk total) but the overhead is
 * minimal (~3 round-trips: begin + put + finalize). The SDK could
 * route to the existing `writeFile` path instead; v1 keeps the code
 * uniform — the consumer can always call `vfs.writeFile` directly
 * for tiny payloads.
 */
export async function parallelUpload(
  client: MossaicHttpClient,
  path: string,
  source: Uint8Array | Blob,
  opts: ParallelUploadOpts = {}
): Promise<{
  fileId: string;
  size: number;
  uploadId: string;
  fileHash: string;
}> {
  const totalSize =
    source instanceof Uint8Array ? source.byteLength : source.size;

  // Begin (or resume).
  const session = await beginUpload(client, path, {
    size: totalSize,
    chunkSize: opts.chunkSize,
    mode: opts.mode,
    mimeType: opts.mimeType,
    metadata: opts.metadata,
    tags: opts.tags,
    version: opts.version,
    encryption: opts.encryption,
    resumeFrom: opts.resumeUploadId,
    ttlMs: opts.ttlMs,
  });

  const totalChunks = session.totalChunks;
  const chunkSize = session.chunkSize;
  const chunkHashList = new Array<string>(totalChunks);
  const landed = new Set<number>(session.landed);

  if (totalChunks === 0) {
    // Empty file. Finalize immediately with an empty hash list.
    const f = await finalizeUpload(client, session, []);
    return {
      fileId: f.fileId,
      size: f.size,
      uploadId: session.uploadId,
      fileHash: f.fileHash,
    };
  }

  // For resumed sessions, we don't have hashes for already-landed
  // chunks. We MUST recompute them locally to populate
  // chunkHashList[] for finalize. This re-hashes (cheap) but does
  // not re-PUT (the server already has them).
  for (const idx of landed) {
    const slice = await sliceSource(source, idx, chunkSize, totalSize);
    const transformed = opts.chunkTransform
      ? await opts.chunkTransform(slice, idx)
      : slice;
    chunkHashList[idx] = await hashChunk(transformed);
  }

  // Adaptive state.
  const initial = opts.concurrency?.initial ?? 4;
  const min = opts.concurrency?.min ?? 1;
  const max = opts.concurrency?.max ?? 64;
  const state: AdaptiveState = {
    active: 0,
    target: Math.min(Math.max(initial, min), max),
    min,
    max,
    rttMs: [],
    successWindow: 0,
    errorBackoffMs: 0,
    endgameActive: false,
    errorsRecovered: 0,
  };

  const endgameThreshold = opts.endgameThreshold ?? 0.9;
  const endgameMaxFanout = opts.endgameMaxFanout ?? 8;

  let uploaded = (() => {
    // bytes already on server = sum of landed chunks' sizes
    let acc = 0;
    for (const idx of landed) {
      const start = idx * chunkSize;
      const end = Math.min(start + chunkSize, totalSize);
      acc += end - start;
    }
    return acc;
  })();
  let chunksDone = landed.size;

  const onProgress = opts.onProgress;
  // Phase 17.6: hoist per-chunk event callback (zero-overhead when undefined).
  const onChunkEvent = opts.onChunkEvent;
  let lastProgressTs = 0;
  function maybeProgress() {
    if (!onProgress) return;
    const now = Date.now();
    if (now - lastProgressTs < 100 && chunksDone < totalChunks) return;
    lastProgressTs = now;
    onProgress({
      uploaded,
      total: totalSize,
      chunksDone,
      chunksTotal: totalChunks,
      currentParallelism: state.active,
      endgameActive: state.endgameActive,
      rttP50Ms: quantile(state.rttMs, 0.5),
      rttP95Ms: quantile(state.rttMs, 0.95),
      errorsRecovered: state.errorsRecovered,
    });
  }

  // Pull a chunk's bytes + apply transform.
  async function chunkBytes(idx: number): Promise<Uint8Array> {
    const slice = await sliceSource(source, idx, chunkSize, totalSize);
    return opts.chunkTransform ? await opts.chunkTransform(slice, idx) : slice;
  }

  // Upload one chunk with retry. Returns hash on success.
  const MAX_RETRIES = 5;
  async function uploadOne(
    idx: number,
    signal?: AbortSignal
  ): Promise<{ hash: string; bytesAccepted: number }> {
    let attempt = 0;
    let backoff = 0;
    let startedEmitted = false;
    while (true) {
      if (signal?.aborted) {
        throw new DOMException("aborted", "AbortError");
      }
      try {
        const bytes = await chunkBytes(idx);
        // Phase 17.6: emit `started` exactly once per index, on first
        // attempt (post-bytes-prep so zero-byte/range errors still
        // fire `failed`). Retries do not re-emit `started`.
        if (!startedEmitted && onChunkEvent) {
          onChunkEvent({ index: idx, state: "started" });
          startedEmitted = true;
        }
        const t0 = performance.now();
        const r = await putChunk(client, session, idx, bytes, signal);
        const elapsed = performance.now() - t0;
        state.rttMs.push(elapsed);
        if (state.rttMs.length > 32) state.rttMs.shift();
        // Plateau detection / scale-up.
        state.successWindow++;
        if (state.target < state.max && state.successWindow >= 8) {
          const p50 = quantile(state.rttMs, 0.5);
          const p95 = quantile(state.rttMs, 0.95);
          if (p95 < 1.2 * Math.max(p50, 1)) {
            state.target = Math.min(state.target + 4, state.max);
          }
          state.successWindow = 0;
        }
        state.errorBackoffMs = 0;
        return { hash: r.hash, bytesAccepted: r.bytesAccepted };
      } catch (err) {
        attempt++;
        const msg = (err as Error)?.message ?? String(err);
        const aborted =
          (err as Error)?.name === "AbortError" || signal?.aborted;
        if (aborted) {
          throw err;
        }
        if (attempt >= MAX_RETRIES) {
          // Phase 17.6: surface the terminal error to consumers.
          if (onChunkEvent) {
            onChunkEvent({
              index: idx,
              state: "failed",
              attempt,
              error: msg,
            });
          }
          throw err;
        }
        // Phase 17.6: emit `retrying` before backoff so consumers
        // can update UI optimistically.
        if (onChunkEvent) {
          onChunkEvent({
            index: idx,
            state: "retrying",
            attempt,
            error: msg,
          });
        }
        // Map status hints from msg — rate-limit / unavail → bigger
        // backoff; transient network → smaller.
        const isRateLimit = /\b429\b/.test(msg) || /EBUSY/.test(msg);
        const isUnavail =
          /\b503\b/.test(msg) ||
          /EMOSSAIC_UNAVAILABLE/.test(msg) ||
          err instanceof MossaicUnavailableError;
        if (isRateLimit || isUnavail) {
          state.target = Math.max(state.min, Math.floor(state.target / 2));
          state.errorBackoffMs =
            state.errorBackoffMs === 0
              ? 100
              : Math.min(state.errorBackoffMs * 2, 30_000);
          backoff = state.errorBackoffMs + Math.random() * 50;
        } else {
          state.target = Math.max(state.min, state.target - 1);
          backoff = 50 + Math.random() * 50;
        }
        state.errorsRecovered++;
        await sleep(backoff);
      }
    }
  }

  // Worker loop: one logical lane.
  const pending = new Set<number>();
  for (let i = 0; i < totalChunks; i++) {
    if (!landed.has(i)) pending.add(i);
  }

  async function lane(): Promise<void> {
    while (true) {
      if (opts.signal?.aborted) {
        throw new DOMException("aborted", "AbortError");
      }
      // Pick the lowest pending index.
      let pick: number | undefined;
      for (const i of pending) {
        if (pick === undefined || i < pick) pick = i;
      }
      if (pick === undefined) return; // nothing left
      pending.delete(pick);
      try {
        const r = await uploadOne(pick, opts.signal);
        chunkHashList[pick] = r.hash;
        chunksDone++;
        const start = pick * chunkSize;
        const end = Math.min(start + chunkSize, totalSize);
        uploaded += end - start;
        // Phase 17.6: per-chunk `completed` event after success.
        if (onChunkEvent) {
          onChunkEvent({
            index: pick,
            state: "completed",
            hash: r.hash,
            bytesAccepted: r.bytesAccepted,
          });
        }
        maybeProgress();
      } catch (err) {
        // Re-add so endgame can pick it up; if we're going to throw,
        // throw after re-adding. (`uploadOne` already emitted `failed`
        // on terminal-retry; aborts propagate without an event.)
        pending.add(pick);
        throw err;
      }
    }
  }

  // Drive lanes up to `state.target`, refreshing as `target` changes.
  state.active = Math.min(state.target, pending.size);
  const lanes: Promise<void>[] = [];
  // Endgame extras are pushed after Promise.all(lanes) snapshots, so we
  // track them separately and await them after the main lanes complete.
  const extras: Promise<void>[] = [];
  for (let w = 0; w < state.active; w++) {
    lanes.push(lane());
  }
  // Endgame scheduler — observes pending state, fires duplicate
  // uploads in the tail. Implementation is a polling lane: when we
  // hit the threshold, spawn additional lanes that pick from the
  // same `pending` set (idempotent re-PUT on the server).
  const endgameLane = (async () => {
    while (pending.size > 0 && !opts.signal?.aborted) {
      const completed = chunksDone;
      const total = totalChunks;
      if (
        !state.endgameActive &&
        completed / total >= endgameThreshold &&
        pending.size <= endgameMaxFanout &&
        pending.size > 0
      ) {
        state.endgameActive = true;
        // Spawn one extra lane per still-pending chunk (capped at
        // endgameMaxFanout).
        const extra = Math.min(pending.size, endgameMaxFanout);
        for (let i = 0; i < extra; i++) {
          extras.push(lane());
        }
        return;
      }
      await sleep(50);
    }
  })();

  try {
    await Promise.all(lanes);
    await endgameLane;
    // After the original lanes drain, any endgame extras that were
    // spawned mid-flight may still be in the middle of uploadOne(); we
    // MUST await them before finalize, otherwise chunkHashList[] can
    // contain undefined slots for chunks the extras claimed.
    await Promise.all(extras);
  } catch (err) {
    // Best-effort abort on the server so chunks aren't pinned.
    try {
      await abortUpload(client, session);
    } catch {
      // ignore
    }
    throw err;
  }

  // Finalize. The server cross-checks our hash list against shard
  // staging — any divergence throws EBADF.
  const f = await finalizeUpload(client, session, chunkHashList);
  return {
    fileId: f.fileId,
    size: f.size,
    uploadId: session.uploadId,
    fileHash: f.fileHash,
  };
}

/** Slice a source (Uint8Array | Blob) into one chunk's bytes. */
async function sliceSource(
  source: Uint8Array | Blob,
  idx: number,
  chunkSize: number,
  totalSize: number
): Promise<Uint8Array> {
  const start = idx * chunkSize;
  const end = Math.min(start + chunkSize, totalSize);
  if (source instanceof Uint8Array) {
    return source.subarray(start, end);
  }
  // Blob path (browser/Node 20+). Read into a fresh Uint8Array.
  const ab = await source.slice(start, end).arrayBuffer();
  return new Uint8Array(ab);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ── Parallel download ──────────────────────────────────────────────────

/**
 * Download a file in parallel, hash-verifying each chunk against the
 * server-supplied manifest. decryption flows via
 * `opts.chunkTransform` (unseal per-chunk envelope).
 *
 * Strategy:
 *   1. POST /multipart/download-token to get the manifest + token.
 *   2. Spawn up to `concurrency.max` parallel chunk fetches against
 *      the cacheable per-chunk endpoint (`?hash=&shard=` hints from
 *      the manifest let us skip a second UserDO touch).
 *   3. Adaptive scaling + endgame tail-mode mirror the upload engine.
 *   4. Concatenate chunks in index order and return Uint8Array.
 */
export async function parallelDownload(
  client: MossaicHttpClient,
  path: string,
  opts: ParallelDownloadOpts = {}
): Promise<Uint8Array> {
  const dl = await client.multipartDownloadToken(path);
  const manifest = dl.manifest;
  const totalSize = manifest.size;
  const totalChunks = manifest.chunkCount;

  // Phase 17.6: emit `onManifest` exactly once, before any chunk
  // events. SPA uses this to seed per-chunk progress UI and capture
  // mimeType for Blob construction. Inlined / empty files still get
  // the manifest event before short-circuit returns.
  if (opts.onManifest) {
    opts.onManifest({
      fileId: manifest.fileId,
      mimeType: manifest.mimeType ?? "application/octet-stream",
      size: totalSize,
      chunkCount: totalChunks,
      chunks: manifest.chunks,
    });
  }

  if (manifest.inlined) {
    // Inlined files — single read via the regular HttpVFS.readFile.
    // Multipart download isn't useful for sub-INLINE_LIMIT files.
    void INLINE_LIMIT; // intentional ref for the constant
    return await client.readFile(path);
  }
  if (totalChunks === 0) {
    return new Uint8Array(0);
  }

  // For each chunk, fetch via the cacheable per-chunk endpoint.
  // The chunk's `shardIndex` is intentionally hidden by the manifest
  // (see vfs-ops.ts:openManifest comment), so we route through the
  // `/api/vfs/readChunk` endpoint which does the lookup server-side.
  // `path` is captured per-call (no shared mutable state on the
  // client) so concurrent `parallelDownload` calls on the same
  // client are race-free.
  const initial = opts.concurrency?.initial ?? 4;
  const min = opts.concurrency?.min ?? 1;
  const max = opts.concurrency?.max ?? 64;
  const state: AdaptiveState = {
    active: 0,
    target: Math.min(Math.max(initial, min), max),
    min,
    max,
    rttMs: [],
    successWindow: 0,
    errorBackoffMs: 0,
    endgameActive: false,
    errorsRecovered: 0,
  };

  // Pre-allocate per-chunk slots. When `chunkTransform` is used (e.g.
  // unseal), each chunk's POST-transform size is unknown
  // until the transform runs — envelopes are larger than plaintext.
  // We collect transformed-bytes into a per-index slot and concat at
  // the end. Without `chunkTransform`, we still concat per-index for
  // uniformity (one alloc per chunk; cheap).
  //
  // Why not pre-allocate the full output? When `chunkTransform`
  // shrinks bytes (decrypt), `manifest.size` would be the envelope
  // size — wrong. Per-slot collection sidesteps the problem entirely.
  const slots = new Array<Uint8Array>(totalChunks);

  const pending = new Set<number>();
  for (let i = 0; i < totalChunks; i++) pending.add(i);
  let downloaded = 0;
  let chunksDone = 0;

  const onProgress = opts.onProgress;
  // Phase 17.6: hoist per-chunk event callback (zero-overhead when undefined).
  const onChunkEvent = opts.onChunkEvent;
  let lastProgressTs = 0;
  function maybeProgress() {
    if (!onProgress) return;
    const now = Date.now();
    if (now - lastProgressTs < 100 && chunksDone < totalChunks) return;
    lastProgressTs = now;
    onProgress({
      uploaded: downloaded, // reuse the field as "bytes transferred"
      total: totalSize,
      chunksDone,
      chunksTotal: totalChunks,
      currentParallelism: state.active,
      endgameActive: state.endgameActive,
      rttP50Ms: quantile(state.rttMs, 0.5),
      rttP95Ms: quantile(state.rttMs, 0.95),
      errorsRecovered: state.errorsRecovered,
    });
  }

  const MAX_RETRIES = 5;
  async function downloadOne(idx: number, signal?: AbortSignal): Promise<void> {
    let attempt = 0;
    let startedEmitted = false;
    while (true) {
      if (signal?.aborted) throw new DOMException("aborted", "AbortError");
      try {
        // Phase 17.6: emit `started` once per index on first attempt.
        if (!startedEmitted && onChunkEvent) {
          onChunkEvent({ index: idx, state: "started" });
          startedEmitted = true;
        }
        const t0 = performance.now();
        const ch = manifest.chunks[idx];
        const bytes = await client.fetchChunkByHash(
          manifest.fileId,
          idx,
          ch.hash,
          dl.token,
          path,
          signal
        );
        // Verify hash matches the manifest's claim. This is a cheap
        // integrity check — if it fails, the download endpoint
        // returned the wrong bytes (corruption / bug).
        const verify = await hashChunk(bytes);
        if (verify !== ch.hash) {
          throw new Error(
            `chunk ${idx} hash mismatch (server=${verify}, manifest=${ch.hash})`
          );
        }
        const transformed = opts.chunkTransform
          ? await opts.chunkTransform(bytes, idx)
          : bytes;
        slots[idx] = transformed;
        downloaded += ch.size;
        chunksDone++;
        const elapsed = performance.now() - t0;
        state.rttMs.push(elapsed);
        if (state.rttMs.length > 32) state.rttMs.shift();
        state.successWindow++;
        if (state.target < state.max && state.successWindow >= 8) {
          const p50 = quantile(state.rttMs, 0.5);
          const p95 = quantile(state.rttMs, 0.95);
          if (p95 < 1.2 * Math.max(p50, 1)) {
            state.target = Math.min(state.target + 4, state.max);
          }
          state.successWindow = 0;
        }
        // Phase 17.6: per-chunk `completed` event after success.
        if (onChunkEvent) {
          onChunkEvent({
            index: idx,
            state: "completed",
            hash: ch.hash,
            bytesAccepted: ch.size,
          });
        }
        maybeProgress();
        return;
      } catch (err) {
        attempt++;
        const msg = (err as Error)?.message ?? String(err);
        const aborted =
          (err as Error)?.name === "AbortError" || signal?.aborted;
        if (aborted) throw err;
        if (attempt >= MAX_RETRIES) {
          // Phase 17.6: terminal failure event.
          if (onChunkEvent) {
            onChunkEvent({
              index: idx,
              state: "failed",
              attempt,
              error: msg,
            });
          }
          throw err;
        }
        // Phase 17.6: retrying notification.
        if (onChunkEvent) {
          onChunkEvent({
            index: idx,
            state: "retrying",
            attempt,
            error: msg,
          });
        }
        state.errorsRecovered++;
        await sleep(50 + attempt * 100 + Math.random() * 50);
      }
    }
  }

  async function lane(): Promise<void> {
    while (true) {
      if (opts.signal?.aborted)
        throw new DOMException("aborted", "AbortError");
      let pick: number | undefined;
      for (const i of pending) {
        if (pick === undefined || i < pick) pick = i;
      }
      if (pick === undefined) return;
      pending.delete(pick);
      try {
        await downloadOne(pick, opts.signal);
      } catch (err) {
        pending.add(pick);
        throw err;
      }
    }
  }

  state.active = Math.min(state.target, pending.size);
  const lanes: Promise<void>[] = [];
  for (let w = 0; w < state.active; w++) {
    lanes.push(lane());
  }
  const endgameThreshold = opts.endgameThreshold ?? 0.9;
  const endgameMaxFanout = opts.endgameMaxFanout ?? 8;
  const endgameLane = (async () => {
    while (pending.size > 0 && !opts.signal?.aborted) {
      if (
        !state.endgameActive &&
        chunksDone / totalChunks >= endgameThreshold &&
        pending.size <= endgameMaxFanout &&
        pending.size > 0
      ) {
        state.endgameActive = true;
        const extra = Math.min(pending.size, endgameMaxFanout);
        for (let i = 0; i < extra; i++) lanes.push(lane());
        return;
      }
      await sleep(50);
    }
  })();
  await Promise.all(lanes);
  await endgameLane;
  // Concatenate per-chunk slots in index order. When `chunkTransform`
  // is the identity, this matches `manifest.size`; when it shrinks
  // bytes (decrypt), the output is the post-transform size.
  let outSize = 0;
  for (const s of slots) outSize += s?.byteLength ?? 0;
  const out = new Uint8Array(outSize);
  let cursor = 0;
  for (const s of slots) {
    if (!s) continue;
    out.set(s, cursor);
    cursor += s.byteLength;
  }
  return out;
}

/**
 * Streaming variant of `parallelDownload`.
 *
 * Returns a `ReadableStream<Uint8Array>` that emits chunks **in
 * index order** as each one finishes downloading. Internally this
 * runs the same adaptive engine + endgame as `parallelDownload`,
 * but instead of materialising the full `Uint8Array` it pushes
 * each chunk to the consumer as soon as the next-in-order chunk
 * is available.
 *
 * Semantics:
 *  - Chunks may complete out of order on the network. The stream
 *    holds a small reorder buffer (max ~`max` chunks ahead) and
 *    emits in monotonic index order so the consumer pipes
 *    contiguous bytes.
 *  - Hash verification per chunk happens server-on-receive, exactly
 *    as in `parallelDownload`. A divergence aborts the stream.
 * - encryption: `opts.chunkTransform` runs on every
 *    chunk before emit. Decryption is therefore concurrent with
 *    further downloads.
 *  - Backpressure: if the consumer reads slowly, the reorder
 *    buffer fills and we throttle by gating new lanes on emitted
 *    progress (the buffer cap is `max` chunks).
 *  - On `signal.abort()`, the stream is errored and pending
 *    fetches are cancelled.
 *
 * Memory ceiling (worst case): `max * chunkSize` bytes ≈ 64 × 1 MB
 * = 64 MB at default settings; use `concurrency.max` to cap.
 */
export async function parallelDownloadStream(
  client: MossaicHttpClient,
  path: string,
  opts: ParallelDownloadOpts = {}
): Promise<ReadableStream<Uint8Array>> {
  const dl = await client.multipartDownloadToken(path);
  const manifest = dl.manifest;
  const totalChunks = manifest.chunkCount;

  // Phase 17.6: emit `onManifest` exactly once before any chunk
  // events. Mirrors `parallelDownload`.
  if (opts.onManifest) {
    opts.onManifest({
      fileId: manifest.fileId,
      mimeType: manifest.mimeType ?? "application/octet-stream",
      size: manifest.size,
      chunkCount: totalChunks,
      chunks: manifest.chunks,
    });
  }

  if (manifest.inlined || totalChunks === 0) {
    // Inlined or empty — short-circuit to a single-shot readFile so
    // the stream API stays consistent. `manifest.inlined` covers the
    // case where the file has bytes stored as inline_data (no chunk
    // fan-out); a 0-size file has neither inline nor chunks.
    const all =
      manifest.size === 0 ? new Uint8Array(0) : await client.readFile(path);
    return new ReadableStream<Uint8Array>({
      start(controller) {
        if (all.byteLength > 0) controller.enqueue(all);
        controller.close();
      },
    });
  }

  // `path` is captured per-call (no shared mutable state on the
  // client) so concurrent downloads on the same client are race-free.
  const initial = opts.concurrency?.initial ?? 4;
  const min = opts.concurrency?.min ?? 1;
  const max = opts.concurrency?.max ?? 64;
  const state: AdaptiveState = {
    active: 0,
    target: Math.min(Math.max(initial, min), max),
    min,
    max,
    rttMs: [],
    successWindow: 0,
    errorBackoffMs: 0,
    endgameActive: false,
    errorsRecovered: 0,
  };

  // Reorder buffer: completed[idx] holds bytes once the chunk
  // download finishes. `nextEmit` is the next index we'll push to
  // the consumer. We only push when `completed[nextEmit]` exists,
  // then advance.
  const completed = new Map<number, Uint8Array>();
  let nextEmit = 0;
  let chunksDone = 0;
  let downloaded = 0;
  let streamController: ReadableStreamDefaultController<Uint8Array> | null =
    null;
  let pulled = false;
  let aborted = false;

  const onProgress = opts.onProgress;
  // Phase 17.6: hoist per-chunk event callback (zero-overhead when undefined).
  const onChunkEvent = opts.onChunkEvent;
  let lastProgressTs = 0;
  function maybeProgress() {
    if (!onProgress) return;
    const now = Date.now();
    if (now - lastProgressTs < 100 && chunksDone < totalChunks) return;
    lastProgressTs = now;
    onProgress({
      uploaded: downloaded,
      total: manifest.size,
      chunksDone,
      chunksTotal: totalChunks,
      currentParallelism: state.active,
      endgameActive: state.endgameActive,
      rttP50Ms: quantile(state.rttMs, 0.5),
      rttP95Ms: quantile(state.rttMs, 0.95),
      errorsRecovered: state.errorsRecovered,
    });
  }

  function emitContiguous(): void {
    while (
      streamController !== null &&
      completed.has(nextEmit) &&
      !aborted
    ) {
      const bytes = completed.get(nextEmit)!;
      completed.delete(nextEmit);
      streamController.enqueue(bytes);
      nextEmit++;
    }
    if (chunksDone === totalChunks && nextEmit === totalChunks && !aborted) {
      streamController?.close();
    }
  }

  const pending = new Set<number>();
  for (let i = 0; i < totalChunks; i++) pending.add(i);

  const MAX_RETRIES = 5;
  async function downloadOne(idx: number, signal?: AbortSignal): Promise<void> {
    let attempt = 0;
    let startedEmitted = false;
    while (true) {
      if (signal?.aborted || aborted) {
        throw new DOMException("aborted", "AbortError");
      }
      try {
        // Phase 17.6: emit `started` once per index on first attempt.
        if (!startedEmitted && onChunkEvent) {
          onChunkEvent({ index: idx, state: "started" });
          startedEmitted = true;
        }
        const t0 = performance.now();
        const ch = manifest.chunks[idx];
        const bytes = await client.fetchChunkByHash(
          manifest.fileId,
          idx,
          ch.hash,
          dl.token,
          path,
          signal
        );
        const verify = await hashChunk(bytes);
        if (verify !== ch.hash) {
          throw new Error(
            `chunk ${idx} hash mismatch (server=${verify}, manifest=${ch.hash})`
          );
        }
        const transformed = opts.chunkTransform
          ? await opts.chunkTransform(bytes, idx)
          : bytes;
        completed.set(idx, transformed);
        downloaded += ch.size;
        chunksDone++;
        const elapsed = performance.now() - t0;
        state.rttMs.push(elapsed);
        if (state.rttMs.length > 32) state.rttMs.shift();
        state.successWindow++;
        if (state.target < state.max && state.successWindow >= 8) {
          const p50 = quantile(state.rttMs, 0.5);
          const p95 = quantile(state.rttMs, 0.95);
          if (p95 < 1.2 * Math.max(p50, 1)) {
            state.target = Math.min(state.target + 4, state.max);
          }
          state.successWindow = 0;
        }
        // Phase 17.6: per-chunk `completed` event after success.
        if (onChunkEvent) {
          onChunkEvent({
            index: idx,
            state: "completed",
            hash: ch.hash,
            bytesAccepted: ch.size,
          });
        }
        maybeProgress();
        emitContiguous();
        return;
      } catch (err) {
        attempt++;
        const msg = (err as Error)?.message ?? String(err);
        const isAborted =
          (err as Error)?.name === "AbortError" || signal?.aborted;
        if (isAborted) throw err;
        if (attempt >= MAX_RETRIES) {
          if (onChunkEvent) {
            onChunkEvent({
              index: idx,
              state: "failed",
              attempt,
              error: msg,
            });
          }
          throw err;
        }
        if (onChunkEvent) {
          onChunkEvent({
            index: idx,
            state: "retrying",
            attempt,
            error: msg,
          });
        }
        state.errorsRecovered++;
        await sleep(50 + attempt * 100 + Math.random() * 50);
      }
    }
  }

  async function lane(): Promise<void> {
    while (true) {
      if (opts.signal?.aborted || aborted) {
        throw new DOMException("aborted", "AbortError");
      }
      let pick: number | undefined;
      for (const i of pending) {
        if (pick === undefined || i < pick) pick = i;
      }
      if (pick === undefined) return;
      pending.delete(pick);
      try {
        await downloadOne(pick, opts.signal);
      } catch (err) {
        pending.add(pick);
        throw err;
      }
    }
  }

  return new ReadableStream<Uint8Array>({
    start(controller) {
      streamController = controller;
    },
    pull(_controller) {
      if (pulled) return; // first-pull only spawns lanes
      pulled = true;
      void (async () => {
        try {
          state.active = Math.min(state.target, pending.size);
          const lanes: Promise<void>[] = [];
          for (let w = 0; w < state.active; w++) lanes.push(lane());
          const endgameThreshold = opts.endgameThreshold ?? 0.9;
          const endgameMaxFanout = opts.endgameMaxFanout ?? 8;
          const endgameLane = (async () => {
            while (pending.size > 0 && !opts.signal?.aborted && !aborted) {
              if (
                !state.endgameActive &&
                chunksDone / totalChunks >= endgameThreshold &&
                pending.size <= endgameMaxFanout &&
                pending.size > 0
              ) {
                state.endgameActive = true;
                const extra = Math.min(pending.size, endgameMaxFanout);
                for (let i = 0; i < extra; i++) lanes.push(lane());
                return;
              }
              await sleep(50);
            }
          })();
          await Promise.all(lanes);
          await endgameLane;
          // Final emit + close.
          emitContiguous();
        } catch (err) {
          aborted = true;
          streamController?.error(err);
        }
      })();
    },
    cancel() {
      aborted = true;
    },
  });
}

// ── Compute math helpers (re-exported for tests / docs) ────────────────

/**
 * Compute the chunk spec the server would derive from `size`. Useful
 * for client-side sanity checks before calling beginUpload.
 */
export function deriveClientChunkSpec(size: number): {
  chunkSize: number;
  chunkCount: number;
} {
  const r = computeChunkSpec(size);
  return { chunkSize: r.chunkSize, chunkCount: r.chunkCount };
}

/** Throughput math from plan §8.1 — exposed for OPERATIONS.md docs. */
export const THROUGHPUT_MATH = Object.freeze({
  perChunkP50Ms: 15,
  perChunkP95Ms: 60,
  defaultChunkSizeBytes: 1_048_576,
  defaultMaxConcurrency: 64,
  /** 64-way × 1 MB / 15 ms ≈ 4.3 GB/s aggregate ceiling. User link is the limit. */
  aggregateCeilingMBs: (64 * 1) / 0.015,
  /** 100 MB / gigabit (~125 MB/s) = 0.8s of transfer + ~0.3s of multipart overhead. */
  hundredMBOnGigabitSec: 100 / 125 + 0.3,
});
