/**
 * Inline tier + per-method size caps.
 *
 * Files at or below INLINE_LIMIT are stored directly in `files.inline_data`
 * (set in write paths) and read back without any ShardDO subrequest
 * (read paths). This is the single biggest unlock for
 * isomorphic-git's loose-object workload, where most blobs are < 1 KB.
 *
 * READFILE_MAX / WRITEFILE_MAX are the per-method caps enforced before
 * fan-out. Above the cap → EFBIG; callers use createReadStream /
 * createWriteStream (memory-bounded streaming) or openManifest +
 * readChunk (caller-orchestrated, multi-invocation).
 *
 * Cap rationale (audit H7): a Worker invocation has ~128 MB of soft
 * memory; readFile allocates `new Uint8Array(file_size)` BEFORE chunk
 * fetches start, so any cap that exceeds soft memory OOMs the Worker
 * mid-read. The plan §11 mandates 100 MB as the canonical cap;
 * previously the constant said 500 MB and the README was misaligned.
 * 100 MB is the truth — callers needing larger files MUST use
 * `createReadStream` (memory-bounded ReadableStream) or
 * `openManifest` + `readChunk` (caller-orchestrated, multi-invocation
 * fan-in).
 */

/** Files ≤ this many bytes are inlined into the manifest's `inline_data` BLOB. */
export const INLINE_LIMIT = 16 * 1024;

/**
 * Phase 32 Fix 5 — inline-tier graceful migration cap.
 *
 * Per-tenant ceiling on cumulative bytes stored in the inline tier
 * (`files.inline_data` BLOBs). Tracked via
 * `quota.inline_bytes_used`. When a write ≤ INLINE_LIMIT would push
 * the tenant past this cap, the dispatch in `vfsWriteFile` falls
 * through to the chunked tier even for tiny payloads — keeping the
 * UserDO's ~10 GiB SQLite quota from being monopolized by a tenant
 * with millions of tiny files.
 *
 * 1 GiB chosen because:
 *  - It accommodates ~65 thousand tiny files at INLINE_LIMIT each,
 *    which is the upper bound of practical \"all-tiny\" workloads
 *    (isomorphic-git loose-object trees, config blobs, JSON
 *    fragments).
 *  - It leaves ≥ 9 GiB headroom on the UserDO for everything else
 *    (folders, file_versions metadata, tags, multipart staging).
 *  - It's well below the workerd ~10 GiB SQLite ceiling so
 *    accounting drift can't catastrophically over-shoot.
 *
 * Existing inline rows past the cap are unchanged — readers find
 * them via `files.inline_data` regardless of `inline_bytes_used`.
 * The cap only gates NEW inline placements.
 */
export const INLINE_TIER_CAP = 1024 * 1024 * 1024;

/**
 * Max bytes returned from a single `readFile` RPC. Above → EFBIG, use
 * streaming. 100 MB matches plan §11 and stays well below the 128 MB
 * Worker soft-memory ceiling so allocation never OOMs.
 */
export const READFILE_MAX = 100 * 1024 * 1024;

/** Max bytes accepted by a single `writeFile` RPC. Above → EFBIG, use streaming. */
export const WRITEFILE_MAX = 100 * 1024 * 1024;
