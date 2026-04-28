/**
 * Inline tier + per-method size caps.
 *
 * Files at or below INLINE_LIMIT are stored directly in `files.inline_data`
 * (set in Phase 3 write paths) and read back without any ShardDO subrequest
 * (Phase 2 read paths). This is the single biggest unlock for
 * isomorphic-git's loose-object workload, where most blobs are < 1 KB.
 *
 * READFILE_MAX / WRITEFILE_MAX are the per-method caps enforced before
 * fan-out. Above the cap → EFBIG; callers use createReadStream /
 * createWriteStream (memory-bounded streaming) or openManifest +
 * readChunk (caller-orchestrated, multi-invocation).
 *
 * Cap rationale: a Worker invocation has ~128 MB of soft memory and
 * the paid-tier subrequest budget is 10,000 / invocation. A 500 MB
 * file at the 1 MB chunk size adaptive cap is 500 chunk fetches — well
 * inside the paid budget, and the Uint8Array result requires holding
 * the whole buffer in memory which is ABOVE the soft limit. So 500 MB
 * is a generous ceiling that still fails fast for pathological
 * payloads; in practice, callers approaching 100 MB should already be
 * streaming. The cap is configurable per-deployment via an env var
 * (consumed by vfs-ops at request time, not baked into this constant).
 */

/** Files ≤ this many bytes are inlined into the manifest's `inline_data` BLOB. */
export const INLINE_LIMIT = 16 * 1024;

/** Max bytes returned from a single `readFile` RPC. Above → EFBIG, use streaming. */
export const READFILE_MAX = 500 * 1024 * 1024;

/** Max bytes accepted by a single `writeFile` RPC. Above → EFBIG, use streaming. */
export const WRITEFILE_MAX = 500 * 1024 * 1024;
