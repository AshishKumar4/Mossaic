/**
 * Inline tier + per-method size caps.
 *
 * Files at or below INLINE_LIMIT are stored directly in `files.inline_data`
 * (set in Phase 3 write paths) and read back without any ShardDO subrequest
 * (Phase 2 read paths). This is the single biggest unlock for
 * isomorphic-git's loose-object workload, where most blobs are < 1 KB.
 *
 * READFILE_MAX / WRITEFILE_MAX are the per-method caps enforced before
 * fan-out so we don't OOM the Worker (~128 MB soft limit). Above those,
 * callers must use createReadStream / createWriteStream (Phase 4) or
 * openManifest + readChunk (Phase 4 / consumer-orchestrated).
 */

/** Files ≤ this many bytes are inlined into the manifest's `inline_data` BLOB. */
export const INLINE_LIMIT = 16 * 1024;

/** Max bytes returned from a single `readFile` RPC. Above → EFBIG, use streaming. */
export const READFILE_MAX = 100 * 1024 * 1024;

/** Max bytes accepted by a single `writeFile` RPC. Above → EFBIG, use streaming. */
export const WRITEFILE_MAX = 100 * 1024 * 1024;
