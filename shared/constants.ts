/**
 * 1 MB — default/fallback chunk size.
 * Adaptive chunk sizing (see shared/chunking.ts) now selects the actual
 * chunk size based on file size, but this constant is kept as a fallback.
 */
export const CHUNK_SIZE = 1_048_576;

/** Alias for CHUNK_SIZE — preferred name in new code */
export const DEFAULT_CHUNK_SIZE = CHUNK_SIZE;

/** Base shard pool size for new users */
export const BASE_POOL_SIZE = 32;

/** Add 1 shard per this many bytes stored */
export const BYTES_PER_SHARD = 5 * 1024 * 1024 * 1024; // 5 GB

/**
 * No per-file size limit. Files are adaptively chunked and distributed
 * across unlimited ShardDOs (each up to 10 GB). The real constraint is
 * the user's storage quota (DEFAULT_STORAGE_LIMIT).
 */

/** Default storage quota: 100 GB */
export const DEFAULT_STORAGE_LIMIT = 100 * 1024 * 1024 * 1024;

/** SQLite BLOB size limit */
export const MAX_BLOB_SIZE = 2 * 1024 * 1024; // 2 MB

// -- AIMD Congestion Control Defaults --

/** Initial congestion window (concurrent requests) */
export const AIMD_INITIAL_CWND = 4;

/** Maximum congestion window */
export const AIMD_MAX_CWND = 64;

/** Minimum congestion window */
export const AIMD_MIN_CWND = 2;

/** Slow-start threshold */
export const AIMD_SSTHRESH = 32;

/** JWT expiration: 30 days */
export const JWT_EXPIRATION_MS = 30 * 24 * 60 * 60 * 1000;

/** Session expiration: 30 days */
export const SESSION_EXPIRATION_MS = 30 * 24 * 60 * 60 * 1000;

/** Max retries for chunk operations */
export const MAX_RETRIES = 3;

/** Retry base delay in ms */
export const RETRY_BASE_DELAY = 500;

/** Retry max delay in ms */
export const RETRY_MAX_DELAY = 5000;
