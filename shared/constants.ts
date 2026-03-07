/** 1 MB — fixed chunk size for all files > 1 MB */
export const CHUNK_SIZE = 1_048_576;

/** Base shard pool size for new users */
export const BASE_POOL_SIZE = 32;

/** Add 1 shard per this many bytes stored */
export const BYTES_PER_SHARD = 5 * 1024 * 1024 * 1024; // 5 GB

/** Maximum file size: 10 GB */
export const MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024;

/** Default storage quota: 100 GB */
export const DEFAULT_STORAGE_LIMIT = 100 * 1024 * 1024 * 1024;

/** SQLite BLOB size limit */
export const MAX_BLOB_SIZE = 2 * 1024 * 1024; // 2 MB

/** Max parallel upload connections */
export const MAX_UPLOAD_CONCURRENCY = 50;

/** Initial upload concurrency */
export const INITIAL_UPLOAD_CONCURRENCY = 20;

/** Min upload concurrency */
export const MIN_UPLOAD_CONCURRENCY = 4;

/** Max parallel download connections */
export const MAX_DOWNLOAD_CONCURRENCY = 50;

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
