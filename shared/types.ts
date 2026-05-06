/// <reference types="@cloudflare/workers-types" />

// ── Chunk & File Types ──

export type ChunkHash = string; // hex-encoded SHA-256, 64 chars

export interface ChunkSpec {
  index: number;
  offset: number;
  size: number;
  hash: ChunkHash;
  shardIndex: number;
}

export interface FileManifest {
  fileId: string;
  fileName: string;
  fileSize: number;
  fileHash: ChunkHash;
  mimeType: string;
  chunkSize: number;
  chunkCount: number;
  poolSize: number;
  chunks: ChunkSpec[];
  createdAt: number;
  // ── VFS additions (sdk-impl-plan §3.1, §11) ──
  // All optional and absent on legacy rows / legacy clients. Phase 2 read
  // paths consult these to short-circuit on inlined files and surface
  // symlinks. Existing consumers (download.ts, gallery.ts) ignore them.
  /** POSIX file mode (defaults to 0o644 / 420 in the schema). */
  mode?: number;
  /** 'file' (default) or 'symlink'. */
  nodeKind?: "file" | "symlink";
  /** Target path when nodeKind === 'symlink'. */
  symlinkTarget?: string | null;
  /** Inline blob for files ≤ INLINE_LIMIT (16 KB). When present, chunks is empty. */
  inlineData?: ArrayBuffer | null;
}

export interface UserFile {
  fileId: string;
  fileName: string;
  fileSize: number;
  fileHash: string;
  mimeType: string;
  chunkCount: number;
  status: "uploading" | "complete" | "failed" | "deleted";
  parentId: string | null;
  createdAt: number;
  updatedAt: number;
}

export interface Folder {
  folderId: string;
  name: string;
  parentId: string | null;
  createdAt: number;
  updatedAt: number;
}

// ── API Request Types ──

export interface SignupRequest {
  email: string;
  password: string;
}

export interface LoginRequest {
  email: string;
  password: string;
}

export interface UploadInitRequest {
  fileName: string;
  fileSize: number;
  mimeType: string;
  parentId?: string | null;
}

export interface UploadCompleteRequest {
  fileId: string;
  chunkHashes: string[];
  fileHash: string;
}

export interface CreateFolderRequest {
  name: string;
  parentId?: string | null;
}

// ── API Response Types ──

export interface AuthResponse {
  token: string;
  userId: string;
  email: string;
}

export interface UploadInitResponse {
  fileId: string;
  chunkSize: number;
  chunkCount: number;
  poolSize: number;
}

export interface FileListResponse {
  files: UserFile[];
  folders: Folder[];
}

export interface QuotaInfo {
  storageUsed: number;
  storageLimit: number;
  fileCount: number;
  poolSize: number;
}

export interface ApiError {
  error: string;
  status?: number;
}

// ── Transfer Progress ──

export type ChunkStatus = "pending" | "uploading" | "complete" | "failed";

export interface ChunkProgress {
  index: number;
  status: ChunkStatus;
  bytesTransferred: number;
  size: number;
}

export interface TransferProgress {
  fileId: string;
  fileName: string;
  direction: "upload" | "download";
  totalChunks: number;
  completedChunks: number;
  failedChunks: number;
  bytesTransferred: number;
  bytesTotal: number;
  activeConcurrency: number;
  throughputBps: number;
  estimatedRemainingMs: number;
  chunks: ChunkProgress[];
  startedAt: number;
  completedAt?: number;
  peakThroughputBps?: number;
  averageThroughputBps?: number;
}

// ── Transfer Speed Stats (frontend) ──

export interface CompletedTransferStats {
  fileId: string;
  fileName: string;
  direction: "upload" | "download";
  fileSize: number;
  durationMs: number;
  averageThroughputBps: number;
  peakThroughputBps: number;
  completedAt: number;
}

// ── Analytics / Stats ──

export interface ShardStats {
  shardIndex: number;
  totalChunks: number;
  totalBytes: number;
  uniqueChunks: number;
  totalRefs: number;
  capacityUsed: number;
  dedupRatio: number;
}

export interface UserFilesByStatus {
  uploading: number;
  complete: number;
  failed: number;
  deleted: number;
}

export interface MimeDistribution {
  mimeType: string;
  count: number;
  totalSize: number;
}

export interface ShardDistribution {
  shardIndex: number;
  chunkCount: number;
}

export interface RecentUpload {
  fileId: string;
  fileName: string;
  fileSize: number;
  mimeType: string;
  status: string;
  createdAt: number;
  uploadDurationMs?: number;
  averageSpeedBps?: number;
}

export interface UserStats {
  totalFiles: number;
  totalStorageUsed: number;
  quotaLimit: number;
  filesByStatus: UserFilesByStatus;
  mimeDistribution: MimeDistribution[];
  recentUploads: RecentUpload[];
  shardDistribution: ShardDistribution[];
}

export interface AnalyticsOverview {
  user: UserStats;
  shards: ShardStats[];
  totals: {
    totalChunksAcrossShards: number;
    totalBytesAcrossShards: number;
    totalUniqueChunks: number;
    totalRefs: number;
    averageDedupRatio: number;
  };
}

// ── Gallery Types ──

export interface GalleryPhoto {
  fileId: string;
  fileName: string;
  fileSize: number;
  mimeType: string;
  parentId: string | null;
  createdAt: number;
  updatedAt: number;
}

export interface GalleryPhotosResponse {
  photos: GalleryPhoto[];
}

export interface Album {
  id: string;
  name: string;
  photoIds: string[];
  coverPhotoId: string | null;
  createdAt: number;
  updatedAt: number;
}

export interface SharedAlbum {
  token: string;
  albumId: string;
  createdAt: number;
}

export interface SharedAlbumData {
  userId: string;
  fileIds: string[];
  albumName: string;
}

export interface SharedAlbumPhotosResponse {
  albumName: string;
  photos: {
    fileId: string;
    fileName: string;
    fileSize: number;
    mimeType: string;
    createdAt: number;
  }[];
}

// ── Env Bindings (Worker) ──
//
// Phase 13: canonical binding names are `MOSSAIC_USER` and `MOSSAIC_SHARD`
// (prefixed for consumer-env safety). `SEARCH_DO` is App-mode only and not
// part of the SDK contract — consumers in library mode never see it.
//
// Renaming the wrangler `name` field while keeping `class_name` is a safe
// no-data-loss change; storage is keyed by `(class_name, idFromName)` per
// https://developers.cloudflare.com/durable-objects/reference/durable-objects-migrations/

/** @internal */
export interface Env {
  MOSSAIC_USER: DurableObjectNamespace;
  MOSSAIC_SHARD: DurableObjectNamespace;
  SEARCH_DO: DurableObjectNamespace;
  ASSETS: Fetcher;
  AI?: Ai;
  JWT_SECRET?: string;
}
