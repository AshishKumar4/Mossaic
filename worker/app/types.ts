/**
 * App-mode wire types — photo-app surface.
 *
 * These types describe the legacy photo-app HTTP contract served by
 * the App-mode worker (signup/login, upload, gallery, albums,
 * analytics, shared-album view). They are NOT part of the VFS SDK
 * contract; SDK consumers never see them.
 *
 * The shared/types.ts module holds VFS-essential types only
 * (ChunkSpec, FileManifest, UserFile, Folder, QuotaInfo, EnvCore,
 * EnvApp). Anything below is App + SPA only.
 */

import type { ChunkSpec } from "../../shared/types";

// ── Auth ───────────────────────────────────────────────────────────────

export interface AuthResponse {
  token: string;
  userId: string;
  email: string;
}

// ── Upload (legacy chunked-upload protocol) ────────────────────────────

export interface UploadInitRequest {
  fileName: string;
  fileSize: number;
  mimeType: string;
  parentId?: string | null;
}

export interface UploadInitResponse {
  fileId: string;
  chunkSize: number;
  chunkCount: number;
  poolSize: number;
}

export interface CreateFolderRequest {
  name: string;
  parentId?: string | null;
}

// ── Files / folders ────────────────────────────────────────────────────

export interface FileListResponse {
  files: import("../../shared/types").UserFile[];
  folders: import("../../shared/types").Folder[];
}

// Re-exported for convenience — App's manifest builder consumes ChunkSpec.
export type { ChunkSpec };

// ── Transfer progress (SPA upload pipeline) ────────────────────────────

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
  /**
   * Set when the entire transfer terminates with an unrecoverable error
   * (e.g. auth bridge mint failed, server returned 4xx/5xx, network
   * dropped during finalize). The UI shows this as a terminal "failed"
   * state with a clear-button affordance and a user-visible message.
   */
  failedAt?: number;
  /** Human-readable error message attached when `failedAt` is set. */
  error?: string;
}

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

// ── Analytics / Stats ──────────────────────────────────────────────────

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
  /**
   * Server-authoritative shard pool size (Phase 23). 32 by default,
   * grows by +1 per 5 GB stored via `recordWriteUsage`. Surfaced
   * here so operators / verification scripts can observe pool growth
   * without poking at the `quota` table directly.
   */
  poolSize: number;
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

// ── Gallery / Albums ───────────────────────────────────────────────────

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
