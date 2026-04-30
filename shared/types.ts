/// <reference types="@cloudflare/workers-types" />

/**
 * VFS-essential shared types.
 *
 * Photo-app domain types (Album, GalleryPhoto, AnalyticsOverview,
 * UploadInit*, etc.) live in `worker/app/types.ts`. This module
 * is strictly the VFS contract surface — what UserDOCore + ShardDO
 * + the SDK fan-out machinery agree on.
 */

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
  // ── VFS additions ──
  // All optional and absent on legacy rows / legacy clients. Read
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

// ── Env Bindings (Worker) ──
//
// `EnvCore` is what worker/core/ + the SDK library mode require.
// `EnvApp` adds the App-mode bindings (SearchDO, ASSETS, AI) used
// by the photo-app routes.
//
// Renaming the wrangler `name` field while keeping `class_name` is
// data-safe; storage is keyed by `(class_name, idFromName)` per
// https://developers.cloudflare.com/durable-objects/reference/durable-objects-migrations/

/**
 * Cloudflare Images binding. Surfaces the `IMAGES.input(stream).
 * transform({...}).output({format})` builder. Marked `unknown`
 * here because the binding's runtime type ships from
 * `@cloudflare/workers-types`'s ambient declarations and we don't
 * want to widen the SDK's surface to the full type. Renderers
 * narrow at use-sites.
 */
export type ImagesBinding = {
  input(stream: ReadableStream<Uint8Array>): {
    transform(opts: {
      width?: number;
      height?: number;
      fit?: "scale-down" | "contain" | "cover" | "crop" | "pad";
      format?: "image/jpeg" | "image/png" | "image/webp" | "image/avif";
      quality?: number;
    }): {
      output(opts: {
        format: "image/jpeg" | "image/png" | "image/webp" | "image/avif";
        quality?: number;
      }): Promise<{
        response(): Response;
        contentType(): string;
      }>;
    };
  };
};

/**
 * Cloudflare Browser Run binding. Reserved for the deferred
 * preview renderers (PDF page-1, video poster, Office). * wires the binding optionally; renderers requiring it fall through
 * to the icon-card when the binding is absent.
 */
export type BrowserBinding = {
  fetch: (request: Request) => Promise<Response>;
};

/**
 * Core VFS bindings. Service-mode worker and SDK library-mode
 * consumers satisfy this shape with two DO namespaces and (optionally)
 * a JWT secret for token issuance.
 */
export interface EnvCore {
  MOSSAIC_USER: DurableObjectNamespace;
  MOSSAIC_SHARD: DurableObjectNamespace;
  JWT_SECRET?: string;
  /** Cloudflare Images binding; optional — renderers fall through. */
  IMAGES?: ImagesBinding;
  /** Cloudflare Browser Run binding; optional — renderers fall through. */
  BROWSER?: BrowserBinding;
}

/**
 * App-mode bindings. Adds the photo-library's vector-search DO,
 * static-asset binding, and Workers AI binding on top of the core
 * VFS surface.
 */
export interface EnvApp extends EnvCore {
  SEARCH_DO: DurableObjectNamespace;
  ASSETS: Fetcher;
  AI?: Ai;
}

/**
 * @deprecated Use `EnvApp` for App-mode workers, `EnvCore` for
 * Service-mode / SDK library consumers. Kept for back-compat with
 * imports that haven't been migrated yet.
 *
 * @internal
 */
export type Env = EnvApp;
