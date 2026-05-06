import type { Folder, ApiError as ApiErrorResponse, UserFile } from "@shared/types";
import type {
  AuthResponse,
  FileListResponse,
  CreateFolderRequest,
  AnalyticsOverview,
  GalleryPhotosResponse,
  SharedAlbumPhotosResponse,
} from "@app/types";
import type {
  SearchResult,
  ProviderStatus,
  SearchProviderConfig,
} from "@shared/embedding-types";

const API_BASE = "/api";

class ApiClient {
  private token: string | null = null;

  setToken(token: string | null) {
    this.token = token;
  }

  getToken(): string | null {
    return this.token;
  }

  private async request<T>(
    path: string,
    options: RequestInit = {}
  ): Promise<T> {
    const headers: Record<string, string> = {
      ...(options.headers as Record<string, string>),
    };

    if (this.token) {
      headers["Authorization"] = `Bearer ${this.token}`;
    }

    if (
      options.body &&
      typeof options.body === "string" &&
      !headers["Content-Type"]
    ) {
      headers["Content-Type"] = "application/json";
    }

    const res = await fetch(`${API_BASE}${path}`, {
      ...options,
      headers,
    });

    if (!res.ok) {
      const err = (await res.json().catch(() => ({
        error: `HTTP ${res.status}`,
      }))) as ApiErrorResponse;
      throw new ApiError(err.error || `Request failed: ${res.status}`, res.status);
    }

    return res.json() as Promise<T>;
  }

  // Auth
  async signup(email: string, password: string): Promise<AuthResponse> {
    return this.request<AuthResponse>("/auth/signup", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    });
  }

  async login(email: string, password: string): Promise<AuthResponse> {
    return this.request<AuthResponse>("/auth/login", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    });
  }

  // ── Auth-bridge: VFS token ──────────────────────────────────────
  //
  // Exchanges the App session JWT (set via `setToken`) for a
  // short-TTL VFS Bearer token. The browser caches the token until
  // 60s before its expiry; subsequent calls return the cached
  // value. Force-refresh by calling `clearVfsToken()` (e.g. on a
  // 401 from a canonical /api/vfs/* call).
  //
  // The transfer-client (`src/lib/transfer-client.ts`) consumes
  // this for the canonical multipart pipeline; the App session
  // JWT continues to authenticate the App's own /api/auth,
  // /api/gallery, /api/files, /api/folders, /api/analytics,
  // /api/search, /api/index routes.

  private vfsToken: { token: string; expiresAtMs: number } | null = null;
  /** Refresh window: re-mint when ≤ 60s remain on the cached token. */
  private static readonly VFS_TOKEN_REFRESH_MS = 60_000;

  /**
   * Get a valid VFS Bearer token. Mints on first use; returns the
   * cached token until ≤ 60s remain on its TTL, then re-mints.
   *
   * Throws if the App session JWT is unset (caller must be
   * authenticated) or if the bridge endpoint returns 503
   * (JWT_SECRET unset on the worker).
   */
  async getVfsToken(): Promise<string> {
    if (!this.token) {
      // The auth-bridge endpoint requires an App session JWT. Reject
      // early with a precise message instead of relying on a 401 from
      // the server. Callers (e.g. `getTransferClient`) gate transfers
      // behind the auth context, so this is a programmer-error path.
      throw new ApiError(
        "getVfsToken: no App session JWT set. Sign in before calling.",
        401
      );
    }
    const cached = this.vfsToken;
    const now = Date.now();
    if (cached && cached.expiresAtMs - now > ApiClient.VFS_TOKEN_REFRESH_MS) {
      return cached.token;
    }
    const fresh = await this.request<{ token: string; expiresAtMs: number }>(
      "/auth/vfs-token",
      { method: "POST" }
    );
    this.vfsToken = fresh;
    return fresh.token;
  }

  /**
   * Drop the cached VFS token. Next `getVfsToken()` call re-mints.
   * Call on logout, on a 401 from canonical /api/vfs/*, or when
   * rotating session JWT.
   */
  clearVfsToken(): void {
    this.vfsToken = null;
  }

  // Files
  async listFiles(parentId?: string | null): Promise<FileListResponse> {
    const params = parentId ? `?parentId=${parentId}` : "";
    return this.request<FileListResponse>(`/files${params}`);
  }

  async deleteFile(fileId: string): Promise<{ ok: boolean }> {
    return this.request<{ ok: boolean }>(`/files/${fileId}`, {
      method: "DELETE",
    });
  }

  // Folders
  async createFolder(data: CreateFolderRequest): Promise<Folder> {
    return this.request<Folder>("/folders", {
      method: "POST",
      body: JSON.stringify(data),
    });
  }

  async getFolder(
    folderId: string
  ): Promise<FileListResponse & { path: Folder[] }> {
    return this.request<FileListResponse & { path: Folder[] }>(
      `/folders/${folderId}`
    );
  }

  // Upload + download flow through `@mossaic/sdk` 's `parallelUpload`
  // / `parallelDownload` against canonical `/api/vfs/multipart/*` and
  // `/api/vfs/readChunk` — see `src/hooks/use-{upload,download}.ts`.
  // No bespoke upload/download methods on this client.

  // Analytics
  async getAnalytics(): Promise<AnalyticsOverview> {
    return this.request<AnalyticsOverview>("/analytics/overview");
  }

  // Gallery
  async getGalleryPhotos(): Promise<GalleryPhotosResponse> {
    return this.request<GalleryPhotosResponse>("/gallery/photos");
  }

  getImageUrl(fileId: string): string {
    return `${API_BASE}/gallery/image/${fileId}`;
  }

  getThumbnailUrl(fileId: string): string {
    return `${API_BASE}/gallery/thumbnail/${fileId}`;
  }

  getImageHeaders(): Record<string, string> {
    return this.token ? { Authorization: `Bearer ${this.token}` } : {};
  }

  // Shared albums (public — no auth)
  async getSharedAlbumPhotos(
    token: string
  ): Promise<SharedAlbumPhotosResponse> {
    const res = await fetch(`${API_BASE}/shared/${token}/photos`);
    if (!res.ok) {
      throw new ApiError("Failed to load shared album", res.status);
    }
    return res.json();
  }

  getSharedImageUrl(token: string, fileId: string): string {
    return `${API_BASE}/shared/${token}/image/${fileId}`;
  }

  // Search
  async semanticSearch(
    query: string,
    topK?: number
  ): Promise<{ results: SearchResult[]; query: string }> {
    return this.request<{ results: SearchResult[]; query: string }>(
      "/search",
      {
        method: "POST",
        body: JSON.stringify({ query, topK }),
      }
    );
  }

  async getSearchProviders(): Promise<{
    providers: ProviderStatus[];
    active: SearchProviderConfig;
    indexedCount: number;
    spaces: { space: string; count: number; dimensions: number | null }[];
  }> {
    return this.request<{
      providers: ProviderStatus[];
      active: SearchProviderConfig;
      indexedCount: number;
      spaces: { space: string; count: number; dimensions: number | null }[];
    }>("/search/providers");
  }

  async setSearchConfig(
    config: Partial<SearchProviderConfig>
  ): Promise<{ ok: boolean; config: Partial<SearchProviderConfig> }> {
    return this.request<{ ok: boolean; config: Partial<SearchProviderConfig> }>(
      "/search/config",
      {
        method: "POST",
        body: JSON.stringify(config),
      }
    );
  }

  async reindexSearch(): Promise<{
    ok: boolean;
    indexed: { text: number; clip: number };
    providers: { text: string; clip: string };
  }> {
    return this.request<{
      ok: boolean;
      indexed: { text: number; clip: number };
      providers: { text: string; clip: string };
    }>("/search/reindex", { method: "POST", body: JSON.stringify({}) });
  }

  /**
   * Notify the App that a file was just written via canonical
   * `/api/vfs/multipart/finalize`. The App resolves the path to a
   * fileId and schedules semantic indexing (text + CLIP). Call from
   * `useUpload` after `parallelUpload` resolves.
   */
  async postIndexFile(path: string): Promise<{ ok: boolean; fileId: string }> {
    return this.request<{ ok: boolean; fileId: string }>("/index/file", {
      method: "POST",
      body: JSON.stringify({ path }),
    });
  }
}

export class ApiError extends Error {
  status: number;
  constructor(message: string, status: number = 500) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

export const api = new ApiClient();
