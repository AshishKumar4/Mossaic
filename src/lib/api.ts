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

  // Upload + Download
  //
  // Phase 17.6: the SPA's chunked transfer pipeline collapsed onto
  // `@mossaic/sdk` 's `parallelUpload` / `parallelDownload` (see
  // `src/hooks/use-upload.ts` + `src/hooks/use-download.ts`). The
  // `uploadInit` / `uploadChunk` / `uploadComplete` / `getManifest` /
  // `downloadChunk` methods are gone — the SDK drives the
  // entire transfer through `/api/upload/multipart/*` (App-pinned
  // bridge) and `/api/download/chunk/*` (legacy chunk download
  // endpoint, addressed via `chunkFetchBaseOverride`).
  //
  // The legacy single-chunk routes on the App
  // (`/api/upload/init`, `/api/upload/chunk/*`,
  // `/api/upload/complete/*`, `/api/download/manifest/*`,
  // `/api/download/chunk/*`) remain mounted on the worker for
  // back-compat during the rollout window. Phase 17.6.1 cleanup
  // (deferred 1–2 weeks post-stability) deletes the unused legacy
  // upload routes and their typed RPCs.

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
