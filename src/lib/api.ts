import type {
  AuthResponse,
  FileListResponse,
  UploadInitRequest,
  UploadInitResponse,
  CreateFolderRequest,
  Folder,
  FileManifest,
  AnalyticsOverview,
  ApiError as ApiErrorResponse,
  UserFile,
  GalleryPhotosResponse,
  SharedAlbumPhotosResponse,
} from "@shared/types";
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
      const err: ApiErrorResponse = await res.json().catch(() => ({
        error: `HTTP ${res.status}`,
      }));
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

  // Upload
  async uploadInit(data: UploadInitRequest): Promise<UploadInitResponse> {
    return this.request<UploadInitResponse>("/upload/init", {
      method: "POST",
      body: JSON.stringify(data),
    });
  }

  async uploadChunk(
    fileId: string,
    chunkIndex: number,
    data: ArrayBuffer,
    chunkHash: string,
    poolSize: number
  ): Promise<{ status: string; bytesStored: number }> {
    const res = await fetch(
      `${API_BASE}/upload/chunk/${fileId}/${chunkIndex}`,
      {
        method: "PUT",
        headers: {
          Authorization: `Bearer ${this.token}`,
          "X-Chunk-Hash": chunkHash,
          "X-Pool-Size": poolSize.toString(),
          "Content-Type": "application/octet-stream",
        },
        body: data,
      }
    );
    if (!res.ok) {
      throw new ApiError("Chunk upload failed", res.status);
    }
    return res.json();
  }

  async uploadComplete(
    fileId: string,
    fileHash: string
  ): Promise<{ ok: boolean; fileId: string }> {
    return this.request<{ ok: boolean; fileId: string }>(
      `/upload/complete/${fileId}`,
      {
        method: "POST",
        body: JSON.stringify({ fileHash }),
      }
    );
  }

  // Download
  async getManifest(fileId: string): Promise<FileManifest> {
    return this.request<FileManifest>(`/download/manifest/${fileId}`);
  }

  async downloadChunk(fileId: string, chunkIndex: number): Promise<ArrayBuffer> {
    const res = await fetch(
      `${API_BASE}/download/chunk/${fileId}/${chunkIndex}`,
      {
        headers: {
          Authorization: `Bearer ${this.token}`,
        },
      }
    );
    if (!res.ok) {
      throw new ApiError("Chunk download failed", res.status);
    }
    return res.arrayBuffer();
  }

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
    topK?: number,
    provider?: string
  ): Promise<{ results: SearchResult[]; provider: string; query: string }> {
    return this.request<{ results: SearchResult[]; provider: string; query: string }>(
      "/search",
      {
        method: "POST",
        body: JSON.stringify({ query, topK, provider }),
      }
    );
  }

  async getSearchProviders(): Promise<{
    providers: ProviderStatus[];
    active: SearchProviderConfig;
    indexedCount: number;
  }> {
    return this.request<{
      providers: ProviderStatus[];
      active: SearchProviderConfig;
      indexedCount: number;
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

  async reindexSearch(): Promise<{ ok: boolean; indexed: number; provider: string }> {
    return this.request<{ ok: boolean; indexed: number; provider: string }>(
      "/search/reindex",
      { method: "POST", body: JSON.stringify({}) }
    );
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
