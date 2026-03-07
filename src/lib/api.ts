import type {
  AuthResponse,
  UploadInitRequest,
  UploadInitResponse,
  FileListResponse,
  FileManifest,
  CreateFolderRequest,
  Folder,
  QuotaInfo,
  AnalyticsOverview,
} from "@shared/types";

const API_BASE = "/api";

class ApiClient {
  private token: string | null = null;

  setToken(token: string | null): void {
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
      const err = await res.json().catch(() => ({ error: "Request failed" }));
      throw new Error(
        (err as { error?: string }).error || `HTTP ${res.status}`
      );
    }

    return res.json() as Promise<T>;
  }

  // Auth
  async signup(email: string, password: string): Promise<AuthResponse> {
    return this.request("/auth/signup", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    });
  }

  async login(email: string, password: string): Promise<AuthResponse> {
    return this.request("/auth/login", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    });
  }

  // Files
  async listFiles(parentId?: string | null): Promise<FileListResponse> {
    const query = parentId ? `?parentId=${parentId}` : "";
    return this.request(`/files${query}`);
  }

  async deleteFile(fileId: string): Promise<void> {
    await this.request(`/files/${fileId}`, { method: "DELETE" });
  }

  // Upload
  async initUpload(data: UploadInitRequest): Promise<UploadInitResponse> {
    return this.request("/upload/init", {
      method: "POST",
      body: JSON.stringify(data),
    });
  }

  async uploadChunk(
    fileId: string,
    chunkIndex: number,
    chunkHash: string,
    poolSize: number,
    data: ArrayBuffer
  ): Promise<{ status: string; bytesStored: number }> {
    const res = await fetch(
      `${API_BASE}/upload/chunk/${fileId}/${chunkIndex}`,
      {
        method: "PUT",
        headers: {
          Authorization: `Bearer ${this.token}`,
          "X-Chunk-Hash": chunkHash,
          "X-Pool-Size": poolSize.toString(),
        },
        body: data,
      }
    );

    if (!res.ok) {
      throw new Error(`Chunk upload failed: ${res.status}`);
    }

    return res.json();
  }

  async completeUpload(
    fileId: string,
    fileHash: string
  ): Promise<{ ok: boolean }> {
    return this.request(`/upload/complete/${fileId}`, {
      method: "POST",
      body: JSON.stringify({ fileHash }),
    });
  }

  // Download
  async getManifest(fileId: string): Promise<FileManifest> {
    return this.request(`/download/manifest/${fileId}`);
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
      throw new Error(`Chunk download failed: ${res.status}`);
    }

    return res.arrayBuffer();
  }

  // Folders
  async createFolder(data: CreateFolderRequest): Promise<Folder> {
    return this.request("/folders", {
      method: "POST",
      body: JSON.stringify(data),
    });
  }

  async getFolderContents(
    folderId: string
  ): Promise<FileListResponse & { path: Folder[] }> {
    return this.request(`/folders/${folderId}`);
  }

  // Analytics
  async getAnalyticsOverview(): Promise<AnalyticsOverview> {
    return this.request("/analytics/overview");
  }
}

export const api = new ApiClient();
