/**
 * HTTP fallback client — same VFS surface, same typed errors, same
 * scope semantics — but speaks HTTP+JSON to the @mossaic/vfs Worker
 * instead of dispatching DO RPC over a binding.
 *
 * Use this from non-Worker consumers: browsers, Node servers,
 * third-party clouds. Inside a Cloudflare Worker, the binding
 * client (`createVFS`) is strictly preferred — same architecture,
 * lower latency, no network hop, no API key on the wire.
 *
 * Auth: Bearer VFS token (signVFSToken / verifyVFSToken). Operator
 * mints the token with embedded scope; the HTTP route refuses any
 * request whose token has scope !== "vfs". The body never controls
 * tenant routing — scope is derived from the token. Cross-tenant
 * impersonation via header/body manipulation is impossible.
 *
 * Surface parity with the binding client: both implement the
 * `VFSClient` interface declared in this module. Streaming methods
 * (createReadStream / createWriteStream) and stream-handle
 * primitives are stubbed with EINVAL on the HTTP client — those
 * require duplex streams which the v1 fallback doesn't ship.
 * Non-Worker consumers can use openManifest + readChunk for
 * caller-orchestrated multi-invocation reads instead.
 */

import { VFSStat } from "./stats";
import { mapServerError, MossaicUnavailableError, EINVAL } from "./errors";
import type { OpenManifestResult, VFSStatRaw } from "../../shared/vfs-types";
import type {
  ReadPreviewOpts,
  ReadPreviewResult,
} from "../../shared/preview-types";
import type { ReadHandle, WriteHandle } from "./streams";
import type {
  VFSClient,
  VersionInfo,
  DropVersionsPolicy,
} from "./vfs";

// Re-export so `import { VFSClient } from "@mossaic/sdk"` continues
// to work — `vfs.ts` is now the source of truth.
export type { VFSClient } from "./vfs";

export interface CreateMossaicHttpClientOptions {
  /** Base URL of the @mossaic/vfs Worker, e.g. "https://mossaic.example.com". The /api/vfs path is appended. */
  url: string;
  /** Pre-issued VFS Bearer token (from `issueVFSToken`). The token's scope is the auth boundary. */
  apiKey: string;
  /** Optional: a fetch implementation. Defaults to globalThis.fetch. Useful for tests. */
  fetcher?: typeof fetch;
}

/**
 * HTTP-backed VFS client. POSTs to `${url}/api/vfs/<method>` with a
 * Bearer token. Uses the same VFSStat / VFSFsError / typed-subclass
 * errors as the binding client.
 *
 * Path / scope: the body never carries scope. Scope lives in the
 * token; the server extracts it via verifyVFSToken. This makes
 * cross-tenant impersonation impossible by construction.
 */
export class HttpVFS implements VFSClient {
  readonly promises: HttpVFS;
  private readonly fetcher: typeof fetch;
  private readonly base: string;
  private readonly apiKey: string;

  constructor(opts: CreateMossaicHttpClientOptions) {
    if (!opts || typeof opts.url !== "string" || opts.url.length === 0) {
      throw new EINVAL({
        syscall: "createMossaicHttpClient",
        path: "(opts.url)",
      });
    }
    if (typeof opts.apiKey !== "string" || opts.apiKey.length === 0) {
      throw new EINVAL({
        syscall: "createMossaicHttpClient",
        path: "(opts.apiKey)",
      });
    }
    // Trim trailing slash to make path joining predictable.
    this.base = opts.url.replace(/\/$/, "");
    this.apiKey = opts.apiKey;
    this.fetcher = opts.fetcher ?? fetch;
    this.promises = this;
  }

  // ── Wire helpers ──────────────────────────────────────────────────────

  private async post(
    method: string,
    body: Record<string, unknown> | Uint8Array,
    syscall: string,
    path: string | undefined,
    expect: "json" | "octet-stream"
  ): Promise<Response> {
    const url = `${this.base}/api/vfs/${method}`;
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.apiKey}`,
    };
    let payload: BodyInit;
    if (body instanceof Uint8Array) {
      headers["Content-Type"] = "application/octet-stream";
      payload = body;
    } else {
      headers["Content-Type"] = "application/json";
      payload = JSON.stringify(body);
    }
    let res: Response;
    try {
      res = await this.fetcher(url, { method: "POST", headers, body: payload });
    } catch (err) {
      // Network-level failure (DNS, TCP RST, etc.) → MossaicUnavailable.
      throw new MossaicUnavailableError({
        message: `HTTP fetch to ${url} failed: ${(err as Error).message}`,
      });
    }
    if (res.ok) return res;
    // Non-2xx: parse JSON body { code, message } and remap to typed error.
    let payloadJson: { code?: string; message?: string };
    try {
      payloadJson = (await res.json()) as { code?: string; message?: string };
    } catch {
      payloadJson = { message: `HTTP ${res.status} ${res.statusText}` };
    }
    // Build a synthetic Error with .code so mapServerError pulls it
    // out of the explicitCode branch.
    const synthetic = Object.assign(
      new Error(payloadJson.message ?? `HTTP ${res.status}`),
      { code: payloadJson.code }
    );
    throw mapServerError(synthetic, { syscall, path });
    // Suppress the never-returns linter — throw above unwinds.
    void expect;
  }

  // ── Reads ─────────────────────────────────────────────────────────────

  async readFile(p: string): Promise<Uint8Array>;
  async readFile(p: string, opts: { encoding: "utf8" }): Promise<string>;
  async readFile(
    p: string,
    opts?: { encoding?: "utf8" }
  ): Promise<Uint8Array | string> {
    const res = await this.post(
      "readFile",
      // Don't pass encoding to the server — it can return either; we
      // just always grab the raw bytes and decode locally if needed.
      // (The server's encoding=utf8 path returns JSON wrapped; raw is
      // simpler for the wire and lets us handle binary uniformly.)
      { path: p },
      "open",
      p,
      "octet-stream"
    );
    const buf = new Uint8Array(await res.arrayBuffer());
    return opts?.encoding === "utf8" ? new TextDecoder().decode(buf) : buf;
  }

  async readdir(p: string): Promise<string[]> {
    const res = await this.post("readdir", { path: p }, "scandir", p, "json");
    const body = (await res.json()) as { entries: string[] };
    return body.entries;
  }

  async stat(p: string): Promise<VFSStat> {
    const res = await this.post("stat", { path: p }, "stat", p, "json");
    const body = (await res.json()) as { stat: VFSStatRaw };
    return new VFSStat(body.stat);
  }

  async lstat(p: string): Promise<VFSStat> {
    const res = await this.post("lstat", { path: p }, "lstat", p, "json");
    const body = (await res.json()) as { stat: VFSStatRaw };
    return new VFSStat(body.stat);
  }

  async exists(p: string): Promise<boolean> {
    const res = await this.post("exists", { path: p }, "access", p, "json");
    const body = (await res.json()) as { exists: boolean };
    return body.exists;
  }

  async readlink(p: string): Promise<string> {
    const res = await this.post(
      "readlink",
      { path: p },
      "readlink",
      p,
      "json"
    );
    const body = (await res.json()) as { target: string };
    return body.target;
  }

  async readManyStat(paths: string[]): Promise<(VFSStat | null)[]> {
    const res = await this.post(
      "readManyStat",
      { paths },
      "lstat",
      undefined,
      "json"
    );
    const body = (await res.json()) as { stats: (VFSStatRaw | null)[] };
    return body.stats.map((s) => (s ? new VFSStat(s) : null));
  }

  // ── Writes ────────────────────────────────────────────────────────────

  async writeFile(
    p: string,
    data: Uint8Array | string,
    opts?: import("./vfs").WriteFileOpts
  ): Promise<void> {
    if (typeof data === "string") {
      // String → JSON body. Server decodes via TextEncoder.
      await this.post(
        "writeFile",
        {
          path: p,
          data,
          mode: opts?.mode,
          mimeType: opts?.mimeType,
          metadata: opts?.metadata,
          tags: opts?.tags,
          version: opts?.version,
        },
        "open",
        p,
        "json"
      );
      return;
    }
    // Bytes path — when opts include metadata/tags/version,
    // switch to multipart/form-data so the meta payload rides along
    // with the binary bytes. Without those opts we keep the legacy
    // octet-stream envelope (smaller, no FormData allocation).
    const hasMeta =
      opts !== undefined &&
      (opts.metadata !== undefined ||
        opts.tags !== undefined ||
        opts.version !== undefined ||
        opts.mode !== undefined ||
        opts.mimeType !== undefined);
    const url = `${this.base}/api/vfs/writeFile?path=${encodeURIComponent(p)}`;
    let res: Response;
    try {
      if (hasMeta) {
        const form = new FormData();
        // The bytes part — server reads via formData().get("bytes").
        form.append("bytes", new Blob([data]));
        form.append(
          "meta",
          JSON.stringify({
            mode: opts!.mode,
            mimeType: opts!.mimeType,
            metadata: opts!.metadata,
            tags: opts!.tags,
            version: opts!.version,
          })
        );
        res = await this.fetcher(url, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${this.apiKey}`,
            // Don't set Content-Type — fetch sets multipart boundary.
          },
          body: form,
        });
      } else {
        res = await this.fetcher(url, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${this.apiKey}`,
            "Content-Type": "application/octet-stream",
          },
          body: data,
        });
      }
    } catch (err) {
      throw new MossaicUnavailableError({
        message: `HTTP fetch to ${url} failed: ${(err as Error).message}`,
      });
    }
    if (!res.ok) {
      let payloadJson: { code?: string; message?: string };
      try {
        payloadJson = (await res.json()) as { code?: string; message?: string };
      } catch {
        payloadJson = { message: `HTTP ${res.status} ${res.statusText}` };
      }
      const synthetic = Object.assign(
        new Error(payloadJson.message ?? `HTTP ${res.status}`),
        { code: payloadJson.code }
      );
      throw mapServerError(synthetic, { syscall: "open", path: p });
    }
  }

  async unlink(p: string): Promise<void> {
    await this.post("unlink", { path: p }, "unlink", p, "json");
  }

  async mkdir(
    p: string,
    opts?: { recursive?: boolean; mode?: number }
  ): Promise<void> {
    await this.post(
      "mkdir",
      {
        path: p,
        recursive: opts?.recursive,
        mode: opts?.mode,
      },
      "mkdir",
      p,
      "json"
    );
  }

  async rmdir(p: string): Promise<void> {
    await this.post("rmdir", { path: p }, "rmdir", p, "json");
  }

  async removeRecursive(p: string): Promise<void> {
    let cursor: string | undefined;
    for (;;) {
      const res = await this.post(
        "removeRecursive",
        { path: p, cursor },
        "rmdir",
        p,
        "json"
      );
      const body = (await res.json()) as { done: boolean; cursor?: string };
      if (body.done) return;
      cursor = body.cursor;
    }
  }

  async symlink(target: string, p: string): Promise<void> {
    await this.post("symlink", { target, path: p }, "symlink", p, "json");
  }

  async chmod(p: string, mode: number): Promise<void> {
    await this.post("chmod", { path: p, mode }, "chmod", p, "json");
  }

  async rename(src: string, dst: string): Promise<void> {
    await this.post("rename", { src, dst }, "rename", dst, "json");
  }

  // ── Streams (NOT supported on HTTP fallback in v1) ────────────────────

  async createReadStream(): Promise<ReadableStream<Uint8Array>> {
    throw new EINVAL({
      syscall: "createReadStream",
      path: "(HTTP fallback: use openManifest + readChunk for caller-orchestrated multi-invocation reads)",
    });
  }
  async createWriteStream(): Promise<WritableStream<Uint8Array>> {
    throw new EINVAL({
      syscall: "createWriteStream",
      path: "(HTTP fallback: streams unsupported in v1; use writeFile or chunked uploads on the binding client)",
    });
  }
  async createWriteStreamWithHandle(): Promise<{
    stream: WritableStream<Uint8Array>;
    handle: WriteHandle;
  }> {
    throw new EINVAL({
      syscall: "createWriteStreamWithHandle",
      path: "(HTTP fallback: streams unsupported in v1)",
    });
  }
  async openReadStream(): Promise<ReadHandle> {
    throw new EINVAL({
      syscall: "openReadStream",
      path: "(HTTP fallback: use openManifest + readChunk instead)",
    });
  }
  async pullReadStream(): Promise<Uint8Array> {
    throw new EINVAL({
      syscall: "pullReadStream",
      path: "(HTTP fallback: use readChunk(path, idx) instead)",
    });
  }

  // ── Low-level escape hatch ────────────────────────────────────────────

  async openManifest(p: string): Promise<OpenManifestResult> {
    const res = await this.post(
      "openManifest",
      { path: p },
      "open",
      p,
      "json"
    );
    const body = (await res.json()) as { manifest: OpenManifestResult };
    return body.manifest;
  }

  /**
   * Batched manifest fetch via the dedicated `/api/vfs/manifests`
   * route. One round-trip for N paths; per-path errors come back
   * as `{ ok: false, code, message }` rather than throwing.
   */
  async openManifests(
    paths: string[]
  ): Promise<
    (
      | { ok: true; manifest: OpenManifestResult }
      | { ok: false; code: string; message: string }
    )[]
  > {
    const res = await this.post(
      "manifests",
      { paths },
      "open",
      undefined,
      "json"
    );
    const body = (await res.json()) as {
      manifests: (
        | { ok: true; manifest: OpenManifestResult }
        | { ok: false; code: string; message: string }
      )[];
    };
    return body.manifests;
  }

  async readPreview(
    p: string,
    opts?: ReadPreviewOpts
  ): Promise<ReadPreviewResult> {
    const url = `${this.base}/api/vfs/readPreview`;
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.apiKey}`,
      "Content-Type": "application/json",
    };
    const payload = JSON.stringify({
      path: p,
      variant: opts?.variant ?? "thumb",
      format: opts?.format,
      renderer: opts?.renderer,
    });
    let res: Response;
    try {
      res = await this.fetcher(url, {
        method: "POST",
        headers,
        body: payload,
      });
    } catch (err) {
      throw new MossaicUnavailableError({
        message: `HTTP fetch to ${url} failed: ${(err as Error).message}`,
      });
    }
    if (!res.ok) {
      let pj: { code?: string; message?: string };
      try {
        pj = (await res.json()) as { code?: string; message?: string };
      } catch {
        pj = { message: `HTTP ${res.status} ${res.statusText}` };
      }
      const synthetic = Object.assign(
        new Error(pj.message ?? `HTTP ${res.status}`),
        { code: pj.code }
      );
      throw mapServerError(synthetic, { syscall: "open", path: p });
    }
    const bytes = new Uint8Array(await res.arrayBuffer());
    const mimeType = res.headers.get("Content-Type") ?? "application/octet-stream";
    const rendererKind = res.headers.get("X-Mossaic-Renderer") ?? "icon-card";
    const cacheHdr = res.headers.get("X-Mossaic-Variant-Cache") ?? "miss";
    const sourceMime =
      res.headers.get("X-Mossaic-Source-Mime") ?? "application/octet-stream";
    const widthHdr = res.headers.get("X-Mossaic-Width");
    const heightHdr = res.headers.get("X-Mossaic-Height");
    return {
      bytes,
      mimeType,
      width: widthHdr === null ? 0 : parseInt(widthHdr, 10),
      height: heightHdr === null ? 0 : parseInt(heightHdr, 10),
      sourceMimeType: sourceMime,
      rendererKind,
      fromVariantTable: cacheHdr === "hit",
    };
  }

  async readChunk(p: string, chunkIndex: number): Promise<Uint8Array> {
    const res = await this.post(
      "readChunk",
      { path: p, chunkIndex },
      "read",
      p,
      "octet-stream"
    );
    return new Uint8Array(await res.arrayBuffer());
  }

  // ── versioning ───────────────────────────────────────────────
  // The HTTP fallback router (worker/routes/vfs.ts) gains matching
  // POST endpoints. Wire-shape mirrors the binding-client surface:
  // listVersions returns VersionInfo[] with `id`; restoreVersion
  // returns { id }; dropVersions returns { dropped, kept }.

  async listVersions(
    p: string,
    opts?: import("./vfs").ListVersionsOpts
  ): Promise<VersionInfo[]> {
    const res = await this.post(
      "listVersions",
      {
        path: p,
        limit: opts?.limit,
        userVisibleOnly: opts?.userVisibleOnly,
        includeMetadata: opts?.includeMetadata,
      },
      "listVersions",
      p,
      "json"
    );
    const body = (await res.json()) as { versions: VersionInfo[] };
    return body.versions;
  }

  async restoreVersion(
    p: string,
    sourceVersionId: string
  ): Promise<{ id: string }> {
    const res = await this.post(
      "restoreVersion",
      { path: p, sourceVersionId },
      "restoreVersion",
      p,
      "json"
    );
    const body = (await res.json()) as { id: string };
    return body;
  }

  async dropVersions(
    p: string,
    policy: DropVersionsPolicy
  ): Promise<{ dropped: number; kept: number }> {
    const res = await this.post(
      "dropVersions",
      { path: p, policy },
      "dropVersions",
      p,
      "json"
    );
    return (await res.json()) as { dropped: number; kept: number };
  }

  // ── ──────────────────────────────────────────────────────────

  async patchMetadata(
    p: string,
    patch: Record<string, unknown> | null,
    opts?: import("./vfs").PatchMetadataOpts
  ): Promise<void> {
    await this.post(
      "patchMetadata",
      { path: p, patch, opts },
      "open",
      p,
      "json"
    );
  }

  async copyFile(
    src: string,
    dest: string,
    opts?: import("./vfs").CopyFileOpts
  ): Promise<void> {
    await this.post(
      "copyFile",
      { src, dest, opts },
      "open",
      src,
      "json"
    );
  }

  async listFiles(
    opts: import("./vfs").ListFilesOpts = {}
  ): Promise<import("./vfs").ListFilesPage> {
    const res = await this.post(
      "listFiles",
      opts as Record<string, unknown>,
      "scandir",
      opts.prefix,
      "json"
    );
    const raw = (await res.json()) as {
      items: Array<{
        path: string;
        pathId: string;
        stat?: import("../../shared/vfs-types").VFSStatRaw;
        metadata?: Record<string, unknown> | null;
        tags: string[];
      }>;
      cursor?: string;
    };
    return {
      items: raw.items.map((r) => ({
        path: r.path,
        pathId: r.pathId,
        stat: r.stat ? new VFSStat(r.stat) : undefined,
        metadata: r.metadata,
        tags: r.tags,
      })),
      cursor: raw.cursor,
    };
  }

  async markVersion(
    p: string,
    versionId: string,
    opts: import("./vfs").VersionMarkOpts
  ): Promise<void> {
    await this.post(
      "markVersion",
      { path: p, versionId, label: opts.label, userVisible: opts.userVisible },
      "open",
      p,
      "json"
    );
  }

  // ── multipart parallel transfer ───────────────────────────
  //
  // Thin wire helpers used by `sdk/src/transfer.ts`. They speak the
  // shapes declared in `shared/multipart.ts`. Each method maps 1:1 to
  // a route under `/api/vfs/multipart/*`. Errors are surfaced
  // unmapped — the transfer engine catches and routes them through
  // `mapServerError` itself so it can implement adaptive backoff.

  async multipartBegin(
    body: import("../../shared/multipart").MultipartBeginRequest,
    signal?: AbortSignal
  ): Promise<import("../../shared/multipart").MultipartBeginResponse> {
    const url = `${this.base}/api/vfs/multipart/begin`;
    const res = await this.fetcher(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
      signal,
    });
    if (!res.ok) {
      await this.throwHttp(res, "open", body.path);
    }
    return (await res.json()) as import("../../shared/multipart").MultipartBeginResponse;
  }

  async multipartPutChunk(
    uploadId: string,
    idx: number,
    bytes: Uint8Array,
    sessionToken: string,
    signal?: AbortSignal
  ): Promise<import("../../shared/multipart").MultipartPutChunkResponse> {
    const url = `${this.base}/api/vfs/multipart/${encodeURIComponent(uploadId)}/chunk/${idx}`;
    const res = await this.fetcher(url, {
      method: "PUT",
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        "X-Session-Token": sessionToken,
        "Content-Type": "application/octet-stream",
        "Content-Length": String(bytes.byteLength),
      },
      body: bytes,
      signal,
    });
    if (!res.ok) {
      await this.throwHttp(res, "open", undefined);
    }
    return (await res.json()) as import("../../shared/multipart").MultipartPutChunkResponse;
  }

  async multipartFinalize(
    uploadId: string,
    chunkHashList: readonly string[]
  ): Promise<import("../../shared/multipart").MultipartFinalizeResponse> {
    const url = `${this.base}/api/vfs/multipart/finalize`;
    const res = await this.fetcher(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ uploadId, chunkHashList }),
    });
    if (!res.ok) {
      await this.throwHttp(res, "open", undefined);
    }
    return (await res.json()) as import("../../shared/multipart").MultipartFinalizeResponse;
  }

  async multipartAbort(
    uploadId: string
  ): Promise<{ ok: true }> {
    const url = `${this.base}/api/vfs/multipart/abort`;
    const res = await this.fetcher(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ uploadId }),
    });
    if (!res.ok) {
      await this.throwHttp(res, "open", undefined);
    }
    return (await res.json()) as { ok: true };
  }

  async multipartStatus(
    uploadId: string,
    sessionToken: string
  ): Promise<import("../../shared/multipart").MultipartStatusResponse> {
    const url = `${this.base}/api/vfs/multipart/${encodeURIComponent(uploadId)}/status`;
    const res = await this.fetcher(url, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        "X-Session-Token": sessionToken,
      },
    });
    if (!res.ok) {
      await this.throwHttp(res, "open", undefined);
    }
    return (await res.json()) as import("../../shared/multipart").MultipartStatusResponse;
  }

  async multipartDownloadToken(
    p: string,
    ttlMs?: number
  ): Promise<import("../../shared/multipart").DownloadTokenResponse> {
    const url = `${this.base}/api/vfs/multipart/download-token`;
    const body: { path: string; ttlMs?: number } = { path: p };
    if (ttlMs !== undefined) body.ttlMs = ttlMs;
    const res = await this.fetcher(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      await this.throwHttp(res, "open", p);
    }
    return (await res.json()) as import("../../shared/multipart").DownloadTokenResponse;
  }

  /**
   * Per-chunk read keyed by `(path, chunkIndex)`. Posts to canonical
   * `POST /api/vfs/readChunk`; Bearer auth via `this.apiKey`.
   *
   * `fileId`, `hash`, and `token` are accepted in the signature for
   * forward-compatibility with a typed `/readChunkByFileId` endpoint
   * (cacheable GET keyed by hash); none are required by the canonical
   * read path. Caller does any post-fetch hash verification.
   */
  async fetchChunkByHash(
    fileId: string,
    idx: number,
    hash: string,
    token: string,
    path: string,
    signal?: AbortSignal
  ): Promise<Uint8Array> {
    void fileId;
    void hash;
    void token;
    const url = `${this.base}/api/vfs/readChunk`;
    const res = await this.fetcher(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        path,
        chunkIndex: idx,
      }),
      signal,
    });
    if (!res.ok) {
      await this.throwHttp(res, "open", path);
    }
    return new Uint8Array(await res.arrayBuffer());
  }

  /**
   * Helper to throw a typed error from a non-2xx multipart response.
   * Mirrors the existing `post()` error mapping but accepts a Response
   * directly so callers that don't use `post()` can reuse it.
   */
  private async throwHttp(
    res: Response,
    syscall: string,
    path: string | undefined
  ): Promise<never> {
    let payloadJson: { code?: string; message?: string };
    try {
      payloadJson = (await res.json()) as { code?: string; message?: string };
    } catch {
      payloadJson = { message: `HTTP ${res.status} ${res.statusText}` };
    }
    const synthetic = Object.assign(
      new Error(payloadJson.message ?? `HTTP ${res.status}`),
      { code: payloadJson.code }
    );
    throw mapServerError(synthetic, { syscall, path });
  }

}

/**
 * Construct an HTTP-backed VFS client. Use from non-Worker consumers
 * (browsers, Node servers, etc.); inside a Cloudflare Worker prefer
 * the binding client `createVFS(env, opts)`.
 */
export function createMossaicHttpClient(
  opts: CreateMossaicHttpClientOptions
): HttpVFS {
  return new HttpVFS(opts);
}

// Surface parity is enforced by the `implements VFSClient` clauses
// on both `VFS` (sdk/src/vfs.ts) and `HttpVFS` (this file). If either
// class diverges from VFSClient, the class declaration fails to
// compile. No additional static assertion needed.
