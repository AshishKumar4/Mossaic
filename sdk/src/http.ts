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
  PreviewInfo,
  PreviewInfoBatchEntry,
  PreviewUrlOpts,
  ReadPreviewOpts,
  ReadPreviewResult,
} from "../../shared/preview-types";
import type { ReadHandle, WriteHandle } from "./streams";
import type {
  VFSClient,
  VersionInfo,
  DropVersionsPolicy,
  BeginMultipartUploadOpts,
  MultipartUploadHandle,
  PutMultipartChunkResult,
  FinalizeMultipartUploadResult,
  AbortMultipartUploadResult,
} from "./vfs";
export { hashChunk } from "../../shared/crypto";

const HTTP_MULTIPART_TIMEOUT_MS = 10 * 60_000;

interface MultipartRequestOpts {
  signal?: AbortSignal;
}

function signalWithTimeout(signal?: AbortSignal): AbortSignal {
  const timeout = AbortSignal.timeout(HTTP_MULTIPART_TIMEOUT_MS);
  return signal ? AbortSignal.any([signal, timeout]) : timeout;
}

// Re-export so `import { VFSClient } from "@mossaic/sdk"` continues
// to work — `vfs.ts` is now the source of truth.
export type { VFSClient } from "./vfs";

/**
 * Token provider. Either a pre-issued static Bearer token, or a callback
 * the SDK invokes before every request. The callback form lets long-
 * lived clients (e.g. a SPA's `getTransferClient()`) refresh the token
 * inside the SDK without recreating the `HttpVFS` instance.
 *
 * The callback may return a string or a `Promise<string>`. Implementations
 * are expected to cache and only fetch a new token near expiry; the SDK
 * does NOT cache. A thrown / rejected callback surfaces to the caller as
 * the originating syscall's error (typically `EACCES` after
 * `mapServerError` on the resulting 401).
 */
export type ApiKeyProvider = string | (() => string | Promise<string>);

export interface CreateMossaicHttpClientOptions {
  /** Base URL of the @mossaic/vfs Worker, e.g. "https://mossaic.example.com". The /api/vfs path is appended. */
  url: string;
  /**
   * Pre-issued VFS Bearer token, or a provider callback the SDK calls
   * before every request. Use the callback form for long-lived clients
   * whose token rotates (e.g. the SPA's auth-bridge token).
   */
  apiKey: ApiKeyProvider;
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
  private readonly apiKeyProvider: ApiKeyProvider;

  constructor(opts: CreateMossaicHttpClientOptions) {
    if (!opts || typeof opts.url !== "string" || opts.url.length === 0) {
      throw new EINVAL({
        syscall: "createMossaicHttpClient",
        path: "(opts.url)",
      });
    }
    if (
      opts.apiKey === undefined ||
      opts.apiKey === null ||
      (typeof opts.apiKey !== "string" && typeof opts.apiKey !== "function") ||
      (typeof opts.apiKey === "string" && opts.apiKey.length === 0)
    ) {
      throw new EINVAL({
        syscall: "createMossaicHttpClient",
        path: "(opts.apiKey)",
      });
    }
    // Trim trailing slash to make path joining predictable.
    this.base = opts.url.replace(/\/$/, "");
    this.apiKeyProvider = opts.apiKey;
    // Bind to globalThis when defaulting to the platform `fetch` —
    // browser DOM `fetch` requires `this === Window` and would
    // otherwise throw `Illegal invocation` when invoked off the
    // HttpVFS instance.
    this.fetcher = opts.fetcher ?? fetch.bind(globalThis);
    this.promises = this;
  }

  /**
   * Resolve the current Bearer token. Static `apiKey` returns immediately;
   * callback `apiKey` is invoked on every request so the consumer can
   * cache + refresh without recreating the client.
   */
  private async getApiKey(): Promise<string> {
    if (typeof this.apiKeyProvider === "string") return this.apiKeyProvider;
    return await this.apiKeyProvider();
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
    const apiKey = await this.getApiKey();
    const headers: Record<string, string> = {
      Authorization: `Bearer ${apiKey}`,
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
    const apiKey = await this.getApiKey();
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
            Authorization: `Bearer ${apiKey}`,
            // Don't set Content-Type — fetch sets multipart boundary.
          },
          body: form,
        });
      } else {
        res = await this.fetcher(url, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${apiKey}`,
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

  async purge(p: string): Promise<void> {
    await this.post("purge", { path: p }, "unlink", p, "json");
  }

  async archive(p: string): Promise<void> {
    await this.post("archive", { path: p }, "chmod", p, "json");
  }

  async unarchive(p: string): Promise<void> {
    await this.post("unarchive", { path: p }, "chmod", p, "json");
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

  async rename(src: string, dst: string, opts?: import("./vfs").RenameOpts): Promise<void> {
    await this.post("rename", { src, dst, overwrite: opts?.overwrite }, "rename", dst, "json");
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

  // ── Manual multipart upload ────────────────────────────────────────────
  //
  // Thin wrappers over the existing `multipartBegin` / `multipartPutChunk` /
  // `multipartFinalize` / `multipartAbort` wire helpers. Surface a stable
  // user-facing handle shape (`MultipartUploadHandle`) decoupled from the
  // server's `MultipartBeginResponse` so future server changes don't break
  // serialised handles cached by external callers.
  //
  // `parallelUpload` (transfer.ts) drives the same wire endpoints but
  // owns its own AIMD controller + concurrency. The methods here are
  // for callers who need to do the chunking themselves.

  async beginMultipartUpload(
    p: string,
    opts: BeginMultipartUploadOpts
  ): Promise<MultipartUploadHandle> {
    if (opts.encryption !== undefined) {
      throw new EINVAL({ syscall: "beginMultipartUpload", path: p });
    }
    const body: import("../../shared/multipart").MultipartBeginRequest = {
      path: p,
      size: opts.size,
      ...(opts.chunkSize !== undefined ? { chunkSize: opts.chunkSize } : {}),
      ...(opts.mode !== undefined ? { mode: opts.mode } : {}),
      ...(opts.mimeType !== undefined ? { mimeType: opts.mimeType } : {}),
      ...(opts.metadata !== undefined ? { metadata: opts.metadata } : {}),
      ...(opts.tags !== undefined ? { tags: opts.tags } : {}),
      ...(opts.version !== undefined ? { version: opts.version } : {}),
      ...(opts.encryption !== undefined ? { encryption: opts.encryption } : {}),
      ...(opts.resumeFrom !== undefined ? { resumeFrom: opts.resumeFrom } : {}),
      ...(opts.ttlMs !== undefined ? { ttlMs: opts.ttlMs } : {}),
    };
    const res = await this.multipartBegin(body, signalWithTimeout(opts.signal));
    return {
      uploadId: res.uploadId,
      path: p,
      chunkSize: res.chunkSize,
      expectedChunks: res.totalChunks,
      poolSize: res.poolSize,
      sessionToken: res.sessionToken,
      expiresAtMs: res.expiresAtMs,
    };
  }

  async putMultipartChunk(
    handle: MultipartUploadHandle,
    index: number,
    chunk: Uint8Array | ArrayBuffer | Blob,
    opts?: MultipartRequestOpts
  ): Promise<PutMultipartChunkResult> {
    const bytes = await coerceChunkToUint8Array(chunk);
    const result = await this.multipartPutChunk(
      handle.uploadId,
      index,
      bytes,
      handle.sessionToken,
      signalWithTimeout(opts?.signal)
    );
    return {
      chunkHash: result.hash,
      accepted: result.ok === true,
      status: result.status,
    };
  }

  async finalizeMultipartUpload(
    handle: MultipartUploadHandle,
    chunkHashList: readonly string[],
    opts?: MultipartRequestOpts
  ): Promise<FinalizeMultipartUploadResult> {
    const r = await this.multipartFinalize(handle.uploadId, chunkHashList, signalWithTimeout(opts?.signal));
    return {
      path: r.path,
      pathId: r.fileId,
      // Server's MultipartFinalizeResponse does not yet surface
      // `versionId` (only `fileId`). Until it does, we expose an
      // empty string — callers that need the version row should
      // follow up with `listVersions(path)`. Documented on the type.
      versionId: "",
      size: r.size,
      fileHash: r.fileHash,
      isEncrypted: r.isEncrypted,
    };
  }

  async abortMultipartUpload(
    handle: MultipartUploadHandle,
    opts?: MultipartRequestOpts
  ): Promise<AbortMultipartUploadResult> {
    try {
      const r = await this.multipartAbort(handle.uploadId, signalWithTimeout(opts?.signal));
      return { aborted: r.ok === true };
    } catch (err) {
      // Server raises ENOENT for an unknown / already-aborted session
      // and EBUSY for a session that has already finalized (cannot
      // un-finalize). Both are "already terminal" — surface as
      // idempotent { aborted: false } rather than throwing so callers
      // can call abortMultipartUpload unconditionally in cleanup
      // paths without try/catch.
      const code = (err as { code?: string }).code;
      if (code === "ENOENT" || code === "EBUSY") {
        return { aborted: false };
      }
      throw err;
    }
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
    const apiKey = await this.getApiKey();
    const headers: Record<string, string> = {
      Authorization: `Bearer ${apiKey}`,
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

  async previewUrl(p: string, opts?: PreviewUrlOpts): Promise<string> {
    const info = await this.previewInfo(p, opts);
    return info.url;
  }

  async previewInfo(
    p: string,
    opts?: PreviewUrlOpts
  ): Promise<PreviewInfo> {
    const res = await this.post(
      "previewInfo",
      {
        path: p,
        variant: opts?.variant ?? "thumb",
        format: opts?.format,
        renderer: opts?.renderer,
        ttlMs: opts?.ttlMs,
      },
      "open",
      p,
      "json"
    );
    return (await res.json()) as PreviewInfo;
  }

  async previewInfoMany(
    paths: readonly string[],
    opts?: PreviewUrlOpts
  ): Promise<PreviewInfoBatchEntry[]> {
    const res = await this.post(
      "previewInfoMany",
      {
        paths: paths as string[],
        variant: opts?.variant ?? "thumb",
        format: opts?.format,
        renderer: opts?.renderer,
        ttlMs: opts?.ttlMs,
      },
      "open",
      paths[0] ?? "",
      "json"
    );
    const body = (await res.json()) as { results: PreviewInfoBatchEntry[] };
    return body.results;
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
        contentHash?: string;
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
        ...(r.contentHash !== undefined ? { contentHash: r.contentHash } : {}),
      })),
      cursor: raw.cursor,
    };
  }

  async fileInfo(
    p: string,
    opts: import("./vfs").FileInfoOpts = {}
  ): Promise<import("./vfs").ListFilesItem> {
    const res = await this.post(
      "fileInfo",
      { path: p, ...opts },
      "stat",
      p,
      "json"
    );
    const raw = (await res.json()) as {
      item: {
        path: string;
        pathId: string;
        stat?: import("../../shared/vfs-types").VFSStatRaw;
        metadata?: Record<string, unknown> | null;
        tags: string[];
        contentHash?: string;
      };
    };
    return {
      path: raw.item.path,
      pathId: raw.item.pathId,
      stat: raw.item.stat ? new VFSStat(raw.item.stat) : undefined,
      metadata: raw.item.metadata,
      tags: raw.item.tags,
      ...(raw.item.contentHash !== undefined
        ? { contentHash: raw.item.contentHash }
        : {}),
    };
  }

  async listChildren(
    p: string,
    opts: import("./vfs").ListChildrenOpts = {}
  ): Promise<import("./vfs").ListChildrenPage> {
    const res = await this.post(
      "listChildren",
      { path: p, ...opts },
      "scandir",
      p,
      "json"
    );
    const raw = (await res.json()) as {
      revision: number;
      entries: Array<
        | {
            kind: "folder";
            path: string;
            pathId: string;
            name: string;
            stat?: import("../../shared/vfs-types").VFSStatRaw;
          }
        | {
            kind: "file";
            path: string;
            pathId: string;
            name: string;
            stat?: import("../../shared/vfs-types").VFSStatRaw;
            metadata?: Record<string, unknown> | null;
            tags: string[];
            contentHash?: string;
          }
        | {
            kind: "symlink";
            path: string;
            pathId: string;
            name: string;
            target: string;
            stat?: import("../../shared/vfs-types").VFSStatRaw;
          }
      >;
      cursor?: string;
    };
    const entries: import("./vfs").VFSChild[] = raw.entries.map((e) => {
      if (e.kind === "folder") {
        const out: import("./vfs").VFSChild = {
          kind: "folder",
          path: e.path,
          pathId: e.pathId,
          name: e.name,
        };
        if (e.stat) out.stat = new VFSStat(e.stat);
        return out;
      }
      if (e.kind === "symlink") {
        const out: import("./vfs").VFSChild = {
          kind: "symlink",
          path: e.path,
          pathId: e.pathId,
          name: e.name,
          target: e.target,
        };
        if (e.stat) out.stat = new VFSStat(e.stat);
        return out;
      }
      const out: import("./vfs").VFSChild = {
        kind: "file",
        path: e.path,
        pathId: e.pathId,
        name: e.name,
        tags: e.tags,
      };
      if (e.stat) out.stat = new VFSStat(e.stat);
      if (e.metadata !== undefined) out.metadata = e.metadata;
      if (e.contentHash !== undefined) out.contentHash = e.contentHash;
      return out;
    });
    return { revision: raw.revision, entries, cursor: raw.cursor };
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

  /**
   * Yjs snapshot read over HTTP fallback.
   *
   * Returns `Y.encodeStateAsUpdate(doc)` bytes for a yjs-mode
   * file. The HTTP route mirrors the binding-mode RPC: POST to
   * `/api/vfs/readYjsSnapshot` with the path in the JSON body;
   * response body is the raw bytes (Content-Type
   * `application/octet-stream`).
   */
  async readYjsSnapshot(p: string): Promise<Uint8Array> {
    const res = await this.post(
      "readYjsSnapshot",
      { path: p },
      "read",
      p,
      "octet-stream"
    );
    return new Uint8Array(await res.arrayBuffer());
  }

  /**
   * Yjs snapshot commit over HTTP fallback.
   *
   * Encodes via `Y.encodeStateAsUpdate(doc)`, wraps with the
   * snapshot magic prefix, and writes via the existing
   * `writeFile` POST. The server detects the magic and routes
   * through `Y.applyUpdate`.
   */
  async commitYjsSnapshot(
    p: string,
    doc: import("yjs").Doc
  ): Promise<void> {
    const Y = await import("yjs");
    const { wrapYjsSnapshot } = await import("./yjs-internal");
    const updateBytes = Y.encodeStateAsUpdate(doc);
    const wrapped = wrapYjsSnapshot(updateBytes);
    await this.writeFile(p, wrapped);
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
    const apiKey = await this.getApiKey();
    const res = await this.fetcher(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
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
    const apiKey = await this.getApiKey();
    const res = await this.fetcher(url, {
      method: "PUT",
      headers: {
        Authorization: `Bearer ${apiKey}`,
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
    chunkHashList: readonly string[],
    signal?: AbortSignal
  ): Promise<import("../../shared/multipart").MultipartFinalizeResponse> {
    const url = `${this.base}/api/vfs/multipart/finalize`;
    const apiKey = await this.getApiKey();
    const res = await this.fetcher(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ uploadId, chunkHashList }),
      signal,
    });
    if (!res.ok) {
      await this.throwHttp(res, "open", undefined);
    }
    return (await res.json()) as import("../../shared/multipart").MultipartFinalizeResponse;
  }

  async multipartAbort(
    uploadId: string,
    signal?: AbortSignal
  ): Promise<{ ok: true }> {
    const url = `${this.base}/api/vfs/multipart/abort`;
    const apiKey = await this.getApiKey();
    const res = await this.fetcher(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ uploadId }),
      signal,
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
    const apiKey = await this.getApiKey();
    const res = await this.fetcher(url, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${apiKey}`,
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
    const apiKey = await this.getApiKey();
    const body: { path: string; ttlMs?: number } = { path: p };
    if (ttlMs !== undefined) body.ttlMs = ttlMs;
    const res = await this.fetcher(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
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
   * `POST /api/vfs/readChunk`; Bearer auth via `apiKey`.
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
    const apiKey = await this.getApiKey();
    const res = await this.fetcher(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
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

// ── Helpers for the manual-multipart surface ────────────────────────────

/**
 * Normalise a chunk argument to a Uint8Array view over an ArrayBuffer
 * (the wire shape `multipartPutChunk` expects). Throws synchronously
 * on unsupported inputs so callers fail fast — a Promise-rejection
 * path through the wire helper would be hard to disambiguate from a
 * server-side EINVAL.
 */
async function coerceChunkToUint8Array(
  chunk: Uint8Array | ArrayBuffer | Blob
): Promise<Uint8Array> {
  if (chunk instanceof Uint8Array) return chunk;
  if (chunk instanceof ArrayBuffer) return new Uint8Array(chunk);
  // Duck-type Blob (covers File too) — `instanceof Blob` is
  // unreliable in some test environments where the global is
  // shimmed. The arrayBuffer() method is the load-bearing contract.
  if (typeof (chunk as Blob)?.arrayBuffer === "function") {
    return new Uint8Array(await (chunk as Blob).arrayBuffer());
  }
  throw new TypeError(
    `putMultipartChunk: unsupported chunk type (got ${
      Object.prototype.toString.call(chunk)
    }; expected Uint8Array | ArrayBuffer | Blob)`
  );
}
