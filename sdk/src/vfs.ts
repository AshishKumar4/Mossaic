/**
 * `VFS` — fs/promises-shaped client over Mossaic's typed DO RPC.
 *
 * Architecture: the consumer Worker holds a `DurableObjectNamespace`
 * binding for `MOSSAIC_USER` (and `MOSSAIC_SHARD`, used internally by
 * the DO). Each VFS method does:
 *
 *     this.user().vfsXxx(this.scope(), ...args)
 *
 * which is **one DO RPC subrequest** in the consumer's invocation,
 * regardless of how many internal subrequests the DO fans out to
 * ShardDOs. This is the load-bearing efficiency claim (sdk-impl-plan
 * §2.1, §5.2) and the consumer fixture test pins it.
 *
 * The class also satisfies isomorphic-git's fs interface:
 *   - `vfs.promises === vfs` (self-reference; igit reads `fs.promises`)
 *   - All read/write/stat methods present
 *   - Errors carry `code` / `errno` / `syscall` / `path`
 */

import { VFSStat } from "./stats";
import {
  createReadStreamRpc,
  createWriteStreamRpc,
  createWriteStreamWithHandleRpc,
  type ReadHandle,
  type WriteHandle,
  type ReadStreamOptions,
} from "./streams";
import { EINVAL, mapServerError } from "./errors";
import type {
  OpenManifestResult,
  VFSScope,
  VFSStatRaw,
} from "../../shared/vfs-types";

/**
 * Public consumer-facing VFS contract. Both the binding `VFS` class
 * and the HTTP fallback `HttpVFS` (sdk/src/http.ts) implement this
 * interface — the `implements VFSClient` clause on each class
 * forces both to honour the same surface, so a future divergence is
 * caught at compile time rather than at integration time.
 *
 * Methods mirror Node's `fs/promises` shape with Mossaic-specific
 * additions (readManyStat, removeRecursive, openManifest, readChunk,
 * stream-handle primitives). `vfs.promises === vfs` (the
 * isomorphic-git contract) is honoured by both implementations.
 */
export interface VFSClient {
  readonly promises: VFSClient;

  // Reads
  readFile(p: string): Promise<Uint8Array>;
  readFile(p: string, opts: { encoding: "utf8" }): Promise<string>;
  readFile(p: string, opts: { version: string }): Promise<Uint8Array>;
  readFile(
    p: string,
    opts: { version: string; encoding: "utf8" }
  ): Promise<string>;
  readdir(p: string): Promise<string[]>;
  stat(p: string): Promise<VFSStat>;
  lstat(p: string): Promise<VFSStat>;
  exists(p: string): Promise<boolean>;
  readlink(p: string): Promise<string>;
  readManyStat(paths: string[]): Promise<(VFSStat | null)[]>;

  // Writes
  writeFile(
    p: string,
    data: Uint8Array | string,
    opts?: WriteFileOpts
  ): Promise<void>;
  unlink(p: string): Promise<void>;
  mkdir(
    p: string,
    opts?: { recursive?: boolean; mode?: number }
  ): Promise<void>;
  rmdir(p: string): Promise<void>;
  removeRecursive(p: string): Promise<void>;
  symlink(target: string, p: string): Promise<void>;
  chmod(p: string, mode: number): Promise<void>;
  rename(src: string, dst: string): Promise<void>;
  /** Phase 12: deep-merge metadata + add/remove tags atomically. */
  patchMetadata(
    p: string,
    patch: Record<string, unknown> | null,
    opts?: PatchMetadataOpts
  ): Promise<void>;
  /** Phase 12: same-tenant copyFile (chunk-refcount-aware). */
  copyFile(src: string, dest: string, opts?: CopyFileOpts): Promise<void>;
  /** Phase 12: indexed listFiles with HMAC-signed cursor pagination. */
  listFiles(opts?: ListFilesOpts): Promise<ListFilesPage>;

  // Streams (HTTP fallback throws EINVAL for these in v1)
  createReadStream(
    p: string,
    opts?: ReadStreamOptions
  ): Promise<ReadableStream<Uint8Array>>;
  createWriteStream(
    p: string,
    opts?: { mode?: number; mimeType?: string }
  ): Promise<WritableStream<Uint8Array>>;
  createWriteStreamWithHandle(
    p: string,
    opts?: { mode?: number; mimeType?: string }
  ): Promise<{
    stream: WritableStream<Uint8Array>;
    handle: WriteHandle;
  }>;

  // Low-level escape hatch
  openManifest(p: string): Promise<OpenManifestResult>;
  readChunk(p: string, chunkIndex: number): Promise<Uint8Array>;
  openReadStream(p: string): Promise<ReadHandle>;
  pullReadStream(
    handle: ReadHandle,
    chunkIndex: number,
    range?: { start?: number; end?: number }
  ): Promise<Uint8Array>;

  // Phase 9: file-level versioning (only meaningful when the tenant
  // has versioning enabled; the binding client surfaces these methods
  // on every VFS instance for type-stability, but they throw ENOENT
  // / EINVAL on tenants without versioning).
  listVersions(
    p: string,
    opts?: ListVersionsOpts
  ): Promise<VersionInfo[]>;
  restoreVersion(p: string, sourceVersionId: string): Promise<{ id: string }>;
  dropVersions(
    p: string,
    policy: DropVersionsPolicy
  ): Promise<{ dropped: number; kept: number }>;
  /** Phase 12: set per-version label and/or user-visible flag. */
  markVersion(
    p: string,
    versionId: string,
    opts: VersionMarkOpts
  ): Promise<void>;
}

/**
 * Subset of the production UserDO RPC surface the SDK uses. Declared
 * structurally so the SDK does NOT take a runtime dep on the worker
 * source — callers pass a typed `DurableObjectNamespace<UserDOClient>`
 * (or a fully-typed UserDO) and TypeScript's structural matching
 * accepts it.
 */
export interface UserDOClient {
  vfsStat(scope: VFSScope, path: string): Promise<VFSStatRaw>;
  vfsLstat(scope: VFSScope, path: string): Promise<VFSStatRaw>;
  vfsExists(scope: VFSScope, path: string): Promise<boolean>;
  vfsReadlink(scope: VFSScope, path: string): Promise<string>;
  vfsReaddir(scope: VFSScope, path: string): Promise<string[]>;
  vfsReadManyStat(
    scope: VFSScope,
    paths: string[]
  ): Promise<(VFSStatRaw | null)[]>;
  vfsReadFile(
    scope: VFSScope,
    path: string,
    opts?: { versionId?: string }
  ): Promise<Uint8Array>;
  vfsWriteFile(
    scope: VFSScope,
    path: string,
    data: Uint8Array,
    opts?: WriteFileOpts
  ): Promise<void>;
  vfsUnlink(scope: VFSScope, path: string): Promise<void>;
  vfsMkdir(
    scope: VFSScope,
    path: string,
    opts?: { recursive?: boolean; mode?: number }
  ): Promise<void>;
  vfsRmdir(scope: VFSScope, path: string): Promise<void>;
  vfsRemoveRecursive(
    scope: VFSScope,
    path: string,
    cursor?: string
  ): Promise<{ done: boolean; cursor?: string }>;
  vfsSymlink(
    scope: VFSScope,
    target: string,
    path: string
  ): Promise<void>;
  vfsChmod(scope: VFSScope, path: string, mode: number): Promise<void>;
  /** Phase 12: deep-merge metadata + add/remove tags atomically. */
  vfsPatchMetadata(
    scope: VFSScope,
    path: string,
    patch: Record<string, unknown> | null,
    opts?: PatchMetadataOpts
  ): Promise<void>;
  /** Phase 12: same-tenant copyFile. */
  vfsCopyFile(
    scope: VFSScope,
    src: string,
    dest: string,
    opts?: CopyFileOpts
  ): Promise<void>;
  /** Phase 12: indexed listFiles + paginated cursor. */
  vfsListFiles(
    scope: VFSScope,
    opts?: ListFilesOpts
  ): Promise<{
    items: Array<{
      path: string;
      pathId: string;
      stat?: VFSStatRaw;
      metadata?: Record<string, unknown> | null;
      tags: string[];
    }>;
    cursor?: string;
  }>;
  /** Phase 10: flip the per-file Yjs-mode bit. */
  vfsSetYjsMode(
    scope: VFSScope,
    path: string,
    enabled: boolean
  ): Promise<void>;
  // NOTE: WebSocket upgrade for live Yjs editing does NOT use typed
  // RPC. Cloudflare DO RPC cannot serialize a Response with a
  // `webSocket` field across the RPC boundary. The SDK calls
  // `stub.fetch(/yjs/ws?...)` directly with `Upgrade: websocket`.
  // See `VFS._openYjsSocketResponse` and `UserDO._fetchWebSocketUpgrade`.
  vfsRename(
    scope: VFSScope,
    src: string,
    dst: string
  ): Promise<void>;
  vfsOpenManifest(
    scope: VFSScope,
    path: string
  ): Promise<OpenManifestResult>;
  vfsReadChunk(
    scope: VFSScope,
    path: string,
    chunkIndex: number
  ): Promise<Uint8Array>;
  vfsCreateReadStream(
    scope: VFSScope,
    path: string,
    range?: { start?: number; end?: number }
  ): Promise<ReadableStream<Uint8Array>>;
  vfsCreateWriteStream(
    scope: VFSScope,
    path: string,
    opts?: { mode?: number; mimeType?: string }
  ): Promise<{ stream: WritableStream<Uint8Array>; handle: WriteHandle }>;
  vfsOpenReadStream(
    scope: VFSScope,
    path: string
  ): Promise<ReadHandle>;
  vfsPullReadStream(
    scope: VFSScope,
    handle: ReadHandle,
    chunkIndex: number,
    range?: { start?: number; end?: number }
  ): Promise<Uint8Array>;

  // Phase 9: versioning RPCs. The wire shape uses `versionId`
  // (server-side VersionRow) — the VFS class maps to the public
  // `VersionInfo` shape with `id` for ergonomic Node-style.
  vfsListVersions(
    scope: VFSScope,
    path: string,
    opts?: ListVersionsOpts
  ): Promise<
    Array<{
      versionId: string;
      mtimeMs: number;
      size: number;
      mode: number;
      deleted: boolean;
      label?: string | null;
      userVisible?: boolean;
      metadata?: Record<string, unknown> | null;
    }>
  >;
  vfsRestoreVersion(
    scope: VFSScope,
    path: string,
    sourceVersionId: string
  ): Promise<{ versionId: string }>;
  vfsDropVersions(
    scope: VFSScope,
    path: string,
    policy: DropVersionsPolicy
  ): Promise<{ dropped: number; kept: number }>;
  /** Phase 12: set per-version label / mark user-visible. */
  vfsMarkVersion(
    scope: VFSScope,
    path: string,
    versionId: string,
    opts: VersionMarkOpts
  ): Promise<void>;
  /** Phase 12: explicit flush of a yjs-mode file → user-visible version. */
  vfsFlushYjs(
    scope: VFSScope,
    path: string,
    opts?: { label?: string }
  ): Promise<{ versionId: string | null; checkpointSeq: number }>;
  adminSetVersioning(
    userId: string,
    enabled: boolean
  ): Promise<{ enabled: boolean }>;
  adminGetVersioning(userId: string): Promise<{ enabled: boolean }>;
}

/**
 * Consumer-side env shape. The consumer's Worker `Env` interface must
 * include `MOSSAIC_USER: DurableObjectNamespace<UserDO>` (and ideally
 * `MOSSAIC_SHARD` as well — though the SDK only directly addresses
 * `MOSSAIC_USER`, the worker-side code dispatches to `SHARD_DO` and
 * the wrangler binding name is fixed at the consumer side).
 *
 * Naming: by convention the binding is `MOSSAIC_USER`, but consumers
 * can choose anything and pass the correct namespace into createVFS.
 *
 * The namespace is typed as `unknown`-but-callable on its `get()`
 * method because the workers-types `DurableObjectNamespace<T>`
 * requires `T extends Rpc.DurableObjectBranded`, which clashes with
 * the structural `UserDOClient` we use for typing. At runtime the
 * stub does have all the typed RPC methods; we cast through the
 * UserDOClient surface in `user()`.
 */
export interface MossaicEnv {
  MOSSAIC_USER: {
    idFromName(name: string): DurableObjectId;
    get(id: DurableObjectId): unknown;
  };
}

export interface CreateVFSOptions {
  /** Logical operator-side namespace. Defaults to "default". */
  namespace?: string;
  /** Required: tenant identifier. */
  tenant: string;
  /** Optional sub-tenant id. */
  sub?: string;
  /**
   * Phase 9: opt-in S3-style versioning. When `'enabled'`, every
   * writeFile creates a new historical version (chunks dedupe via
   * content-addressing); unlink writes a tombstone version
   * (chunks NOT decremented). The default `'disabled'` is
   * byte-equivalent to Phase 8 — no version rows touched, no head
   * pointer used.
   *
   * The flag is also stored server-side per tenant; the SDK option
   * here serves two purposes:
   *   1. Auto-enable on first use (the SDK calls
   *      adminSetVersioning(userId, true) lazily before the first
   *      write when the option is 'enabled').
   *   2. Surface listVersions / readFile-by-version /
   *      restoreVersion / dropVersions on the VFS instance only
   *      when the consumer signaled intent.
   *
   * Operators can also flip the server-side flag manually via
   * stub.adminSetVersioning(userId, true).
   */
  versioning?: "enabled" | "disabled";
}

/**
 * Phase 9: shape of a row returned by `vfs.listVersions(path)`.
 * Newest-first iteration order.
 */
export interface VersionInfo {
  /** Server-issued version_id (ULID-like). */
  id: string;
  /** Modification time (ms since epoch) — also the sort key. */
  mtimeMs: number;
  /** Bytes in this version (0 for tombstones). */
  size: number;
  /** POSIX mode bits (0 for tombstones). */
  mode: number;
  /** True iff this version is a tombstone (an unlink mark). */
  deleted: boolean;
  /** Phase 12: optional human-readable label. */
  label?: string | null;
  /**
   * Phase 12: true if this version was created by an explicit
   * user-facing operation (writeFile, restoreVersion, flush()).
   * False for opportunistic Yjs compactions and pre-Phase-12 rows.
   */
  userVisible?: boolean;
  /** Phase 12: snapshot of files.metadata at this version (when requested). */
  metadata?: Record<string, unknown> | null;
}

/**
 * Phase 12: per-version flags accepted by `writeFile`, `copyFile`,
 * and `markVersion`.
 */
export interface VersionMarkOpts {
  /** Optional ≤128-char human label. Replaces any prior label. */
  label?: string;
  /**
   * Mark the version user-visible. Default `true` for explicit
   * writeFile / copyFile calls. `false` is REJECTED EINVAL on
   * `markVersion` — the bit is monotonic (once visible, always
   * visible) by design.
   */
  userVisible?: boolean;
}

/**
 * Phase 12: extended writeFile options. Defaults preserve Phase 11
 * behavior bit-identically.
 */
export interface WriteFileOpts {
  mode?: number;
  mimeType?: string;
  /**
   * Plain JSON-shaped object. `undefined` keeps existing metadata;
   * `null` clears; an object SETs (validated against caps in
   * `@shared/metadata-caps.ts`).
   */
  metadata?: Record<string, unknown> | null;
  /**
   * Tag set. `undefined` keeps; `[]` drops all; `[...]` REPLACES.
   * Each tag must match `[A-Za-z0-9._:/-]{1,128}` and the array
   * must contain at most 32 unique tags.
   */
  tags?: readonly string[];
  /**
   * Per-version flags (only meaningful when versioning is enabled
   * for the tenant). On non-versioning tenants the flags are
   * silently no-ops.
   */
  version?: VersionMarkOpts;
}

/**
 * Phase 12: copyFile options.
 */
export interface CopyFileOpts {
  /** Overrides src metadata for the dest. `undefined` inherits src. */
  metadata?: Record<string, unknown> | null;
  /** Overrides src tags for the dest. `undefined` inherits src. */
  tags?: readonly string[];
  version?: VersionMarkOpts;
  /** When dest exists: false → EEXIST, true (default) → supersede. */
  overwrite?: boolean;
}

/**
 * Phase 12: patchMetadata options.
 */
export interface PatchMetadataOpts {
  addTags?: readonly string[];
  removeTags?: readonly string[];
}

/**
 * Phase 12: listFiles options.
 */
export interface ListFilesOpts {
  /** Path prefix (e.g. "/photos/2026/"). Resolves to a folder. */
  prefix?: string;
  /** AND semantics. Up to 8 tags per query. */
  tags?: readonly string[];
  /**
   * Exact-match metadata filter. Post-filtered (not indexed) — pair
   * with prefix or tags for index-driven performance.
   */
  metadata?: Record<string, unknown>;
  /** 1..1000, default 50. */
  limit?: number;
  /** Opaque cursor returned from a prior call. */
  cursor?: string;
  /** Default 'mtime'. */
  orderBy?: "mtime" | "name" | "size";
  /** Default 'desc' for mtime/size, 'asc' for name. */
  direction?: "asc" | "desc";
  /** Default true. */
  includeStat?: boolean;
  /** Default false (size pressure). */
  includeMetadata?: boolean;
}

/** Phase 12: a single row returned by listFiles. */
export interface ListFilesItem {
  /** Absolute path with leading slash. */
  path: string;
  /** Stable file_id for this path. */
  pathId: string;
  /** Present when `includeStat !== false`. */
  stat?: VFSStat;
  /** Present when `includeMetadata === true` AND the file has metadata. */
  metadata?: Record<string, unknown> | null;
  /** Always present — the file's tag set, sorted alphabetically. */
  tags: string[];
}

export interface ListFilesPage {
  items: ListFilesItem[];
  /** Present iff there's another page. */
  cursor?: string;
}

/** Phase 12: listVersions options. */
export interface ListVersionsOpts {
  limit?: number;
  /** When true, filter to versions with user_visible = 1. */
  userVisibleOnly?: boolean;
  /** When true, return `metadata` snapshots on each VersionInfo. */
  includeMetadata?: boolean;
}

/**
 * Phase 9: retention-policy parameters for `vfs.dropVersions(path, policy)`.
 *
 * The CURRENT head version is ALWAYS preserved, regardless of filters
 * (S3 invariant). Surviving versions = (head) ∪ (exceptVersions) ∪
 * (newest `keepLast`) ∪ (versions not older than `olderThan`).
 *
 * Pass an empty policy `{}` to drop everything except the head.
 */
export interface DropVersionsPolicy {
  /** ms-since-epoch cutoff: keep versions with mtimeMs ≥ olderThan. */
  olderThan?: number;
  /** Keep the N newest versions (in addition to the head). */
  keepLast?: number;
  /** Explicit allowlist of version_ids to preserve. */
  exceptVersions?: string[];
}

import { vfsUserDOName } from "../../worker/core/lib/utils";

/**
 * fs/promises-shaped client.
 *
 * Every method maps to one DO RPC. Errors are normalized to
 * `VFSFsError` subclasses with Node-fs-like `code` / `errno` /
 * `syscall` / `path`.
 */
export class VFS implements VFSClient {
  /**
   * Self-reference so `vfs.promises === vfs` (isomorphic-git reads
   * `.promises`). Typed as `VFS` (a subtype of `VFSClient`) so the
   * binding client and the HTTP fallback (HttpVFS) both satisfy the
   * shared `VFSClient` interface; `implements VFSClient` ensures
   * any future surface change is caught at compile time.
   */
  readonly promises: VFS;

  /**
   * Phase 9: auto-enable-versioning latch. The first write or
   * versioning-related call on a VFS instance with
   * `versioning: 'enabled'` triggers a one-shot
   * `adminSetVersioning(userId, true)` server call. Subsequent
   * calls skip the round-trip thanks to this flag. The latch is
   * idempotent — flipping it on a tenant that's already enabled
   * is a no-op server-side.
   */
  private versioningLatched = false;

  constructor(
    private readonly env: MossaicEnv,
    private readonly opts: CreateVFSOptions
  ) {
    if (
      !opts ||
      typeof opts.tenant !== "string" ||
      opts.tenant.length === 0
    ) {
      throw new EINVAL({
        syscall: "createVFS",
        path: "(opts.tenant)",
      });
    }
    this.promises = this;
  }

  /**
   * If the consumer constructed with `versioning: 'enabled'`, ensure
   * the server-side flag is on before issuing the actual operation.
   * Skipped after the first successful latch.
   */
  private async ensureVersioning(): Promise<void> {
    if (this.versioningLatched) return;
    if (this.opts.versioning !== "enabled") return;
    const userId = this.opts.sub
      ? `${this.opts.tenant}::${this.opts.sub}`
      : this.opts.tenant;
    try {
      await this.user().adminSetVersioning(userId, true);
      this.versioningLatched = true;
    } catch (err) {
      throw mapServerError(err, { syscall: "adminSetVersioning" });
    }
  }

  // ── DO stub resolution ────────────────────────────────────────────────

  private user(): UserDOClient {
    const name = vfsUserDOName(
      this.opts.namespace ?? "default",
      this.opts.tenant,
      this.opts.sub
    );
    const id = this.env.MOSSAIC_USER.idFromName(name);
    // The runtime stub has all the typed RPC methods; the
    // workers-types DO namespace generic doesn't structurally
    // overlap with our UserDOClient interface, so we cast.
    return this.env.MOSSAIC_USER.get(id) as UserDOClient;
  }

  private scope(): VFSScope {
    return {
      ns: this.opts.namespace ?? "default",
      tenant: this.opts.tenant,
      sub: this.opts.sub,
    };
  }

  // ── Reads ─────────────────────────────────────────────────────────────

  async readFile(p: string): Promise<Uint8Array>;
  async readFile(p: string, opts: { encoding: "utf8" }): Promise<string>;
  async readFile(p: string, opts: { version: string }): Promise<Uint8Array>;
  async readFile(
    p: string,
    opts: { version: string; encoding: "utf8" }
  ): Promise<string>;
  async readFile(
    p: string,
    opts?: { encoding?: "utf8"; version?: string }
  ): Promise<Uint8Array | string> {
    let buf: Uint8Array;
    try {
      buf = await this.user().vfsReadFile(
        this.scope(),
        p,
        opts?.version ? { versionId: opts.version } : undefined
      );
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "open" });
    }
    return opts?.encoding === "utf8"
      ? new TextDecoder().decode(buf)
      : buf;
  }

  async readdir(p: string): Promise<string[]> {
    try {
      return await this.user().vfsReaddir(this.scope(), p);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "scandir" });
    }
  }

  async stat(p: string): Promise<VFSStat> {
    try {
      return new VFSStat(await this.user().vfsStat(this.scope(), p));
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "stat" });
    }
  }

  async lstat(p: string): Promise<VFSStat> {
    try {
      return new VFSStat(await this.user().vfsLstat(this.scope(), p));
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "lstat" });
    }
  }

  async exists(p: string): Promise<boolean> {
    try {
      return await this.user().vfsExists(this.scope(), p);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "access" });
    }
  }

  async readlink(p: string): Promise<string> {
    try {
      return await this.user().vfsReadlink(this.scope(), p);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "readlink" });
    }
  }

  async readManyStat(paths: string[]): Promise<(VFSStat | null)[]> {
    let raws: (VFSStatRaw | null)[];
    try {
      raws = await this.user().vfsReadManyStat(this.scope(), paths);
    } catch (err) {
      throw mapServerError(err, { syscall: "lstat" });
    }
    return raws.map((r) => (r ? new VFSStat(r) : null));
  }

  // ── Writes ────────────────────────────────────────────────────────────

  async writeFile(
    p: string,
    data: Uint8Array | string,
    opts?: WriteFileOpts
  ): Promise<void> {
    await this.ensureVersioning();
    const bytes =
      typeof data === "string" ? new TextEncoder().encode(data) : data;
    try {
      await this.user().vfsWriteFile(this.scope(), p, bytes, opts);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "open" });
    }
  }

  async unlink(p: string): Promise<void> {
    await this.ensureVersioning();
    try {
      await this.user().vfsUnlink(this.scope(), p);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "unlink" });
    }
  }

  async mkdir(
    p: string,
    opts?: { recursive?: boolean; mode?: number }
  ): Promise<void> {
    try {
      await this.user().vfsMkdir(this.scope(), p, opts);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "mkdir" });
    }
  }

  async rmdir(p: string): Promise<void> {
    try {
      await this.user().vfsRmdir(this.scope(), p);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "rmdir" });
    }
  }

  /**
   * removeRecursive — paginated rm -rf. Loops the cursor-returning
   * RPC until done, so a single call will handle subtrees of any
   * size. Each iteration is one DO RPC.
   */
  async removeRecursive(p: string): Promise<void> {
    let cursor: string | undefined;
    for (;;) {
      let r: { done: boolean; cursor?: string };
      try {
        r = await this.user().vfsRemoveRecursive(this.scope(), p, cursor);
      } catch (err) {
        throw mapServerError(err, { path: p, syscall: "rmdir" });
      }
      if (r.done) return;
      cursor = r.cursor;
    }
  }

  async symlink(target: string, p: string): Promise<void> {
    try {
      await this.user().vfsSymlink(this.scope(), target, p);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "symlink" });
    }
  }

  async chmod(p: string, mode: number): Promise<void>;
  /**
   * Phase 10 overload: pass `{ yjs: true }` to flip the per-file
   * Yjs-mode bit. Demoting back to plain (`{ yjs: false }`) is
   * rejected EINVAL on the server — it would lose CRDT history.
   *
   * NOTE: this is NOT a posix mode change — the bit is stored in
   * a separate column (`mode_yjs`) and surfaces on `stat.mode` as
   * `VFS_MODE_YJS_BIT` (0o4000). If you want to change BOTH posix
   * mode and the yjs bit, call `chmod(p, mode)` and
   * `chmod(p, { yjs: true })` separately.
   */
  async chmod(p: string, opts: { yjs: boolean }): Promise<void>;
  async chmod(p: string, modeOrOpts: number | { yjs: boolean }): Promise<void> {
    try {
      if (typeof modeOrOpts === "number") {
        await this.user().vfsChmod(this.scope(), p, modeOrOpts);
      } else {
        await this.user().vfsSetYjsMode(this.scope(), p, modeOrOpts.yjs);
      }
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "chmod" });
    }
  }

  /**
   * Phase 10 alias: explicit, type-stable form of
   * `chmod(p, { yjs: enabled })`. Prefer this over the chmod
   * overload when you don't need the dual-shape ergonomics —
   * isomorphic-git and other fs/promises consumers will find
   * the numeric-only `chmod` familiar; the alias is for code that
   * cares about Yjs explicitly.
   */
  async setYjsMode(p: string, enabled: boolean): Promise<void> {
    try {
      await this.user().vfsSetYjsMode(this.scope(), p, enabled);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "chmod" });
    }
  }

  /**
   * Phase 12: deep-merge a metadata patch onto a file, optionally
   * adding/removing tags atomically.
   *
   * - `patch === null`: clear the metadata blob (UPDATE files SET
   *   metadata = NULL). Tag opts may still be applied.
   * - `patch === {...}`: deep-merge with existing metadata. A `null`
   *   leaf in the patch DELETES that key from the merged result
   *   (tombstone semantics — the only way to remove a key without
   *   replacing the entire blob). Arrays are REPLACED, not merged.
   * - `opts.addTags`: idempotent INSERT (existing tags skipped).
   * - `opts.removeTags`: drops only the listed tags.
   *
   * Atomic in the worker. Throws `EINVAL` on cap violations
   * (post-merge metadata size, tag charset/length, depth, etc.).
   */
  async patchMetadata(
    p: string,
    patch: Record<string, unknown> | null,
    opts?: PatchMetadataOpts
  ): Promise<void> {
    try {
      await this.user().vfsPatchMetadata(this.scope(), p, patch, opts);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "open" });
    }
  }

  /**
   * Phase 12: same-tenant copyFile.
   *
   * Three-tier behavior:
   *   - Inline files: bytes-only copy (no shard work).
   *   - Chunked / versioned files: manifest copy + chunk refcount
   *     bumps. ZERO chunk bytes traverse the wire — content-addressing
   *     means the existing chunks already live on the right shards.
   *   - Yjs-mode src: bytes-snapshot fork — dest is a plain file
   *     materialized from src's current Y.Doc state. Future src edits
   *     do NOT propagate to dest. Documented behavior.
   *
   * Cross-tenant copies are rejected EACCES at the SDK layer
   * (the binding scope already pins the tenant).
   *
   * Throws:
   *   - ENOENT — src doesn't exist.
   *   - EISDIR — src is a directory.
   *   - EEXIST — dest exists and `opts.overwrite === false`.
   *   - EINVAL — src === dest, or cap violations on metadata/tags.
   */
  async copyFile(
    src: string,
    dest: string,
    opts?: CopyFileOpts
  ): Promise<void> {
    await this.ensureVersioning();
    try {
      await this.user().vfsCopyFile(this.scope(), src, dest, opts);
    } catch (err) {
      throw mapServerError(err, { path: src, syscall: "open" });
    }
  }

  /**
   * Phase 12: indexed listFiles with HMAC-signed cursor pagination.
   *
   * Filters: `prefix` (path), `tags` (AND, ≤8 per query),
   * `metadata` (post-filter; pair with prefix or tags for index-driven
   * latency).
   *
   * Pagination: `cursor` is opaque — pass the previous page's cursor
   * to fetch the next slice. Cursors are HMAC-signed; tampering or
   * orderBy/direction mismatch surfaces as `EINVAL`.
   *
   * Performance gates (DO SQLite, 100k files):
   *   - prefix-only: ≤20ms p99
   *   - tag-only:    ≤10ms p99
   *   - default:     ≤50ms p99
   */
  async listFiles(opts: ListFilesOpts = {}): Promise<ListFilesPage> {
    let raw: {
      items: Array<{
        path: string;
        pathId: string;
        stat?: VFSStatRaw;
        metadata?: Record<string, unknown> | null;
        tags: string[];
      }>;
      cursor?: string;
    };
    try {
      raw = await this.user().vfsListFiles(this.scope(), opts);
    } catch (err) {
      throw mapServerError(err, {
        path: opts.prefix ?? "/",
        syscall: "scandir",
      });
    }
    const items: ListFilesItem[] = raw.items.map((r) => ({
      path: r.path,
      pathId: r.pathId,
      stat: r.stat ? new VFSStat(r.stat) : undefined,
      metadata: r.metadata,
      tags: r.tags,
    }));
    return { items, cursor: raw.cursor };
  }

  /**
   * Phase 10: open a WebSocket against a yjs-mode file. Internal —
   * the consumer-facing API is `openYDoc` from the
   * `@mossaic/sdk/yjs` subpath, which wraps this in a Yjs-aware
   * client object.
   *
   * Why fetch() instead of typed RPC: Cloudflare DO RPC currently
   * cannot serialize a `Response` carrying a `webSocket` field
   * across the RPC boundary — only `stub.fetch(req)` is permitted
   * to return such a Response. We encode the path/scope as URL
   * query params and use a synthetic internal URL.
   */
  async _openYjsSocketResponse(p: string): Promise<Response> {
    const scope = this.scope();
    const url = new URL("http://internal/yjs/ws");
    url.searchParams.set("path", p);
    url.searchParams.set("ns", scope.ns ?? "default");
    url.searchParams.set("tenant", scope.tenant);
    if (scope.sub !== undefined) url.searchParams.set("sub", scope.sub);
    const name = vfsUserDOName(
      this.opts.namespace ?? "default",
      this.opts.tenant,
      this.opts.sub
    );
    const id = this.env.MOSSAIC_USER.idFromName(name);
    const stub = this.env.MOSSAIC_USER.get(id) as {
      fetch(req: Request): Promise<Response>;
    };
    let response: Response;
    try {
      response = await stub.fetch(
        new Request(url, {
          headers: { Upgrade: "websocket" },
        })
      );
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "open" });
    }
    if (response.status !== 101) {
      // Surface server-side error JSON as a structured error.
      let msg = `openYjsSocket: server returned ${response.status}`;
      try {
        const j = (await response.json()) as { error?: string; code?: string };
        if (j.error) msg = j.error;
      } catch {
        /* ignore parse failure */
      }
      throw mapServerError(
        Object.assign(new Error(msg), { code: "EINVAL" }),
        { path: p, syscall: "open" }
      );
    }
    return response;
  }

  async rename(src: string, dst: string): Promise<void> {
    try {
      await this.user().vfsRename(this.scope(), src, dst);
    } catch (err) {
      throw mapServerError(err, { path: dst, syscall: "rename" });
    }
  }

  // ── Streams ───────────────────────────────────────────────────────────

  async createReadStream(
    p: string,
    opts?: ReadStreamOptions
  ): Promise<ReadableStream<Uint8Array>> {
    return createReadStreamRpc(this.user(), this.scope(), p, opts);
  }

  async createWriteStream(
    p: string,
    opts?: { mode?: number; mimeType?: string }
  ): Promise<WritableStream<Uint8Array>> {
    return createWriteStreamRpc(this.user(), this.scope(), p, opts);
  }

  /** Variant that surfaces the underlying write handle for resumable use cases. */
  async createWriteStreamWithHandle(
    p: string,
    opts?: { mode?: number; mimeType?: string }
  ): Promise<{ stream: WritableStream<Uint8Array>; handle: WriteHandle }> {
    return createWriteStreamWithHandleRpc(this.user(), this.scope(), p, opts);
  }

  // ── Low-level escape hatch (caller-orchestrated multi-invocation reads) ──

  async openManifest(p: string): Promise<OpenManifestResult> {
    try {
      return await this.user().vfsOpenManifest(this.scope(), p);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "open" });
    }
  }

  async readChunk(p: string, chunkIndex: number): Promise<Uint8Array> {
    try {
      return await this.user().vfsReadChunk(this.scope(), p, chunkIndex);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "read" });
    }
  }

  async openReadStream(p: string): Promise<ReadHandle> {
    try {
      return await this.user().vfsOpenReadStream(this.scope(), p);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "open" });
    }
  }

  async pullReadStream(
    handle: ReadHandle,
    chunkIndex: number,
    range?: { start?: number; end?: number }
  ): Promise<Uint8Array> {
    try {
      return await this.user().vfsPullReadStream(
        this.scope(),
        handle,
        chunkIndex,
        range
      );
    } catch (err) {
      throw mapServerError(err, { syscall: "read" });
    }
  }

  // ── Phase 9: file-level versioning ────────────────────────────────────

  /**
   * List historical versions of a path, newest-first. Includes
   * tombstones (deleted=true). Backed by the
   * idx_file_versions_path_mtime index — sub-millisecond at 10k
   * versions per path.
   */
  async listVersions(
    p: string,
    opts?: ListVersionsOpts
  ): Promise<VersionInfo[]> {
    try {
      const rows = await this.user().vfsListVersions(this.scope(), p, opts);
      return rows.map((r) => ({
        id: r.versionId,
        mtimeMs: r.mtimeMs,
        size: r.size,
        mode: r.mode,
        deleted: r.deleted,
        label: r.label,
        userVisible: r.userVisible,
        metadata: opts?.includeMetadata ? r.metadata ?? null : undefined,
      }));
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "listVersions" });
    }
  }

  /**
   * Phase 12: set per-version metadata flags. `userVisible` is
   * monotonic — the worker rejects `false` with EINVAL.
   */
  async markVersion(
    p: string,
    versionId: string,
    opts: VersionMarkOpts
  ): Promise<void> {
    try {
      await this.user().vfsMarkVersion(this.scope(), p, versionId, opts);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "open" });
    }
  }

  /**
   * Phase 12: explicit flush of a yjs-mode file. Triggers a Yjs
   * compaction whose checkpoint emits a USER-VISIBLE Mossaic
   * version row (when versioning is enabled for the tenant).
   * Internal — called by `YDocHandle.flush` from the
   * `@mossaic/sdk/yjs` subpath.
   */
  async _flushYjs(
    p: string,
    opts?: { label?: string }
  ): Promise<{ versionId: string | null; checkpointSeq: number }> {
    try {
      return await this.user().vfsFlushYjs(this.scope(), p, opts);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "open" });
    }
  }

  /**
   * Restore a historical version: creates a NEW version row whose
   * content is a copy of the source. Source must not be a
   * tombstone. Returns the new version's id.
   */
  async restoreVersion(
    p: string,
    sourceVersionId: string
  ): Promise<{ id: string }> {
    await this.ensureVersioning();
    try {
      const r = await this.user().vfsRestoreVersion(
        this.scope(),
        p,
        sourceVersionId
      );
      return { id: r.versionId };
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "restoreVersion" });
    }
  }

  /**
   * Drop versions per a retention policy. Head version is always
   * preserved. Chunks whose last reference was dropped are reaped
   * by the Phase 3 alarm sweeper after its 30s grace.
   */
  async dropVersions(
    p: string,
    policy: DropVersionsPolicy
  ): Promise<{ dropped: number; kept: number }> {
    try {
      return await this.user().vfsDropVersions(this.scope(), p, policy);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "dropVersions" });
    }
  }
}
