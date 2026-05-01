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
import type {
  PreviewInfo,
  PreviewInfoBatchEntry,
  PreviewUrlOpts,
  ReadPreviewOpts,
  ReadPreviewResult,
} from "../../shared/preview-types";

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
  fileInfo(p: string, opts?: FileInfoOpts): Promise<ListFilesItem>;

  // Writes
  writeFile(
    p: string,
    data: Uint8Array | string,
    opts?: WriteFileOpts
  ): Promise<void>;
  unlink(p: string): Promise<void>;
  /**
   * Destructive cleanup.
   *
   * Drops the file row + every version + decrements ShardDO chunk
   * refs. Independent of versioning state — acts like a
   * versioning-off `unlink` even when versioning is on. Idempotent.
   *
   * Three-tier delete model:
   *  - `unlink(path)`  — POSIX-style (versioned tombstone if
   *    versioning is on; hard delete if off).
   *  - `purge(path)`   — wipe all history for one path.
   *  - `archive(path)` — cosmetic-only hide. Reversible via
   *    `unarchive`. Read surfaces are unchanged.
   */
  purge(p: string): Promise<void>;
  /**
   * Hide a path from the default `listFiles` / `fileInfo` results
   * without touching data. Reversible via `unarchive`. Read
   * surfaces (`stat`, `readFile`, etc.) are NOT gated — an archived
   * file is fully readable by anyone who knows the path.
   * Idempotent. Throws `EISDIR` for directories, `ENOENT` for
   * non-existent paths.
   */
  archive(p: string): Promise<void>;
  /** Inverse of `archive`. Idempotent. */
  unarchive(p: string): Promise<void>;
  mkdir(
    p: string,
    opts?: { recursive?: boolean; mode?: number }
  ): Promise<void>;
  rmdir(p: string): Promise<void>;
  removeRecursive(p: string): Promise<void>;
  symlink(target: string, p: string): Promise<void>;
  chmod(p: string, mode: number): Promise<void>;
  rename(src: string, dst: string): Promise<void>;
  /** deep-merge metadata + add/remove tags atomically. */
  patchMetadata(
    p: string,
    patch: Record<string, unknown> | null,
    opts?: PatchMetadataOpts
  ): Promise<void>;
  /** same-tenant copyFile (chunk-refcount-aware). */
  copyFile(src: string, dest: string, opts?: CopyFileOpts): Promise<void>;
  /** indexed listFiles with HMAC-signed cursor pagination. */
  listFiles(opts?: ListFilesOpts): Promise<ListFilesPage>;
  /**
   * Phase 46 — batched directory listing. Returns folder revision
   * counter + a single page of merged folder/file/symlink entries
   * with stat / metadata / tags / contentHash hydrated in one
   * round-trip. Replaces the pre-Phase-46 `readdir + lstat × N`
   * loop with O(1) RPCs per page.
   *
   * `revision` is a monotonic per-folder counter; cache it client-
   * side and re-fetch only when it changes (or use it as an ETag).
   */
  listChildren(p: string, opts?: ListChildrenOpts): Promise<ListChildrenPage>;

  // Streams (HTTP fallback throws EINVAL for these in v1)
  createReadStream(
    p: string,
    opts?: ReadStreamOptions
  ): Promise<ReadableStream<Uint8Array>>;
  createWriteStream(
    p: string,
    opts?: WriteFileOpts
  ): Promise<WritableStream<Uint8Array>>;
  createWriteStreamWithHandle(
    p: string,
    opts?: WriteFileOpts
  ): Promise<{
    stream: WritableStream<Uint8Array>;
    handle: WriteHandle;
  }>;

  // Low-level escape hatch
  openManifest(p: string): Promise<OpenManifestResult>;
  /**
   * Batched manifest fetch. Returns one result per input path; misses
   * surface as `{ ok: false, code, message }` rather than throwing,
   * so a single bad path doesn't tank a gallery render. Max 256
   * paths per call (server-enforced).
   */
  openManifests(
    paths: string[]
  ): Promise<
    (
      | { ok: true; manifest: OpenManifestResult }
      | { ok: false; code: string; message: string }
    )[]
  >;
  readChunk(p: string, chunkIndex: number): Promise<Uint8Array>;
  openReadStream(p: string): Promise<ReadHandle>;
  pullReadStream(
    handle: ReadHandle,
    chunkIndex: number,
    range?: { start?: number; end?: number }
  ): Promise<Uint8Array>;

  /**
   * Universal preview pipeline. Returns rendered preview bytes
   * (image/* or image/svg+xml depending on the renderer dispatched
   * for the file's MIME). Variant rows are cached server-side and
   * content-addressed; identical inputs across users dedupe.
   *
   * Encrypted files throw `ENOTSUP` — server cannot render
   * ciphertext. Custom variants (`{width, height?, fit?}`) cache
   * under a stable encoded key.
   */
  readPreview(
    p: string,
    opts?: ReadPreviewOpts
  ): Promise<ReadPreviewResult>;

  /**
   * Mint a signed preview-variant URL.
   *
   * Returns a string suitable for embedding in an `<img src=...>`
   * tag (or any other browser-direct fetch). The browser fetches
   * the bytes WITHOUT going through this Worker's RPC surface;
   * the URL hits Workers Cache + a CDN edge tier on subsequent
   * loads.
   *
   * For multi-image grids prefer `previewInfoMany` (one RPC
   * batch) so each grid item doesn't pay a per-mint network
   * round trip.
   *
   * Encrypted files throw `ENOTSUP`. Tombstoned heads throw
   * `ENOENT`.
   */
  previewUrl(p: string, opts?: PreviewUrlOpts): Promise<string>;

  /**
   * Mint a signed preview-variant URL + return all the metadata
   * an SPA needs to render the IMG element
   * (mimeType, width, height) and revalidate via ETag.
   *
   * Same auth + lifecycle as `previewUrl`; the additional metadata
   * costs nothing (already computed during the mint).
   */
  previewInfo(p: string, opts?: PreviewUrlOpts): Promise<PreviewInfo>;

  /**
   * Batched preview-info mint. One RPC per N paths
   * (max 256 per call). Per-path failures land as
   * `{ ok: false, code, message }` entries; callers can render a
   * placeholder for those without aborting the whole batch.
   *
   * Designed for thumbnail grids: a 50-photo gallery becomes one
   * RPC + 50 direct browser fetches against the CDN-cached
   * route, instead of 50 RPCs that each tunnel bytes through the
   * Worker.
   */
  previewInfoMany(
    paths: readonly string[],
    opts?: PreviewUrlOpts
  ): Promise<PreviewInfoBatchEntry[]>;

  // file-level versioning (only meaningful when the tenant
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
  /** set per-version label and/or user-visible flag. */
  markVersion(
    p: string,
    versionId: string,
    opts: VersionMarkOpts
  ): Promise<void>;

  /**
   * Yjs snapshot read.
   *
   * Returns the full `Y.encodeStateAsUpdate(doc)` bytes for a
   * yjs-mode file. Decode with `Y.applyUpdate(localDoc, bytes)`
   * to recover the entire `Y.Doc` — including every named shared
   * type (`Y.XmlFragment`, `Y.Map`, `Y.Array`, `Y.Text`, …) the
   * server's live doc currently holds.
   *
   * Use cases: Tiptap/ProseMirror editors (default
   * `Y.XmlFragment("default")`), Notion-style block editors
   * (Y.Map of Y.XmlFragments + Y.Array for block ordering),
   * any consumer seeding a fresh `Y.Doc` without first opening
   * a WebSocket.
   *
   * Pairs with `commitYjsSnapshot(path, doc)` for round-trip.
   *
   * Throws `EINVAL` for non-yjs paths, `EACCES` for encrypted
   * yjs files (server cannot materialise — use `openYDoc`).
   */
  readYjsSnapshot(p: string): Promise<Uint8Array>;

  /**
   * Yjs snapshot commit.
   *
   * Encodes the supplied `Y.Doc` via
   * `Y.encodeStateAsUpdate(doc)`, wraps with the
   * `YJS_SNAPSHOT_MAGIC` 4-byte prefix, and routes through
   * `writeFile`. The server detects the magic and applies the
   * bytes via `Y.applyUpdate` — merging with the live
   * server-side doc so concurrent editors (via `openYDoc`) see
   * the new state immediately.
   *
   * Yjs CRDT semantics guarantee that applying the same update
   * on every peer converges to the same state — safe with active
   * editors.
   *
   * Composes with versioning: when the tenant has versioning
   * enabled, this writeFile creates a `file_versions` row whose
   * content IS the snapshot bytes, so `listVersions` /
   * `restoreVersion` work as expected with snapshot history.
   */
  commitYjsSnapshot(p: string, doc: import("yjs").Doc): Promise<void>;
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
    /**
     * Wire-shape opts. Consumer-facing `WriteFileOpts` are normalized
     * by `VFS.writeFile` before reaching the RPC — `encrypted` is
     * stripped and `encryption: { mode, keyId }` is added for the
     * server. This typed surface is intentionally permissive so the
     * normalization can pass either shape through.
     */
    opts?: WriteFileOpts & {
      encryption?: { mode: "convergent" | "random"; keyId?: string };
    }
  ): Promise<void>;
  vfsUnlink(scope: VFSScope, path: string): Promise<void>;
  vfsPurge(scope: VFSScope, path: string): Promise<void>;
  /** Set archived=1 on the path's files row. */
  vfsArchive(scope: VFSScope, path: string): Promise<void>;
  /** Set archived=0 on the path's files row. */
  vfsUnarchive(scope: VFSScope, path: string): Promise<void>;
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
  /** deep-merge metadata + add/remove tags atomically. */
  vfsPatchMetadata(
    scope: VFSScope,
    path: string,
    patch: Record<string, unknown> | null,
    opts?: PatchMetadataOpts
  ): Promise<void>;
  /** same-tenant copyFile. */
  vfsCopyFile(
    scope: VFSScope,
    src: string,
    dest: string,
    opts?: CopyFileOpts
  ): Promise<void>;
  /** indexed listFiles + paginated cursor. */
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
      contentHash?: string;
    }>;
    cursor?: string;
  }>;
  vfsFileInfo(
    scope: VFSScope,
    path: string,
    opts?: FileInfoOpts
  ): Promise<{
    path: string;
    pathId: string;
    stat?: VFSStatRaw;
    metadata?: Record<string, unknown> | null;
    tags: string[];
    contentHash?: string;
  }>;
  /** Phase 46 — batched directory listing (folder revision + entries). */
  vfsListChildren(
    scope: VFSScope,
    opts: ListChildrenOpts & { path: string }
  ): Promise<{
    revision: number;
    entries: Array<
      | {
          kind: "folder";
          path: string;
          pathId: string;
          name: string;
          stat?: VFSStatRaw;
        }
      | {
          kind: "file";
          path: string;
          pathId: string;
          name: string;
          stat?: VFSStatRaw;
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
          stat?: VFSStatRaw;
        }
    >;
    cursor?: string;
  }>;
  /** flip the per-file Yjs-mode bit. */
  vfsSetYjsMode(
    scope: VFSScope,
    path: string,
    enabled: boolean
  ): Promise<void>;
  /**
   * Return `Y.encodeStateAsUpdate(doc)` bytes for a yjs-mode file
   * so SDK consumers can decode the FULL Y.Doc and use arbitrary
   * named shared types (Y.XmlFragment, Y.Map, …).
   */
  vfsReadYjsSnapshot(
    scope: VFSScope,
    path: string
  ): Promise<Uint8Array>;
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
  vfsReadPreview(
    scope: VFSScope,
    path: string,
    opts?: ReadPreviewOpts
  ): Promise<ReadPreviewResult>;
  /** Mint a signed preview-variant URL. */
  vfsMintPreviewToken(
    scope: VFSScope,
    path: string,
    opts?: PreviewUrlOpts
  ): Promise<PreviewInfo>;
  /** Batched mint (max 256 paths). */
  vfsPreviewInfoMany(
    scope: VFSScope,
    paths: string[],
    opts?: PreviewUrlOpts
  ): Promise<PreviewInfoBatchEntry[]>;
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
    opts?: WriteFileOpts
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

  // versioning RPCs. The wire shape uses `versionId`
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
  /** set per-version label / mark user-visible. */
  vfsMarkVersion(
    scope: VFSScope,
    path: string,
    versionId: string,
    opts: VersionMarkOpts
  ): Promise<void>;
  /** explicit flush of a yjs-mode file → user-visible version. */
  vfsFlushYjs(
    scope: VFSScope,
    path: string,
    opts?: { label?: string }
  ): Promise<{ versionId: string | null; checkpointSeq: number }>;
  /**
   * client-driven compaction of an encrypted yjs file. The
   * client supplies a checkpoint envelope (encrypted state-as-update)
   * + the expected `next_seq` for CAS. Server appends the checkpoint
   * atomically and drops oplog rows below it.
   */
  vfsCompactEncryptedYjs(
    scope: VFSScope,
    path: string,
    checkpointEnvelope: Uint8Array,
    expectedNextSeq: number
  ): Promise<{ checkpointSeq: number; opsReaped: number }>;
  /**
   * read raw oplog rows for a yjs-mode file. Used by the
   * client-side compactor to fetch encrypted op envelopes for
   * local replay.
   */
  vfsReadYjsOplog(
    scope: VFSScope,
    path: string,
    opts?: { afterSeq?: number; limit?: number }
  ): Promise<{
    rows: { seq: number; kind: "op" | "checkpoint"; envelope: Uint8Array }[];
    nextSeq: number;
    hasMore: boolean;
  }>;
  adminSetVersioning(
    userId: string,
    enabled: boolean
  ): Promise<{ enabled: boolean }>;
  adminGetVersioning(userId: string): Promise<{ enabled: boolean }>;
}

/**
 * Consumer-side env shape. The consumer's Worker `Env` interface must
 * include BOTH:
 *   - `MOSSAIC_USER: DurableObjectNamespace<UserDO>`  — the SDK addresses
 *     this directly.
 *   - `MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>` — internal-only;
 *     the SDK never reads `env.MOSSAIC_SHARD`, but UserDO's bundled
 *     placement code dispatches to this binding via its OWN env, so the
 *     consumer's wrangler MUST declare it for the DO class to instantiate.
 *
 * Naming: the canonical binding names are `MOSSAIC_USER` / `MOSSAIC_SHARD`
 * (prefixed for consumer-env safety). Renaming the binding
 * `name` while keeping `class_name` is data-safe per CF docs:
 * https://developers.cloudflare.com/durable-objects/reference/durable-objects-migrations/
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
  /**
   * Required at the wrangler level even though the SDK never reads it
   * directly — the bundled UserDO code calls `env.MOSSAIC_SHARD` from
   * inside its own Worker context.
   */
  MOSSAIC_SHARD: {
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
   * opt-in S3-style versioning. When `'enabled'`, every
   * writeFile creates a new historical version (chunks dedupe via
   * content-addressing); unlink writes a tombstone version
   * (chunks NOT decremented). The default `'disabled'` is
   * byte-equivalent to no version rows touched, no head
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
  /**
   * opt-in end-to-end encryption.
   *
   * When set, `writeFile(p, d, { encrypted: true })` AES-GCM-encrypts
   * `d` client-side before sending; `readFile(p)` auto-detects
   * encrypted files via `stat.encryption` and decrypts before
   * returning. The Mossaic server NEVER decrypts — it only stamps
   * `files.encryption_mode` + `files.encryption_key_id` so the SDK
   * knows what to do on read.
   *
   * Custody is the consumer's responsibility:
   *  - Browser: WebCrypto + IndexedDB non-extractable CryptoKey.
   *  - Node/Worker: KMS unwrap on cold start.
   *  - CLI: PBKDF2 (`deriveMasterFromPassword`) over a password.
   *
   * Loss of `masterKey` = permanent data loss. There is no recovery
   * path — Mossaic does not store master keys anywhere.
   *
   * See `local/phase-15-plan.md` §4.1 + the SDK README's
   * "End-to-end encryption" section for full details.
   */
  encryption?: {
    /** 32-byte raw AES-GCM-256 key material. */
    masterKey: Uint8Array;
    /** 32-byte stable per-tenant salt. */
    tenantSalt: Uint8Array;
    /**
     * Default mode for writes that don't specify per-call. Defaults
     * to `convergent` (preserves dedup; documented within-tenant
     * equality oracle as the cost). Use `random` for high-secrecy
     * tenants that don't need dedup.
     */
    mode?: "convergent" | "random";
    /** ≤128B opaque label embedded in every envelope. */
    keyId?: string;
  };
}

/**
 * shape of a row returned by `vfs.listVersions(path)`.
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
  /** optional human-readable label. */
  label?: string | null;
  /**
   * true if this version was created by an explicit
   * user-facing operation (writeFile, restoreVersion, flush()).
   * False for opportunistic Yjs compactions and legacy rows that
   * pre-date the column.
   */
  userVisible?: boolean;
  /** snapshot of files.metadata at this version (when requested). */
  metadata?: Record<string, unknown> | null;
}

/**
 * per-version flags accepted by `writeFile`, `copyFile`,
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
 * Extended writeFile options. Defaults preserve plain `writeFile`
 * behavior bit-identically — every option is opt-in.
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
  /**
   * opt-in encryption for this write.
   *
   * - `true`: use the VFS instance's `encryption` config defaults.
   *   EINVAL if `createVFS` was called without `encryption`.
   * - `false`: explicit plaintext (the default). Server rejects with
   *   EBADF if the path's history is encrypted.
   * - `{ mode?, keyId? }`: per-call override. Empty object inherits
   *   defaults; non-empty overrides. EINVAL if no `encryption` config
   *   on createVFS.
   *
   * Mode-history is monotonic per path: once a path is written
   * encrypted with mode X, all future writes must also be encrypted
   * with mode X (server enforces with EBADF).
   */
  encrypted?:
    | true
    | false
    | { mode?: "convergent" | "random"; keyId?: string };
}

/**
 * copyFile options.
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
 * patchMetadata options.
 */
export interface PatchMetadataOpts {
  addTags?: readonly string[];
  removeTags?: readonly string[];
}

/**
 * listFiles options.
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
  /**
   * Default `false`. When `true`, results include rows whose head
   * version is a tombstone (`file_versions.deleted = 1`). Steady-
   * state consumers must NEVER set this — the default mirrors
   * `vfsStat`/`vfsReadFile`/`vfsExists` which all treat a
   * tombstoned head as ENOENT, so listing surfaces stay stat-able.
   * Reserved for admin/recovery flows that need to enumerate
   * tombstoned-head rows for cleanup (e.g.
   * `adminReapTombstonedHeads`).
   */
  includeTombstones?: boolean;
  /**
   * Default `false`. When `true`, results include archived rows
   * (`files.archived = 1`). Use to build a "Hidden"
   * / "Trash" UI: pass `true` to fetch all rows including
   * archived; combine with the `archived` flag on each item to
   * partition the view.
   *
   * NOTE: read surfaces (`stat`, `readFile`, `readPreview`,
   * `createReadStream`) are NOT gated by this — an archived file
   * remains readable by anyone who knows the path. Only listing
   * surfaces apply the filter.
   */
  includeArchived?: boolean;
  /**
   * Phase 46 — opt-in: include each file row's `contentHash`
   * (hex SHA-256). Default false to keep wire payloads compact
   * for typical UIs that don't need the hash.
   */
  includeContentHash?: boolean;
}

export interface FileInfoOpts {
  /** Default true. */
  includeStat?: boolean;
  /** Default false (size pressure). */
  includeMetadata?: boolean;
  /**
   * Default `false`. When `true`, a path resolving to a row whose
   * head version is tombstoned still returns the fileInfo metadata
   * instead of throwing ENOENT. Reserved for admin/recovery flows;
   * the steady-state SDK consumer must never set this.
   */
  includeTombstones?: boolean;
  /**
   * Default `false`. When `true`, an archived path returns its
   * info instead of throwing ENOENT. Note this is STRICTER than
   * `stat` / `readFile` (which never gate on archived) — `fileInfo`
   * is the listing-shape surface, so it mirrors the `listFiles`
   * exclusion by default.
   */
  includeArchived?: boolean;
  /** Phase 46 — opt-in: include `contentHash` (hex SHA-256). */
  includeContentHash?: boolean;
}

/** a single row returned by listFiles. */
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
  /**
   * Phase 46 — present iff `includeContentHash: true` was passed.
   * Hex SHA-256 of the file's contents.
   */
  contentHash?: string;
}

export interface ListFilesPage {
  items: ListFilesItem[];
  /** Present iff there's another page. */
  cursor?: string;
}

/**
 * Phase 46 — options for `listChildren`.
 *
 * Like `ListFilesOpts` but tied to a specific folder (no `prefix` /
 * `tags` / `metadata` filter). The result merges folders + files +
 * symlinks under that folder in a single round-trip with full stat
 * / metadata / tags / contentHash hydration.
 */
export interface ListChildrenOpts {
  /** Default 'mtime'. */
  orderBy?: "mtime" | "name" | "size";
  /** Default 'desc' for mtime/size, 'asc' for name. */
  direction?: "asc" | "desc";
  /** 1..1000, default 50. */
  limit?: number;
  /** Opaque cursor returned from a prior call. */
  cursor?: string;
  /** Default true. */
  includeStat?: boolean;
  /** Default false. */
  includeMetadata?: boolean;
  /** Default false. Hex SHA-256 on each file entry. */
  includeContentHash?: boolean;
  /** Default false. Reserved for admin/recovery surfaces. */
  includeTombstones?: boolean;
  /** Default false. */
  includeArchived?: boolean;
}

/**
 * Phase 46 — discriminated-union entry returned by `listChildren`.
 *
 * - `kind: 'folder'` — `name`, `pathId`, optional `stat` (`type: 'dir'`).
 * - `kind: 'file'` — `name`, `pathId`, optional `stat` (`type: 'file'`),
 *   `tags`, optional `metadata`, optional `contentHash`.
 * - `kind: 'symlink'` — `name`, `pathId`, `target` (the symlink's
 *   destination string), optional `stat` (`type: 'symlink'`).
 *
 * `name` is the leaf segment without leading slash; `path` is the
 * absolute path with leading slash. Both pre-computed by the server.
 */
export type VFSChild =
  | {
      kind: "folder";
      path: string;
      pathId: string;
      name: string;
      stat?: VFSStat;
    }
  | {
      kind: "file";
      path: string;
      pathId: string;
      name: string;
      stat?: VFSStat;
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
      stat?: VFSStat;
    };

/**
 * Phase 46 — `listChildren` page.
 *
 * `revision` is the per-folder mutation counter — strictly monotonic
 * within a single tenant DO. Use it as a client-side ETag: when
 * `revision` is unchanged across two reads, the directory contents
 * are guaranteed identical (no need to diff `entries`).
 */
export interface ListChildrenPage {
  revision: number;
  entries: VFSChild[];
  /** Present iff there's another page. */
  cursor?: string;
}

/** listVersions options. */
export interface ListVersionsOpts {
  limit?: number;
  /** When true, filter to versions with user_visible = 1. */
  userVisibleOnly?: boolean;
  /** When true, return `metadata` snapshots on each VersionInfo. */
  includeMetadata?: boolean;
}

/**
 * retention-policy parameters for `vfs.dropVersions(path, policy)`.
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
   * auto-enable-versioning latch. The first write or
   * versioning-related call on a VFS instance with
   * `versioning: 'enabled'` triggers a one-shot
   * `adminSetVersioning(userId, true)` server call. Subsequent
   * calls skip the round-trip thanks to this flag. The latch is
   * idempotent — flipping it on a tenant that's already enabled
   * is a no-op server-side.
   */
  private versioningLatched = false;
  // Explicit fields instead of constructor parameter properties —
  // `erasableSyntaxOnly` rejects the shorthand.
  protected readonly env: MossaicEnv;
  protected readonly opts: CreateVFSOptions;

  constructor(env: MossaicEnv, opts: CreateVFSOptions) {
    this.env = env;
    this.opts = opts;
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
   * best-effort cleanup of in-memory key material when the
   * consumer is done with this VFS instance.
   *
   * If `opts.encryption` was supplied with raw 32-byte master key
   * bytes, this method overwrites those bytes with zeroes. Note this
   * does NOT zero references the consumer still holds — it's the
   * responsibility of the consumer to drop their `masterKey`
   * reference. The zero is best-effort because JavaScript's GC may
   * have already moved the buffer; treat it as a hardening
   * measure rather than a guarantee.
   *
   * Idempotent. Safe to call multiple times.
   */
  destroy(): void {
    if (this.opts.encryption?.masterKey) {
      try {
        this.opts.encryption.masterKey.fill(0);
      } catch {
        // The buffer may already be detached / non-writable in some
        // host environments. Best-effort.
      }
    }
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

  protected user(): UserDOClient {
    const scope = this.scope();
    const name = vfsUserDOName(scope.ns, scope.tenant, scope.sub);
    const id = this.env.MOSSAIC_USER.idFromName(name);
    // The runtime stub has all the typed RPC methods; the
    // workers-types DO namespace generic doesn't structurally
    // overlap with our UserDOClient interface, so we cast.
    return this.env.MOSSAIC_USER.get(id) as UserDOClient;
  }

  protected scope(): VFSScope {
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
    // pre-flight stat to detect encryption ONLY when the
    // consumer's VFS instance has an encryption config. Without
    // encryption config, behaviour is byte-identical to
    // exactly one outbound RPC (the consumer-fixture test pins this).
    // With encryption config, we pay one extra RPC per readFile in
    // exchange for the security feature; this is the documented cost.
    let fileEnc: { mode: "convergent" | "random"; keyId?: string } | undefined;
    if (this.opts.encryption) {
      let stat: VFSStatRaw;
      try {
        stat = await this.user().vfsStat(this.scope(), p);
      } catch (err) {
        throw mapServerError(err, { path: p, syscall: "stat" });
      }
      fileEnc = stat.encryption;
    }
    // If a file IS encrypted but the caller has no encryption config,
    // the readFile path below will still succeed and return the
    // envelope bytes verbatim — which is wrong. We can't detect that
    // case without a stat. The Step 3 invariant: clients that don't
    // configure encryption MUST NOT be reading encrypted files. The
    // server's writeFile mode-history-monotonicity prevents accidental
    // mixed-tenant scenarios; an explicit attempt by an
    // encryption-unaware client to read an encrypted file returns the
    // raw envelope (which they will fail to use as a regular file).
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
    if (fileEnc !== undefined && this.opts.encryption) {
      const { decryptPayload } = await import("./encryption");
      buf = await decryptPayload(buf, this.opts.encryption, "ck", {
        path: p,
        syscall: "open",
      });
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
    let bytes =
      typeof data === "string" ? new TextEncoder().encode(data) : data;

    // encryption flow. If the consumer signaled
    // `encrypted: truthy`, we encrypt `bytes` to a single envelope
    // before sending. The server stamps `files.encryption_*` columns
    // via the `opts.encryption` payload we pass; on read, the SDK's
    // readFile reverses the flow.
    let serverEncryption:
      | { mode: "convergent" | "random"; keyId?: string }
      | undefined;
    if (opts?.encrypted !== undefined && opts.encrypted !== false) {
      const { resolveCallEncryption, encryptPayload } = await import(
        "./encryption"
      );
      const resolved = resolveCallEncryption(this.opts.encryption, opts.encrypted);
      if (!resolved) {
        // resolveCallEncryption already threw on the explicit-true
        // case without config; reaching here means the consumer
        // passed `encrypted: false` somehow (typed-narrowing escape).
        throw new EINVAL({ syscall: "open", path: p });
      }
      bytes = await encryptPayload(
        bytes,
        this.opts.encryption!,
        resolved.mode,
        resolved.keyId,
        "ck"
      );
      serverEncryption = resolved;
    }

    // Build the server-side opts. We strip the SDK-only `encrypted`
    // field and add the wire-level `encryption: { mode, keyId }` field
    // expected by the server's vfsWriteFile RPC.
    let serverOpts:
      | (WriteFileOpts & {
          encryption?: { mode: "convergent" | "random"; keyId?: string };
        })
      | undefined;
    if (opts) {
      const { encrypted: _ignored, ...rest } = opts;
      serverOpts = { ...rest };
      if (serverEncryption !== undefined) {
        serverOpts.encryption = serverEncryption;
      }
    } else if (serverEncryption !== undefined) {
      serverOpts = { encryption: serverEncryption };
    }

    try {
      await this.user().vfsWriteFile(this.scope(), p, bytes, serverOpts);
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

  async purge(p: string): Promise<void> {
    try {
      await this.user().vfsPurge(this.scope(), p);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "unlink" });
    }
  }

  async archive(p: string): Promise<void> {
    try {
      await this.user().vfsArchive(this.scope(), p);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "chmod" });
    }
  }

  async unarchive(p: string): Promise<void> {
    try {
      await this.user().vfsUnarchive(this.scope(), p);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "chmod" });
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
   * overload: pass `{ yjs: true }` to flip the per-file
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
   * alias: explicit, type-stable form of
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
   * deep-merge a metadata patch onto a file, optionally
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
   * same-tenant copyFile.
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
   * indexed listFiles with HMAC-signed cursor pagination.
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
        contentHash?: string;
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
      ...(r.contentHash !== undefined ? { contentHash: r.contentHash } : {}),
    }));
    return { items, cursor: raw.cursor };
  }

  async fileInfo(p: string, opts: FileInfoOpts = {}): Promise<ListFilesItem> {
    try {
      const raw = await this.user().vfsFileInfo(this.scope(), p, opts);
      return {
        path: raw.path,
        pathId: raw.pathId,
        stat: raw.stat ? new VFSStat(raw.stat) : undefined,
        metadata: raw.metadata,
        tags: raw.tags,
        ...(raw.contentHash !== undefined
          ? { contentHash: raw.contentHash }
          : {}),
      };
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "stat" });
    }
  }

  /**
   * Phase 46 — batched directory listing. Single round-trip returns
   * folder revision + a sorted page of merged folder/file/symlink
   * entries with stat / metadata / tags / contentHash hydrated.
   *
   * Use the returned `revision` as a client-side ETag — when it's
   * unchanged across two reads the directory contents are guaranteed
   * identical (no need to re-render).
   *
   * Performance: O(1) RPCs per page (vs O(N) for the legacy
   * `readdir + lstat × N` loop). Default `limit` is 50; max 1000.
   * `orderBy` defaults to `mtime` (descending).
   */
  async listChildren(
    p: string,
    opts: ListChildrenOpts = {}
  ): Promise<ListChildrenPage> {
    let raw: {
      revision: number;
      entries: Array<
        | {
            kind: "folder";
            path: string;
            pathId: string;
            name: string;
            stat?: VFSStatRaw;
          }
        | {
            kind: "file";
            path: string;
            pathId: string;
            name: string;
            stat?: VFSStatRaw;
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
            stat?: VFSStatRaw;
          }
      >;
      cursor?: string;
    };
    try {
      raw = await this.user().vfsListChildren(this.scope(), {
        path: p,
        ...opts,
      });
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "scandir" });
    }
    const entries: VFSChild[] = raw.entries.map((e) => {
      if (e.kind === "folder") {
        const out: VFSChild = {
          kind: "folder",
          path: e.path,
          pathId: e.pathId,
          name: e.name,
        };
        if (e.stat) out.stat = new VFSStat(e.stat);
        return out;
      }
      if (e.kind === "symlink") {
        const out: VFSChild = {
          kind: "symlink",
          path: e.path,
          pathId: e.pathId,
          name: e.name,
          target: e.target,
        };
        if (e.stat) out.stat = new VFSStat(e.stat);
        return out;
      }
      const out: VFSChild = {
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

  /**
   * open a WebSocket against a yjs-mode file. Internal —
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
    const name = vfsUserDOName(scope.ns ?? "default", scope.tenant, scope.sub);
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

  /**
   * createWriteStream accepts the same `WriteFileOpts` shape
   * as `writeFile` — including `metadata`, `tags`, and `version`. The
   * fields are validated at begin-time (caller fails fast on cap
   * violations) and applied at commit-time.
   */
  async createWriteStream(
    p: string,
    opts?: WriteFileOpts
  ): Promise<WritableStream<Uint8Array>> {
    return createWriteStreamRpc(this.user(), this.scope(), p, opts);
  }

  /** Variant that surfaces the underlying write handle for resumable use cases. */
  async createWriteStreamWithHandle(
    p: string,
    opts?: WriteFileOpts
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

  /**
   * Batched manifest fetch. Implemented as a serial loop in the
   * binding client because a single DO is single-threaded —
   * Promise.all here would not parallelize. The HTTP client uses
   * the dedicated `/api/vfs/manifests` route to amortize the
   * network hop.
   */
  async openManifests(
    paths: string[]
  ): Promise<
    (
      | { ok: true; manifest: OpenManifestResult }
      | { ok: false; code: string; message: string }
    )[]
  > {
    const stub = this.user();
    const results: (
      | { ok: true; manifest: OpenManifestResult }
      | { ok: false; code: string; message: string }
    )[] = [];
    for (const p of paths) {
      try {
        const m = await stub.vfsOpenManifest(this.scope(), p);
        results.push({ ok: true, manifest: m });
      } catch (err) {
        const mapped = mapServerError(err, { path: p, syscall: "open" });
        results.push({
          ok: false,
          code: (mapped as { code?: string }).code ?? "EINTERNAL",
          message: mapped.message,
        });
      }
    }
    return results;
  }

  async readPreview(
    p: string,
    opts?: ReadPreviewOpts
  ): Promise<ReadPreviewResult> {
    try {
      return await this.user().vfsReadPreview(this.scope(), p, opts ?? {});
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "open" });
    }
  }

  async previewUrl(p: string, opts?: PreviewUrlOpts): Promise<string> {
    const info = await this.previewInfo(p, opts);
    return info.url;
  }

  async previewInfo(
    p: string,
    opts?: PreviewUrlOpts
  ): Promise<PreviewInfo> {
    try {
      return await this.user().vfsMintPreviewToken(
        this.scope(),
        p,
        opts ?? {}
      );
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "open" });
    }
  }

  async previewInfoMany(
    paths: readonly string[],
    opts?: PreviewUrlOpts
  ): Promise<PreviewInfoBatchEntry[]> {
    try {
      return await this.user().vfsPreviewInfoMany(
        this.scope(),
        paths as string[],
        opts ?? {}
      );
    } catch (err) {
      throw mapServerError(err, {
        path: paths[0] ?? "",
        syscall: "open",
      });
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

  // ── file-level versioning ────────────────────────────────────

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
   * set per-version metadata flags. `userVisible` is
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
   * explicit flush of a yjs-mode file. Triggers a Yjs
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
   * Return the FULL `Y.encodeStateAsUpdate(doc)` bytes for a
   * yjs-mode file. Decode with `Y.applyUpdate(localDoc, bytes)`
   * to recover the entire `Y.Doc` — including all named shared
   * types (`Y.XmlFragment`, `Y.Map`, `Y.Array`, multiple `Y.Text`s).
   *
   * Use cases:
   *  - Tiptap / ProseMirror editors (default `Y.XmlFragment("default")`)
   *  - Notion-style block editors (`Y.Map<blockId, Y.XmlFragment>` plus
   *    a top-level `Y.Array` for block ordering)
   *  - Any consumer that needs to seed a new `Y.Doc` from the
   *    server's current state without first opening a WebSocket.
   *
   * Pairs with `commitYjsSnapshot(path, doc)` for the round-trip.
   *
   * Throws:
   *  - `EINVAL` if path is not a yjs-mode regular file.
   *  - `EACCES` if the file is encrypted (server cannot materialise
   *    an encrypted doc; round-trip via `openYDoc` instead).
   */
  async readYjsSnapshot(p: string): Promise<Uint8Array> {
    try {
      return await this.user().vfsReadYjsSnapshot(this.scope(), p);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "read" });
    }
  }

  /**
   * Write the current state of a `Y.Doc` as a snapshot update.
   * Encodes via `Y.encodeStateAsUpdate(doc)`,
   * wraps with the `YJS_SNAPSHOT_MAGIC` 4-byte prefix, and routes
   * through the standard `writeFile` path. The server detects
   * the magic and applies the bytes via `Y.applyUpdate` —
   * merging with the live server-side doc so concurrent editors
   * (via `openYDoc`) see the new state immediately.
   *
   * The Yjs CRDT semantics guarantee that applying the same
   * update on every peer converges to the same state, so this
   * is a safe operation even with active editors.
   *
   * Use cases:
   *  - Initialise a new yjs-mode file from a Y.Doc built offline.
   *  - Seed a path with a Tiptap document built via the editor's
   *    JSON-to-Y.XmlFragment converter.
   *  - Commit a "save point" snapshot under versioning ON
   *    (snapshot writes compose with versioning: the snapshot
   *    write creates a `file_versions` row with the snapshot
   *    bytes preserved as the version's content).
   *
   * Pairs with `readYjsSnapshot(path)` for the round-trip.
   *
   * @param p path to a yjs-mode file
   * @param doc the Y.Doc whose current state to commit
   */
  async commitYjsSnapshot(
    p: string,
    doc: import("yjs").Doc
  ): Promise<void> {
    // Lazy-load yjs so the main bundle stays free of the
    // 250 KB peer dep — same boundary as `openYDoc`.
    const Y = await import("yjs");
    const { wrapYjsSnapshot } = await import("./yjs-internal");
    const updateBytes = Y.encodeStateAsUpdate(doc);
    const wrapped = wrapYjsSnapshot(updateBytes);
    await this.writeFile(p, wrapped);
  }

  /**
   * client-driven compaction for an encrypted yjs file.
   *
   * The server cannot materialise an encrypted yjs doc, so this
   * method runs entirely in the SDK:
   *   1. `vfsReadYjsOplog` to fetch all op envelopes since the last
   *      checkpoint.
   *   2. Decrypt each envelope locally via the configured master key.
   *   3. Build a fresh `Y.Doc`, apply the decrypted updates.
   *   4. Encode `Y.encodeStateAsUpdate(doc)` + encrypt as one
   *      checkpoint envelope.
   *   5. Submit via `vfsCompactEncryptedYjs` with CAS-on-`next_seq`.
   *      On EBUSY (race), retry up to 3 times with exp backoff.
   *
   * Plain (non-encrypted) yjs files compact server-side automatically
   * on every 50 ops or 60 seconds — calling `compactYjs`
   * on a plain file is a no-op.
   *
   * @param p path to the yjs-mode file
   * @returns checkpoint seq + ops reaped, or `null` if the file is
   *   plaintext (compaction is server-driven there)
   */
  async compactYjs(p: string): Promise<{
    checkpointSeq: number;
    opsReaped: number;
  } | null> {
    if (!this.opts.encryption) {
      // Plaintext yjs files compact server-side; nothing to do.
      return null;
    }
    const stat = await this.stat(p);
    if (!stat.encryption) {
      // File is plaintext yjs — server-driven compaction handles it.
      return null;
    }
    const Y = await import("yjs");
    const { decryptPayload, encryptPayload } = await import("./encryption");
    const config = this.opts.encryption;

    // Retry loop with exponential backoff on CAS races.
    const backoffMs = [100, 400, 1600];
    let lastErr: unknown;
    for (let attempt = 0; attempt <= backoffMs.length; attempt++) {
      try {
        // Step 1: read all oplog rows in pages.
        let cursor = -1;
        let allRows: {
          seq: number;
          kind: "op" | "checkpoint";
          envelope: Uint8Array;
        }[] = [];
        for (;;) {
          const page = await this.user().vfsReadYjsOplog(this.scope(), p, {
            afterSeq: cursor,
            limit: 1000,
          });
          allRows = allRows.concat(page.rows);
          if (!page.hasMore) break;
          cursor = page.nextSeq;
        }
        if (allRows.length === 0) {
          // Nothing to compact.
          return { checkpointSeq: -1, opsReaped: 0 };
        }
        const expectedNextSeq = allRows[allRows.length - 1]!.seq + 1;

        // Step 2 + 3: decrypt + apply onto a fresh Y.Doc.
        const doc = new Y.Doc();
        for (const row of allRows) {
          const plaintext = await decryptPayload(row.envelope, config, "yj", {
            path: p,
            syscall: "compactYjs",
          });
          // Empty bytes can occur for the server's "no state" reply;
          // skip them.
          if (plaintext.byteLength === 0) continue;
          Y.applyUpdate(doc, plaintext, "compactor");
        }

        // Step 4: encode state-as-update + encrypt as checkpoint.
        const stateBytes = Y.encodeStateAsUpdate(doc);
        const checkpointEnvelope = await encryptPayload(
          stateBytes,
          config,
          "random",
          config.keyId,
          "yj"
        );

        // Step 5: submit with CAS.
        const result = await this.user().vfsCompactEncryptedYjs(
          this.scope(),
          p,
          checkpointEnvelope,
          expectedNextSeq
        );
        return result;
      } catch (err) {
        // RPC-thrown errors don't carry `.code` directly — the code
        // is encoded in the message. Inspect both.
        const msg =
          err && typeof err === "object" && "message" in err
            ? String((err as { message: unknown }).message)
            : String(err);
        const codeFromMsg = msg.match(/EBUSY/);
        const codeFromField =
          err && typeof err === "object" && "code" in err
            ? (err as { code: string }).code
            : undefined;
        const isBusy = codeFromField === "EBUSY" || codeFromMsg !== null;
        if (isBusy && attempt < backoffMs.length) {
          // CAS race; back off and retry.
          await new Promise((resolve) =>
            setTimeout(resolve, backoffMs[attempt])
          );
          lastErr = err;
          continue;
        }
        throw mapServerError(err, { path: p, syscall: "compactYjs" });
      }
    }
    throw mapServerError(lastErr, { path: p, syscall: "compactYjs" });
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
   * by the alarm sweeper after its 30s grace.
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
