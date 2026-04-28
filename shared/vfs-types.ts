/**
 * VFS-side types shared by the worker, the SDK, and tests.
 *
 * The plan splits these from shared/types.ts so the legacy app's wire
 * shapes stay independent of the new VFS contract. Anything imported by
 * SDK consumers lives here.
 */

/** Multi-tenant scope. Phase 2 uses tenant as the SQL `user_id`; Phase 4 wires the full vfs:ns:tenant DO-name pattern. */
export interface VFSScope {
  /** Logical namespace (operator-side). Defaults to "default" in the SDK factory. */
  ns: string;
  /** Tenant identifier — consumer's customer/repo/org. Maps to `files.user_id`. */
  tenant: string;
  /** Optional sub-tenant (consumer's end-user). */
  sub?: string;
}

/**
 * Wire-shape stat object. The SDK wraps this in a `VFSStat` class with
 * isFile()/isDirectory()/isSymbolicLink() helpers. Kept POJO-shaped so
 * Workers RPC can serialize it across the binding boundary.
 */
export interface VFSStatRaw {
  type: "file" | "dir" | "symlink";
  mode: number;
  size: number;
  /** Modification time (ms since epoch). For files this is `files.updated_at`; for folders, `folders.updated_at`. */
  mtimeMs: number;
  /** Synthesised — stable per tenant via murmurhash3(scope.tenant). */
  uid: number;
  gid: number;
  /** 53-bit safe integer derived deterministically from the row's id (file_id / folder_id). */
  ino: number;
}

/** Public, shard-index-stripped manifest returned by `vfsOpenManifest` for caller-driven multi-invocation reads. */
export interface OpenManifestResult {
  fileId: string;
  size: number;
  chunkSize: number;
  chunkCount: number;
  /** Hash + index + size only — shardIndex is intentionally hidden (internal placement detail). */
  chunks: { index: number; size: number; hash: string }[];
  /** True when content is fully inlined (no chunks); caller should `readFile` directly. */
  inlined: boolean;
}

/**
 * Path resolution result. Phase 2's read-side ops (vfsStat/lstat/exists/readlink/...)
 * all flow through resolvePath() and discriminate on `kind`.
 */
export type ResolveResult =
  | { kind: "ENOENT"; parentId: string | null }
  | { kind: "file"; parentId: string | null; leafId: string }
  | { kind: "dir"; parentId: string | null; leafId: string }
  | {
      kind: "symlink";
      parentId: string | null;
      leafId: string;
      target: string;
    }
  | { kind: "ELOOP"; parentId: string | null }
  | { kind: "ENOTDIR"; parentId: string | null };

/** Maximum symlink chase depth before giving up with ELOOP. POSIX uses 40 on Linux. */
export const SYMLINK_MAX_HOPS = 40;

/** Stable error codes thrown by the VFS RPC surface. The SDK maps these to typed Error subclasses. */
export type VFSErrorCode =
  | "ENOENT"
  | "EEXIST"
  | "EISDIR"
  | "ENOTDIR"
  // ENOTEMPTY: rmdir on a non-empty dir. README + SDK promise this
  // code; audit H2 added it to the union so the server can actually
  // throw it. Routed to HTTP 409 in worker/routes/vfs.ts.
  | "ENOTEMPTY"
  | "EFBIG"
  | "ELOOP"
  | "EBUSY"
  | "EINVAL"
  | "EAGAIN";

/**
 * Error class thrown by VFS RPC methods. Workers RPC serializes the
 * `code` field across the binding boundary so consumer-side `catch`
 * blocks can `if (e.code === "ENOENT")`.
 *
 * The constructed `message` always begins with the code (Node.js fs
 * convention: "ENOENT: no such file or directory, open '/x'") so
 * `String(err)` and `err.toString()` both include the code without
 * requiring callers to inspect `err.code` separately.
 */
export class VFSError extends Error {
  readonly code: VFSErrorCode;
  constructor(code: VFSErrorCode, message?: string) {
    super(message ? `${code}: ${message}` : code);
    this.code = code;
    this.name = "VFSError";
  }
}
