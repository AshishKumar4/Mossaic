/**
 * VFS-side types shared by the worker, the SDK, and tests.
 *
 * The plan splits these from shared/types.ts so the legacy app's wire
 * shapes stay independent of the new VFS contract. Anything imported by
 * SDK consumers lives here.
 */

/** Multi-tenant scope. uses tenant as the SQL `user_id`; wires the full vfs:ns:tenant DO-name pattern. */
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
  /**
   * per-file encryption stamp. Undefined for plaintext
   * (default for legacy rows and explicit plaintext writes).
   * The SDK consults this on readFile to decide whether to attempt
   * decryption. NULL/undefined → return server bytes verbatim.
   */
  encryption?: { mode: "convergent" | "random"; keyId?: string };
}

/**
 * Cache-bust state for a path. One SQL JOIN inside the UserDO; cheaper
 * than a full read. Routes that wrap heavy reads in `caches.default`
 * call `vfsResolveCacheKey` and fold the returned fields into the
 * cache key. SDK consumers building their own read caches should do
 * the same.
 *
 * The shape mirrors `apps/mossaic/worker/core/objects/user/vfs/cache-resolve.ts`
 * exactly — kept here so consumers don't import across the worker
 * boundary just for this type.
 *
 * @lean-invariant Mossaic.Vfs.Cache.bust_state_changes_when_signal_changes
 * The abstract theorem proves that a genuinely changed input signal changes
 * BustState; it does not mechanically verify TypeScript write coverage.
 */
export interface CacheResolveResult {
	/** Stable file_id (immutable for the lifetime of the path). */
	fileId: string;
	/**
	 * Current head version_id, or `null` for versioning-OFF / yjs
	 * tenants (in which case `updatedAt` is the bust signal).
	 */
	headVersionId: string | null;
	/** `files.updated_at` in ms-since-epoch. Bumped by every meaningful write. */
	updatedAt: number;
	/** Per-file encryption stamp. NULL for plaintext. */
	encryptionMode: string | null;
	encryptionKeyId: string | null;
}

/**
 * Boundary validator for `CacheResolveResult` JSON shipped over the
 * HTTP fallback. The SDK avoids a zod dep so this is a hand-rolled
 * structural check — same pattern used by `VFSStat` in `stats.ts`.
 * Returns the validated value or throws a `TypeError` with a precise
 * field path. RFC-009: external boundaries must validate before
 * narrowing to the typed shape.
 */
export function parseCacheResolveResult(raw: unknown): CacheResolveResult {
	if (raw === null || typeof raw !== "object") {
		throw new TypeError(
			`CacheResolveResult: expected object, got ${raw === null ? "null" : typeof raw}`,
		);
	}
	const r = raw as Record<string, unknown>;
	if (typeof r.fileId !== "string" || r.fileId.length === 0) {
		throw new TypeError("CacheResolveResult.fileId: expected non-empty string");
	}
	if (r.headVersionId !== null && typeof r.headVersionId !== "string") {
		throw new TypeError("CacheResolveResult.headVersionId: expected string|null");
	}
	if (typeof r.updatedAt !== "number" || !Number.isFinite(r.updatedAt)) {
		throw new TypeError("CacheResolveResult.updatedAt: expected finite number");
	}
	if (r.encryptionMode !== null && typeof r.encryptionMode !== "string") {
		throw new TypeError("CacheResolveResult.encryptionMode: expected string|null");
	}
	if (r.encryptionKeyId !== null && typeof r.encryptionKeyId !== "string") {
		throw new TypeError("CacheResolveResult.encryptionKeyId: expected string|null");
	}
	return {
		fileId: r.fileId,
		headVersionId: r.headVersionId as string | null,
		updatedAt: r.updatedAt,
		encryptionMode: r.encryptionMode as string | null,
		encryptionKeyId: r.encryptionKeyId as string | null,
	};
}

// Boundary validator for `readManyFile` HTTP response. Hand-rolled
// to match `parseCacheResolveResult` (SDK avoids zod dep). RFC-009.
export function parseReadManyFileBytes(raw: unknown): (Uint8Array | null)[] {
	if (raw === null || typeof raw !== "object") {
		throw new TypeError(
			`readManyFile: expected object, got ${raw === null ? "null" : typeof raw}`,
		);
	}
	const body = raw as Record<string, unknown>;
	if (!Array.isArray(body.bytes)) {
		throw new TypeError("readManyFile.bytes: expected array");
	}
	return body.bytes.map((entry, i) => {
		if (entry === null) return null;
		if (entry === undefined || typeof entry !== "object") {
			throw new TypeError(`readManyFile.bytes[${i}]: expected object|null`);
		}
		const e = entry as Record<string, unknown>;
		if (typeof e.base64 !== "string") {
			throw new TypeError(`readManyFile.bytes[${i}].base64: expected string`);
		}
		const bin = atob(e.base64);
		const arr = new Uint8Array(bin.length);
		for (let j = 0; j < bin.length; j++) arr[j] = bin.charCodeAt(j);
		return arr;
	});
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
 * Path resolution result. read-side ops (vfsStat/lstat/exists/readlink/...)
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
  | "EAGAIN"
  // opt-in E2E encryption error surface (per plan §4.6).
  // - EBADF: writeFile attempted with a different encryption mode than
  //   the existing path's history, OR plaintext write to an encrypted
  //   path. Mode-history-monotonic enforcement.
  // - EACCES: readFile / openYDoc on an encrypted file without
  //   `encryption` config on the VFS instance.
  // - ENOTSUP: chmod-style encryption toggle (encrypt/decrypt-in-place)
  //   not supported in v15.
  | "EBADF"
  | "EACCES"
  | "ENOTSUP";

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
