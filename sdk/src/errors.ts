/**
 * Typed error classes mirroring Node.js `fs/promises` semantics.
 *
 * Node's fs throws `Error` instances with `code`, `errno`, `syscall`,
 * `path`. isomorphic-git pattern-matches on `code`, so the SDK MUST
 * surface a `code` field on every thrown error. The `errno` integer
 * matches Linux libc errnos to keep tooling that asserts on numbers
 * (including isomorphic-git's index-lock retry path) happy.
 *
 * The server-side `VFSError` (worker/lib/...) carries `code` over the
 * RPC boundary as a plain string. The SDK's `mapServerError(err)` is
 * the load-bearing converter that turns wire-error → typed
 * `VFSFsError`. All VFS RPC calls go through it.
 */

/** All error codes the VFS surface emits, plus a few that may flow through if isomorphic-git invents new ones. */
export type VFSErrorCode =
  | "ENOENT"
  | "EEXIST"
  | "EISDIR"
  | "ENOTDIR"
  | "EFBIG"
  | "ELOOP"
  | "EBUSY"
  | "EINVAL"
  | "EACCES"
  | "EROFS"
  | "ENOTEMPTY"
  | "EMOSSAIC_UNAVAILABLE";

/**
 * Linux errno integers (libc convention). Matters because some
 * consumers (notably git-style code) compare `err.errno` numerically.
 */
const ERRNO: Record<VFSErrorCode, number> = {
  ENOENT: -2,
  EEXIST: -17,
  EISDIR: -21,
  ENOTDIR: -20,
  EFBIG: -27,
  ELOOP: -40,
  EBUSY: -16,
  EINVAL: -22,
  EACCES: -13,
  EROFS: -30,
  ENOTEMPTY: -39,
  EMOSSAIC_UNAVAILABLE: -111, // ECONNREFUSED-equivalent
};

const HUMAN: Record<VFSErrorCode, string> = {
  ENOENT: "no such file or directory",
  EEXIST: "file already exists",
  EISDIR: "illegal operation on a directory",
  ENOTDIR: "not a directory",
  EFBIG: "file too large",
  ELOOP: "too many symbolic links encountered",
  EBUSY: "resource busy",
  EINVAL: "invalid argument",
  EACCES: "permission denied",
  EROFS: "read-only file system",
  ENOTEMPTY: "directory not empty",
  EMOSSAIC_UNAVAILABLE: "Mossaic VFS unavailable",
};

/**
 * Base class for all SDK-thrown fs-style errors. Mirrors Node's
 * `SystemError` shape (code, errno, syscall, path) so isomorphic-git
 * and any other Node-style consumer recognises it.
 */
export class VFSFsError extends Error {
  readonly code: VFSErrorCode;
  readonly errno: number;
  readonly syscall?: string;
  readonly path?: string;

  constructor(
    code: VFSErrorCode,
    opts: { syscall?: string; path?: string; message?: string } = {}
  ) {
    const path = opts.path;
    const syscall = opts.syscall;
    const tail = path ? `, ${syscall ?? "fs"} '${path}'` : "";
    const msg =
      opts.message ?? `${code}: ${HUMAN[code] ?? "error"}${tail}`;
    super(msg);
    this.code = code;
    this.errno = ERRNO[code];
    this.syscall = syscall;
    this.path = path;
    this.name = "VFSFsError";
  }
}

// Subclass per code so consumers can `if (e instanceof ENOENT)` or
// `try { ... } catch (e: ENOENT) { ... }` if they prefer that style.
export class ENOENT extends VFSFsError {
  constructor(opts: { syscall?: string; path?: string } = {}) {
    super("ENOENT", opts);
  }
}
export class EEXIST extends VFSFsError {
  constructor(opts: { syscall?: string; path?: string } = {}) {
    super("EEXIST", opts);
  }
}
export class EISDIR extends VFSFsError {
  constructor(opts: { syscall?: string; path?: string } = {}) {
    super("EISDIR", opts);
  }
}
export class ENOTDIR extends VFSFsError {
  constructor(opts: { syscall?: string; path?: string } = {}) {
    super("ENOTDIR", opts);
  }
}
export class EFBIG extends VFSFsError {
  constructor(opts: { syscall?: string; path?: string } = {}) {
    super("EFBIG", opts);
  }
}
export class ELOOP extends VFSFsError {
  constructor(opts: { syscall?: string; path?: string } = {}) {
    super("ELOOP", opts);
  }
}
export class EBUSY extends VFSFsError {
  constructor(opts: { syscall?: string; path?: string } = {}) {
    super("EBUSY", opts);
  }
}
export class EINVAL extends VFSFsError {
  constructor(opts: { syscall?: string; path?: string } = {}) {
    super("EINVAL", opts);
  }
}
export class EACCES extends VFSFsError {
  constructor(opts: { syscall?: string; path?: string } = {}) {
    super("EACCES", opts);
  }
}
export class EROFS extends VFSFsError {
  constructor(opts: { syscall?: string; path?: string } = {}) {
    super("EROFS", opts);
  }
}
export class ENOTEMPTY extends VFSFsError {
  constructor(opts: { syscall?: string; path?: string } = {}) {
    super("ENOTEMPTY", opts);
  }
}
export class MossaicUnavailableError extends VFSFsError {
  constructor(opts: { message?: string } = {}) {
    super("EMOSSAIC_UNAVAILABLE", opts);
  }
}

/**
 * Map a server-thrown error (or any wire error) to a typed VFSFsError.
 *
 * The server's `VFSError` carries `code` and a `"CODE: msg"` message.
 * We re-extract the code via inspection of `.code` (preferred) or
 * pattern-match on the message prefix (fallback for cases where the
 * RPC layer flattened the error to a plain Error with only `.message`).
 *
 * `path` and `syscall` enrich the error with caller context so
 * isomorphic-git's logs show "ENOENT: no such file or directory, open
 * '/foo/bar'" exactly like Node's fs.
 */
export function mapServerError(
  err: unknown,
  ctx: { path?: string; syscall?: string } = {}
): VFSFsError {
  if (err instanceof VFSFsError) {
    // Already typed; pass-through.
    return err;
  }
  const e = err as { code?: unknown; message?: unknown };
  const explicitCode =
    typeof e?.code === "string" ? (e.code as VFSErrorCode) : undefined;

  // Server message convention: "CODE: rest". Extract the prefix.
  let codeFromMsg: VFSErrorCode | undefined;
  const rawMsg = typeof e?.message === "string" ? e.message : String(err);
  const m = rawMsg.match(/^([A-Z_]+):/);
  if (m) {
    const candidate = m[1] as VFSErrorCode;
    if (candidate in ERRNO) codeFromMsg = candidate;
  }

  const code: VFSErrorCode =
    (explicitCode && explicitCode in ERRNO ? explicitCode : codeFromMsg) ??
    "EINVAL";

  return new VFSFsError(code, {
    path: ctx.path,
    syscall: ctx.syscall,
    message: rawMsg,
  });
}
