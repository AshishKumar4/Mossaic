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
  | "EAGAIN"
  | "EMOSSAIC_UNAVAILABLE"
  // Phase 15 — encryption surface.
  | "EBADF"
  | "ENOTSUP";

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
  EAGAIN: -11, // POSIX EAGAIN — rate-limit / try-again
  EMOSSAIC_UNAVAILABLE: -111, // ECONNREFUSED-equivalent
  EBADF: -9, // Phase 15: encryption-mode mismatch on a path's history.
  ENOTSUP: -95, // Phase 15: chmod-style encrypt-in-place not supported.
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
  EAGAIN: "rate limit exceeded; retry",
  EMOSSAIC_UNAVAILABLE: "Mossaic VFS unavailable",
  EBADF: "encryption mode does not match path history",
  ENOTSUP: "operation not supported",
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
export class EAGAIN extends VFSFsError {
  constructor(opts: { syscall?: string; path?: string; message?: string } = {}) {
    super("EAGAIN", opts);
  }
}
export class EBADF extends VFSFsError {
  constructor(opts: { syscall?: string; path?: string; message?: string } = {}) {
    super("EBADF", opts);
  }
}
export class ENOTSUP extends VFSFsError {
  constructor(opts: { syscall?: string; path?: string; message?: string } = {}) {
    super("ENOTSUP", opts);
  }
}
export class MossaicUnavailableError extends VFSFsError {
  constructor(opts: { message?: string } = {}) {
    super("EMOSSAIC_UNAVAILABLE", opts);
  }
}

/**
 * Heuristic detector for transport-level failures that should surface
 * as `MossaicUnavailableError` rather than a generic EINVAL.
 *
 * Workers RPC and Durable Object stub calls can fail with several
 * patterns that aren't VFSError-shaped:
 *   - Plain `Error` with messages like "Internal error", "Network
 *     connection lost.", "Durable Object hibernation timed out",
 *     "The Durable Object's code threw an exception", "fetch failed",
 *     "ECONNREFUSED", "Failed to fetch".
 *   - `TypeError("Network request failed")` from the global fetch.
 *   - undici `fetch failed` wrappers.
 *
 * We pattern-match on `e.message` (case-insensitive) and on `e.name`
 * (TypeError + 'fetch' anywhere). Anything that matches gets
 * remapped to EMOSSAIC_UNAVAILABLE so consumers can soft-fail or
 * retry with backoff. False positives are bounded — server-side
 * VFSError messages start with "CODE: …", which won't trigger
 * because none of the patterns are substrings of any VFSError code.
 */
const UNAVAILABLE_PATTERNS = [
  /network connection lost/i,
  /durable object.*(hibernat|timed? ?out|threw)/i,
  /fetch failed/i,
  /failed to fetch/i,
  /econnrefused/i,
  /econnreset/i,
  /network (?:error|request failed)/i,
  /service binding.*(unavailable|unreachable)/i,
];

export function isLikelyUnavailable(err: unknown): boolean {
  const e = err as { name?: unknown; message?: unknown; code?: unknown };
  // Already-typed pass-through.
  if (e?.code === "EMOSSAIC_UNAVAILABLE") return true;
  const msg = typeof e?.message === "string" ? e.message : String(err);
  // TypeError thrown by fetch on network failure.
  if (
    typeof e?.name === "string" &&
    e.name === "TypeError" &&
    /fetch/i.test(msg)
  ) {
    return true;
  }
  return UNAVAILABLE_PATTERNS.some((re) => re.test(msg));
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
  // Transport-level unavailability detection happens BEFORE code
  // extraction. A network failure / DO hibernation / fetch reject
  // doesn't carry a VFS code; we surface MossaicUnavailableError so
  // consumers can soft-fail or retry with backoff rather than seeing
  // a misleading EINVAL.
  if (isLikelyUnavailable(err)) {
    const rawMsg =
      typeof (err as { message?: unknown })?.message === "string"
        ? ((err as { message: string }).message)
        : String(err);
    const inst = new VFSFsError("EMOSSAIC_UNAVAILABLE", {
      path: ctx.path,
      syscall: ctx.syscall,
      message: rawMsg,
    });
    Object.setPrototypeOf(inst, MossaicUnavailableError.prototype);
    return inst;
  }
  const e = err as { code?: unknown; message?: unknown };
  const explicitCode =
    typeof e?.code === "string" ? (e.code as VFSErrorCode) : undefined;

  // Server message convention: "CODE: rest". Extract by scanning the
  // message for any token that matches a known code. We don't anchor
  // to ^ because workerd's Error serialisation may prepend the
  // class name (`"VFSError: ENOENT: …"`) so the code is the SECOND
  // colon-delimited token, not the first. Take the FIRST recognised
  // code and use it.
  let codeFromMsg: VFSErrorCode | undefined;
  const rawMsg = typeof e?.message === "string" ? e.message : String(err);
  const tokens = rawMsg.match(/[A-Z_]{3,}/g) ?? [];
  for (const tok of tokens) {
    if (tok in ERRNO) {
      codeFromMsg = tok as VFSErrorCode;
      break;
    }
  }

  const code: VFSErrorCode =
    (explicitCode && explicitCode in ERRNO ? explicitCode : codeFromMsg) ??
    "EINVAL";

  // Construct the base VFSFsError with the server's contextualised
  // message, then re-set the prototype to the matching subclass so
  // `instanceof ENOENT` / `instanceof EFBIG` etc. work for consumer
  // code that prefers type-pattern matching over `.code` string
  // comparison. The base class's runtime fields (code, errno,
  // syscall, path, message) are unchanged.
  const inst = new VFSFsError(code, {
    path: ctx.path,
    syscall: ctx.syscall,
    message: rawMsg,
  });
  const SubProto = SUBCLASS_PROTO[code];
  if (SubProto) {
    Object.setPrototypeOf(inst, SubProto);
  }
  return inst;
}

/**
 * Map of code → subclass.prototype for instanceof retro-fitting in
 * mapServerError. Defined AFTER the subclasses are declared above,
 * so the prototype chain is valid at module-init time.
 */
const SUBCLASS_PROTO: Partial<Record<VFSErrorCode, object>> = {
  ENOENT: ENOENT.prototype,
  EEXIST: EEXIST.prototype,
  EISDIR: EISDIR.prototype,
  ENOTDIR: ENOTDIR.prototype,
  EFBIG: EFBIG.prototype,
  ELOOP: ELOOP.prototype,
  EBUSY: EBUSY.prototype,
  EINVAL: EINVAL.prototype,
  EACCES: EACCES.prototype,
  EROFS: EROFS.prototype,
  ENOTEMPTY: ENOTEMPTY.prototype,
  EAGAIN: EAGAIN.prototype,
  EMOSSAIC_UNAVAILABLE: MossaicUnavailableError.prototype,
  EBADF: EBADF.prototype,
  ENOTSUP: ENOTSUP.prototype,
};
