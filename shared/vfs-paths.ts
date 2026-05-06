/**
 * Pure path utilities for the VFS. No I/O, no env access — safe to call
 * from worker, DO, SDK, or tests.
 *
 * The VFS uses POSIX-style absolute paths: `/foo/bar/baz.txt`. Empty
 * segments and `.` are collapsed; `..` walks up. Trailing slashes are
 * trimmed. The root path `/` normalizes to an empty segment array.
 */

/**
 * Normalize a POSIX-style path into an array of segments.
 *
 *   "/"               → []
 *   "/foo"            → ["foo"]
 *   "/foo/bar"        → ["foo", "bar"]
 *   "/foo/./bar/"     → ["foo", "bar"]
 *   "/foo/../bar"     → ["bar"]
 *   "//foo///bar"     → ["foo", "bar"]
 *
 * Throws EINVAL on:
 *   - non-absolute paths (must start with "/")
 *   - paths containing NUL bytes
 *   - .. that walks above root
 */
export function normalizePath(path: string): string[] {
  if (typeof path !== "string") {
    throw new VFSPathError("EINVAL", "path must be a string");
  }
  if (path.length === 0 || path[0] !== "/") {
    throw new VFSPathError("EINVAL", `path must be absolute: ${path}`);
  }
  if (path.indexOf("\0") !== -1) {
    throw new VFSPathError("EINVAL", "path contains NUL byte");
  }

  const out: string[] = [];
  for (const seg of path.split("/")) {
    if (seg === "" || seg === ".") continue;
    if (seg === "..") {
      if (out.length === 0) {
        throw new VFSPathError("EINVAL", "path walks above root");
      }
      out.pop();
      continue;
    }
    out.push(seg);
  }
  return out;
}

/** True iff the path normalizes to root ([]). */
export function isRoot(path: string): boolean {
  return normalizePath(path).length === 0;
}

/** Last segment of the normalized path, or "" for root. */
export function basename(path: string): string {
  const segs = normalizePath(path);
  return segs.length === 0 ? "" : segs[segs.length - 1];
}

/** Parent path (always normalized, leading slash). `dirname("/")` is `"/"`. */
export function dirname(path: string): string {
  const segs = normalizePath(path);
  if (segs.length <= 1) return "/";
  return "/" + segs.slice(0, -1).join("/");
}

/** Re-join a segment array back to a leading-slash path. */
export function joinSegments(segs: string[]): string {
  return "/" + segs.join("/");
}

/**
 * Resolve a (possibly relative) symlink target against the path that
 * pointed at the symlink. POSIX semantics: an absolute target replaces
 * everything; a relative target is resolved against `dirname(linkPath)`.
 */
export function resolveSymlinkTarget(linkPath: string, target: string): string {
  if (target.length === 0) {
    throw new VFSPathError("EINVAL", "symlink target is empty");
  }
  if (target[0] === "/") return joinSegments(normalizePath(target));
  // Relative — resolve against the link's parent directory.
  const parent = normalizePath(dirname(linkPath));
  const targetSegs = target.split("/").filter((s) => s !== "" && s !== ".");
  for (const seg of targetSegs) {
    if (seg === "..") {
      if (parent.length === 0) {
        throw new VFSPathError("EINVAL", "symlink target walks above root");
      }
      parent.pop();
    } else {
      parent.push(seg);
    }
  }
  return joinSegments(parent);
}

/**
 * Local error class used inside path utilities. The DO-side catches and
 * re-throws as VFSError("EINVAL"); kept separate so the path module has
 * zero dependencies on shared/vfs-types.ts (avoids a cycle if vfs-types
 * later wants to import from here).
 */
export class VFSPathError extends Error {
  readonly code: "EINVAL";
  constructor(code: "EINVAL", message: string) {
    super(message);
    this.code = code;
    this.name = "VFSPathError";
  }
}
