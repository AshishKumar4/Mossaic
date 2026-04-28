/**
 * VFSStat — Node-fs-Stats-shaped object returned by `stat` / `lstat`.
 *
 * Field-level compatibility with `import("node:fs").Stats`:
 *   - `mode`, `size`, `uid`, `gid`, `dev`, `ino`        ✅ direct
 *   - `mtimeMs`                                         ✅ direct
 *   - `mtime`/`atime`/`ctime`/`birthtime`               ✅ Date wrappers around mtimeMs
 *   - `isFile()` / `isDirectory()` / `isSymbolicLink()` ✅ Methods
 *   - `isBlockDevice()` / `isCharacterDevice()` / `isFIFO()` / `isSocket()` ✅ always-false
 *   - `nlink`, `rdev`, `blksize`, `blocks`              ✅ sensible defaults
 *
 * isomorphic-git compatibility (verified against igit's TS contract):
 * igit reads .mode, .size, .ino, .mtimeMs, .ctimeMs, .uid, .gid and
 * calls .isFile() / .isDirectory() / .isSymbolicLink(). All present.
 */

import type { VFSStatRaw } from "../../shared/vfs-types";

export class VFSStat {
  /** Discriminator used internally; not on Node's Stats but harmless extra. */
  readonly type: "file" | "dir" | "symlink";

  readonly mode: number;
  readonly size: number;
  readonly mtimeMs: number;
  /** Mossaic doesn't track atime / ctime separately; we mirror mtimeMs (study §3.3). */
  readonly atimeMs: number;
  readonly ctimeMs: number;
  readonly birthtimeMs: number;

  /** Date views — Node's Stats expose these as Date objects too. */
  readonly mtime: Date;
  readonly atime: Date;
  readonly ctime: Date;
  readonly birthtime: Date;

  readonly uid: number;
  readonly gid: number;

  /** Mossaic is a single virtual filesystem per tenant ⇒ `dev` is always 0. */
  readonly dev = 0;
  readonly ino: number;

  /** Defaults that aren't tracked by Mossaic but Node's Stats always carries. */
  readonly nlink = 1;
  readonly rdev = 0;
  readonly blksize = 4096;
  readonly blocks: number;

  constructor(raw: VFSStatRaw) {
    this.type = raw.type;
    this.mode = raw.mode;
    this.size = raw.size;
    this.mtimeMs = raw.mtimeMs;
    this.atimeMs = raw.mtimeMs;
    this.ctimeMs = raw.mtimeMs;
    this.birthtimeMs = raw.mtimeMs;
    const d = new Date(raw.mtimeMs);
    this.mtime = d;
    this.atime = d;
    this.ctime = d;
    this.birthtime = d;
    this.uid = raw.uid;
    this.gid = raw.gid;
    this.ino = raw.ino;
    // POSIX-ish: blocks count = ceil(size / 512)
    this.blocks = Math.ceil(raw.size / 512);
  }

  isFile(): boolean {
    return this.type === "file";
  }
  isDirectory(): boolean {
    return this.type === "dir";
  }
  isSymbolicLink(): boolean {
    return this.type === "symlink";
  }
  isBlockDevice(): boolean {
    return false;
  }
  isCharacterDevice(): boolean {
    return false;
  }
  isFIFO(): boolean {
    return false;
  }
  isSocket(): boolean {
    return false;
  }
}
