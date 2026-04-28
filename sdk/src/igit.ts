/**
 * isomorphic-git fs adapter.
 *
 * isomorphic-git's `fs` plugin contract (verified against
 * isomorphic-git's TypeScript definitions, igit/index.d.ts):
 *
 *     interface FsClient {
 *       promises: {
 *         readFile(path, opts?): Promise<Uint8Array | string>;
 *         writeFile(path, data, opts?): Promise<void>;
 *         unlink(path): Promise<void>;
 *         readdir(path): Promise<string[]>;
 *         mkdir(path, opts?): Promise<void>;
 *         rmdir(path): Promise<void>;
 *         stat(path): Promise<Stats>;
 *         lstat(path): Promise<Stats>;
 *         readlink?(path): Promise<string>;
 *         symlink?(target, path): Promise<void>;
 *         chmod?(path, mode): Promise<void>;
 *       }
 *     }
 *
 * `Stats` must expose isFile()/isDirectory()/isSymbolicLink(),
 * plus mode/size/ino/mtimeMs/uid/gid (used by igit's index logic).
 *
 * The SDK's `VFS` class already satisfies all of this directly because
 * `vfs.promises === vfs`. So `createIgitFs(vfs)` just returns the
 * VFS instance as-is, but the explicit constructor is kept for
 * readability at consumer call sites:
 *
 *     const fs = createIgitFs(createVFS(env, { tenant: "acme" }));
 *     await git.clone({ fs, dir: "/repo", url: "..." });
 */

import type { VFS } from "./vfs";

/**
 * Wrap a `VFS` instance into the shape isomorphic-git expects.
 *
 * Returns the VFS unchanged because it already has a `.promises`
 * self-reference and all the required methods. The function exists so
 * consumer code reads as `git.clone({ fs: createIgitFs(vfs), ... })`,
 * which signals intent more clearly than `git.clone({ fs: vfs })`.
 */
export function createIgitFs(vfs: VFS): VFS {
  // No transformation needed. The function is intentionally a pass-through.
  // It exists for documentation and so that future igit-version
  // adaptations have a single place to land.
  return vfs;
}

// Re-export VFS + createVFS so consumers can do
// `import { createVFS, createIgitFs } from "@mossaic/sdk/fs"` in one go.
export { VFS } from "./vfs";
export { createVFS } from "./index";
