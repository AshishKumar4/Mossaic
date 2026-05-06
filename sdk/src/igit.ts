/**
 * isomorphic-git fs adapter, with optional auto-batched lstat for
 * large-tree workloads (git status, git checkout).
 *
 * isomorphic-git's `fs` plugin contract (verified against
 * isomorphic-git's TypeScript definitions, node_modules/isomorphic-git/
 * index.d.ts:499):
 *
 *     interface PromiseFsClient {
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
 * The SDK's `VFS` class already satisfies the entire contract
 * (vfs.promises === vfs). So at minimum, `createIgitFs(vfs)` is a
 * pass-through with intent-signalling.
 *
 * For repos with many files, `git status` issues N lstat calls in
 * tight bursts as it walks the working tree. With one DO RPC per
 * lstat, that's N subrequests on Mossaic's UserDO budget. The
 * `batchLstat` option queues lstat calls within a small
 * microtask-or-N-ms window, then dispatches one `vfs.readManyStat`
 * RPC for the whole batch. N→1 RPC reduction with no correctness
 * change (lstat is read-only and idempotent within the batch
 * window).
 */

import type { VFS, VFSClient, WriteFileOpts } from "./vfs";
import type { VFSStat } from "./stats";
import { ENOENT } from "./errors";

export interface CreateIgitFsOptions {
  /**
   * Enable auto-batching of lstat calls. When true, igit-side bursts
   * of vfs.lstat() are coalesced into one vfs.readManyStat RPC per
   * batch window. Default false (pass-through preserves existing
   * behavior; opt-in for performance-sensitive workloads).
   */
  batchLstat?: boolean;
  /**
   * Batch flush window in milliseconds. Lower = lower latency on
   * single-call lstat, lower batching gain on bursts. Higher =
   * better batching, slightly higher single-call latency.
   * Default 10ms — short enough to be invisible in interactive
   * use, long enough to coalesce all the lstat calls in one
   * synchronous burst from isomorphic-git.
   */
  batchWindowMs?: number;
}

/**
 * Wrap a `VFS` instance into the shape isomorphic-git expects.
 *
 * Default: pass-through (vfs.promises === vfs already satisfies the
 * fs plugin). With `{ batchLstat: true }`, lstat is replaced by a
 * batching wrapper. All other methods pass through unchanged.
 */
export function createIgitFs(
  vfs: VFS,
  opts: CreateIgitFsOptions = {}
): VFSClient {
  if (!opts.batchLstat) return vfs;
  return new BatchedLstatFs(vfs, opts.batchWindowMs ?? 10);
}

/**
 * Forwarding wrapper that intercepts `lstat(path)` to coalesce
 * concurrent calls into a single `readManyStat([…paths])` RPC.
 *
 * Correctness:
 *   1. Each batch is owned by exactly one timer. Once the timer
 *      fires, the batch is flushed atomically: the queue is read,
 *      reset to a fresh array, then dispatched. Subsequent
 *      `lstat()` calls during the in-flight RPC start a NEW batch
 *      (no cross-batch leakage).
 *   2. Per-path resolution: the request map is keyed by path, so
 *      duplicate `lstat(p)` calls within the same window share one
 *      RPC entry but each caller's Promise resolves with the same
 *      result. If the result is `null` (ENOENT), we throw on each
 *      caller's resolution so it matches the non-batched lstat
 *      surface (which throws ENOENT, not returns null).
 *   3. Errors from the batched RPC reject ALL pending callers in
 *      the batch — failure mode is consistent.
 *   4. Other methods are pure delegation; no global state
 *      mutation, no aliasing.
 */
class BatchedLstatFs implements VFSClient {
  readonly promises: BatchedLstatFs;
  private pendingPaths: string[] = [];
  /**
   * Map from path → array of (resolve, reject) waiters. Multiple
   * callers asking for the same path within the window share a
   * single batch entry but each gets their own promise.
   */
  private waiters: Map<
    string,
    Array<{
      resolve: (s: VFSStat) => void;
      reject: (e: unknown) => void;
    }>
  > = new Map();
  private timer: ReturnType<typeof setTimeout> | null = null;
  // explicit fields instead of constructor parameter
  // properties — `erasableSyntaxOnly` (TS 5.8+) rejects the
  // shorthand because `private readonly foo: T` in a constructor
  // signature emits runtime field initializers.
  private readonly inner: VFS;
  private readonly windowMs: number;

  constructor(inner: VFS, windowMs: number) {
    this.inner = inner;
    this.windowMs = windowMs;
    this.promises = this;
  }

  // ── Batched method ────────────────────────────────────────────────────

  lstat(p: string): Promise<VFSStat> {
    return new Promise<VFSStat>((resolve, reject) => {
      let bucket = this.waiters.get(p);
      if (!bucket) {
        bucket = [];
        this.waiters.set(p, bucket);
        this.pendingPaths.push(p);
      }
      bucket.push({ resolve, reject });
      if (!this.timer) {
        this.timer = setTimeout(() => this.flush(), this.windowMs);
      }
    });
  }

  /**
   * Drain pendingPaths + waiters atomically into a snapshot, reset
   * fresh state, then dispatch the readManyStat RPC. New arrivals
   * during the in-flight RPC populate the fresh state and are
   * dispatched by a future timer.
   */
  private async flush(): Promise<void> {
    const paths = this.pendingPaths;
    const waiters = this.waiters;
    this.pendingPaths = [];
    this.waiters = new Map();
    this.timer = null;
    try {
      const stats = await this.inner.readManyStat(paths);
      for (let i = 0; i < paths.length; i++) {
        const p = paths[i];
        const stat = stats[i];
        const bucket = waiters.get(p);
        if (!bucket) continue;
        if (stat === null) {
          // readManyStat returns null on miss; lstat's contract
          // throws ENOENT. Surface a typed error to each caller.
          // Construct via the class to retain instanceof matching.
          // Lazy-import to avoid a top-of-file cycle on errors.
          const err = makeENOENT(p);
          for (const w of bucket) w.reject(err);
        } else {
          for (const w of bucket) w.resolve(stat);
        }
      }
    } catch (err) {
      // RPC-level failure: reject every waiter in the snapshot
      // identically. mapServerError + isLikelyUnavailable on the
      // inner VFS already turned this into a typed error.
      for (const [, bucket] of waiters) {
        for (const w of bucket) w.reject(err);
      }
    }
  }

  // ── Pass-through methods ──────────────────────────────────────────────

  readFile(p: string): Promise<Uint8Array>;
  readFile(p: string, opts: { encoding: "utf8" }): Promise<string>;
  readFile(
    p: string,
    opts?: { encoding?: "utf8" }
  ): Promise<Uint8Array | string> {
    return opts?.encoding === "utf8"
      ? this.inner.readFile(p, { encoding: "utf8" })
      : this.inner.readFile(p);
  }
  readdir(p: string) { return this.inner.readdir(p); }
  stat(p: string) { return this.inner.stat(p); }
  exists(p: string) { return this.inner.exists(p); }
  readlink(p: string) { return this.inner.readlink(p); }
  readManyStat(paths: string[]) { return this.inner.readManyStat(paths); }
  fileInfo(p: string, opts?: Parameters<VFSClient["fileInfo"]>[1]) {
    return this.inner.fileInfo(p, opts);
  }
  writeFile(
    p: string,
    data: Uint8Array | string,
    opts?: Parameters<VFSClient["writeFile"]>[2]
  ) { return this.inner.writeFile(p, data, opts); }
  unlink(p: string) { return this.inner.unlink(p); }
  purge(p: string) { return this.inner.purge(p); }
  archive(p: string) { return this.inner.archive(p); }
  unarchive(p: string) { return this.inner.unarchive(p); }
  mkdir(
    p: string,
    opts?: { recursive?: boolean; mode?: number }
  ) { return this.inner.mkdir(p, opts); }
  rmdir(p: string) { return this.inner.rmdir(p); }
  removeRecursive(p: string) { return this.inner.removeRecursive(p); }
  symlink(target: string, p: string) {
    return this.inner.symlink(target, p);
  }
  chmod(p: string, mode: number) { return this.inner.chmod(p, mode); }
  rename(src: string, dst: string) { return this.inner.rename(src, dst); }
  createReadStream(
    p: string,
    opts?: { start?: number; end?: number }
  ) { return this.inner.createReadStream(p, opts); }
  createWriteStream(
    p: string,
    opts?: WriteFileOpts
  ) { return this.inner.createWriteStream(p, opts); }
  createWriteStreamWithHandle(
    p: string,
    opts?: WriteFileOpts
  ) { return this.inner.createWriteStreamWithHandle(p, opts); }
  openManifest(p: string) { return this.inner.openManifest(p); }
  openManifests(paths: string[]) { return this.inner.openManifests(paths); }
  readPreview(p: string, opts?: Parameters<VFSClient["readPreview"]>[1]) {
    return this.inner.readPreview(p, opts);
  }
  readChunk(p: string, idx: number) {
    return this.inner.readChunk(p, idx);
  }
  openReadStream(p: string) { return this.inner.openReadStream(p); }
  pullReadStream(
    handle: Parameters<VFSClient["pullReadStream"]>[0],
    chunkIndex: number,
    range?: { start?: number; end?: number }
  ) { return this.inner.pullReadStream(handle, chunkIndex, range); }
  // versioning pass-through.
  listVersions(p: string, opts?: Parameters<VFSClient["listVersions"]>[1]) {
    return this.inner.listVersions(p, opts);
  }
  restoreVersion(p: string, sourceVersionId: string) {
    return this.inner.restoreVersion(p, sourceVersionId);
  }
  dropVersions(
    p: string,
    policy: Parameters<VFSClient["dropVersions"]>[1]
  ) {
    return this.inner.dropVersions(p, policy);
  }
  // pass-throughs.
  patchMetadata(
    p: string,
    patch: Parameters<VFSClient["patchMetadata"]>[1],
    opts?: Parameters<VFSClient["patchMetadata"]>[2]
  ) {
    return this.inner.patchMetadata(p, patch, opts);
  }
  copyFile(
    src: string,
    dest: string,
    opts?: Parameters<VFSClient["copyFile"]>[2]
  ) {
    return this.inner.copyFile(src, dest, opts);
  }
  listFiles(opts?: Parameters<VFSClient["listFiles"]>[0]) {
    return this.inner.listFiles(opts);
  }
  markVersion(
    p: string,
    versionId: string,
    opts: Parameters<VFSClient["markVersion"]>[2]
  ) {
    return this.inner.markVersion(p, versionId, opts);
  }
  readYjsSnapshot(p: string) {
    return this.inner.readYjsSnapshot(p);
  }
  commitYjsSnapshot(
    p: string,
    doc: Parameters<VFSClient["commitYjsSnapshot"]>[1]
  ) {
    return this.inner.commitYjsSnapshot(p, doc);
  }
}

/** ENOENT factory for missing-path entries returned from readManyStat. */
function makeENOENT(p: string): ENOENT {
  return new ENOENT({ syscall: "lstat", path: p });
}

// Re-export VFS + createVFS so consumers can do
// `import { createVFS, createIgitFs } from "@mossaic/sdk/fs"` in one go.
export { VFS } from "./vfs";
export { createVFS } from "./index";
