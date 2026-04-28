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
  vfsReadFile(scope: VFSScope, path: string): Promise<Uint8Array>;
  vfsWriteFile(
    scope: VFSScope,
    path: string,
    data: Uint8Array,
    opts?: { mode?: number; mimeType?: string }
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
}

import { vfsUserDOName } from "../../worker/lib/utils";

/**
 * fs/promises-shaped client.
 *
 * Every method maps to one DO RPC. Errors are normalized to
 * `VFSFsError` subclasses with Node-fs-like `code` / `errno` /
 * `syscall` / `path`.
 */
export class VFS {
  /** Self-reference so `vfs.promises === vfs` (isomorphic-git wants `.promises`). */
  readonly promises: VFS;

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
  async readFile(
    p: string,
    opts?: { encoding?: "utf8" }
  ): Promise<Uint8Array | string> {
    let buf: Uint8Array;
    try {
      buf = await this.user().vfsReadFile(this.scope(), p);
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
    opts?: { mode?: number; mimeType?: string }
  ): Promise<void> {
    const bytes =
      typeof data === "string" ? new TextEncoder().encode(data) : data;
    try {
      await this.user().vfsWriteFile(this.scope(), p, bytes, opts);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "open" });
    }
  }

  async unlink(p: string): Promise<void> {
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

  async chmod(p: string, mode: number): Promise<void> {
    try {
      await this.user().vfsChmod(this.scope(), p, mode);
    } catch (err) {
      throw mapServerError(err, { path: p, syscall: "chmod" });
    }
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
}
