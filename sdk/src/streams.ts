/**
 * Stream helpers used by the SDK's `VFS` class.
 *
 * The DO RPC surface returns native Web Streams (`ReadableStream` /
 * `WritableStream`) over the binding boundary. The SDK's only job is
 * to await the RPC method and surface the stream to the consumer, with
 * mapped errors.
 *
 * For `createWriteStream`, the DO RPC returns `{ stream, handle }` so
 * the consumer can grab the handle for resumable / progress use cases.
 * The SDK's default surface returns just the `WritableStream` (matches
 * the plan §5.2 signature); a separate `createWriteStreamWithHandle`
 * exposes the handle when needed.
 */

import { mapServerError } from "./errors";
import type { VFSScope } from "../../shared/vfs-types";
import type { UserDOClient, WriteFileOpts } from "./vfs";

export interface ReadStreamOptions {
  /** Inclusive starting byte offset. */
  start?: number;
  /** Exclusive ending byte offset. */
  end?: number;
}

/**
 * streaming writes accept the same opts shape as `writeFile`,
 * including `metadata`, `tags`, and `version`. The opts are validated
 * at begin-time (caller fails fast on cap violations) and applied at
 * commit-time (rename-from-tmp).
 */
export type WriteStreamOpts = WriteFileOpts;

export async function createReadStreamRpc(
  user: UserDOClient,
  scope: VFSScope,
  path: string,
  opts?: ReadStreamOptions
): Promise<ReadableStream<Uint8Array>> {
  try {
    return await user.vfsCreateReadStream(scope, path, opts);
  } catch (err) {
    throw mapServerError(err, { path, syscall: "open" });
  }
}

export async function createWriteStreamRpc(
  user: UserDOClient,
  scope: VFSScope,
  path: string,
  opts?: WriteStreamOpts
): Promise<WritableStream<Uint8Array>> {
  try {
    const res = await user.vfsCreateWriteStream(scope, path, opts);
    return res.stream;
  } catch (err) {
    throw mapServerError(err, { path, syscall: "open" });
  }
}

/**
 * `createWriteStreamWithHandle` — variant that surfaces the underlying
 * handle so callers can pause/resume across separate consumer
 * invocations or surface progress (handle.tmpId is stable for the
 * lifetime of the write).
 */
export async function createWriteStreamWithHandleRpc(
  user: UserDOClient,
  scope: VFSScope,
  path: string,
  opts?: WriteStreamOpts
): Promise<{ stream: WritableStream<Uint8Array>; handle: WriteHandle }> {
  try {
    return await user.vfsCreateWriteStream(scope, path, opts);
  } catch (err) {
    throw mapServerError(err, { path, syscall: "open" });
  }
}

/** Re-export of the server-side handle shape for SDK consumers. */
export interface WriteHandle {
  tmpId: string;
  parentId: string | null;
  leaf: string;
  chunkSize: number;
  poolSize: number;
}

export interface ReadHandle {
  fileId: string;
  size: number;
  chunkCount: number;
  /**
   * server-authoritative chunk size, in bytes. 0 for inlined / yjs /
   * empty files. Pinned by the worker at `vfsOpenReadStream` time so
   * downstream chunk math is stable across concurrent writes.
   */
  chunkSize: number;
  inlined: boolean;
  /**
   * Phase 25 — when set, the worker resolves chunks via
   * `version_chunks` keyed by this versionId. Captured at open-time
   * so a concurrent write doesn't move the head out from under an
   * in-flight stream. Undefined for non-versioned tenants.
   */
  versionId?: string;
  /**
   * Phase 27.5 — pre-materialized bytes for yjs-mode files. Yjs
   * content lives in the op-log + checkpoint pair, NOT in
   * file/version chunks; the worker materializes the live `Y.Doc`
   * once at open-time and stashes the bytes here. When present,
   * `inlined === true` and `chunkCount === 0`.
   */
  inlineBytes?: Uint8Array;
}
