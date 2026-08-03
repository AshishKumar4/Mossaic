/**
 * The published multipart methods, once, over an injected transport.
 *
 * `VFS` reaches the server through typed Durable Object RPC and `HttpVFS`
 * through `/api/vfs/multipart/*`, but *which* requests a method may issue, in
 * what order, under what budget, and what it returns are properties of the
 * protocol rather than of either transport. They live here so the two clients
 * cannot drift: each supplies the six requests below and nothing else.
 *
 * The policy every completion method follows:
 *
 *   - spend one request when one server invocation can finish the work, which
 *     is the common case and the reason a small upload still costs a single
 *     subrequest;
 *   - otherwise spend bounded pages, capped by the shared completion budget;
 *   - refuse before mutating anything when the work is knowably past that cap,
 *     naming the checkpointable pair to drive instead;
 *   - and if the cap is reached anyway — a publication displacing a manifest
 *     whose length nothing in the session predicted — surface the durable
 *     operation so the pages that did run are not repeated.
 */

import {
  completionBudgetExceeded,
  DEFAULT_COMPLETION_REQUEST_BUDGET,
  RequestBudget,
} from "./bounded-operation";
import { EINVAL, mapServerError, VFSFsError } from "./errors";
import {
  collectMultipartStatusPages,
  driveMultipartAbort,
  driveMultipartFinalize,
  multipartAbortFitsOneRequest,
  multipartFinalizeFitsOneRequest,
  multipartFinalizeRequestBound,
  multipartSessionShape,
  multipartStatusRequestBound,
  parseMultipartAbortResponse,
  parseMultipartBeginResponse,
  parseMultipartFinalizeResponse,
  parseMultipartStatusPageResponse,
  usesPagedMultipartProtocol,
  type MultipartAbortOperation,
  type MultipartFinalizeOperation,
  type MultipartSessionShape,
} from "./multipart-protocol";
import { MULTIPART_PROTOCOL_VERSION } from "../../shared/multipart";
import type {
  AbortMultipartUploadResult,
  BeginMultipartUploadOpts,
  BoundedAbortMultipartUploadResult,
  BoundedFinalizeMultipartUploadResult,
  FinalizeMultipartUploadResult,
  MultipartOperationOpts,
  MultipartStatusOpts,
  MultipartStatusPageOpts,
  MultipartUploadHandle,
  MultipartUploadStatus,
  MultipartUploadStatusPage,
  ResumeMultipartUploadOpts,
  ResumeMultipartUploadResult,
} from "./vfs";

/**
 * Begin/resume body both clients send, with the control plane declared.
 *
 * `signal` is deliberately absent: cancellation is the transport's business and
 * an `AbortSignal` is not something a Durable Object RPC can carry.
 */
export type MultipartBeginWireOpts = Omit<
  BeginMultipartUploadOpts,
  "signal"
> & {
  protocolVersion: number;
};

/**
 * The requests a client contributes. Each performs exactly one round-trip and
 * returns the server's answer unvalidated — validation belongs to the protocol.
 */
export interface MultipartTransport {
  beginUpload(
    path: string,
    opts: MultipartBeginWireOpts,
    signal?: AbortSignal
  ): Promise<unknown>;
  stageHashes(
    uploadId: string,
    startIndex: number,
    hashes: readonly string[],
    signal?: AbortSignal
  ): Promise<unknown>;
  finalizeOneRequest(
    uploadId: string,
    chunkHashList: readonly string[],
    signal?: AbortSignal
  ): Promise<unknown>;
  finalizeStep(uploadId: string, signal?: AbortSignal): Promise<unknown>;
  abortOneRequest(uploadId: string, signal?: AbortSignal): Promise<unknown>;
  abortStep(uploadId: string, signal?: AbortSignal): Promise<unknown>;
  statusPage(
    handle: MultipartUploadHandle,
    continuation: string | undefined,
    signal?: AbortSignal
  ): Promise<unknown>;
}

const FINALIZE_BOUNDED_APIS =
  "startFinalizeMultipartUpload() and stepFinalizeMultipartUpload()";
const ABORT_BOUNDED_APIS =
  "startAbortMultipartUpload() and stepAbortMultipartUpload()";
const STATUS_BOUNDED_APIS =
  "getMultipartUploadStatusPage() and its continuation";
const RESUME_BOUNDED_APIS =
  "resumeMultipartUploadPage() and getMultipartUploadStatusPage()";

/** What a status walk had left to read when the budget ran out. */
export interface MultipartStatusCheckpoint {
  continuation: string;
  landed: number[];
}

/** What a resume had left to read, including the session it already re-minted. */
export interface MultipartResumeCheckpoint extends MultipartStatusCheckpoint {
  handle: MultipartUploadHandle;
}

export class MultipartOperations {
  private readonly transport: MultipartTransport;

  constructor(transport: MultipartTransport) {
    this.transport = transport;
  }

  async begin(
    path: string,
    opts: BeginMultipartUploadOpts
  ): Promise<MultipartUploadHandle> {
    const { signal: _cancel, ...body } = opts;
    try {
      const response = parseMultipartBeginResponse(
        await this.transport.beginUpload(
          path,
          { ...body, protocolVersion: MULTIPART_PROTOCOL_VERSION },
          opts.signal
        )
      );
      opts.signal?.throwIfAborted();
      return multipartHandle(path, response, opts.size);
    } catch (err) {
      throw multipartError(err, opts.signal, path);
    }
  }

  async statusPage(
    handle: MultipartUploadHandle,
    opts: MultipartStatusPageOpts = {}
  ): Promise<MultipartUploadStatusPage> {
    try {
      opts.signal?.throwIfAborted();
      const page = parseMultipartStatusPageResponse(
        await this.transport.statusPage(handle, opts.continuation, opts.signal)
      );
      opts.signal?.throwIfAborted();
      return page;
    } catch (err) {
      throw multipartError(err, opts.signal, handle.path);
    }
  }

  async status(
    handle: MultipartUploadHandle,
    opts: MultipartStatusOpts = {}
  ): Promise<MultipartUploadStatus> {
    const budget = new RequestBudget(
      "status",
      DEFAULT_COMPLETION_REQUEST_BUDGET,
      opts
    );
    budget.throwIfAborted();
    this.refuseUnreadableStatus(
      "getMultipartUploadStatus",
      handle,
      budget,
      STATUS_BOUNDED_APIS
    );
    const first = await budget.spend(() => this.statusPage(handle, opts));
    if (!first.affordable) {
      throw completionBudgetExceeded({
        syscall: "getMultipartUploadStatus",
        requestBudget: budget.limit,
        boundedApis: STATUS_BOUNDED_APIS,
        path: handle.path,
      });
    }
    const complete = await this.followStatusPages(
      first.value,
      handle,
      budget,
      opts.signal
    );
    if (complete.continuation !== undefined) {
      throw completionBudgetExceeded<MultipartStatusCheckpoint>({
        syscall: "getMultipartUploadStatus",
        requestBudget: budget.limit,
        boundedApis: STATUS_BOUNDED_APIS,
        path: handle.path,
        checkpoint: {
          continuation: complete.continuation,
          landed: complete.landed,
        },
      });
    }
    const { continuation: _unread, ...status } = complete;
    return status;
  }

  async resumePage(
    handle: MultipartUploadHandle,
    opts: ResumeMultipartUploadOpts = {}
  ): Promise<ResumeMultipartUploadResult> {
    const size = handle.size ?? opts.size;
    if (size === undefined) {
      // The server holds the session's dimensions but insists the caller
      // restate its size, so a handle from before the field existed cannot
      // resume without being told.
      throw new EINVAL({
        syscall: "resumeMultipartUpload",
        path: handle.path,
      });
    }
    try {
      opts.signal?.throwIfAborted();
      const response = parseMultipartBeginResponse(
        await this.transport.beginUpload(
          handle.path,
          {
            size,
            chunkSize: handle.chunkSize,
            resumeFrom: handle.uploadId,
            protocolVersion: MULTIPART_PROTOCOL_VERSION,
            ...(opts.ttlMs === undefined ? {} : { ttlMs: opts.ttlMs }),
          },
          opts.signal
        )
      );
      opts.signal?.throwIfAborted();
      return {
        handle: multipartHandle(handle.path, response, size),
        landed: response.landed,
        ...(response.continuation === undefined
          ? {}
          : { continuation: response.continuation }),
      };
    } catch (err) {
      throw multipartError(err, opts.signal, handle.path);
    }
  }

  async resume(
    handle: MultipartUploadHandle,
    opts: ResumeMultipartUploadOpts = {}
  ): Promise<ResumeMultipartUploadResult> {
    const budget = new RequestBudget(
      "resume",
      DEFAULT_COMPLETION_REQUEST_BUDGET,
      opts
    );
    budget.throwIfAborted();
    // Re-minting extends the session's expiry and rotates its fence, so the
    // refusal has to come first to leave the session untouched.
    this.refuseUnreadableStatus(
      "resumeMultipartUpload",
      handle,
      budget,
      RESUME_BOUNDED_APIS
    );
    const first = await budget.spend(() => this.resumePage(handle, opts));
    if (!first.affordable) {
      throw completionBudgetExceeded({
        syscall: "resumeMultipartUpload",
        requestBudget: budget.limit,
        boundedApis: RESUME_BOUNDED_APIS,
        path: handle.path,
      });
    }
    const resumed = first.value.handle;
    const complete = await this.followStatusPages(
      {
        landed: first.value.landed,
        total: resumed.expectedChunks,
        // Resume reports which chunks landed, not how many bytes they were;
        // a caller that needs bytes reads status.
        bytesUploaded: 0,
        expiresAtMs: resumed.expiresAtMs,
        ...(first.value.continuation === undefined
          ? {}
          : { continuation: first.value.continuation }),
      },
      resumed,
      budget,
      opts.signal
    );
    if (complete.continuation !== undefined) {
      throw completionBudgetExceeded<MultipartResumeCheckpoint>({
        syscall: "resumeMultipartUpload",
        requestBudget: budget.limit,
        boundedApis: RESUME_BOUNDED_APIS,
        path: handle.path,
        checkpoint: {
          handle: resumed,
          continuation: complete.continuation,
          landed: complete.landed,
        },
      });
    }
    return { handle: resumed, landed: complete.landed };
  }

  async finalize(
    handle: MultipartUploadHandle,
    chunkHashList: readonly string[],
    opts: MultipartOperationOpts = {}
  ): Promise<FinalizeMultipartUploadResult> {
    const budget = new RequestBudget(
      "finalize",
      DEFAULT_COMPLETION_REQUEST_BUDGET,
      opts
    );
    budget.throwIfAborted();
    const shape = finalizeShape(handle, chunkHashList);
    if (
      this.pagedFinalizeOnly(handle, shape) &&
      multipartFinalizeRequestBound(shape) > budget.limit
    ) {
      throw completionBudgetExceeded({
        syscall: "finalizeMultipartUpload",
        requestBudget: budget.limit,
        boundedApis: FINALIZE_BOUNDED_APIS,
        path: handle.path,
      });
    }
    const result = await this.runFinalize(
      handle,
      chunkHashList,
      undefined,
      budget,
      opts
    );
    if ("operation" in result) {
      throw completionBudgetExceeded<MultipartFinalizeOperation>({
        syscall: "finalizeMultipartUpload",
        requestBudget: budget.limit,
        boundedApis: FINALIZE_BOUNDED_APIS,
        path: handle.path,
        checkpoint: result.operation,
      });
    }
    return result;
  }

  async boundedFinalize(
    handle: MultipartUploadHandle,
    chunkHashList: readonly string[],
    operation: MultipartFinalizeOperation | undefined,
    opts: MultipartOperationOpts = {}
  ): Promise<BoundedFinalizeMultipartUploadResult> {
    const budget = new RequestBudget(
      "finalize",
      DEFAULT_COMPLETION_REQUEST_BUDGET,
      opts
    );
    budget.throwIfAborted();
    return this.runFinalize(handle, chunkHashList, operation, budget, opts);
  }

  async abort(
    handle: MultipartUploadHandle,
    opts: MultipartOperationOpts = {}
  ): Promise<AbortMultipartUploadResult> {
    const budget = new RequestBudget(
      "abort",
      DEFAULT_COMPLETION_REQUEST_BUDGET,
      opts
    );
    budget.throwIfAborted();
    const shape = sessionShape(handle, "abortMultipartUpload");
    if (
      usesPagedMultipartProtocol(handle.protocolVersion) &&
      !multipartAbortFitsOneRequest(shape)
    ) {
      // Nothing has been fenced yet, so the session is exactly as the caller
      // left it and the bounded pair starts from the beginning.
      throw completionBudgetExceeded({
        syscall: "abortMultipartUpload",
        requestBudget: budget.limit,
        boundedApis: ABORT_BOUNDED_APIS,
        path: handle.path,
      });
    }
    let result: BoundedAbortMultipartUploadResult;
    try {
      result = await this.runAbort(handle, undefined, budget, opts);
    } catch (err) {
      // A session that already finalized cannot be un-finalized, and one the
      // server has never heard of needs nothing done to it. Both are the
      // documented idempotent answer rather than a failure — which is a
      // promise only this convenience form makes; a caller driving the bounded
      // pair is owed the refusal.
      const code = err instanceof VFSFsError ? err.code : undefined;
      if (code === "ENOENT" || code === "EBUSY") return { aborted: false };
      throw err;
    }
    if ("operation" in result) {
      throw completionBudgetExceeded<MultipartAbortOperation>({
        syscall: "abortMultipartUpload",
        requestBudget: budget.limit,
        boundedApis: ABORT_BOUNDED_APIS,
        path: handle.path,
        checkpoint: result.operation,
      });
    }
    return result;
  }

  async boundedAbort(
    handle: MultipartUploadHandle,
    operation: MultipartAbortOperation | undefined,
    opts: MultipartOperationOpts = {}
  ): Promise<BoundedAbortMultipartUploadResult> {
    const budget = new RequestBudget(
      "abort",
      DEFAULT_COMPLETION_REQUEST_BUDGET,
      opts
    );
    budget.throwIfAborted();
    return this.runAbort(handle, operation, budget, opts);
  }

  // ── Shared bodies ────────────────────────────────────────────────────

  /**
   * Whether this session can only be finalized through the paged plane.
   *
   * A server that never offered the plane leaves one request as the only
   * option — and refused an upload it could not finalize that way at begin, so
   * there is nothing to preflight here.
   */
  private pagedFinalizeOnly(
    handle: MultipartUploadHandle,
    shape: MultipartSessionShape
  ): boolean {
    return (
      usesPagedMultipartProtocol(handle.protocolVersion) &&
      !multipartFinalizeFitsOneRequest(shape)
    );
  }

  private async runFinalize(
    handle: MultipartUploadHandle,
    chunkHashList: readonly string[],
    operation: MultipartFinalizeOperation | undefined,
    budget: RequestBudget,
    opts: MultipartOperationOpts
  ): Promise<BoundedFinalizeMultipartUploadResult> {
    const shape = finalizeShape(handle, chunkHashList);
    try {
      if (!this.pagedFinalizeOnly(handle, shape)) {
        if (operation !== undefined) {
          // The one-request form has no cursor to resume from.
          throw new EINVAL({ syscall: "finalizeMultipartUpload" });
        }
        const one = await budget.spend(() =>
          this.transport.finalizeOneRequest(
            handle.uploadId,
            chunkHashList,
            opts.signal
          )
        );
        if (!one.affordable) {
          throw completionBudgetExceeded({
            syscall: "finalizeMultipartUpload",
            requestBudget: budget.limit,
            boundedApis: FINALIZE_BOUNDED_APIS,
            path: handle.path,
          });
        }
        budget.report({ phase: "done" });
        return finalizedResult(parseMultipartFinalizeResponse(one.value));
      }
      const result = await driveMultipartFinalize(
        {
          uploadId: handle.uploadId,
          chunkHashList,
          ...(operation === undefined ? {} : { operation }),
          stageHashes: (startIndex, hashes) =>
            this.transport.stageHashes(
              handle.uploadId,
              startIndex,
              hashes,
              opts.signal
            ),
          finalizeStep: () =>
            this.transport.finalizeStep(handle.uploadId, opts.signal),
        },
        budget
      );
      return result.done
        ? finalizedResult(result.result)
        : { operation: result.state };
    } catch (err) {
      throw multipartError(err, opts.signal, handle.path);
    }
  }

  private async runAbort(
    handle: MultipartUploadHandle,
    operation: MultipartAbortOperation | undefined,
    budget: RequestBudget,
    opts: MultipartOperationOpts
  ): Promise<BoundedAbortMultipartUploadResult> {
    try {
      if (!usesPagedMultipartProtocol(handle.protocolVersion)) {
        if (operation !== undefined) {
          throw new EINVAL({ syscall: "abortMultipartUpload" });
        }
        const one = await budget.spend(() =>
          this.transport.abortOneRequest(handle.uploadId, opts.signal)
        );
        if (!one.affordable) {
          throw completionBudgetExceeded({
            syscall: "abortMultipartUpload",
            requestBudget: budget.limit,
            boundedApis: ABORT_BOUNDED_APIS,
            path: handle.path,
          });
        }
        parseMultipartAbortResponse(one.value);
        budget.report({ phase: "done" });
        return { aborted: true };
      }
      const result = await driveMultipartAbort(
        {
          uploadId: handle.uploadId,
          ...(operation === undefined ? {} : { operation }),
          abortStep: () =>
            this.transport.abortStep(handle.uploadId, opts.signal),
        },
        budget
      );
      return result.done ? { aborted: true } : { operation: result.state };
    } catch (err) {
      throw multipartError(err, opts.signal, handle.path);
    }
  }

  /** Refuse a landed set the budget could not read in full. */
  private refuseUnreadableStatus(
    syscall: string,
    handle: MultipartUploadHandle,
    budget: RequestBudget,
    boundedApis: string
  ): void {
    const shape = sessionShape(handle, syscall);
    if (multipartStatusRequestBound(shape) > budget.limit) {
      throw completionBudgetExceeded({
        syscall,
        requestBudget: budget.limit,
        boundedApis,
        path: handle.path,
      });
    }
  }

  private async followStatusPages(
    first: MultipartUploadStatusPage,
    handle: MultipartUploadHandle,
    budget: RequestBudget,
    signal: AbortSignal | undefined
  ): Promise<MultipartUploadStatusPage> {
    try {
      return await collectMultipartStatusPages(
        first,
        (continuation) =>
          this.transport.statusPage(handle, continuation, signal),
        budget
      );
    } catch (err) {
      throw multipartError(err, signal, handle.path);
    }
  }
}

// ── Shared helpers ─────────────────────────────────────────────────────

/**
 * Assemble the caller-facing handle from a begin or resume response.
 *
 * The handle's numbers are informational: routing, verification and every
 * server-side bound come from the signed session token, and the fields here
 * only tell the client which path to take.
 */
function multipartHandle(
  path: string,
  response: {
    uploadId: string;
    chunkSize: number;
    totalChunks: number;
    poolSize: number;
    sessionToken: string;
    expiresAtMs: number;
    protocolVersion?: number;
  },
  size: number
): MultipartUploadHandle {
  return {
    uploadId: response.uploadId,
    path,
    chunkSize: response.chunkSize,
    expectedChunks: response.totalChunks,
    poolSize: response.poolSize,
    sessionToken: response.sessionToken,
    expiresAtMs: response.expiresAtMs,
    size,
    ...(response.protocolVersion === undefined
      ? {}
      : { protocolVersion: response.protocolVersion }),
  };
}

function sessionShape(
  handle: MultipartUploadHandle,
  syscall: string
): MultipartSessionShape {
  return multipartSessionShape(
    handle.expectedChunks,
    handle.poolSize,
    syscall
  );
}

/**
 * The shape a finalize is costed against.
 *
 * The declared manifest's length is what the server will verify, and it is
 * checked against the session's chunk count server-side, so a mismatch is its
 * verdict to give rather than a reason to cost the call differently.
 */
function finalizeShape(
  handle: MultipartUploadHandle,
  chunkHashList: readonly string[]
): MultipartSessionShape {
  return multipartSessionShape(
    chunkHashList.length,
    handle.poolSize,
    "finalizeMultipartUpload"
  );
}

/**
 * The completed shape `finalizeMultipartUpload` has always returned.
 *
 * `versionId` is empty because the server's finalize response does not carry
 * one; callers that need the version row follow up with `listVersions`.
 */
function finalizedResult(result: {
  path: string;
  fileId: string;
  size: number;
  fileHash: string;
  isEncrypted: boolean;
}): FinalizeMultipartUploadResult {
  return {
    path: result.path,
    pathId: result.fileId,
    versionId: "",
    size: result.size,
    fileHash: result.fileHash,
    isEncrypted: result.isEncrypted,
  };
}

/**
 * The typed error a failed multipart request surfaces as.
 *
 * A caller who cancelled is owed their own abort reason rather than a syscall
 * verdict read out of the request cancellation caused, so that case rethrows
 * instead of mapping.
 */
function multipartError(
  err: unknown,
  signal: AbortSignal | undefined,
  path: string
): VFSFsError {
  if (signal?.aborted) throw err;
  return mapServerError(err, { path, syscall: "open" });
}
