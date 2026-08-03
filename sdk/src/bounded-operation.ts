/**
 * Request budgets for the SDK's bounded completion methods.
 *
 * A completion method drives a durable server-side machine one request at a
 * time, and the only honest way to do that from inside a Worker invocation is
 * with a fixed ceiling on the requests it may spend. This module owns that
 * ceiling: counting requests, refusing the one that would exceed it,
 * cancelling between them, and reporting progress that never goes backwards.
 *
 * It knows nothing about multipart, retention, or transport — the protocol
 * modules inject what one request is, so the binding client and the HTTP
 * client share one accounting policy instead of each growing their own.
 */

import { CompletionBudgetExceededError } from "./errors";

/**
 * Page/RPC requests one default completion method may issue.
 *
 * Shared by `finalizeMultipartUpload`, `abortMultipartUpload`,
 * `getMultipartUploadStatus`, `resumeMultipartUpload` and `dropVersions`, so
 * "how much can one convenience call cost me" has a single answer. Work past
 * it is refused with `CompletionBudgetExceededError`, which names the bounded
 * start/step pair that has no ceiling.
 */
export const DEFAULT_COMPLETION_REQUEST_BUDGET = 16;

/** Outcome of one bounded pass: the result, or the state that resumes it. */
export type BoundedResult<Result, State> =
  | { done: true; result: Result }
  | { done: false; state: State };

/** The bounded operations a budget can be opened for. */
export type BoundedOperationName =
  | "finalize"
  | "abort"
  | "status"
  | "resume"
  | "dropVersions";

/** Progress of one bounded operation, reported per request it spends. */
export interface BoundedOperationProgress {
  operation: BoundedOperationName;
  /** Server-reported phase, or `"done"` on the request that completed it. */
  phase: string;
  requestsUsed: number;
  requestBudget: number;
  /** Present together with `total`, and never lower than last reported. */
  completed?: number;
  total?: number;
}

export interface BoundedOperationOptions {
  signal?: AbortSignal;
  onProgress?: (progress: BoundedOperationProgress) => void;
}

/** What one request would have cost more budget than remained. */
const BUDGET_GONE = { affordable: false } as const;

/**
 * The requests one bounded pass may spend, and the cancellation that bounds
 * it further.
 *
 * `spend` is the only way to issue a request, so `spent` is always the truth
 * about what the pass cost, and a caller cannot accidentally issue the request
 * that would overrun. Cancellation is checked before and after each request:
 * before, so a cancelled pass issues nothing more; after, so a caller that
 * cancelled during a request is not handed its result as if it had waited.
 */
export class RequestBudget {
  readonly limit: number;
  private readonly signal?: AbortSignal;
  private used = 0;
  private readonly operation: BoundedOperationName;
  private readonly onProgress?: (progress: BoundedOperationProgress) => void;
  /** Last reported (completed, total) per phase, for monotonicity. */
  private readonly reported = new Map<
    string,
    { completed: number; total: number }
  >();

  constructor(
    operation: BoundedOperationName,
    limit: number,
    opts: BoundedOperationOptions = {}
  ) {
    if (!Number.isSafeInteger(limit) || limit < 1) {
      throw new TypeError("request budget must be a positive integer");
    }
    this.operation = operation;
    this.limit = limit;
    this.signal = opts.signal;
    this.onProgress = opts.onProgress;
  }

  /** Throw the caller's abort reason if it has cancelled. */
  throwIfAborted(): void {
    this.signal?.throwIfAborted();
  }

  async spend<T>(
    request: () => Promise<T>
  ): Promise<{ affordable: true; value: T } | { affordable: false }> {
    this.throwIfAborted();
    if (this.used >= this.limit) return BUDGET_GONE;
    this.used++;
    const value = await request();
    this.throwIfAborted();
    return { affordable: true, value };
  }

  /**
   * Report progress for the request just spent.
   *
   * A phase whose `total` is unchanged never reports a lower `completed` than
   * it already did, so a caller driving a progress bar cannot be made to
   * rewind by a replayed page. A phase whose total moves is one whose length
   * the server only learns by reaching the end of it — that resets rather than
   * clamps, because the old high-water mark was measured against a different
   * whole.
   */
  report(
    progress: Omit<
      BoundedOperationProgress,
      "operation" | "requestsUsed" | "requestBudget"
    >
  ): void {
    if (this.onProgress === undefined) return;
    let completed = progress.completed;
    if (completed !== undefined && progress.total !== undefined) {
      if (completed > progress.total) {
        throw new TypeError("operation progress exceeds its total");
      }
      const prior = this.reported.get(progress.phase);
      if (prior !== undefined && prior.total === progress.total) {
        completed = Math.max(prior.completed, completed);
      }
      this.reported.set(progress.phase, { completed, total: progress.total });
    }
    this.onProgress({
      ...progress,
      ...(completed === undefined ? {} : { completed }),
      operation: this.operation,
      requestsUsed: this.used,
      requestBudget: this.limit,
    });
    this.throwIfAborted();
  }
}

/**
 * Refuse work a request budget cannot finish.
 *
 * `checkpoint` is supplied only when the server already made progress durable,
 * so the message tells the caller what to drive instead and the checkpoint
 * tells them where it left off. A refusal raised before anything was mutated
 * carries none, because there is nothing to resume.
 */
export function completionBudgetExceeded<Checkpoint>(opts: {
  syscall: string;
  requestBudget: number;
  boundedApis: string;
  path?: string;
  checkpoint?: Checkpoint;
}): CompletionBudgetExceededError<Checkpoint> {
  return new CompletionBudgetExceededError<Checkpoint>({
    syscall: opts.syscall,
    message:
      `EFBIG: ${opts.syscall} needs more than the ` +
      `${opts.requestBudget}-request completion budget; ` +
      `drive ${opts.boundedApis} instead`,
    ...(opts.path === undefined ? {} : { path: opts.path }),
    ...(opts.checkpoint === undefined ? {} : { checkpoint: opts.checkpoint }),
  });
}
