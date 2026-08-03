/**
 * Client half of bounded version retention.
 *
 * The server applies a retention policy a bounded page of history at a time
 * and keys the whole walk on an operation id the caller owns. Everything about
 * driving that — minting the id, turning one step's wire shape into progress,
 * and running steps until the operation finishes — is transport-independent, so
 * the binding client and the HTTP client share it here and differ only in how
 * one step is issued.
 */

import { generateId } from "../../worker/core/lib/utils";
import { mapServerError } from "./errors";
import type { DropVersionsStepResult } from "../../shared/vfs-types";

/**
 * Handle for one durable retention operation.
 *
 * `operationId` is what makes retention resumable: the server keys the
 * operation on it, so repeating a step with the same handle continues the same
 * walk of the history — after a lost response, an evicted object, or a client
 * that came back much later — instead of starting a second pass.
 */
export interface DropVersionsOperation {
  readonly kind: "drop-versions";
  readonly operationId: string;
}

/** One bounded retention step: more work remains, or these are the counts. */
export type DropVersionsProgress =
  | { done: false; operation: DropVersionsOperation }
  | {
      done: true;
      operation: DropVersionsOperation;
      dropped: number;
      kept: number;
    };

/**
 * Bounded steps `dropVersions` drives before it asks the caller to take over.
 * At 128 versions a step this covers histories far deeper than any interactive
 * caller has, while keeping the subrequests one convenience call can spend
 * finite.
 */
export const DROP_VERSIONS_STEP_BUDGET = 64;

/** Mint the handle a retention operation is keyed on for its whole life. */
export function newDropVersionsOperation(): DropVersionsOperation {
  return { kind: "drop-versions", operationId: `dv-${generateId()}` };
}

export function dropVersionsProgress(
  operation: DropVersionsOperation,
  step: DropVersionsStepResult
): DropVersionsProgress {
  return step.done
    ? { done: true, operation, dropped: step.dropped, kept: step.kept }
    : { done: false, operation };
}

/** Counts a completed retention reports, whichever surface produced them. */
interface DropVersionsCounts {
  dropped: number;
  kept: number;
}

/** One bounded step. `undefined` asks for the first, which mints the operation. */
export type DropVersionsStepper = (
  operation: DropVersionsOperation | undefined
) => Promise<DropVersionsProgress>;

/**
 * Apply a retention policy through whichever surface the server has.
 *
 * The one-call form goes first, because it is the contract every deployed
 * server answers — a client newer than its server keeps working, and a history
 * that fits in one bounded invocation costs one request. A server that does
 * have the bounded surface refuses what it cannot finish in one invocation with
 * `EFBIG`, and that refusal is the signal to drive the steps instead. Either
 * way the caller gets counts, so the published shape never changes.
 *
 * Every error is normalised through `mapServerError`, including the legacy
 * call's, so the typed `VFSFsError` contract holds across both surfaces.
 */
export async function applyDropVersions(
  path: string,
  legacy: () => Promise<DropVersionsCounts>,
  step: DropVersionsStepper
): Promise<DropVersionsCounts> {
  try {
    return await legacy();
  } catch (err) {
    const refusal = mapServerError(err, { path, syscall: "dropVersions" });
    if (refusal.code !== "EFBIG") throw refusal;
  }
  return await driveDropVersions(path, step);
}

/**
 * Run bounded retention steps until the operation finishes or the budget runs
 * out.
 *
 * Exhausting the budget is reported as `EFBIG` through `mapServerError`, so a
 * caller sees the same typed error surface it would from any other refusal —
 * and every completed step is durable, so taking over with `startDropVersions`
 * picks up rather than restarts.
 */
async function driveDropVersions(
  path: string,
  step: DropVersionsStepper
): Promise<DropVersionsCounts> {
  let progress = await step(undefined);
  for (
    let request = 1;
    !progress.done && request < DROP_VERSIONS_STEP_BUDGET;
    request++
  ) {
    progress = await step(progress.operation);
  }
  if (progress.done) {
    return { dropped: progress.dropped, kept: progress.kept };
  }
  throw mapServerError(
    Object.assign(
      new Error(
        `EFBIG: dropVersions: history outlasted ${DROP_VERSIONS_STEP_BUDGET} ` +
          "bounded steps; drive startDropVersions / stepDropVersions to finish it"
      ),
      { code: "EFBIG" }
    ),
    { path, syscall: "dropVersions" }
  );
}
