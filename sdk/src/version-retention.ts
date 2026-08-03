/**
 * Client half of bounded version retention.
 *
 * The server applies a retention policy a bounded page of history at a time
 * and keys the whole walk on an operation id the caller owns. Everything about
 * driving that — minting the id, validating one step's wire shape, and
 * spending a request budget on steps until the operation finishes — is
 * transport-independent, so the binding client and the HTTP client share it
 * here and differ only in how one step is issued.
 */

import { generateId } from "../../worker/core/lib/utils";
import {
  completionBudgetExceeded,
  RequestBudget,
  type BoundedResult,
} from "./bounded-operation";
import { mapServerError } from "./errors";
import {
  parseDropVersionsResult,
  parseDropVersionsStepResult,
  type DropVersionsResult,
} from "../../shared/vfs-types";

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

const DROP_VERSIONS_BOUNDED_APIS =
  "startDropVersions() and stepDropVersions()";

/** Mint the handle a retention operation is keyed on for its whole life. */
export function newDropVersionsOperation(): DropVersionsOperation {
  return { kind: "drop-versions", operationId: `dv-${generateId()}` };
}

/** Turn one validated step response into the published progress shape. */
export function dropVersionsProgress(
  operation: DropVersionsOperation,
  step: unknown
): DropVersionsProgress {
  const parsed = parseDropVersionsStepResult(step);
  return parsed.done
    ? { done: true, operation, dropped: parsed.dropped, kept: parsed.kept }
    : { done: false, operation };
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
 * `EFBIG`, and that refusal is the signal to spend the budget on steps
 * instead. Either way the caller gets counts, so the published shape never
 * changes.
 *
 * A history that outlasts the budget is refused with the operation attached,
 * so the caller takes over from where the steps got to rather than from the
 * beginning. Every error is normalised through `mapServerError`, including the
 * legacy call's, so the typed `VFSFsError` contract holds across both surfaces.
 */
export async function applyDropVersions(
  path: string,
  legacy: () => Promise<unknown>,
  step: DropVersionsStepper,
  budget: RequestBudget
): Promise<DropVersionsResult> {
  const oneCall = await budget.spend(async () => {
    try {
      return parseDropVersionsResult(await legacy());
    } catch (err) {
      // A caller who cancelled mid-call is owed their abort, not a retention
      // verdict read out of it.
      budget.throwIfAborted();
      const refusal = mapServerError(err, { path, syscall: "dropVersions" });
      if (refusal.code !== "EFBIG") throw refusal;
      return undefined;
    }
  });
  if (oneCall.affordable && oneCall.value !== undefined) {
    budget.report({ phase: "done" });
    return oneCall.value;
  }
  const stepped = await driveDropVersions(step, budget);
  if (stepped.done) return stepped.result;
  throw completionBudgetExceeded({
    syscall: "dropVersions",
    requestBudget: budget.limit,
    boundedApis: DROP_VERSIONS_BOUNDED_APIS,
    path,
    checkpoint: stepped.state,
  });
}

/**
 * Run bounded retention steps until the operation finishes or the budget runs
 * out.
 *
 * Exhausting the budget is not a failure: every step that completed is
 * durable, so the returned operation continues the same walk.
 */
async function driveDropVersions(
  step: DropVersionsStepper,
  budget: RequestBudget
): Promise<BoundedResult<DropVersionsResult, DropVersionsOperation>> {
  let operation: DropVersionsOperation | undefined;
  for (;;) {
    const pending = operation;
    const outcome = await budget.spend(() => step(pending));
    if (!outcome.affordable) {
      // The budget can only run out after a step landed, because the first
      // spend is what mints the operation this returns.
      if (operation === undefined) {
        throw completionBudgetExceeded({
          syscall: "dropVersions",
          requestBudget: budget.limit,
          boundedApis: DROP_VERSIONS_BOUNDED_APIS,
        });
      }
      return { done: false, state: operation };
    }
    const progress = outcome.value;
    operation = progress.operation;
    budget.report({ phase: "step" });
    if (progress.done) {
      return {
        done: true,
        result: { dropped: progress.dropped, kept: progress.kept },
      };
    }
  }
}
