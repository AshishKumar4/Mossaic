/**
 * Client half of the bounded multipart control plane.
 *
 * The server exposes multipart finalize, abort and status as durable machines
 * that advance one bounded page per call. Everything about driving them —
 * what a page's response is allowed to say, how many requests one pass may
 * spend, which continuation or operation resumes it, and what "this pass
 * completed the work" means — is transport-independent, so the binding client
 * and the HTTP client share it here and differ only in how one page is issued.
 *
 * Every response is validated before it is read. A page is the one thing
 * standing between a caller and an unbounded scan, so a server that answers
 * with a landed set larger than the protocol allows, a total that changes
 * between pages, or a continuation that leads back to itself is refused rather
 * than followed.
 */

import { z } from "zod/v4";

import {
  MULTIPART_HASH_PAGE_SIZE,
  MULTIPART_ONE_REQUEST_ABORT_MAX_PAGES,
  MULTIPART_ONE_REQUEST_FINALIZE_MAX_FANOUT,
  MULTIPART_PROTOCOL_VERSION,
  MULTIPART_STATUS_CURSOR_MAX_BYTES,
  MULTIPART_STATUS_ENTRY_PAGE_SIZE,
  multipartAbortPageCount,
  multipartFinalizeFanout,
  multipartFinalizeRequestCount,
  multipartStatusPageCount,
  type MultipartAbortProgress,
  type MultipartBeginResponse,
  type MultipartFinalizeProgress,
  type MultipartFinalizeResponse,
  type MultipartHashPageResponse,
  type MultipartPutChunkResponse,
  type MultipartStatusPageResponse,
} from "../../shared/multipart";
import { EINVAL, MossaicUnavailableError } from "./errors";
import type { BoundedResult, RequestBudget } from "./bounded-operation";

// ── Response validation ────────────────────────────────────────────────

function invalidMultipartResponse(message: string): MossaicUnavailableError {
  return new MossaicUnavailableError({
    message: `EMOSSAIC_UNAVAILABLE: invalid multipart response: ${message}`,
  });
}

/**
 * Narrow a server response, or refuse it.
 *
 * A response the SDK cannot make sense of means the thing on the other end is
 * not speaking this protocol, which is an availability failure rather than a
 * caller mistake — so it surfaces as `EMOSSAIC_UNAVAILABLE` naming the field
 * that failed, not as an untyped `TypeError` escaping the SDK's error contract.
 */
function parseWire<T>(schema: z.ZodType<T>, raw: unknown, what: string): T {
  const parsed = schema.safeParse(raw);
  if (parsed.success) return parsed.data;
  throw invalidMultipartResponse(`${what}: ${parsed.error.message}`);
}

const Count = z.number().int().nonnegative();
const NonEmpty = z.string().min(1);
const Sha256Hex = z.string().regex(/^[0-9a-f]{64}$/);
const Continuation = z.string().min(1).max(MULTIPART_STATUS_CURSOR_MAX_BYTES);
const ChunkStatus = z.enum(["created", "deduplicated", "superseded"]);
const LandedIndices = z.array(Count).max(MULTIPART_STATUS_ENTRY_PAGE_SIZE);

/**
 * Hold one page of landed indices to the bounds paging exists to enforce:
 * each index below the session's chunk count, and each reported once.
 *
 * The page-size ceiling is on `LandedIndices` itself — a server that answers
 * with the whole set at once has defeated the point of a page.
 */
function refineLandedPage(
  landed: readonly number[],
  total: number,
  ctx: z.RefinementCtx
): void {
  const seen = new Set<number>();
  for (const index of landed) {
    if (index >= total) {
      ctx.addIssue({
        code: "custom",
        message: `landed index ${index} is not below total ${total}`,
      });
      return;
    }
    if (seen.has(index)) {
      ctx.addIssue({
        code: "custom",
        message: `landed index ${index} was reported twice`,
      });
      return;
    }
    seen.add(index);
  }
}

const StatusPageResponse = z
  .object({
    landed: LandedIndices,
    total: Count,
    bytesUploaded: Count,
    expiresAtMs: Count,
    continuation: Continuation.optional(),
  })
  .superRefine((page, ctx) => {
    refineLandedPage(page.landed, page.total, ctx);
  });

const BeginResponse = z
  .object({
    uploadId: NonEmpty,
    chunkSize: Count,
    totalChunks: Count,
    poolSize: z.number().int().min(1),
    sessionToken: NonEmpty,
    putEndpoint: NonEmpty,
    expiresAtMs: Count,
    landed: LandedIndices,
    continuation: Continuation.optional(),
    recommendedConcurrency: z.number().int().min(1).optional(),
    // The SDK declares exactly one control plane, so an echo of any other
    // version is a server whose finalize this client cannot drive.
    protocolVersion: z.literal(MULTIPART_PROTOCOL_VERSION).optional(),
  })
  .superRefine((response, ctx) => {
    refineLandedPage(response.landed, response.totalChunks, ctx);
  });

const HashPageResponse = z
  .object({ staged: Count, total: Count })
  .refine((page) => page.staged <= page.total, {
    message: "staged exceeds total",
  });

const PutChunkResponse = z.object({
  ok: z.literal(true),
  hash: Sha256Hex,
  idx: Count,
  bytesAccepted: Count,
  status: ChunkStatus,
});

const ShardPutResponse = z.object({
  status: ChunkStatus,
  bytesStored: Count,
});

const AbortResponse = z.object({ ok: z.literal(true) });

const AbortProgress = z.union([
  z.object({ done: z.literal(true) }),
  z.object({
    done: z.literal(false),
    phase: z.enum(["fencing", "intents", "cleanup", "old_intents", "local"]),
    cursor: Count,
    total: Count,
  }),
]);

const FinalizeResponse = z.object({
  fileId: NonEmpty,
  size: Count,
  chunkCount: Count,
  fileHash: Sha256Hex,
  path: NonEmpty,
  mimeType: NonEmpty,
  isEncrypted: z.boolean(),
});

const FinalizeProgress = z.union([
  z.object({
    done: z.literal(true),
    result: FinalizeResponse,
    fresh: z.boolean().default(false),
  }),
  z.object({
    done: z.literal(false),
    phase: z.enum([
      "fencing",
      "verifying",
      "preparing",
      "publishing",
      "cleaning",
    ]),
    cursor: Count,
    total: Count,
  }),
]);

export function parseMultipartBeginResponse(
  raw: unknown
): MultipartBeginResponse {
  return parseWire(BeginResponse, raw, "begin");
}

export function parseMultipartStatusPageResponse(
  raw: unknown
): MultipartStatusPageResponse {
  return parseWire(StatusPageResponse, raw, "status page");
}

export function parseMultipartHashPageResponse(
  raw: unknown
): MultipartHashPageResponse {
  return parseWire(HashPageResponse, raw, "hash page");
}

export function parseMultipartPutChunkResponse(
  raw: unknown
): MultipartPutChunkResponse {
  return parseWire(PutChunkResponse, raw, "put chunk");
}

export function parseMultipartShardPutResponse(raw: unknown): {
  status: "created" | "deduplicated" | "superseded";
  bytesStored: number;
} {
  return parseWire(ShardPutResponse, raw, "shard put");
}

export function parseMultipartAbortResponse(raw: unknown): { ok: true } {
  return parseWire(AbortResponse, raw, "abort");
}

export function parseMultipartAbortProgress(
  raw: unknown
): MultipartAbortProgress {
  return parseWire(AbortProgress, raw, "abort step");
}

export function parseMultipartFinalizeResponse(
  raw: unknown
): MultipartFinalizeResponse {
  return parseWire(FinalizeResponse, raw, "finalize");
}

export function parseMultipartFinalizeProgress(
  raw: unknown
): MultipartFinalizeProgress {
  return parseWire(FinalizeProgress, raw, "finalize step");
}

// ── Protocol negotiation and preflight ─────────────────────────────────

/**
 * Whether the session's server offered the paged control plane.
 *
 * `beginMultipartUpload` declares it and the server echoes the version it
 * accepted, so a handle minted against a server that predates the paged plane
 * carries none — and its finalize and abort stay the single requests they have
 * always been.
 */
export function usesPagedMultipartProtocol(
  protocolVersion: number | undefined
): boolean {
  return protocolVersion === MULTIPART_PROTOCOL_VERSION;
}

/** Session dimensions the request-count arithmetic is derived from. */
export interface MultipartSessionShape {
  readonly chunkCount: number;
  readonly poolSize: number;
}

/**
 * The dimensions a handle carries, refused if they are not dimensions at all.
 *
 * A handle round-trips through caller memory and often through JSON, so the
 * numbers the budget arithmetic reads are checked before it reads them: a
 * `NaN` pool size would otherwise make every "does this fit" comparison answer
 * `false` and quietly disable the ceiling. They only ever decide which path to
 * take — placement and verification come from the signed session token — so a
 * caller who edits them costs itself requests rather than correctness.
 */
export function multipartSessionShape(
  chunkCount: number,
  poolSize: number,
  syscall: string
): MultipartSessionShape {
  if (
    !Number.isSafeInteger(chunkCount) ||
    chunkCount < 0 ||
    !Number.isSafeInteger(poolSize) ||
    poolSize < 1
  ) {
    throw new EINVAL({ syscall });
  }
  return { chunkCount, poolSize };
}

/**
 * Whether one server invocation can finalize this shape.
 *
 * The server applies the same ceiling and refuses the call otherwise, so
 * asking here is what lets a session that fits keep costing exactly one
 * request while a session that does not goes straight to the paged plane
 * instead of spending a round-trip to be told so.
 */
export function multipartFinalizeFitsOneRequest(
  shape: MultipartSessionShape
): boolean {
  return (
    multipartFinalizeFanout(shape.chunkCount, shape.poolSize) <=
    MULTIPART_ONE_REQUEST_FINALIZE_MAX_FANOUT
  );
}

/** Whether one server invocation can run every page this abort owes. */
export function multipartAbortFitsOneRequest(
  shape: MultipartSessionShape
): boolean {
  return (
    multipartAbortPageCount(shape.chunkCount, shape.poolSize) <=
    MULTIPART_ONE_REQUEST_ABORT_MAX_PAGES
  );
}

/**
 * Requests driving this shape's finalize through the paged plane costs.
 *
 * This is the fresh path's cost. A publication that displaces an existing file
 * owes further pages over that file's manifest, whose length this session's
 * dimensions say nothing about — so a finalize inside this bound can still run
 * out, which is what the returned operation is for.
 */
export function multipartFinalizeRequestBound(
  shape: MultipartSessionShape
): number {
  return multipartFinalizeRequestCount(shape.chunkCount, shape.poolSize);
}

/** Status pages this shape's landed set is spread across. */
export function multipartStatusRequestBound(
  shape: MultipartSessionShape
): number {
  return multipartStatusPageCount(shape.chunkCount, shape.poolSize);
}

// ── Durable checkpoints ────────────────────────────────────────────────

/**
 * Where a bounded finalize left off.
 *
 * The server holds the only cursor over its own machine; what the client owns
 * is how much of the declared manifest it has handed over, which is why that
 * is the one number here.
 */
export interface MultipartFinalizeOperation {
  readonly kind: "multipart-finalize";
  readonly uploadId: string;
  readonly nextHashIndex: number;
}

/** Where a bounded abort left off — entirely in the server's cursor. */
export interface MultipartAbortOperation {
  readonly kind: "multipart-abort";
  readonly uploadId: string;
}

// ── State machines ─────────────────────────────────────────────────────

/** The two requests a bounded finalize is composed of. */
export interface MultipartFinalizeTransport {
  readonly uploadId: string;
  readonly chunkHashList: readonly string[];
  readonly operation?: MultipartFinalizeOperation;
  stageHashes(startIndex: number, hashes: readonly string[]): Promise<unknown>;
  finalizeStep(): Promise<unknown>;
}

/**
 * Drive a finalize as far as the budget allows.
 *
 * The declared manifest goes over in pages first — a finalize has to survive
 * eviction, so it cannot be handed the whole list in its last request — and
 * then the machine advances a page per request until it publishes and reaps
 * what publication left behind. Running out of budget is not a failure: every
 * page that completed is durable, so the returned operation resumes the same
 * finalize rather than starting a second one.
 */
export async function driveMultipartFinalize(
  transport: MultipartFinalizeTransport,
  budget: RequestBudget
): Promise<
  BoundedResult<MultipartFinalizeResponse, MultipartFinalizeOperation>
> {
  const total = transport.chunkHashList.length;
  const resumed = transport.operation;
  if (
    resumed !== undefined &&
    (resumed.kind !== "multipart-finalize" ||
      resumed.uploadId !== transport.uploadId ||
      !Number.isSafeInteger(resumed.nextHashIndex) ||
      resumed.nextHashIndex < 0 ||
      resumed.nextHashIndex > total)
  ) {
    throw new EINVAL({ syscall: "finalizeMultipartUpload" });
  }
  let nextHashIndex = resumed?.nextHashIndex ?? 0;
  const checkpoint = (): BoundedResult<
    MultipartFinalizeResponse,
    MultipartFinalizeOperation
  > => ({
    done: false,
    state: {
      kind: "multipart-finalize",
      uploadId: transport.uploadId,
      nextHashIndex,
    },
  });

  while (nextHashIndex < total) {
    const start = nextHashIndex;
    const end = Math.min(start + MULTIPART_HASH_PAGE_SIZE, total);
    const page = await budget.spend(() =>
      transport.stageHashes(start, transport.chunkHashList.slice(start, end))
    );
    if (!page.affordable) return checkpoint();
    const staged = parseMultipartHashPageResponse(page.value);
    if (staged.total !== total || staged.staged < end) {
      throw invalidMultipartResponse(
        `staged ${staged.staged}/${staged.total} does not cover the ` +
          `${total}-hash manifest through index ${end}`
      );
    }
    nextHashIndex = staged.staged;
    budget.report({
      phase: "staging",
      completed: nextHashIndex,
      total,
    });
  }

  for (;;) {
    const step = await budget.spend(() => transport.finalizeStep());
    if (!step.affordable) return checkpoint();
    const progress = parseMultipartFinalizeProgress(step.value);
    if (progress.done) {
      budget.report({ phase: "done" });
      return { done: true, result: progress.result };
    }
    budget.report({
      phase: progress.phase,
      completed: progress.cursor,
      total: progress.total,
    });
  }
}

/** The single request a bounded abort is composed of. */
export interface MultipartAbortTransport {
  readonly uploadId: string;
  readonly operation?: MultipartAbortOperation;
  abortStep(): Promise<unknown>;
}

/**
 * Drive an abort as far as the budget allows.
 *
 * A step is safe to repeat: the server refuses a page whose row already moved
 * rather than applying it twice, and an already-terminal session answers
 * `done` without doing anything.
 */
export async function driveMultipartAbort(
  transport: MultipartAbortTransport,
  budget: RequestBudget
): Promise<BoundedResult<{ ok: true }, MultipartAbortOperation>> {
  const resumed = transport.operation;
  if (
    resumed !== undefined &&
    (resumed.kind !== "multipart-abort" ||
      resumed.uploadId !== transport.uploadId)
  ) {
    throw new EINVAL({ syscall: "abortMultipartUpload" });
  }
  const checkpoint: BoundedResult<{ ok: true }, MultipartAbortOperation> = {
    done: false,
    state: { kind: "multipart-abort", uploadId: transport.uploadId },
  };
  for (;;) {
    const step = await budget.spend(() => transport.abortStep());
    if (!step.affordable) return checkpoint;
    const progress = parseMultipartAbortProgress(step.value);
    if (progress.done) {
      budget.report({ phase: "done" });
      return { done: true, result: { ok: true } };
    }
    budget.report({
      phase: progress.phase,
      completed: progress.cursor,
      total: progress.total,
    });
  }
}

/**
 * Follow status continuations as far as the budget allows.
 *
 * The returned page carries a continuation exactly when pages remain, so a
 * caller either has the complete landed set or is told, explicitly, what is
 * still unread. Silently returning the pages that fit would have a resumed
 * upload re-PUT every chunk the unread pages knew about.
 *
 * `first` has already been paid for by the caller — it is the status page, or
 * the resume response, that started the walk — and the budget it was paid from
 * is what names the operation this progress belongs to.
 */
export async function collectMultipartStatusPages(
  first: MultipartStatusPageResponse,
  fetchPage: (continuation: string) => Promise<unknown>,
  budget: RequestBudget
): Promise<MultipartStatusPageResponse> {
  const landed = new Set<number>(first.landed);
  const followed = new Set<string>();
  let bytesUploaded = first.bytesUploaded;
  let page = first;
  const report = (): void =>
    budget.report({
      phase: page.continuation === undefined ? "done" : "paging",
      completed: landed.size,
      total: page.total,
    });
  report();

  while (page.continuation !== undefined) {
    const continuation = page.continuation;
    if (followed.has(continuation)) {
      throw invalidMultipartResponse("continuation leads back to itself");
    }
    followed.add(continuation);
    const next = await budget.spend(() => fetchPage(continuation));
    if (!next.affordable) break;
    page = parseMultipartStatusPageResponse(next.value);
    if (page.total !== first.total) {
      throw invalidMultipartResponse(
        `total changed between pages (${first.total} then ${page.total})`
      );
    }
    for (const index of page.landed) {
      if (landed.has(index)) {
        throw invalidMultipartResponse(
          `landed index ${index} was reported by two pages`
        );
      }
      landed.add(index);
    }
    bytesUploaded += page.bytesUploaded;
    report();
  }

  return {
    landed: [...landed].sort((left, right) => left - right),
    total: first.total,
    bytesUploaded,
    expiresAtMs: page.expiresAtMs,
    ...(page.continuation === undefined
      ? {}
      : { continuation: page.continuation }),
  };
}
