import { MossaicUnavailableError } from "./errors";
import {
  MULTIPART_HASH_PAGE_SIZE,
  MULTIPART_FENCE_PAGE_SIZE,
  MULTIPART_PAGED_CONTROL_CAPABILITY,
  MULTIPART_PROTOCOL_VERSION,
  MULTIPART_STATUS_CURSOR_MAX_BYTES,
  MULTIPART_STATUS_ENTRY_PAGE_SIZE,
  MULTIPART_STATUS_SHARD_PAGE_SIZE,
  type MultipartAbortProgress,
  type MultipartBeginResponse,
  type MultipartFinalizeProgress,
  type MultipartFinalizeResponse,
  type MultipartHashPageResponse,
  type MultipartPutChunkResponse,
  type MultipartStatusPageResponse,
} from "../../shared/multipart";
import {
  parseDropVersionsResult,
  parseDropVersionsStepResult,
  type DropVersionsResult,
} from "../../shared/vfs-types";

/** Maximum page/RPC requests issued by one SDK completion call. */
export const DEFAULT_COMPLETION_REQUEST_BUDGET = 16;

/** @deprecated Use {@link DEFAULT_COMPLETION_REQUEST_BUDGET}. */
export const MULTIPART_OPERATION_RPC_BUDGET =
  DEFAULT_COMPLETION_REQUEST_BUDGET;

/** Retries available to each idempotent protocol request. */
export const MULTIPART_OPERATION_RETRY_BUDGET = 2;

function invalidResponse(message: string): never {
  throw new MossaicUnavailableError({
    message: `invalid multipart response: ${message}`,
  });
}

function record(value: unknown, name: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    invalidResponse(`${name} must be an object`);
  }
  return value as Record<string, unknown>;
}

function stringField(value: unknown, name: string): string {
  if (typeof value !== "string" || value.length === 0) {
    invalidResponse(`${name} must be a non-empty string`);
  }
  return value;
}

function integerField(value: unknown, name: string, minimum = 0): number {
  if (!Number.isSafeInteger(value) || (value as number) < minimum) {
    invalidResponse(`${name} must be an integer >= ${minimum}`);
  }
  return value as number;
}

export function parseMultipartBeginResponse(
  value: unknown
): MultipartBeginResponse {
  const raw = record(value, "begin response");
  const protocolVersion =
    raw.protocolVersion === undefined
      ? undefined
      : integerField(raw.protocolVersion, "protocolVersion", 1);
  if (
    protocolVersion !== undefined &&
    protocolVersion !== MULTIPART_PROTOCOL_VERSION
  ) {
    invalidResponse(`unsupported protocolVersion ${protocolVersion}`);
  }
  if (!Array.isArray(raw.landed)) invalidResponse("landed must be an array");
  if (
    raw.capabilities !== undefined &&
    (!Array.isArray(raw.capabilities) ||
      raw.capabilities.some((capability) => typeof capability !== "string"))
  ) {
    invalidResponse("capabilities must be a string array");
  }
  const continuation = optionalContinuation(raw.continuation);
  if (
    continuation !== undefined &&
    raw.landed.length > MULTIPART_STATUS_ENTRY_PAGE_SIZE
  ) {
    invalidResponse("landed page exceeds protocol limit");
  }
  const landed = raw.landed.map((index, offset) =>
    integerField(index, `landed[${offset}]`)
  );
  return {
    uploadId: stringField(raw.uploadId, "uploadId"),
    chunkSize: integerField(raw.chunkSize, "chunkSize"),
    totalChunks: integerField(raw.totalChunks, "totalChunks"),
    poolSize: integerField(raw.poolSize, "poolSize", 1),
    sessionToken: stringField(raw.sessionToken, "sessionToken"),
    putEndpoint: stringField(raw.putEndpoint, "putEndpoint"),
    expiresAtMs: integerField(raw.expiresAtMs, "expiresAtMs"),
    landed,
    ...(continuation === undefined ? {} : { continuation }),
    ...(protocolVersion === undefined ? {} : { protocolVersion }),
    ...(raw.capabilities === undefined
      ? {}
      : { capabilities: raw.capabilities as string[] }),
    ...(raw.recommendedConcurrency === undefined
      ? {}
      : {
          recommendedConcurrency: integerField(
            raw.recommendedConcurrency,
            "recommendedConcurrency",
            1
          ),
        }),
  };
}

function optionalContinuation(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > MULTIPART_STATUS_CURSOR_MAX_BYTES
  ) {
    invalidResponse("continuation is invalid");
  }
  return value;
}

export function parseMultipartStatusPageResponse(
  value: unknown
): MultipartStatusPageResponse {
  const raw = record(value, "status-page response");
  if (!Array.isArray(raw.landed)) invalidResponse("landed must be an array");
  const continuation = optionalContinuation(raw.continuation);
  if (
    continuation !== undefined &&
    raw.landed.length > MULTIPART_STATUS_ENTRY_PAGE_SIZE
  ) {
    invalidResponse("landed page exceeds protocol limit");
  }
  const landed = raw.landed.map((index, offset) =>
    integerField(index, `landed[${offset}]`)
  );
  return {
    landed,
    total: integerField(raw.total, "total"),
    bytesUploaded: integerField(raw.bytesUploaded, "bytesUploaded"),
    expiresAtMs: integerField(raw.expiresAtMs, "expiresAtMs"),
    ...(continuation === undefined ? {} : { continuation }),
  };
}

export function validateMultipartStatusPage(
  page: MultipartStatusPageResponse
): void {
  const landed = new Set<number>();
  for (const index of page.landed) {
    if (index >= page.total || landed.has(index)) {
      invalidResponse("landed indices must be unique and below total");
    }
    landed.add(index);
  }
}

export async function collectMultipartStatusPages(
  first: MultipartStatusPageResponse,
  fetchPage: (continuation: string) => Promise<MultipartStatusPageResponse>,
  pageBudget: number,
  onPage?: (
    page: MultipartStatusPageResponse,
    pagesRead: number,
    landedRead: number
  ) => void,
  signal?: AbortSignal
): Promise<MultipartStatusPageResponse> {
  if (!Number.isSafeInteger(pageBudget) || pageBudget < 1) {
    invalidResponse("page budget must be a positive integer");
  }
  signal?.throwIfAborted();
  validateMultipartStatusPage(first);
  const landed = [...first.landed];
  const landedSet = new Set<number>();
  for (const index of landed) {
    landedSet.add(index);
  }
  let bytesUploaded = first.bytesUploaded;
  let page = first;
  let pagesRead = 1;
  const seen = new Set<string>();
  onPage?.(first, pagesRead, landed.length);
  signal?.throwIfAborted();
  while (
    page.continuation !== undefined &&
    pagesRead < pageBudget
  ) {
    const continuation = page.continuation;
    if (seen.has(continuation)) {
      invalidResponse("continuation cycle detected");
    }
    seen.add(continuation);
    signal?.throwIfAborted();
    page = await fetchPage(continuation);
    signal?.throwIfAborted();
    pagesRead++;
    if (page.total !== first.total) {
      invalidResponse("status total changed between pages");
    }
    validateMultipartStatusPage(page);
    for (const index of page.landed) {
      if (index >= first.total || landedSet.has(index)) {
        invalidResponse("landed indices must be unique and below total");
      }
      landedSet.add(index);
    }
    landed.push(...page.landed);
    bytesUploaded += page.bytesUploaded;
    onPage?.(page, pagesRead, landed.length);
    signal?.throwIfAborted();
  }
  landed.sort((left, right) => left - right);
  const result: MultipartStatusPageResponse = {
    landed,
    total: first.total,
    bytesUploaded,
    expiresAtMs: page.expiresAtMs,
  };
  if (page.continuation !== undefined) {
    result.continuation = page.continuation;
  }
  return result;
}

function pageCount(total: number, pageSize: number): number {
  if (!Number.isSafeInteger(total) || total < 0) {
    invalidResponse("operation size must be a non-negative integer");
  }
  return Math.ceil(total / pageSize);
}

function shardPageCount(poolSize: number): number {
  if (!Number.isSafeInteger(poolSize) || poolSize < 1) {
    invalidResponse("multipart pool size must be a positive integer");
  }
  return Math.ceil(poolSize / MULTIPART_STATUS_SHARD_PAGE_SIZE);
}

/** Maximum status pages implied by a server-issued multipart handle. */
export function multipartStatusPageUpperBound(
  totalChunks: number,
  poolSize: number
): number {
  pageCount(totalChunks, MULTIPART_STATUS_ENTRY_PAGE_SIZE);
  return (
    Math.floor(totalChunks / MULTIPART_STATUS_ENTRY_PAGE_SIZE) +
    shardPageCount(poolSize)
  );
}

/** Known request upper bound for finalizing a new multipart path. */
export function multipartFinalizeRequestUpperBound(
  totalChunks: number,
  poolSize: number
): number {
  const hashPages = pageCount(totalChunks, MULTIPART_HASH_PAGE_SIZE);
  if (!Number.isSafeInteger(poolSize) || poolSize < 1) {
    invalidResponse("multipart pool size must be a positive integer");
  }
  const poolPages = Math.ceil(poolSize / MULTIPART_FENCE_PAGE_SIZE);
  if (hashPages === 0) return poolPages * 2 + 1;
  const cleanupRoutePages = Math.ceil(
    Math.min(totalChunks, poolSize) / MULTIPART_FENCE_PAGE_SIZE
  );
  return (
    hashPages +
    poolPages +
    hashPages * poolPages +
    1 +
    cleanupRoutePages +
    hashPages
  );
}

/** Known request upper bound for aborting an open multipart session. */
export function multipartAbortRequestUpperBound(
  totalChunks: number,
  poolSize: number
): number {
  if (!Number.isSafeInteger(poolSize) || poolSize < 1) {
    invalidResponse("multipart pool size must be a positive integer");
  }
  const poolPages = Math.ceil(poolSize / MULTIPART_FENCE_PAGE_SIZE);
  const cleanupPages = Math.max(
    1,
    pageCount(totalChunks, MULTIPART_HASH_PAGE_SIZE)
  );
  return poolPages * 2 + cleanupPages + 1;
}

export function parseMultipartHashPageResponse(
  value: unknown
): MultipartHashPageResponse {
  const raw = record(value, "hash-page response");
  const staged = integerField(raw.staged, "staged");
  const total = integerField(raw.total, "total");
  if (staged > total) invalidResponse("staged exceeds total");
  return { staged, total };
}

export function parseMultipartPutChunkResponse(
  value: unknown
): MultipartPutChunkResponse {
  const raw = record(value, "put-chunk response");
  const hash = stringField(raw.hash, "hash");
  if (!/^[0-9a-f]{64}$/.test(hash)) invalidResponse("hash must be SHA-256 hex");
  if (
    raw.ok !== true ||
    (raw.status !== "created" &&
      raw.status !== "deduplicated" &&
      raw.status !== "superseded")
  ) {
    invalidResponse("put-chunk status is invalid");
  }
  return {
    ok: true,
    hash,
    idx: integerField(raw.idx, "idx"),
    bytesAccepted: integerField(raw.bytesAccepted, "bytesAccepted"),
    status: raw.status,
  };
}

export function parseMultipartShardPutResponse(value: unknown): {
  status: "created" | "deduplicated" | "superseded";
  bytesStored: number;
} {
  const raw = record(value, "shard put response");
  if (
    raw.status !== "created" &&
    raw.status !== "deduplicated" &&
    raw.status !== "superseded"
  ) {
    invalidResponse("shard put status is invalid");
  }
  return {
    status: raw.status,
    bytesStored: integerField(raw.bytesStored, "bytesStored"),
  };
}

export function parseMultipartAbortResponse(value: unknown): { ok: true } {
  const raw = record(value, "abort response");
  if (raw.ok !== true) invalidResponse("abort response must contain ok=true");
  return { ok: true };
}

export function parseMultipartAbortProgress(
  value: unknown
): MultipartAbortProgress {
  const raw = record(value, "abort-step response");
  if (raw.done === true) return { done: true };
  if (raw.done !== false) invalidResponse("done must be boolean");
  if (
    raw.phase !== "fencing" &&
    raw.phase !== "intents" &&
    raw.phase !== "cleanup" &&
    raw.phase !== "old_intents" &&
    raw.phase !== "local"
  ) {
    invalidResponse("abort phase is invalid");
  }
  return {
    done: false,
    phase: raw.phase,
    cursor: integerField(raw.cursor, "cursor"),
    total: integerField(raw.total, "total"),
  };
}

export function parseMultipartFinalizeResponse(
  value: unknown
): MultipartFinalizeResponse {
  const raw = record(value, "finalize response");
  const fileHash = stringField(raw.fileHash, "fileHash");
  if (!/^[0-9a-f]{64}$/.test(fileHash)) {
    invalidResponse("fileHash must be lowercase SHA-256 hex");
  }
  if (typeof raw.isEncrypted !== "boolean") {
    invalidResponse("isEncrypted must be boolean");
  }
  return {
    fileId: stringField(raw.fileId, "fileId"),
    versionId:
      raw.versionId === undefined
        ? ""
        : typeof raw.versionId === "string"
          ? raw.versionId
          : invalidResponse("versionId must be a string"),
    size: integerField(raw.size, "size"),
    chunkCount: integerField(raw.chunkCount, "chunkCount"),
    fileHash,
    path: stringField(raw.path, "path"),
    mimeType: stringField(raw.mimeType, "mimeType"),
    isEncrypted: raw.isEncrypted,
  };
}

export function parseMultipartFinalizeProgress(
  value: unknown
): MultipartFinalizeProgress {
  const raw = record(value, "finalize-step response");
  if (raw.done === true) {
    return {
      done: true,
      result: parseMultipartFinalizeResponse(raw.result),
      fresh: raw.fresh === true,
    };
  }
  if (raw.done !== false) invalidResponse("done must be boolean");
  if (
    raw.phase !== "fencing" &&
    raw.phase !== "verifying" &&
    raw.phase !== "preparing" &&
    raw.phase !== "publishing" &&
    raw.phase !== "cleaning"
  ) {
    invalidResponse("phase is invalid");
  }
  return {
    done: false,
    phase: raw.phase,
    cursor: integerField(raw.cursor, "cursor"),
    total: integerField(raw.total, "total"),
  };
}

export function usesPagedMultipartProtocol(
  protocolVersion: number | undefined,
  capabilities?: readonly string[]
): boolean {
  if (capabilities?.includes(MULTIPART_PAGED_CONTROL_CAPABILITY)) return true;
  if (protocolVersion === undefined) return false;
  if (protocolVersion !== MULTIPART_PROTOCOL_VERSION) {
    invalidResponse(`unsupported protocolVersion ${protocolVersion}`);
  }
  return true;
}

export function throwIfMultipartAborted(signal: AbortSignal | undefined): void {
  signal?.throwIfAborted();
}

export interface MultipartFinalizeProtocolState {
  kind: "multipart-finalize";
  uploadId: string;
  nextHashIndex: number;
}

export interface MultipartAbortProtocolState {
  kind: "multipart-abort";
  uploadId: string;
}

export interface DropVersionsProtocolState {
  kind: "drop-versions";
  operationId: string;
}

export interface MultipartProtocolProgress {
  operation: "finalize" | "abort";
  phase: string;
  requestsUsed: number;
  requestBudget: number;
  completed?: number;
  total?: number;
}

export interface DropVersionsProtocolProgress {
  operation: "dropVersions";
  requestsUsed: number;
  requestBudget: number;
}

export type BoundedProtocolResult<Result, State> =
  | { done: true; result: Result }
  | { done: false; state: State };

interface ProtocolExecutionOptions<Progress> {
  signal?: AbortSignal;
  requestBudget: number;
  retryBudget?: number;
  isRetryable?: (error: unknown) => boolean;
  onProgress?: (progress: Progress) => void;
}

interface ProtocolExecution {
  requestsUsed: number;
  readonly requestBudget: number;
  readonly retryBudget: number;
  readonly signal?: AbortSignal;
  readonly isRetryable: (error: unknown) => boolean;
}

type ProtocolRequestResult<Result> =
  | { available: true; value: Result }
  | { available: false };

function protocolExecution<Progress>(
  options: ProtocolExecutionOptions<Progress>
): ProtocolExecution {
  if (!Number.isSafeInteger(options.requestBudget) || options.requestBudget < 1) {
    invalidResponse("request budget must be a positive integer");
  }
  const retryBudget =
    options.retryBudget ?? MULTIPART_OPERATION_RETRY_BUDGET;
  if (!Number.isSafeInteger(retryBudget) || retryBudget < 0) {
    invalidResponse("retry budget must be a non-negative integer");
  }
  return {
    requestsUsed: 0,
    requestBudget: options.requestBudget,
    retryBudget,
    signal: options.signal,
    isRetryable: options.isRetryable ?? (() => false),
  };
}

async function protocolRequest<Result>(
  execution: ProtocolExecution,
  request: () => Promise<unknown>,
  parse: (value: unknown) => Result
): Promise<ProtocolRequestResult<Result>> {
  let retriesRemaining = execution.retryBudget;
  for (;;) {
    execution.signal?.throwIfAborted();
    if (execution.requestsUsed >= execution.requestBudget) {
      return { available: false };
    }
    execution.requestsUsed++;
    try {
      const value = parse(await request());
      execution.signal?.throwIfAborted();
      return { available: true, value };
    } catch (error) {
      if (execution.signal?.aborted) throw error;
      if (retriesRemaining === 0 || !execution.isRetryable(error)) throw error;
      retriesRemaining--;
    }
  }
}

function reportMultipartProgress(
  options: ProtocolExecutionOptions<MultipartProtocolProgress>,
  execution: ProtocolExecution,
  progress: Omit<
    MultipartProtocolProgress,
    "requestsUsed" | "requestBudget"
  >,
  previous: Map<string, { completed: number; total: number }>
): void {
  let normalized = progress;
  if (progress.completed !== undefined || progress.total !== undefined) {
    if (progress.completed === undefined || progress.total === undefined) {
      invalidResponse("progress completed and total must be reported together");
    }
    const prior = previous.get(progress.phase);
    if (progress.completed > progress.total) {
      invalidResponse("operation progress exceeds its total");
    }
    const completed =
      prior?.total === progress.total
        ? Math.max(prior.completed, progress.completed)
        : progress.completed;
    previous.set(progress.phase, {
      completed,
      total: progress.total,
    });
    normalized = { ...progress, completed };
  }
  options.onProgress?.({
    ...normalized,
    requestsUsed: execution.requestsUsed,
    requestBudget: execution.requestBudget,
  });
  execution.signal?.throwIfAborted();
}

export interface MultipartFinalizeProtocolOptions
  extends ProtocolExecutionOptions<MultipartProtocolProgress> {
  uploadId: string;
  chunkHashList: readonly string[];
  state?: MultipartFinalizeProtocolState;
  stageHashes: (
    startIndex: number,
    hashes: readonly string[]
  ) => Promise<unknown>;
  finalizeStep: () => Promise<unknown>;
}

export async function driveMultipartFinalize(
  options: MultipartFinalizeProtocolOptions
): Promise<
  BoundedProtocolResult<
    MultipartFinalizeResponse,
    MultipartFinalizeProtocolState
  >
> {
  const execution = protocolExecution(options);
  const progress = new Map<string, { completed: number; total: number }>();
  const state = options.state ?? {
    kind: "multipart-finalize" as const,
    uploadId: options.uploadId,
    nextHashIndex: 0,
  };
  if (
    state.kind !== "multipart-finalize" ||
    state.uploadId !== options.uploadId ||
    !Number.isSafeInteger(state.nextHashIndex) ||
    state.nextHashIndex < 0 ||
    state.nextHashIndex > options.chunkHashList.length
  ) {
    invalidResponse("finalize operation state is invalid");
  }

  let nextHashIndex = state.nextHashIndex;
  while (nextHashIndex < options.chunkHashList.length) {
    const end = Math.min(
      nextHashIndex + MULTIPART_HASH_PAGE_SIZE,
      options.chunkHashList.length
    );
    const page = await protocolRequest(
      execution,
      () => options.stageHashes(nextHashIndex, options.chunkHashList.slice(nextHashIndex, end)),
      parseMultipartHashPageResponse
    );
    if (!page.available) {
      return {
        done: false,
        state: { ...state, nextHashIndex },
      };
    }
    if (
      page.value.total !== options.chunkHashList.length ||
      page.value.staged < end ||
      page.value.staged > page.value.total
    ) {
      invalidResponse("staged hash progress is inconsistent");
    }
    nextHashIndex = page.value.staged;
    reportMultipartProgress(
      options,
      execution,
      {
        operation: "finalize",
        phase: "staging",
        completed: nextHashIndex,
        total: options.chunkHashList.length,
      },
      progress
    );
  }

  for (;;) {
    const step = await protocolRequest(
      execution,
      options.finalizeStep,
      parseMultipartFinalizeProgress
    );
    if (!step.available) {
      return {
        done: false,
        state: { ...state, nextHashIndex },
      };
    }
    reportMultipartProgress(
      options,
      execution,
      step.value.done
        ? { operation: "finalize", phase: "done" }
        : {
            operation: "finalize",
            phase: step.value.phase,
            completed: step.value.cursor,
            total: step.value.total,
          },
      progress
    );
    if (step.value.done) return { done: true, result: step.value.result };
  }
}

export interface MultipartAbortProtocolOptions
  extends ProtocolExecutionOptions<MultipartProtocolProgress> {
  uploadId: string;
  state?: MultipartAbortProtocolState;
  abortStep: () => Promise<unknown>;
}

export async function driveMultipartAbort(
  options: MultipartAbortProtocolOptions
): Promise<BoundedProtocolResult<{ ok: true }, MultipartAbortProtocolState>> {
  const execution = protocolExecution(options);
  const progress = new Map<string, { completed: number; total: number }>();
  const state = options.state ?? {
    kind: "multipart-abort" as const,
    uploadId: options.uploadId,
  };
  if (state.kind !== "multipart-abort" || state.uploadId !== options.uploadId) {
    invalidResponse("abort operation state is invalid");
  }
  for (;;) {
    const step = await protocolRequest(
      execution,
      options.abortStep,
      parseMultipartAbortProgress
    );
    if (!step.available) return { done: false, state };
    reportMultipartProgress(
      options,
      execution,
      step.value.done
        ? { operation: "abort", phase: "done" }
        : {
            operation: "abort",
            phase: step.value.phase,
            completed: step.value.cursor,
            total: step.value.total,
          },
      progress
    );
    if (step.value.done) return { done: true, result: { ok: true } };
  }
}

export interface DropVersionsProtocolOptions
  extends ProtocolExecutionOptions<DropVersionsProtocolProgress> {
  state: DropVersionsProtocolState;
  dropVersionsStep: () => Promise<unknown>;
  dropVersionsLegacy?: () => Promise<unknown>;
  isStepUnsupported?: (error: unknown) => boolean;
}

export async function driveDropVersions(
  options: DropVersionsProtocolOptions
): Promise<BoundedProtocolResult<DropVersionsResult, DropVersionsProtocolState>> {
  const execution = protocolExecution(options);
  if (
    options.state.kind !== "drop-versions" ||
    options.state.operationId.length === 0
  ) {
    invalidResponse("dropVersions operation state is invalid");
  }
  for (;;) {
    let step: ProtocolRequestResult<ReturnType<typeof parseDropVersionsStepResult>>;
    try {
      step = await protocolRequest(
        execution,
        options.dropVersionsStep,
        parseDropVersionsStepResult
      );
    } catch (error) {
      if (
        options.dropVersionsLegacy === undefined ||
        !options.isStepUnsupported?.(error)
      ) {
        throw error;
      }
      const legacy = await protocolRequest(
        execution,
        options.dropVersionsLegacy,
        parseDropVersionsResult
      );
      if (!legacy.available) return { done: false, state: options.state };
      options.onProgress?.({
        operation: "dropVersions",
        requestsUsed: execution.requestsUsed,
        requestBudget: execution.requestBudget,
      });
      execution.signal?.throwIfAborted();
      return { done: true, result: legacy.value };
    }
    if (!step.available) return { done: false, state: options.state };
    options.onProgress?.({
      operation: "dropVersions",
      requestsUsed: execution.requestsUsed,
      requestBudget: execution.requestBudget,
    });
    execution.signal?.throwIfAborted();
    if (step.value.done) {
      return {
        done: true,
        result: { dropped: step.value.dropped, kept: step.value.kept },
      };
    }
  }
}
