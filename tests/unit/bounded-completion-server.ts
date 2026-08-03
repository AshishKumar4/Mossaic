/**
 * A scripted multipart/retention server, and the two clients that talk to it.
 *
 * The bounded completion methods are the same state machines on both the
 * binding client and the HTTP client, so the only way to pin that they behave
 * identically is to point both at one server and compare what each did. This
 * model is that server: it records every request, answers from the pages a
 * scenario configures, and can be told to lie.
 *
 * Both adapters are the real clients — `createVFS` over a fake
 * `MOSSAIC_USER` namespace, and `createMossaicHttpClient` over a fake
 * `fetcher`. Only the transport is substituted, which is where the real seam
 * between the SDK and the server is.
 */

import {
  createMossaicHttpClient,
  createVFS,
  type MossaicEnv,
  type VFSBoundedOperationsClient,
  type VFSClient,
} from "../../sdk/src/index";
import {
  MULTIPART_PROTOCOL_VERSION,
  MULTIPART_STATUS_ENTRY_PAGE_SIZE,
} from "../../shared/multipart";

/** Every request the clients may issue, in the order the server saw them. */
export type ServerRequest =
  | { kind: "begin"; path: string; resumeFrom?: string; protocolVersion?: number }
  | { kind: "hash-page"; startIndex: number; count: number }
  | { kind: "finalize"; chunkCount: number }
  | { kind: "finalize-step" }
  | { kind: "abort" }
  | { kind: "abort-step" }
  | { kind: "status"; continuation?: string }
  | { kind: "drop-versions" }
  | { kind: "drop-versions-step"; operationId: string };

/** A refusal both transports can carry, so both map to the same typed error. */
export class ServerFault extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(`${code}: ${message}`);
    this.code = code;
    this.name = "ServerFault";
  }
}

export interface FakeServerOptions {
  /** Session dimensions the begin response reports. */
  totalChunks: number;
  poolSize: number;
  /** Chunk indices the session already holds, paged like the real server. */
  landed?: number[];
  /** Echo the paged control plane. `false` models a server that predates it. */
  paged?: boolean;
  /** Finalize pages before publication, when driven through the paged plane. */
  finalizeSteps?: number;
  /** Abort pages before the session is terminal. */
  abortSteps?: number;
  /** Retention steps before the counts arrive. */
  dropVersionsSteps?: number;
  /** Refuse the one-call retention with `EFBIG`, as a paged server does. */
  dropVersionsRefusesOneCall?: boolean;
  /** Replace any response just before it is returned. */
  corrupt?: (request: ServerRequest, response: unknown) => unknown;
  /** Raise instead of answering. */
  fault?: (request: ServerRequest) => ServerFault | undefined;
  /** Called after every request, for cancelling mid-operation. */
  onRequest?: (request: ServerRequest, count: number) => void;
}

const UPLOAD_ID = "u-scripted";
const SESSION_TOKEN = "st-scripted";
const FILE_HASH = "a".repeat(64);

export class FakeMultipartServer {
  readonly requests: ServerRequest[] = [];
  private readonly opts: FakeServerOptions;
  private readonly landedPages: number[][];
  private staged = 0;
  private finalizeStepsLeft: number;
  private abortStepsLeft: number;
  private dropVersionsLeft: number;
  /** Retention operation ids the server has been asked to advance. */
  readonly dropVersionsOperations: string[] = [];

  constructor(opts: FakeServerOptions) {
    this.opts = opts;
    this.landedPages = pageLanded(opts.landed ?? [], opts.poolSize);
    this.finalizeStepsLeft = opts.finalizeSteps ?? 1;
    this.abortStepsLeft = opts.abortSteps ?? 1;
    this.dropVersionsLeft = opts.dropVersionsSteps ?? 1;
  }

  /** Requests issued so far, by kind. */
  count(kind: ServerRequest["kind"]): number {
    return this.requests.filter((request) => request.kind === kind).length;
  }

  handle(request: ServerRequest): unknown {
    this.requests.push(request);
    this.opts.onRequest?.(request, this.requests.length);
    const fault = this.opts.fault?.(request);
    if (fault !== undefined) throw fault;
    const response = this.answer(request);
    return this.opts.corrupt?.(request, response) ?? response;
  }

  private answer(request: ServerRequest): unknown {
    switch (request.kind) {
      case "begin":
        return this.beginResponse(request.protocolVersion);
      case "hash-page": {
        this.staged = Math.max(this.staged, request.startIndex + request.count);
        return { staged: this.staged, total: this.opts.totalChunks };
      }
      case "finalize":
        return this.finalizeResult();
      case "finalize-step": {
        if (--this.finalizeStepsLeft > 0) {
          return {
            done: false,
            phase: "cleaning",
            cursor: cursorAt(this.finalizeStepsLeft, this.opts.totalChunks),
            total: this.opts.totalChunks,
          };
        }
        return { done: true, result: this.finalizeResult(), fresh: true };
      }
      case "abort":
        return { ok: true };
      case "abort-step": {
        if (--this.abortStepsLeft > 0) {
          return {
            done: false,
            phase: "fencing",
            cursor: cursorAt(this.abortStepsLeft, this.opts.poolSize),
            total: this.opts.poolSize,
          };
        }
        return { done: true };
      }
      case "status":
        return this.statusPage(request.continuation);
      case "drop-versions":
        if (this.opts.dropVersionsRefusesOneCall === true) {
          throw new ServerFault(
            "EFBIG",
            "dropVersions: history needs the one-call retention capability"
          );
        }
        return { dropped: 7, kept: 1 };
      case "drop-versions-step": {
        this.dropVersionsOperations.push(request.operationId);
        return --this.dropVersionsLeft > 0
          ? { done: false }
          : { done: true, dropped: 9, kept: 2 };
      }
    }
  }

  private beginResponse(protocolVersion: number | undefined): unknown {
    const first = this.landedPages[0] ?? [];
    return {
      uploadId: UPLOAD_ID,
      chunkSize: 1024,
      totalChunks: this.opts.totalChunks,
      poolSize: this.opts.poolSize,
      sessionToken: SESSION_TOKEN,
      putEndpoint: `/api/vfs/multipart/${UPLOAD_ID}`,
      expiresAtMs: 1_700_000_000_000,
      landed: first,
      ...(this.landedPages.length > 1 ? { continuation: "c1" } : {}),
      ...(this.opts.paged === false || protocolVersion === undefined
        ? {}
        : { protocolVersion: MULTIPART_PROTOCOL_VERSION }),
    };
  }

  private statusPage(continuation: string | undefined): unknown {
    const index =
      continuation === undefined ? 0 : Number(continuation.slice(1));
    const page = this.landedPages[index] ?? [];
    return {
      landed: page,
      total: this.opts.totalChunks,
      bytesUploaded: page.length,
      expiresAtMs: 1_700_000_000_000,
      ...(index + 1 < this.landedPages.length
        ? { continuation: `c${index + 1}` }
        : {}),
    };
  }

  private finalizeResult(): unknown {
    return {
      fileId: "f-scripted",
      size: this.opts.totalChunks * 1024,
      chunkCount: this.opts.totalChunks,
      fileHash: FILE_HASH,
      path: "/scripted.bin",
      mimeType: "application/octet-stream",
      isEncrypted: false,
    };
  }
}

/** A cursor that climbs toward `total` as the steps left run down. */
function cursorAt(stepsLeft: number, total: number): number {
  return Math.max(0, total - stepsLeft);
}

/**
 * Split a landed set the way the server's own scan does: a page ends when it
 * has reported one entry page's worth, and the walk ends when the pool is
 * exhausted, so a pool wider than one shard page always costs a second page.
 */
function pageLanded(landed: readonly number[], poolSize: number): number[][] {
  const pages: number[][] = [];
  for (
    let start = 0;
    start < landed.length;
    start += MULTIPART_STATUS_ENTRY_PAGE_SIZE
  ) {
    pages.push([...landed.slice(start, start + MULTIPART_STATUS_ENTRY_PAGE_SIZE)]);
  }
  if (pages.length === 0) pages.push([]);
  void poolSize;
  return pages;
}

/** The handle shape both clients hand back, for scenarios that skip begin. */
export function scriptedHandle(opts: {
  totalChunks: number;
  poolSize: number;
  paged?: boolean;
}): {
  uploadId: string;
  path: string;
  chunkSize: number;
  expectedChunks: number;
  poolSize: number;
  sessionToken: string;
  expiresAtMs: number;
  size: number;
  protocolVersion?: number;
} {
  return {
    uploadId: UPLOAD_ID,
    path: "/scripted.bin",
    chunkSize: 1024,
    expectedChunks: opts.totalChunks,
    poolSize: opts.poolSize,
    sessionToken: SESSION_TOKEN,
    expiresAtMs: 1_700_000_000_000,
    size: opts.totalChunks * 1024,
    ...(opts.paged === false
      ? {}
      : { protocolVersion: MULTIPART_PROTOCOL_VERSION }),
  };
}

/** Hashes a manifest of `count` chunks declares. */
export function scriptedHashList(count: number): string[] {
  return Array.from({ length: count }, (_unused, index) =>
    index.toString(16).padStart(64, "0")
  );
}

/** Both shipped clients satisfy the published surface plus the bounded one. */
export type BoundedClient = VFSClient & VFSBoundedOperationsClient;

type Client = BoundedClient;

/** `createVFS` over a `MOSSAIC_USER` namespace backed by the model. */
export function bindingClient(server: FakeMultipartServer): Client {
  const stub = {
    vfsBeginMultipart: async (
      _scope: unknown,
      path: string,
      opts: { resumeFrom?: string; protocolVersion?: number }
    ) =>
      server.handle({
        kind: "begin",
        path,
        ...(opts.resumeFrom === undefined
          ? {}
          : { resumeFrom: opts.resumeFrom }),
        ...(opts.protocolVersion === undefined
          ? {}
          : { protocolVersion: opts.protocolVersion }),
      }),
    vfsStageMultipartHashes: async (
      _scope: unknown,
      _uploadId: string,
      startIndex: number,
      hashes: readonly string[]
    ) =>
      server.handle({
        kind: "hash-page",
        startIndex,
        count: hashes.length,
      }),
    vfsFinalizeMultipart: async (
      _scope: unknown,
      _uploadId: string,
      chunkHashList: readonly string[]
    ) => server.handle({ kind: "finalize", chunkCount: chunkHashList.length }),
    vfsFinalizeMultipartStep: async () =>
      server.handle({ kind: "finalize-step" }),
    vfsAbortMultipart: async () => server.handle({ kind: "abort" }),
    vfsAbortMultipartStep: async () => server.handle({ kind: "abort-step" }),
    vfsGetMultipartStatus: async (
      _scope: unknown,
      _uploadId: string,
      continuation?: string
    ) =>
      server.handle({
        kind: "status",
        ...(continuation === undefined ? {} : { continuation }),
      }),
    vfsDropVersions: async () => server.handle({ kind: "drop-versions" }),
    vfsDropVersionsStep: async (
      _scope: unknown,
      _path: string,
      _policy: unknown,
      operationId: string
    ) => server.handle({ kind: "drop-versions-step", operationId }),
  };
  const namespace = {
    idFromName: (name: string) => name as unknown as DurableObjectId,
    get: () => stub,
  };
  const env: MossaicEnv = {
    MOSSAIC_USER: namespace,
    MOSSAIC_SHARD: namespace,
  };
  return createVFS(env, { tenant: "scripted" }) as unknown as Client;
}

/** `createMossaicHttpClient` over a `fetcher` backed by the model. */
export function httpClient(server: FakeMultipartServer): Client {
  const fetcher: typeof fetch = async (input, init) => {
    const url = new URL(
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : input.url
    );
    const body =
      typeof init?.body === "string"
        ? (JSON.parse(init.body) as Record<string, unknown>)
        : {};
    try {
      return Response.json(routeHttp(server, url, body));
    } catch (err) {
      if (err instanceof ServerFault) {
        return Response.json(
          { code: err.code, message: err.message },
          { status: 400 }
        );
      }
      throw err;
    }
  };
  return createMossaicHttpClient({
    url: "https://scripted.test",
    apiKey: "key",
    fetcher,
  }) as unknown as Client;
}

function routeHttp(
  server: FakeMultipartServer,
  url: URL,
  body: Record<string, unknown>
): unknown {
  const route = url.pathname.replace("/api/vfs/", "");
  if (route === "multipart/begin") {
    return server.handle({
      kind: "begin",
      path: String(body.path),
      ...(typeof body.resumeFrom === "string"
        ? { resumeFrom: body.resumeFrom }
        : {}),
      ...(typeof body.protocolVersion === "number"
        ? { protocolVersion: body.protocolVersion }
        : {}),
    });
  }
  if (route === "multipart/hash-page") {
    return server.handle({
      kind: "hash-page",
      startIndex: Number(body.startIndex),
      count: Array.isArray(body.hashes) ? body.hashes.length : 0,
    });
  }
  if (route === "multipart/finalize") {
    return server.handle({
      kind: "finalize",
      chunkCount: Array.isArray(body.chunkHashList)
        ? body.chunkHashList.length
        : 0,
    });
  }
  if (route === "multipart/finalize-step") {
    return server.handle({ kind: "finalize-step" });
  }
  if (route === "multipart/abort") return server.handle({ kind: "abort" });
  if (route === "multipart/abort-step") {
    return server.handle({ kind: "abort-step" });
  }
  if (route.endsWith("/status")) {
    const continuation = url.searchParams.get("continuation");
    return server.handle({
      kind: "status",
      ...(continuation === null ? {} : { continuation }),
    });
  }
  if (route === "dropVersions") return server.handle({ kind: "drop-versions" });
  if (route === "dropVersionsStep") {
    return server.handle({
      kind: "drop-versions-step",
      operationId: String(body.operationId),
    });
  }
  throw new ServerFault("ENOENT", `unscripted route ${url.pathname}`);
}

/** Both clients over one server model each, for parity assertions. */
export function bothClients(opts: FakeServerOptions): Array<{
  name: "binding" | "http";
  client: Client;
  server: FakeMultipartServer;
}> {
  const binding = new FakeMultipartServer(opts);
  const http = new FakeMultipartServer(opts);
  return [
    { name: "binding", client: bindingClient(binding), server: binding },
    { name: "http", client: httpClient(http), server: http },
  ];
}

/** What one client did: what it returned, and what the server saw. */
export interface Behaviour<T> {
  outcome: T;
  requests: ServerRequest[];
}

/**
 * Run one scenario on both clients and assert they behaved identically.
 *
 * "Identically" means the same result *and* the same requests in the same
 * order — a shared state machine that issued different pages on one transport
 * would be a difference the return value alone could hide.
 */
export async function bothAgree<T>(
  opts: FakeServerOptions,
  run: (client: Client) => Promise<T>,
  assertEqual: (left: Behaviour<T>, right: Behaviour<T>) => void
): Promise<Behaviour<T>> {
  const seen: Array<Behaviour<T>> = [];
  for (const { client, server } of bothClients(opts)) {
    seen.push({ outcome: await run(client), requests: [...server.requests] });
  }
  const [binding, http] = seen;
  if (binding === undefined || http === undefined) {
    throw new Error("bothAgree: expected one behaviour per client");
  }
  assertEqual(binding, http);
  return binding;
}
