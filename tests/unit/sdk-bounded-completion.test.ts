import { describe, expect, it, vi } from "vitest";

/**
 * Bounded completion methods, on both clients.
 *
 * `finalizeMultipartUpload`, `abortMultipartUpload`, `getMultipartUploadStatus`,
 * `resumeMultipartUpload` and `dropVersions` each drive a durable server-side
 * machine, and each has to do it inside a fixed request budget. These pin what
 * that means:
 *
 *   - the binding client and the HTTP client issue the same requests in the
 *     same order and return the same results, because the machines are shared,
 *   - work knowably past the cap is refused before anything is mutated, and
 *     the refusal names the checkpointable pair to drive instead,
 *   - work that reaches the cap anyway surfaces the durable operation, so the
 *     pages that did run are not repeated,
 *   - a landed set spread over more pages than the cap allows is never
 *     silently truncated — which is what would make a resumed upload re-PUT
 *     chunks the unread pages knew about,
 *   - progress never rewinds and cancellation stops the walk, and
 *   - a server that answers with a page larger than the protocol allows, a
 *     total that moves, or a continuation that loops is refused rather than
 *     followed.
 */

import {
  CompletionBudgetExceededError,
  DEFAULT_COMPLETION_REQUEST_BUDGET,
  MossaicUnavailableError,
  statusUpload,
  type BoundedOperationProgress,
  type MultipartFinalizeOperation,
  type MultipartResumeCheckpoint,
  type MultipartStatusCheckpoint,
} from "../../sdk/src/index";
import type { HttpVFS } from "../../sdk/src/http";
import {
  bothAgree,
  bothClients,
  FakeMultipartServer,
  httpClient,
  scriptedHandle,
  scriptedHashList,
  ServerFault,
  type BoundedClient,
  type ServerRequest,
} from "./bounded-completion-server";

/** Pool the server hands out by default, so page arithmetic is predictable. */
const POOL = 32;

function indices(count: number): number[] {
  return Array.from({ length: count }, (_unused, index) => index);
}

function kinds(server: FakeMultipartServer): ServerRequest["kind"][] {
  return server.requests.map((request) => request.kind);
}

/**
 * Requests with the client-minted operation ids replaced by their order of
 * appearance.
 *
 * Which id a client mints is its own randomness; that it mints one and keeps
 * using it is the parity property, and numbering them preserves exactly that.
 */
function comparable(requests: readonly ServerRequest[]): unknown[] {
  const ids = new Map<string, string>();
  return requests.map((request) => {
    if (request.kind !== "drop-versions-step") return request;
    const seen = ids.get(request.operationId) ?? `operation#${ids.size}`;
    ids.set(request.operationId, seen);
    return { ...request, operationId: seen };
  });
}

/** Run a scenario on both clients, asserting they did exactly the same thing. */
function agree<T>(
  opts: Parameters<typeof bothAgree>[0],
  run: (client: BoundedClient) => Promise<T>
): Promise<{ outcome: T; requests: ServerRequest[] }> {
  return bothAgree(opts, run, (binding, http) => {
    expect(http.outcome).toEqual(binding.outcome);
    expect(comparable(http.requests)).toEqual(comparable(binding.requests));
  });
}

describe("bounded completion — binding and HTTP parity", () => {
  it("finalizes a small manifest in one request on both clients", async () => {
    const hashes = scriptedHashList(4);
    const { outcome, requests } = await agree(
      { totalChunks: 4, poolSize: POOL },
      async (client) => {
        const handle = await client.beginMultipartUpload("/scripted.bin", {
          size: 4096,
        });
        return await client.finalizeMultipartUpload(handle, hashes);
      }
    );
    expect(requests.map((request) => request.kind)).toEqual([
      "begin",
      "finalize",
    ]);
    expect(outcome).toMatchObject({
      path: "/scripted.bin",
      pathId: "f-scripted",
      size: 4096,
      isEncrypted: false,
    });
  });

  it("stages and steps a manifest one request cannot verify, identically", async () => {
    // 768 chunks over a 128-shard pool costs 512 shard round-trips, past what
    // one server invocation may spend, so both clients page instead.
    const hashes = scriptedHashList(768);
    const { requests } = await agree(
      { totalChunks: 768, poolSize: 128, finalizeSteps: 3 },
      (client) =>
        client.finalizeMultipartUpload(
          scriptedHandle({ totalChunks: 768, poolSize: 128 }),
          hashes
        )
    );
    expect(requests).toEqual([
      { kind: "hash-page", startIndex: 0, count: 256 },
      { kind: "hash-page", startIndex: 256, count: 256 },
      { kind: "hash-page", startIndex: 512, count: 256 },
      { kind: "finalize-step" },
      { kind: "finalize-step" },
      { kind: "finalize-step" },
    ]);
  });

  it("drives the abort machine identically", async () => {
    const { outcome, requests } = await agree(
      { totalChunks: 4, poolSize: POOL, abortSteps: 4 },
      (client) =>
        client.abortMultipartUpload(
          scriptedHandle({ totalChunks: 4, poolSize: POOL })
        )
    );
    expect(outcome).toEqual({ aborted: true });
    expect(requests.map((request) => request.kind)).toEqual([
      "abort-step",
      "abort-step",
      "abort-step",
      "abort-step",
    ]);
  });

  it("reads a paged landed set identically", async () => {
    const landed = indices(600);
    const { outcome, requests } = await agree(
      { totalChunks: 1000, poolSize: POOL, landed },
      (client) =>
        client.getMultipartUploadStatus(
          scriptedHandle({ totalChunks: 1000, poolSize: POOL })
        )
    );
    expect(requests).toEqual([
      { kind: "status" },
      { kind: "status", continuation: "c1" },
      { kind: "status", continuation: "c2" },
    ]);
    expect(outcome).toEqual({
      landed,
      total: 1000,
      bytesUploaded: 600,
      expiresAtMs: 1_700_000_000_000,
    });
  });

  it("resumes a paged session identically", async () => {
    const landed = indices(600);
    const { outcome, requests } = await agree(
      { totalChunks: 1000, poolSize: POOL, landed },
      (client) =>
        client.resumeMultipartUpload(
          scriptedHandle({ totalChunks: 1000, poolSize: POOL })
        )
    );
    expect(requests.map((request) => request.kind)).toEqual([
      "begin",
      "status",
      "status",
    ]);
    expect(outcome.landed).toEqual(landed);
    expect(outcome.handle.protocolVersion).toBe(2);
    // The complete form never hands back a continuation — it either read every
    // page or refused.
    expect(outcome.continuation).toBeUndefined();
  });

  it("applies retention identically, one call then bounded steps", async () => {
    const opts = {
      totalChunks: 1,
      poolSize: POOL,
      dropVersionsRefusesOneCall: true,
      dropVersionsSteps: 3,
    };
    const { outcome, requests } = await agree(opts, (client) =>
      client.dropVersions("/history.txt", {})
    );
    expect(outcome).toEqual({ dropped: 9, kept: 2 });
    expect(requests.map((request) => request.kind)).toEqual([
      "drop-versions",
      "drop-versions-step",
      "drop-versions-step",
      "drop-versions-step",
    ]);
    // Every step names the same operation, so the server continues one walk.
    for (const { client, server } of bothClients(opts)) {
      await client.dropVersions("/history.txt", {});
      expect(new Set(server.dropVersionsOperations).size).toBe(1);
    }
  });

  it("answers a history one call can finish in one request", async () => {
    for (const { client, server } of bothClients({
      totalChunks: 1,
      poolSize: POOL,
    })) {
      await expect(client.dropVersions("/history.txt", {})).resolves.toEqual({
        dropped: 7,
        kept: 1,
      });
      expect(kinds(server)).toEqual(["drop-versions"]);
    }
  });

  it("keeps finalize and abort single requests against a server with no paged plane", async () => {
    for (const { client, server } of bothClients({
      totalChunks: 4,
      poolSize: POOL,
      paged: false,
    })) {
      const handle = await client.beginMultipartUpload("/scripted.bin", {
        size: 4096,
      });
      expect(handle.protocolVersion).toBeUndefined();
      await client.finalizeMultipartUpload(handle, scriptedHashList(4));
      await client.abortMultipartUpload(handle);
      expect(kinds(server)).toEqual(["begin", "finalize", "abort"]);
    }
  });
});

describe("bounded completion — the cap", () => {
  it("reads a landed set that needs exactly the budget", async () => {
    // 4095 chunks spread over 16 status pages: the last of the budget.
    const landed = indices(4095);
    for (const { client, server } of bothClients({
      totalChunks: 4095,
      poolSize: POOL,
      landed,
    })) {
      const status = await client.getMultipartUploadStatus(
        scriptedHandle({ totalChunks: 4095, poolSize: POOL })
      );
      expect(status.landed).toEqual(landed);
      expect(server.count("status")).toBe(DEFAULT_COMPLETION_REQUEST_BUDGET);
    }
  });

  it("refuses one page past the budget without issuing a request", async () => {
    for (const { client, server } of bothClients({
      totalChunks: 4096,
      poolSize: POOL,
      landed: indices(4096),
    })) {
      await expect(
        client.getMultipartUploadStatus(
          scriptedHandle({ totalChunks: 4096, poolSize: POOL })
        )
      ).rejects.toThrowError(
        /EFBIG: getMultipartUploadStatus needs more than the 16-request completion budget; drive getMultipartUploadStatusPage/
      );
      expect(server.requests).toEqual([]);
    }
  });

  it("refuses a resume past the budget before re-minting the session", async () => {
    for (const { client, server } of bothClients({
      totalChunks: 4096,
      poolSize: POOL,
      landed: indices(4096),
    })) {
      const failure = await client
        .resumeMultipartUpload(
          scriptedHandle({ totalChunks: 4096, poolSize: POOL })
        )
        .catch((err: unknown) => err);
      expect(failure).toBeInstanceOf(CompletionBudgetExceededError);
      expect((failure as CompletionBudgetExceededError).code).toBe("EFBIG");
      // Re-minting rotates the session's fence, so the refusal has to land
      // before begin is called at all.
      expect(server.requests).toEqual([]);
    }
  });

  it("finalizes a manifest that needs exactly the budget", async () => {
    // 768 chunks over 128 shards: 3 staging pages, 2 fence pages, 6 verify
    // pages, a publication and 3 cleaning pages is 15 requests.
    for (const { client, server } of bothClients({
      totalChunks: 768,
      poolSize: 128,
      finalizeSteps: 12,
    })) {
      await expect(
        client.finalizeMultipartUpload(
          scriptedHandle({ totalChunks: 768, poolSize: 128 }),
          scriptedHashList(768)
        )
      ).resolves.toMatchObject({ pathId: "f-scripted" });
      expect(server.requests.length).toBe(15);
    }
  });

  it("refuses a manifest past the budget without staging a page", async () => {
    for (const { client, server } of bothClients({
      totalChunks: 1024,
      poolSize: 128,
    })) {
      await expect(
        client.finalizeMultipartUpload(
          scriptedHandle({ totalChunks: 1024, poolSize: 128 }),
          scriptedHashList(1024)
        )
      ).rejects.toThrowError(
        /EFBIG: finalizeMultipartUpload needs more than the 16-request completion budget; drive startFinalizeMultipartUpload/
      );
      expect(server.requests).toEqual([]);
    }
  });

  it("refuses an abort no single invocation can finish, before fencing", async () => {
    // The server runs at most 512 abort pages per request; a session owing
    // 513 is refused rather than half-aborted.
    for (const { client, server } of bothClients({
      totalChunks: 130_049,
      poolSize: 64,
    })) {
      await expect(
        client.abortMultipartUpload(
          scriptedHandle({ totalChunks: 130_049, poolSize: 64 })
        )
      ).rejects.toThrowError(
        /EFBIG: abortMultipartUpload needs more than the 16-request completion budget; drive startAbortMultipartUpload/
      );
      expect(server.requests).toEqual([]);
    }
    // One page fewer and the same abort goes through.
    for (const { client, server } of bothClients({
      totalChunks: 130_048,
      poolSize: 64,
      abortSteps: 1,
    })) {
      await expect(
        client.abortMultipartUpload(
          scriptedHandle({ totalChunks: 130_048, poolSize: 64 })
        )
      ).resolves.toEqual({ aborted: true });
      expect(kinds(server)).toEqual(["abort-step"]);
    }
  });
});

describe("bounded completion — checkpoints", () => {
  it("hands back the finalize operation when the budget runs out mid-machine", async () => {
    for (const { client, server } of bothClients({
      totalChunks: 768,
      poolSize: 128,
      // More cleaning pages than the budget can pay for; every one of them is
      // durable, which is the whole reason the operation comes back.
      finalizeSteps: 40,
    })) {
      const handle = scriptedHandle({ totalChunks: 768, poolSize: 128 });
      const hashes = scriptedHashList(768);
      const failure = await client
        .finalizeMultipartUpload(handle, hashes)
        .catch((err: unknown) => err);
      expect(failure).toBeInstanceOf(CompletionBudgetExceededError);
      const checkpoint = (
        failure as CompletionBudgetExceededError<MultipartFinalizeOperation>
      ).checkpoint;
      expect(checkpoint).toEqual({
        kind: "multipart-finalize",
        uploadId: "u-scripted",
        nextHashIndex: 768,
      });
      expect(server.requests.length).toBe(DEFAULT_COMPLETION_REQUEST_BUDGET);

      // Resuming with the checkpoint stages nothing again — the manifest is
      // already on the server — and finishes the machine.
      const staged = server.count("hash-page");
      let outcome = await client.stepFinalizeMultipartUpload(
        handle,
        hashes,
        checkpoint as MultipartFinalizeOperation
      );
      while ("operation" in outcome) {
        outcome = await client.stepFinalizeMultipartUpload(
          handle,
          hashes,
          outcome.operation
        );
      }
      expect(outcome).toMatchObject({ pathId: "f-scripted" });
      expect(server.count("hash-page")).toBe(staged);
    }
  });

  it("hands back the abort operation when the budget runs out", async () => {
    for (const { client } of bothClients({
      totalChunks: 4,
      poolSize: POOL,
      abortSteps: 40,
    })) {
      const handle = scriptedHandle({ totalChunks: 4, poolSize: POOL });
      const failure = await client
        .abortMultipartUpload(handle)
        .catch((err: unknown) => err);
      expect(failure).toBeInstanceOf(CompletionBudgetExceededError);
      expect(
        (failure as CompletionBudgetExceededError<{ uploadId: string }>)
          .checkpoint
      ).toEqual({ kind: "multipart-abort", uploadId: "u-scripted" });
    }
  });

  it("hands back the retention operation when the budget runs out", async () => {
    for (const { client, server } of bothClients({
      totalChunks: 1,
      poolSize: POOL,
      dropVersionsRefusesOneCall: true,
      dropVersionsSteps: 40,
    })) {
      const failure = await client
        .dropVersions("/history.txt", {})
        .catch((err: unknown) => err);
      expect(failure).toBeInstanceOf(CompletionBudgetExceededError);
      const checkpoint = (
        failure as CompletionBudgetExceededError<{
          kind: string;
          operationId: string;
        }>
      ).checkpoint;
      expect(checkpoint?.kind).toBe("drop-versions");
      expect(server.dropVersionsOperations).toContain(checkpoint?.operationId);
      expect(server.requests.length).toBe(DEFAULT_COMPLETION_REQUEST_BUDGET);

      // The caller finishes the same walk rather than starting a second one.
      let progress = await client.stepDropVersions("/history.txt", {}, {
        kind: "drop-versions",
        operationId: checkpoint?.operationId ?? "",
      });
      while (!progress.done) {
        progress = await client.stepDropVersions(
          "/history.txt",
          {},
          progress.operation
        );
      }
      expect(new Set(server.dropVersionsOperations).size).toBe(1);
    }
  });

  it("carries the unread continuation when a status walk is cut short", async () => {
    // A handle whose dimensions understate its own landed set: the preflight
    // has nothing to refuse, so the walk is what runs out of budget.
    for (const { client } of bothClients({
      totalChunks: 8192,
      poolSize: POOL,
      landed: indices(4352),
    })) {
      const handle = scriptedHandle({ totalChunks: 8192, poolSize: POOL });
      const failure = await client
        .getMultipartUploadStatus({ ...handle, expectedChunks: 512 })
        .catch((err: unknown) => err);
      expect(failure).toBeInstanceOf(CompletionBudgetExceededError);
      const checkpoint = (
        failure as CompletionBudgetExceededError<MultipartStatusCheckpoint>
      ).checkpoint;
      // Sixteen pages read, the seventeenth named rather than skipped.
      expect(checkpoint?.continuation).toBe("c16");
      expect(checkpoint?.landed).toEqual(indices(4096));
    }
  });

  it("carries the re-minted handle when a resume is cut short", async () => {
    for (const { client } of bothClients({
      totalChunks: 8192,
      poolSize: POOL,
      landed: indices(4352),
    })) {
      const handle = scriptedHandle({ totalChunks: 8192, poolSize: POOL });
      const failure = await client
        .resumeMultipartUpload({ ...handle, expectedChunks: 512 })
        .catch((err: unknown) => err);
      expect(failure).toBeInstanceOf(CompletionBudgetExceededError);
      const checkpoint = (
        failure as CompletionBudgetExceededError<MultipartResumeCheckpoint>
      ).checkpoint;
      // Without the handle the caller would lose the session token the resume
      // just minted and have to mint another.
      expect(checkpoint?.handle.sessionToken).toBe("st-scripted");
      expect(checkpoint?.continuation).toBe("c16");
      expect(checkpoint?.landed).toEqual(indices(4096));
    }
  });
});

describe("bounded completion — resume without re-upload", () => {
  it("reports every landed chunk of a set past the completion budget", async () => {
    // 4200 landed chunks span 17 status pages: one more than a completion
    // method may spend, and exactly the case where stopping early would have
    // the caller re-PUT a thousand chunks that are already there.
    const landed = indices(4200);
    for (const { client, server } of bothClients({
      totalChunks: 4200,
      poolSize: POOL,
      landed,
    })) {
      const handle = scriptedHandle({ totalChunks: 4200, poolSize: POOL });
      const first = await client.resumeMultipartUploadPage(handle);
      const seen = new Set(first.landed);
      let continuation = first.continuation;
      while (continuation !== undefined) {
        const page = await client.getMultipartUploadStatusPage(first.handle, {
          continuation,
        });
        for (const index of page.landed) seen.add(index);
        continuation = page.continuation;
      }
      expect([...seen].sort((a, b) => a - b)).toEqual(landed);
      expect(server.count("status")).toBe(16);
    }
  });

  it("follows every continuation from the transfer engine's statusUpload", async () => {
    const landed = indices(4200);
    const server = new FakeMultipartServer({
      totalChunks: 4200,
      poolSize: POOL,
      landed,
    });
    const client = httpClient(server) as unknown as HttpVFS;
    const session = await client.multipartBegin({
      path: "/scripted.bin",
      size: 4200 * 1024,
    });
    const status = await statusUpload(client, session);
    expect(status.landed).toEqual(landed);
    expect(status.continuation).toBeUndefined();
    expect(status.bytesUploaded).toBe(4200);
  });
});

describe("bounded completion — cancellation and progress", () => {
  it("stops the walk when the caller cancels, spending nothing more", async () => {
    for (const { client, server } of bothClients({
      totalChunks: 1000,
      poolSize: POOL,
      landed: indices(1000),
      onRequest: (_request, count) => {
        if (count === 2) controller.abort(new Error("caller gave up"));
      },
    })) {
      var controller = new AbortController();
      await expect(
        client.getMultipartUploadStatus(
          scriptedHandle({ totalChunks: 1000, poolSize: POOL }),
          { signal: controller.signal }
        )
      ).rejects.toThrowError(/caller gave up/);
      expect(server.count("status")).toBe(2);
    }
  });

  it("refuses a cancelled call before its first request", async () => {
    for (const { client, server } of bothClients({
      totalChunks: 4,
      poolSize: POOL,
    })) {
      const controller = new AbortController();
      controller.abort(new Error("cancelled up front"));
      await expect(
        client.finalizeMultipartUpload(
          scriptedHandle({ totalChunks: 4, poolSize: POOL }),
          scriptedHashList(4),
          { signal: controller.signal }
        )
      ).rejects.toThrowError(/cancelled up front/);
      expect(server.requests).toEqual([]);
    }
  });

  it("never reports progress that rewinds within a phase", async () => {
    // A server replaying a page reports a cursor it already passed; the
    // caller's progress bar must not go backwards because of it.
    let step = 0;
    const server = new FakeMultipartServer({
      totalChunks: 768,
      poolSize: 128,
      finalizeSteps: 5,
      corrupt: (request, response) => {
        if (request.kind !== "finalize-step") return response;
        step++;
        return step === 2
          ? { done: false, phase: "cleaning", cursor: 1, total: 768 }
          : response;
      },
    });
    const client = httpClient(server);
    const reported: BoundedOperationProgress[] = [];
    await client.finalizeMultipartUpload(
      scriptedHandle({ totalChunks: 768, poolSize: 128 }),
      scriptedHashList(768),
      { onProgress: (progress) => reported.push(progress) }
    );
    const cleaning = reported
      .filter((progress) => progress.phase === "cleaning")
      .map((progress) => progress.completed ?? 0);
    expect(cleaning.length).toBeGreaterThan(2);
    for (let index = 1; index < cleaning.length; index++) {
      expect(cleaning[index]).toBeGreaterThanOrEqual(cleaning[index - 1] ?? 0);
    }
    // Requests spent only ever climbs, and never past the budget.
    const spent = reported.map((progress) => progress.requestsUsed);
    expect(spent).toEqual([...spent].sort((a, b) => a - b));
    expect(Math.max(...spent)).toBeLessThanOrEqual(
      DEFAULT_COMPLETION_REQUEST_BUDGET
    );
  });
});

describe("bounded completion — hostile responses", () => {
  async function refuses(
    opts: Parameters<typeof bothClients>[0],
    run: (client: ReturnType<typeof bothClients>[number]["client"]) => Promise<unknown>,
    message: RegExp
  ): Promise<void> {
    for (const { client } of bothClients(opts)) {
      const failure = await run(client).catch((err: unknown) => err);
      expect(failure).toBeInstanceOf(MossaicUnavailableError);
      expect((failure as Error).message).toMatch(message);
    }
  }

  const handle = scriptedHandle({ totalChunks: 1000, poolSize: POOL });

  it("refuses a landed page larger than the protocol allows", async () => {
    await refuses(
      {
        totalChunks: 1000,
        poolSize: POOL,
        corrupt: (request, response) =>
          request.kind === "status"
            ? { ...(response as object), landed: indices(300) }
            : response,
      },
      (client) => client.getMultipartUploadStatus(handle),
      /invalid multipart response: status page/
    );
  });

  it("refuses a landed index at or above the total", async () => {
    await refuses(
      {
        totalChunks: 10,
        poolSize: POOL,
        corrupt: (request, response) =>
          request.kind === "status"
            ? { ...(response as object), landed: [0, 10] }
            : response,
      },
      (client) =>
        client.getMultipartUploadStatus(
          scriptedHandle({ totalChunks: 10, poolSize: POOL })
        ),
      /landed index 10 is not below total 10/
    );
  });

  it("refuses the same landed index reported by two pages", async () => {
    await refuses(
      {
        totalChunks: 1000,
        poolSize: POOL,
        landed: indices(600),
        corrupt: (request, response) =>
          request.kind === "status" && request.continuation === "c1"
            ? { ...(response as object), landed: [0], continuation: "c2" }
            : response,
      },
      (client) => client.getMultipartUploadStatus(handle),
      /landed index 0 was reported by two pages/
    );
  });

  it("refuses a continuation that leads back to itself", async () => {
    await refuses(
      {
        totalChunks: 1000,
        poolSize: POOL,
        landed: indices(600),
        corrupt: (request, response) =>
          request.kind === "status"
            ? { ...(response as object), landed: [], continuation: "c1" }
            : response,
      },
      (client) => client.getMultipartUploadStatus(handle),
      /continuation leads back to itself/
    );
  });

  it("refuses a total that changes between pages", async () => {
    await refuses(
      {
        totalChunks: 1000,
        poolSize: POOL,
        landed: indices(600),
        corrupt: (request, response) =>
          request.kind === "status" && request.continuation === "c1"
            ? { ...(response as object), total: 999 }
            : response,
      },
      (client) => client.getMultipartUploadStatus(handle),
      /total changed between pages \(1000 then 999\)/
    );
  });

  it("refuses a finalize result whose file hash is not a digest", async () => {
    await refuses(
      {
        totalChunks: 4,
        poolSize: POOL,
        corrupt: (request, response) =>
          request.kind === "finalize"
            ? { ...(response as object), fileHash: "nope" }
            : response,
      },
      (client) =>
        client.finalizeMultipartUpload(
          scriptedHandle({ totalChunks: 4, poolSize: POOL }),
          scriptedHashList(4)
        ),
      /invalid multipart response: finalize/
    );
  });

  it("refuses staging progress that does not cover the page it was sent", async () => {
    await refuses(
      {
        totalChunks: 768,
        poolSize: 128,
        corrupt: (request, response) =>
          request.kind === "hash-page"
            ? { ...(response as object), staged: 1 }
            : response,
      },
      (client) =>
        client.finalizeMultipartUpload(
          scriptedHandle({ totalChunks: 768, poolSize: 128 }),
          scriptedHashList(768)
        ),
      /staged 1\/768 does not cover the 768-hash manifest through index 256/
    );
  });

  it("refuses a begin response echoing a control plane this client cannot drive", async () => {
    await refuses(
      {
        totalChunks: 4,
        poolSize: POOL,
        corrupt: (request, response) =>
          request.kind === "begin"
            ? { ...(response as object), protocolVersion: 3 }
            : response,
      },
      (client) =>
        client.beginMultipartUpload("/scripted.bin", { size: 4096 }),
      /invalid multipart response: begin/
    );
  });

  it("refuses an abort step whose phase is not one of the machine's", async () => {
    await refuses(
      {
        totalChunks: 4,
        poolSize: POOL,
        corrupt: (request, response) =>
          request.kind === "abort-step"
            ? { done: false, phase: "elsewhere", cursor: 0, total: 4 }
            : response,
      },
      (client) =>
        client.startAbortMultipartUpload(
          scriptedHandle({ totalChunks: 4, poolSize: POOL })
        ),
      /invalid multipart response: abort step/
    );
  });

  it("refuses a response that is not an object at all", async () => {
    await refuses(
      {
        totalChunks: 4,
        poolSize: POOL,
        corrupt: (request, response) =>
          request.kind === "finalize" ? "fine, trust me" : response,
      },
      (client) =>
        client.finalizeMultipartUpload(
          scriptedHandle({ totalChunks: 4, poolSize: POOL }),
          scriptedHashList(4)
        ),
      /invalid multipart response: finalize/
    );
  });

  it("refuses retention counts that are not counts", async () => {
    for (const { client } of bothClients({
      totalChunks: 1,
      poolSize: POOL,
      corrupt: (request, response) =>
        request.kind === "drop-versions"
          ? { dropped: -1, kept: 0 }
          : response,
    })) {
      await expect(client.dropVersions("/history.txt", {})).rejects.toThrow(
        /dropped: expected non-negative integer/
      );
    }
  });
});

describe("bounded completion — idempotent abort", () => {
  it("reports an already-terminal session as not aborted", async () => {
    for (const code of ["ENOENT", "EBUSY"] as const) {
      for (const { client } of bothClients({
        totalChunks: 4,
        poolSize: POOL,
        fault: (request) =>
          request.kind === "abort-step"
            ? new ServerFault(code, "already terminal")
            : undefined,
      })) {
        await expect(
          client.abortMultipartUpload(
            scriptedHandle({ totalChunks: 4, poolSize: POOL })
          )
        ).resolves.toEqual({ aborted: false });
      }
    }
  });

  it("surfaces the refusal to a caller driving the bounded pair", async () => {
    for (const { client } of bothClients({
      totalChunks: 4,
      poolSize: POOL,
      fault: (request) =>
        request.kind === "abort-step"
          ? new ServerFault("EBUSY", "finalize is in progress")
          : undefined,
    })) {
      await expect(
        client.startAbortMultipartUpload(
          scriptedHandle({ totalChunks: 4, poolSize: POOL })
        )
      ).rejects.toThrow(/EBUSY/);
    }
  });
});

// Every case here drives a full state machine over a scripted transport, and
// the paging cases walk sixteen-page landed sets on two clients apiece.
vi.setConfig({ testTimeout: 30_000 });
