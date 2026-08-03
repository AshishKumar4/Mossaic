import { describe, it, expect } from "vitest";
import {
  beginUpload,
  beginUploadPage,
  finalizeUpload,
  statusUpload,
  statusUploadPage,
  TRANSFER_OPERATION_REQUEST_BUDGET,
} from "@mossaic/sdk/http";
import type { MossaicHttpClient } from "@mossaic/sdk/http";

/**
 * The transfer calls the CLI's multipart commands are built on.
 *
 * `mossaic upload-status` and `mossaic upload-finalize` resume a session by
 * `uploadId` and then read or commit it, and both of those are now paged: the
 * landed set arrives a page at a time, and a manifest one server invocation
 * cannot verify is staged and stepped. What the CLI needs from that is exactly
 * what these pin:
 *
 *   - `beginUpload` and `statusUpload` follow every continuation, so
 *     `upload-status` reports the whole landed set rather than its first page —
 *     the difference between "resume 8 chunks" and "re-upload 1000",
 *   - `finalizeUpload` keeps the operation handle across passes, so
 *     `upload-finalize` commits a large manifest without restarting it, and
 *   - the single-request forms are available for a caller that would rather
 *     page itself.
 */

const UPLOAD_ID = "u-cli";
const SESSION_TOKEN = "st-cli";
const FILE_HASH = "b".repeat(64);
const PAGE = 256;

interface Recorded {
  kind: "begin" | "status" | "hash-page" | "finalize-step" | "finalize";
  continuation?: string;
  startIndex?: number;
}

/**
 * A client whose landed set spans `pages` status pages and whose finalize can
 * only be driven through the paged control plane.
 */
function pagedClient(pages: number): {
  client: MossaicHttpClient;
  recorded: Recorded[];
} {
  const recorded: Recorded[] = [];
  const landedPage = (index: number): number[] =>
    Array.from({ length: PAGE }, (_unused, offset) => index * PAGE + offset);
  const total = pages * PAGE;
  let finalizeSteps = 3;
  const client = {
    multipartBegin: async () => {
      recorded.push({ kind: "begin" });
      return {
        uploadId: UPLOAD_ID,
        chunkSize: 1024,
        totalChunks: total,
        poolSize: 32,
        sessionToken: SESSION_TOKEN,
        putEndpoint: `/api/vfs/multipart/${UPLOAD_ID}`,
        expiresAtMs: 1_700_000_000_000,
        landed: landedPage(0),
        ...(pages > 1 ? { continuation: "c1" } : {}),
        protocolVersion: 2,
      };
    },
    multipartStatus: async (
      _uploadId: string,
      _token: string,
      continuation?: string
    ) => {
      recorded.push({
        kind: "status",
        ...(continuation === undefined ? {} : { continuation }),
      });
      const index = continuation === undefined ? 0 : Number(continuation.slice(1));
      return {
        landed: landedPage(index),
        total,
        bytesUploaded: PAGE,
        expiresAtMs: 1_700_000_000_000,
        ...(index + 1 < pages ? { continuation: `c${index + 1}` } : {}),
      };
    },
    multipartStageHashes: async (
      _uploadId: string,
      startIndex: number,
      hashes: readonly string[]
    ) => {
      recorded.push({ kind: "hash-page", startIndex });
      return { staged: startIndex + hashes.length, total };
    },
    multipartFinalizeStep: async () => {
      recorded.push({ kind: "finalize-step" });
      if (--finalizeSteps > 0) {
        return {
          done: false as const,
          phase: "cleaning" as const,
          cursor: total - finalizeSteps,
          total,
        };
      }
      return {
        done: true as const,
        fresh: true,
        result: {
          fileId: "f-cli",
          size: total * 1024,
          chunkCount: total,
          fileHash: FILE_HASH,
          path: "/cli.bin",
          mimeType: "application/octet-stream",
          isEncrypted: false,
        },
      };
    },
    multipartFinalize: async () => {
      recorded.push({ kind: "finalize" });
      throw new Error("EINVAL: this session cannot use the one-request finalize");
    },
  } as unknown as MossaicHttpClient;
  return { client, recorded };
}

function hashList(count: number): string[] {
  return Array.from({ length: count }, (_unused, index) =>
    index.toString(16).padStart(64, "0")
  );
}

describe("CLI / paged multipart transfer", () => {
  it("beginUpload follows every continuation, so resume skips only what landed", async () => {
    const { client, recorded } = pagedClient(4);
    const session = await beginUpload(client, "/cli.bin", { size: 4 * PAGE * 1024 });
    expect(session.landed).toHaveLength(4 * PAGE);
    expect(session.continuation).toBeUndefined();
    // The begin response was one page; the other three were followed.
    expect(recorded.filter((entry) => entry.kind === "status")).toHaveLength(3);
  });

  it("beginUploadPage is the single-request form", async () => {
    const { client, recorded } = pagedClient(4);
    const page = await beginUploadPage(client, "/cli.bin", {
      size: 4 * PAGE * 1024,
    });
    expect(page.landed).toHaveLength(PAGE);
    expect(page.continuation).toBe("c1");
    expect(recorded).toEqual([{ kind: "begin" }]);
  });

  it("statusUpload reports the whole landed set, statusUploadPage one page", async () => {
    const { client, recorded } = pagedClient(3);
    const session = await beginUploadPage(client, "/cli.bin", {
      size: 3 * PAGE * 1024,
    });
    recorded.length = 0;

    const complete = await statusUpload(client, session);
    expect(complete.landed).toHaveLength(3 * PAGE);
    expect(complete.continuation).toBeUndefined();
    expect(complete.bytesUploaded).toBe(3 * PAGE);
    expect(recorded).toEqual([
      { kind: "status" },
      { kind: "status", continuation: "c1" },
      { kind: "status", continuation: "c2" },
    ]);

    recorded.length = 0;
    const one = await statusUploadPage(client, session);
    expect(one.landed).toHaveLength(PAGE);
    expect(one.continuation).toBe("c1");
    expect(recorded).toEqual([{ kind: "status" }]);
  });

  it("finalizeUpload stages the manifest in pages and steps it to a result", async () => {
    const { client, recorded } = pagedClient(2);
    const session = await beginUploadPage(client, "/cli.bin", {
      size: 2 * PAGE * 1024,
    });
    recorded.length = 0;

    const result = await finalizeUpload(client, session, hashList(2 * PAGE));
    expect(result).toMatchObject({ fileId: "f-cli", fileHash: FILE_HASH });
    // The one-request route is never touched for a session the server would
    // refuse it on; the manifest goes over a page at a time instead.
    expect(recorded.filter((entry) => entry.kind === "finalize")).toEqual([]);
    expect(recorded.filter((entry) => entry.kind === "hash-page")).toEqual([
      { kind: "hash-page", startIndex: 0 },
      { kind: "hash-page", startIndex: PAGE },
    ]);
    expect(recorded.filter((entry) => entry.kind === "finalize-step"))
      .toHaveLength(3);
  });

  it("bounds what one transfer call may spend", () => {
    expect(TRANSFER_OPERATION_REQUEST_BUDGET).toBeGreaterThan(16);
    expect(Number.isSafeInteger(TRANSFER_OPERATION_REQUEST_BUDGET)).toBe(true);
  });
});
