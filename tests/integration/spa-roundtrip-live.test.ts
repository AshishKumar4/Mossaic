import { describe, it, expect } from "vitest";
import { SELF, env } from "cloudflare:test";

/**
 * Phase 17.6 — SPA-style roundtrip tests.
 *
 * Drives the App-pinned multipart route end-to-end as the SPA's
 * collapsed `useUpload` / `useDownload` hooks would, but without
 * the React state — a pure HTTP roundtrip via `SELF.fetch`.
 *
 *   S1.  Upload via `/api/upload/multipart/*` → download via
 *        `/api/download/chunk/*` (legacy chunk endpoint, the same
 *        endpoint the SPA's `chunkFetchBaseOverride` targets) →
 *        bytes round-trip.
 *
 *   S2.  Photo-library data integrity: bytes uploaded via the new
 *        path are byte-for-byte equal when fetched via the legacy
 *        download path. This is the load-bearing migration-safety
 *        invariant.
 *
 *   S3.  download-token route returns a manifest with `mimeType`
 *        populated — required for the SPA's Blob construction in
 *        `useDownload`.
 */

import { signJWT } from "@core/lib/auth";
import { hashChunk } from "@shared/crypto";
import { userDOName } from "@core/lib/utils";
import type { UserDO } from "../../worker/app/objects/user/user-do";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  JWT_SECRET?: string;
}
const TEST_ENV = env as unknown as E;

async function mintAppJWT(userId: string, email: string): Promise<string> {
  return signJWT(TEST_ENV as never, { userId, email });
}

function userStub(userId: string) {
  const id = TEST_ENV.MOSSAIC_USER.idFromName(userDOName(userId));
  return TEST_ENV.MOSSAIC_USER.get(id);
}

async function seedUser(userId: string, email: string): Promise<string> {
  const stub = userStub(userId);
  await stub.appHandleSignup(email, "password-12345");
  return mintAppJWT(userId, email);
}

describe("Phase 17.6 — SPA-style roundtrip via App-pinned multipart route", () => {
  it("S1 — upload via /api/upload/multipart/* → download via /api/download/chunk/* round-trips bytes", async () => {
    const userId = "spa-rt-s1";
    const tok = await seedUser(userId, "s1@x.test");

    const original = new Uint8Array(300);
    for (let i = 0; i < 300; i++) original[i] = (i * 7 + 13) & 0xff;

    // Begin.
    const beginRes = await SELF.fetch(
      "https://test/api/upload/multipart/begin",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${tok}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          path: "spa-roundtrip.bin",
          size: 300,
          mimeType: "image/png",
          chunkSize: 100,
        }),
      }
    );
    const begin = (await beginRes.json()) as {
      uploadId: string;
      sessionToken: string;
      totalChunks: number;
    };
    expect(begin.totalChunks).toBe(3);

    // Put chunks.
    const hashes: string[] = [];
    for (let i = 0; i < 3; i++) {
      const slice = original.slice(i * 100, (i + 1) * 100);
      const h = await hashChunk(slice);
      hashes.push(h);
      const r = await SELF.fetch(
        `https://test/api/upload/multipart/${begin.uploadId}/chunk/${i}`,
        {
          method: "PUT",
          headers: {
            Authorization: `Bearer ${tok}`,
            "X-Session-Token": begin.sessionToken,
            "Content-Type": "application/octet-stream",
          },
          body: slice,
        }
      );
      expect(r.ok).toBe(true);
    }

    // Finalize.
    const fin = await SELF.fetch("https://test/api/upload/multipart/finalize", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${tok}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        uploadId: begin.uploadId,
        chunkHashList: hashes,
      }),
    });
    const finalized = (await fin.json()) as { fileId: string };
    expect(finalized.fileId).toBe(begin.uploadId);

    // Download via legacy /api/download/chunk/*. This is the path the
    // SPA's `chunkFetchBaseOverride: "/api/download"` resolves to via
    // `fetchChunkByHash` in `sdk/src/http.ts`.
    const readBack = new Uint8Array(300);
    for (let i = 0; i < 3; i++) {
      const r = await SELF.fetch(
        `https://test/api/download/chunk/${finalized.fileId}/${i}`,
        {
          headers: { Authorization: `Bearer ${tok}` },
        }
      );
      expect(r.ok).toBe(true);
      const slice = new Uint8Array(await r.arrayBuffer());
      readBack.set(slice, i * 100);
    }
    expect(readBack).toEqual(original);
  });

  it("S2 — photo-library data integrity: same bytes via /api/files + /api/download path", async () => {
    const userId = "spa-rt-s2";
    const tok = await seedUser(userId, "s2@x.test");

    const original = new Uint8Array(150);
    for (let i = 0; i < 150; i++) original[i] = (i + 50) & 0xff;

    // Upload via new multipart path.
    const beginRes = await SELF.fetch(
      "https://test/api/upload/multipart/begin",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${tok}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          path: "integrity.jpg",
          size: 150,
          mimeType: "image/jpeg",
          chunkSize: 150,
        }),
      }
    );
    const begin = (await beginRes.json()) as {
      uploadId: string;
      sessionToken: string;
    };
    const h = await hashChunk(original);
    await SELF.fetch(
      `https://test/api/upload/multipart/${begin.uploadId}/chunk/0`,
      {
        method: "PUT",
        headers: {
          Authorization: `Bearer ${tok}`,
          "X-Session-Token": begin.sessionToken,
          "Content-Type": "application/octet-stream",
        },
        body: original,
      }
    );
    await SELF.fetch("https://test/api/upload/multipart/finalize", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${tok}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ uploadId: begin.uploadId, chunkHashList: [h] }),
    });

    // List via legacy /api/files.
    const listRes = await SELF.fetch("https://test/api/files", {
      headers: { Authorization: `Bearer ${tok}` },
    });
    const list = (await listRes.json()) as {
      files: Array<{ fileName: string; fileId: string; status: string }>;
    };
    const photo = list.files.find((f) => f.fileName === "integrity.jpg");
    expect(photo).toBeDefined();
    expect(photo!.fileId).toBe(begin.uploadId);
    expect(photo!.status).toBe("complete");

    // Read back via legacy /api/download/chunk.
    const r = await SELF.fetch(
      `https://test/api/download/chunk/${begin.uploadId}/0`,
      { headers: { Authorization: `Bearer ${tok}` } }
    );
    const bytes = new Uint8Array(await r.arrayBuffer());
    expect(bytes).toEqual(original);
    expect(await hashChunk(bytes)).toBe(h);
  });

  it("S3 — download-token returns manifest with mimeType populated", async () => {
    const userId = "spa-rt-s3";
    const tok = await seedUser(userId, "s3@x.test");

    // Upload with a specific mimeType.
    const beginRes = await SELF.fetch(
      "https://test/api/upload/multipart/begin",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${tok}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          path: "dl-token.png",
          size: 50,
          mimeType: "image/png",
          chunkSize: 50,
        }),
      }
    );
    const begin = (await beginRes.json()) as {
      uploadId: string;
      sessionToken: string;
    };
    const slice = new Uint8Array(50);
    const h = await hashChunk(slice);
    await SELF.fetch(
      `https://test/api/upload/multipart/${begin.uploadId}/chunk/0`,
      {
        method: "PUT",
        headers: {
          Authorization: `Bearer ${tok}`,
          "X-Session-Token": begin.sessionToken,
          "Content-Type": "application/octet-stream",
        },
        body: slice,
      }
    );
    await SELF.fetch("https://test/api/upload/multipart/finalize", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${tok}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ uploadId: begin.uploadId, chunkHashList: [h] }),
    });

    // Request download token.
    const dlRes = await SELF.fetch(
      "https://test/api/upload/multipart/download-token",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${tok}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path: begin.uploadId }),
      }
    );
    expect(dlRes.ok).toBe(true);
    const dl = (await dlRes.json()) as {
      token: string;
      manifest: {
        fileId: string;
        size: number;
        mimeType?: string;
        chunks: Array<{ index: number; size: number; hash: string }>;
      };
    };
    expect(dl.token).toBeTruthy();
    expect(dl.manifest.fileId).toBe(begin.uploadId);
    expect(dl.manifest.size).toBe(50);
    // Phase 17.6 — mimeType is required for SPA Blob construction.
    expect(dl.manifest.mimeType).toBe("image/png");
    expect(dl.manifest.chunks.length).toBe(1);
  });
});
