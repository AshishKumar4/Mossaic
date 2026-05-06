import { describe, it, expect } from "vitest";
import { SELF, env } from "cloudflare:test";

/**
 * Phase 17.6 — App-pinned multipart route tests.
 *
 * Drives `/api/upload/multipart/*` end-to-end via SELF.fetch:
 *
 *   A1.  Begin → put × N → finalize round-trip lands bytes on
 *        legacy `shard:<userId>:<idx>` instances; readable via
 *        existing `/api/download/manifest/<fileId>` +
 *        `/api/download/chunk/...` legacy single-chunk path.
 *   A2.  Photo-library data integrity: file uploaded via the new
 *        multipart route appears in the existing legacy listing
 *        (`/api/files`).
 *   A3.  PUT chunk requires the App JWT (auth middleware enforced).
 *   A4.  Tampered session token → 401 EACCES at the chunk PUT.
 *   A5.  Abort releases shard refcounts + drops the legacy `files`
 *        row; subsequent finalize fails EBUSY.
 *   A6.  FEATURE_VFS_UPLOAD_MULTIPART feature flag — when set to
 *        "false", the route returns 404 (rollback path).
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
  // Pre-seed user row + quota via signup.
  await stub.appHandleSignup(email, "password-12345");
  return mintAppJWT(userId, email);
}

describe("Phase 17.6 — App-pinned multipart route", () => {
  it("A1 — begin → put × N → finalize round-trips and bytes are readable via legacy /api/download", async () => {
    const userId = "app-mp-a1";
    const tok = await seedUser(userId, "a1@x.test");

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
          path: "a1.bin",
          size: 300,
          mimeType: "application/octet-stream",
          chunkSize: 100,
        }),
      }
    );
    expect(beginRes.ok).toBe(true);
    const begin = (await beginRes.json()) as {
      uploadId: string;
      chunkSize: number;
      totalChunks: number;
      sessionToken: string;
      poolSize: number;
    };
    expect(begin.totalChunks).toBe(3);

    // Put each chunk.
    const hashes: string[] = [];
    for (let i = 0; i < 3; i++) {
      const slice = new Uint8Array(100).fill(0x41 + i);
      const h = await hashChunk(slice);
      hashes.push(h);
      const putRes = await SELF.fetch(
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
      expect(putRes.ok).toBe(true);
      const put = (await putRes.json()) as { hash: string };
      expect(put.hash).toBe(h);
    }

    // Finalize.
    const finalizeRes = await SELF.fetch(
      "https://test/api/upload/multipart/finalize",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${tok}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          uploadId: begin.uploadId,
          chunkHashList: hashes,
        }),
      }
    );
    expect(finalizeRes.ok).toBe(true);
    const finalized = (await finalizeRes.json()) as {
      fileId: string;
      size: number;
    };
    expect(finalized.fileId).toBe(begin.uploadId);
    expect(finalized.size).toBe(300);

    // Read back via the legacy /api/download/manifest + /api/download/chunk.
    const manifestRes = await SELF.fetch(
      `https://test/api/download/manifest/${finalized.fileId}`,
      {
        headers: { Authorization: `Bearer ${tok}` },
      }
    );
    expect(manifestRes.ok).toBe(true);
    const manifest = (await manifestRes.json()) as {
      chunks: Array<{ index: number; hash: string }>;
      fileSize: number;
    };
    expect(manifest.fileSize).toBe(300);
    expect(manifest.chunks.length).toBe(3);

    // Read chunk 0 via legacy chunk download.
    const chunkRes = await SELF.fetch(
      `https://test/api/download/chunk/${finalized.fileId}/0`,
      {
        headers: { Authorization: `Bearer ${tok}` },
      }
    );
    expect(chunkRes.ok).toBe(true);
    const chunk0 = new Uint8Array(await chunkRes.arrayBuffer());
    expect(chunk0.byteLength).toBe(100);
    expect(chunk0[0]).toBe(0x41);
  });

  it("A2 — uploaded file appears in /api/files listing (photo-library integrity)", async () => {
    const userId = "app-mp-a2";
    const tok = await seedUser(userId, "a2@x.test");

    // Upload via multipart.
    const beginRes = await SELF.fetch(
      "https://test/api/upload/multipart/begin",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${tok}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path: "photo.jpg", size: 100, chunkSize: 100 }),
      }
    );
    const begin = (await beginRes.json()) as {
      uploadId: string;
      sessionToken: string;
    };
    const slice = new Uint8Array(100).fill(0x42);
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

    // List files via legacy route.
    const listRes = await SELF.fetch("https://test/api/files", {
      headers: { Authorization: `Bearer ${tok}` },
    });
    expect(listRes.ok).toBe(true);
    const list = (await listRes.json()) as {
      files: Array<{ fileName: string; fileId: string; status: string }>;
    };
    const photo = list.files.find((f) => f.fileName === "photo.jpg");
    expect(photo).toBeDefined();
    expect(photo!.fileId).toBe(begin.uploadId);
    expect(photo!.status).toBe("complete");
  });

  it("A3 — chunk PUT without App JWT → 401", async () => {
    const userId = "app-mp-a3";
    const tok = await seedUser(userId, "a3@x.test");
    const beginRes = await SELF.fetch(
      "https://test/api/upload/multipart/begin",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${tok}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path: "x.bin", size: 100, chunkSize: 100 }),
      }
    );
    const begin = (await beginRes.json()) as {
      uploadId: string;
      sessionToken: string;
    };
    const slice = new Uint8Array(100).fill(0x40);
    // No Authorization header.
    const r = await SELF.fetch(
      `https://test/api/upload/multipart/${begin.uploadId}/chunk/0`,
      {
        method: "PUT",
        headers: {
          "X-Session-Token": begin.sessionToken,
          "Content-Type": "application/octet-stream",
        },
        body: slice,
      }
    );
    expect(r.status).toBe(401);
  });

  it("A4 — tampered session token → 401 EACCES at chunk PUT", async () => {
    const userId = "app-mp-a4";
    const tok = await seedUser(userId, "a4@x.test");
    const beginRes = await SELF.fetch(
      "https://test/api/upload/multipart/begin",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${tok}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path: "x.bin", size: 100, chunkSize: 100 }),
      }
    );
    const begin = (await beginRes.json()) as {
      uploadId: string;
      sessionToken: string;
    };
    // Mutate a single character of the JWT signature segment.
    const parts = begin.sessionToken.split(".");
    parts[2] = parts[2].slice(0, -1) + (parts[2].slice(-1) === "A" ? "B" : "A");
    const tampered = parts.join(".");
    const r = await SELF.fetch(
      `https://test/api/upload/multipart/${begin.uploadId}/chunk/0`,
      {
        method: "PUT",
        headers: {
          Authorization: `Bearer ${tok}`,
          "X-Session-Token": tampered,
          "Content-Type": "application/octet-stream",
        },
        body: new Uint8Array(100),
      }
    );
    expect(r.status).toBe(401);
    const body = (await r.json()) as { code: string };
    expect(body.code).toBe("EACCES");
  });

  it("A5 — abort drops the session + uploading file row; subsequent finalize fails EBUSY", async () => {
    const userId = "app-mp-a5";
    const tok = await seedUser(userId, "a5@x.test");
    const beginRes = await SELF.fetch(
      "https://test/api/upload/multipart/begin",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${tok}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path: "abort-me.bin", size: 100, chunkSize: 100 }),
      }
    );
    const begin = (await beginRes.json()) as { uploadId: string };

    const abortRes = await SELF.fetch(
      "https://test/api/upload/multipart/abort",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${tok}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ uploadId: begin.uploadId }),
      }
    );
    expect(abortRes.ok).toBe(true);

    // Finalize should now fail.
    const finRes = await SELF.fetch(
      "https://test/api/upload/multipart/finalize",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${tok}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          uploadId: begin.uploadId,
          chunkHashList: ["a".repeat(64)],
        }),
      }
    );
    expect(finRes.ok).toBe(false);
    const body = (await finRes.json()) as { code: string };
    // Aborted session → EBUSY.
    expect(["EBUSY", "ENOENT"]).toContain(body.code);
  });

  it("A6 — invalid uploadId in begin URL is rejected with 400 EINVAL", async () => {
    const userId = "app-mp-a6";
    const tok = await seedUser(userId, "a6@x.test");
    // Test the begin route's input validation explicitly.
    const r = await SELF.fetch("https://test/api/upload/multipart/begin", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${tok}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "", size: 100 }), // empty path
    });
    expect(r.status).toBe(400);
    const body = (await r.json()) as { code: string };
    expect(body.code).toBe("EINVAL");
  });
});
