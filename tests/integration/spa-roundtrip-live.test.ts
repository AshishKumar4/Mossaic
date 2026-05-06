import { describe, it, expect } from "vitest";
import { SELF, env } from "cloudflare:test";

/**
 * SPA-style roundtrip via canonical /api/vfs/* + auth-bridge token.
 *
 * Drives the SPA's collapsed `useUpload` / `useDownload` flow as the
 * SPA itself runs it post-unification: mint a VFS token from the App
 * session JWT (auth bridge), then write + read via the canonical
 * multipart route. Photo-library data integrity invariant — bytes
 * round-trip via the SAME path that SPA users hit in production.
 */

import { signJWT } from "@core/lib/auth";
import { hashChunk } from "@shared/crypto";
import type { UserDO } from "../../worker/app/objects/user/user-do";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  JWT_SECRET?: string;
}
const TEST_ENV = env as unknown as E;

async function mintVfsToken(userId: string, email: string): Promise<string> {
  // Mint a session JWT identical to what `/api/auth/login` produces, then
  // exchange it via the auth bridge for a 15-min VFS token (the SPA's
  // `getVfsToken()` flow).
  const sessionToken = await signJWT(TEST_ENV as never, { userId, email });
  const res = await SELF.fetch("https://test/api/auth/vfs-token", {
    method: "POST",
    headers: { Authorization: `Bearer ${sessionToken}` },
  });
  expect(res.ok).toBe(true);
  const { token } = (await res.json()) as { token: string };
  return token;
}

describe("SPA roundtrip via canonical /api/vfs/* + auth-bridge", () => {
  it("upload via /api/vfs/multipart/* → download by hash → bytes round-trip", async () => {
    const userId = "spa-rt-canonical";
    const vfsToken = await mintVfsToken(userId, "rt@x.test");

    const original = new Uint8Array(20 * 1024); // chunked tier
    for (let i = 0; i < original.length; i++) original[i] = (i * 7 + 13) & 0xff;

    // Begin.
    const beginRes = await SELF.fetch(
      "https://test/api/vfs/multipart/begin",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${vfsToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          path: "/spa-roundtrip.bin",
          size: original.byteLength,
          mimeType: "application/octet-stream",
          chunkSize: 8192,
        }),
      }
    );
    expect(beginRes.ok).toBe(true);
    const begin = (await beginRes.json()) as {
      uploadId: string;
      sessionToken: string;
      totalChunks: number;
    };
    expect(begin.totalChunks).toBe(Math.ceil(original.byteLength / 8192));

    // PUT each chunk.
    const hashes: string[] = [];
    for (let i = 0; i < begin.totalChunks; i++) {
      const start = i * 8192;
      const end = Math.min(start + 8192, original.byteLength);
      const slice = original.slice(start, end);
      const h = await hashChunk(slice);
      hashes.push(h);
      const r = await SELF.fetch(
        `https://test/api/vfs/multipart/${begin.uploadId}/chunk/${i}`,
        {
          method: "PUT",
          headers: {
            Authorization: `Bearer ${vfsToken}`,
            "X-Session-Token": begin.sessionToken,
            "Content-Type": "application/octet-stream",
          },
          body: slice,
        }
      );
      expect(r.ok).toBe(true);
    }

    // Finalize.
    const fin = await SELF.fetch(
      "https://test/api/vfs/multipart/finalize",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${vfsToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          uploadId: begin.uploadId,
          chunkHashList: hashes,
        }),
      }
    );
    expect(fin.ok).toBe(true);

    // Read back via canonical /api/vfs/readFile (POST { path }).
    const readRes = await SELF.fetch("https://test/api/vfs/readFile", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${vfsToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/spa-roundtrip.bin" }),
    });
    expect(readRes.ok).toBe(true);
    const readBack = new Uint8Array(await readRes.arrayBuffer());
    expect(readBack).toEqual(original);
  });
});
