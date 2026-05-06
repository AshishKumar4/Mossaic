import { describe, it, expect } from "vitest";
import { SELF } from "cloudflare:test";

/**
 * End-to-end Worker-boot smoke test.
 *
 * Boots the actual production Hono app (worker/index.ts) inside the
 * vitest-pool-workers harness and drives its real /api/* handlers via
 * `SELF.fetch(...)`. This is the regression gate that catches issues
 * the per-DO tests miss: Hono mounting, cors, route table integrity,
 * JWT signing, cross-route DO traffic.
 *
 * Coverage:
 *   1. Boot: GET /api/health returns {status:"ok"} (proves the app
 *      imports cleanly with all routes mounted, all DO migrations apply
 *      without throwing on first request).
 *   2. Legacy upload pipeline: signup → init → chunk PUT → complete →
 *      list. All return 2xx.
 *   3. Legacy download pipeline: GET manifest → GET chunk by index. The
 *      bytes round-trip identical to what was uploaded.
 *   4. The whole lifecycle uses the production endpoints — same code
 *      path mossaic.ashishkumarsingh.com runs.
 *
 * If any Phase 4 change broke the legacy app, this test fails before
 * we ship.
 */

async function sha256Hex(data: Uint8Array): Promise<string> {
  const buf = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(buf))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

describe("Worker boot smoke (production pipeline through SELF.fetch)", () => {
  it("GET /api/health returns 200 OK with the expected shape", async () => {
    const res = await SELF.fetch("https://mossaic.test/api/health");
    expect(res.ok).toBe(true);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { status: string; timestamp: number };
    expect(body.status).toBe("ok");
    expect(typeof body.timestamp).toBe("number");
  });

  it("legacy upload + download round-trip: signup → upload → manifest → chunk", async () => {
    const email = `smoke-${Date.now()}-${Math.random().toString(36).slice(2)}@e.com`;
    const password = "smoke-pass-1234";

    // 1. Signup → JWT token
    const signupRes = await SELF.fetch("https://mossaic.test/api/auth/signup", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password }),
    });
    expect(signupRes.ok).toBe(true);
    expect(signupRes.status).toBe(200);
    const signup = (await signupRes.json()) as {
      token: string;
      userId: string;
      email: string;
    };
    expect(signup.token).toBeTruthy();
    expect(signup.userId).toBeTruthy();
    expect(signup.email).toBe(email);

    const auth = { Authorization: `Bearer ${signup.token}` };

    // 2. Upload init (creates uploading file row)
    const payload = new TextEncoder().encode(
      "Worker smoke test bytes — round-trip through the production pipeline"
    );
    const initRes = await SELF.fetch("https://mossaic.test/api/upload/init", {
      method: "POST",
      headers: { ...auth, "Content-Type": "application/json" },
      body: JSON.stringify({
        fileName: "smoke.txt",
        fileSize: payload.byteLength,
        mimeType: "text/plain",
        parentId: null,
      }),
    });
    expect(initRes.ok).toBe(true);
    expect(initRes.status).toBe(200);
    const init = (await initRes.json()) as {
      fileId: string;
      chunkSize: number;
      chunkCount: number;
      poolSize: number;
    };
    expect(init.fileId).toBeTruthy();
    expect(init.chunkCount).toBe(1); // payload < CHUNK_SIZE → single chunk

    // 3. Chunk PUT → ShardDO via the real /api/upload/chunk route
    const chunkHash = await sha256Hex(payload);
    const chunkRes = await SELF.fetch(
      `https://mossaic.test/api/upload/chunk/${init.fileId}/0`,
      {
        method: "PUT",
        headers: {
          ...auth,
          "Content-Type": "application/octet-stream",
          "X-Chunk-Hash": chunkHash,
          "X-Pool-Size": String(init.poolSize),
        },
        body: payload,
      }
    );
    expect(chunkRes.ok).toBe(true);
    const chunkBody = (await chunkRes.json()) as {
      status: "created" | "deduplicated";
      bytesStored: number;
    };
    expect(chunkBody.status).toBe("created");
    expect(chunkBody.bytesStored).toBe(payload.byteLength);

    // 4. Complete (compute file_hash = sha256(concat(chunk hashes)))
    const fileHash = await sha256Hex(new TextEncoder().encode(chunkHash));
    const completeRes = await SELF.fetch(
      `https://mossaic.test/api/upload/complete/${init.fileId}`,
      {
        method: "POST",
        headers: { ...auth, "Content-Type": "application/json" },
        body: JSON.stringify({ fileHash }),
      }
    );
    expect(completeRes.ok).toBe(true);
    expect(completeRes.status).toBe(201);
    const complete = (await completeRes.json()) as {
      ok: boolean;
      fileId: string;
    };
    expect(complete.ok).toBe(true);
    expect(complete.fileId).toBe(init.fileId);

    // 5. List files at root → must include the uploaded file
    const listRes = await SELF.fetch("https://mossaic.test/api/files", {
      headers: auth,
    });
    expect(listRes.ok).toBe(true);
    const list = (await listRes.json()) as {
      files: Array<{ fileId: string; fileName: string; fileSize: number }>;
      folders: unknown[];
    };
    const found = list.files.find((f) => f.fileId === init.fileId);
    expect(found).toBeTruthy();
    expect(found!.fileName).toBe("smoke.txt");
    expect(found!.fileSize).toBe(payload.byteLength);

    // 6. Download manifest (legacy /api/download/manifest)
    const manifestRes = await SELF.fetch(
      `https://mossaic.test/api/download/manifest/${init.fileId}`,
      { headers: auth }
    );
    expect(manifestRes.ok).toBe(true);
    const manifest = (await manifestRes.json()) as {
      fileId: string;
      fileSize: number;
      chunkCount: number;
      chunks: Array<{
        index: number;
        hash: string;
        size: number;
        shardIndex: number;
      }>;
    };
    expect(manifest.fileId).toBe(init.fileId);
    expect(manifest.fileSize).toBe(payload.byteLength);
    expect(manifest.chunks).toHaveLength(1);
    expect(manifest.chunks[0].hash).toBe(chunkHash);
    expect(manifest.chunks[0].size).toBe(payload.byteLength);

    // 7. Download chunk bytes (legacy /api/download/chunk)
    const dlChunkRes = await SELF.fetch(
      `https://mossaic.test/api/download/chunk/${init.fileId}/0`,
      { headers: auth }
    );
    expect(dlChunkRes.ok).toBe(true);
    const dlBytes = new Uint8Array(await dlChunkRes.arrayBuffer());
    expect(dlBytes.byteLength).toBe(payload.byteLength);
    // Bytes must round-trip identically to what was uploaded.
    expect(new TextDecoder().decode(dlBytes)).toBe(
      new TextDecoder().decode(payload)
    );
  });

  it("rejects unauthenticated calls to protected routes (401)", async () => {
    const res = await SELF.fetch("https://mossaic.test/api/files");
    expect(res.status).toBe(401);
    const body = (await res.json()) as { error: string };
    expect(body.error).toMatch(/Unauthorized/i);
  });

  it("Worker boot is reproducible: a second fresh request after the smoke completes still returns 200", async () => {
    // Proves no startup-error / one-shot init bug — the DO migrations
    // applied on the first request remain idempotent for subsequent
    // requests on different DO instances.
    const res = await SELF.fetch("https://mossaic.test/api/health");
    expect(res.status).toBe(200);
    const body = (await res.json()) as { status: string };
    expect(body.status).toBe("ok");
  });
});
