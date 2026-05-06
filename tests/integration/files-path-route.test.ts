import { describe, it, expect } from "vitest";
import { SELF, env } from "cloudflare:test";
import { signJWT } from "@core/lib/auth";
import type { UserDO } from "../../worker/app/objects/user/user-do";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * `GET /api/files/:fileId/path` — fileId → absolute VFS path
 *
 * The SPA's `useDownload` hook calls this before invoking the SDK's
 * `parallelDownload(client, path, ...)`. The SDK addresses files by
 * path; the SPA's UI addresses files by fileId; this route is the
 * documented bridge.
 *
 *   F.1  authenticated lookup of an existing file → 200 + correct path
 *   F.2  fileId not in caller's tenant → 404 (tenant isolation)
 *   F.3  unauthenticated → 401
 *   F.4  file in a nested folder → path reconstructs the full chain
 *   F.5  soft-deleted file → 404
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  JWT_SECRET?: string;
}
const TEST_ENV = env as unknown as E;

async function mintSessionJWT(userId: string, email: string): Promise<string> {
  return signJWT(TEST_ENV as never, { userId, email });
}

function userStub(tenant: string): DurableObjectStub<UserDO> {
  return TEST_ENV.MOSSAIC_USER.get(
    TEST_ENV.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
  );
}

async function getFileIdAt(
  stub: DurableObjectStub<UserDO>,
  tenant: string,
  path: string
): Promise<string> {
  const row = await stub.appResolveFileByPath(tenant, path);
  if (!row) throw new Error(`no file row at ${path}`);
  return row.file_id;
}

describe("GET /api/files/:fileId/path", () => {
  it("F.1 — authenticated lookup returns the absolute path", async () => {
    const userId = "files-path-f1";
    const stub = userStub(userId);
    const scope = { ns: "default", tenant: userId } as const;
    await stub.vfsWriteFile(scope, "/photo.png", new Uint8Array([1, 2, 3]), {
      mimeType: "image/png",
    });
    const fileId = await getFileIdAt(stub, userId, "/photo.png");
    const token = await mintSessionJWT(userId, "f1@x.test");

    const res = await SELF.fetch(
      `https://test/api/files/${fileId}/path`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { path: string; mimeType: string };
    expect(body.path).toBe("/photo.png");
    expect(body.mimeType).toBe("image/png");
  });

  it("F.2 — fileId in a different tenant → 404 (tenant isolation)", async () => {
    const aliceId = "files-path-f2-alice";
    const bobId = "files-path-f2-bob";
    const aliceStub = userStub(aliceId);
    const aliceScope = { ns: "default", tenant: aliceId } as const;
    await aliceStub.vfsWriteFile(
      aliceScope,
      "/secret.txt",
      new TextEncoder().encode("alice-only"),
      { mimeType: "text/plain" }
    );
    const aliceFileId = await getFileIdAt(aliceStub, aliceId, "/secret.txt");

    // Bob (a different user) tries to look up alice's fileId via his own token.
    const bobToken = await mintSessionJWT(bobId, "f2-bob@x.test");
    const res = await SELF.fetch(
      `https://test/api/files/${aliceFileId}/path`,
      { headers: { Authorization: `Bearer ${bobToken}` } }
    );
    // Bob's UserDO has no row with alice's file_id → 404, not 403.
    // This is sufficient because the DO instance addressed is bob's,
    // and bob's `files` table genuinely doesn't have alice's row.
    expect(res.status).toBe(404);
  });

  it("F.3 — unauthenticated → 401", async () => {
    const res = await SELF.fetch(`https://test/api/files/abc123/path`);
    expect(res.status).toBe(401);
  });

  it("F.4 — file in a nested folder → path reconstructs the full chain", async () => {
    const userId = "files-path-f4";
    const stub = userStub(userId);
    const { userId: signedUid } = await stub.appHandleSignup(
      "f4@x.test",
      "abcd1234"
    );
    void signedUid;
    const home = await stub.appCreateFolder(userId, "home", null);
    const work = await stub.appCreateFolder(userId, "work", home.folderId);
    const scope = { ns: "default", tenant: userId } as const;
    await stub.vfsWriteFile(
      scope,
      "/home/work/notes.md",
      new TextEncoder().encode("# hi"),
      { mimeType: "text/markdown" }
    );
    const fileId = await getFileIdAt(stub, userId, "/home/work/notes.md");
    const token = await mintSessionJWT(userId, "f4@x.test");

    const res = await SELF.fetch(
      `https://test/api/files/${fileId}/path`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { path: string; mimeType: string };
    expect(body.path).toBe("/home/work/notes.md");
    expect(body.mimeType).toBe("text/markdown");
  });

  it("F.5 — soft-deleted file → 404", async () => {
    const userId = "files-path-f5";
    const stub = userStub(userId);
    const scope = { ns: "default", tenant: userId } as const;
    await stub.vfsWriteFile(scope, "/gone.txt", new TextEncoder().encode("x"), {
      mimeType: "text/plain",
    });
    const fileId = await getFileIdAt(stub, userId, "/gone.txt");
    // Soft-delete via the App's typed RPC (status='deleted').
    await stub.appDeleteFile(fileId, userId);
    const token = await mintSessionJWT(userId, "f5@x.test");

    const res = await SELF.fetch(
      `https://test/api/files/${fileId}/path`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    expect(res.status).toBe(404);
  });

  it("F.6 — empty file (0 bytes) resolves correctly", async () => {
    const userId = "files-path-f6";
    const stub = userStub(userId);
    const scope = { ns: "default", tenant: userId } as const;
    await stub.vfsWriteFile(scope, "/empty.bin", new Uint8Array(0), {
      mimeType: "application/octet-stream",
    });
    const fileId = await getFileIdAt(stub, userId, "/empty.bin");
    const token = await mintSessionJWT(userId, "f6@x.test");
    const res = await SELF.fetch(
      `https://test/api/files/${fileId}/path`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { path: string; mimeType: string };
    expect(body.path).toBe("/empty.bin");
  });

  it("F.7 — non-ASCII filename round-trips", async () => {
    const userId = "files-path-f7";
    const stub = userStub(userId);
    const scope = { ns: "default", tenant: userId } as const;
    // Note: vfs name validation rejects forbidden chars per
    // shared/vfs-paths.ts; emoji + unicode letters are accepted.
    const name = "café-naïve-écrit.txt";
    await stub.vfsWriteFile(
      scope,
      `/${name}`,
      new TextEncoder().encode("hi"),
      { mimeType: "text/plain" }
    );
    const fileId = await getFileIdAt(stub, userId, `/${name}`);
    const token = await mintSessionJWT(userId, "f7@x.test");
    const res = await SELF.fetch(
      `https://test/api/files/${fileId}/path`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { path: string };
    expect(body.path).toBe(`/${name}`);
  });

  it("F.8 — deeply nested path (8 levels) reconstructs full chain", async () => {
    const userId = "files-path-f8";
    const stub = userStub(userId);
    // Build /a/b/c/d/e/f/g/h folder chain.
    let parent: string | null = null;
    const segments = ["a", "b", "c", "d", "e", "f", "g", "h"];
    for (const seg of segments) {
      const created = await stub.appCreateFolder(userId, seg, parent);
      parent = created.folderId;
    }
    const scope = { ns: "default", tenant: userId } as const;
    const fullPath = "/" + segments.join("/") + "/deep.txt";
    await stub.vfsWriteFile(
      scope,
      fullPath,
      new TextEncoder().encode("nested"),
      { mimeType: "text/plain" }
    );
    const fileId = await getFileIdAt(stub, userId, fullPath);
    const token = await mintSessionJWT(userId, "f8@x.test");
    const res = await SELF.fetch(
      `https://test/api/files/${fileId}/path`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { path: string };
    expect(body.path).toBe(fullPath);
  });
});
