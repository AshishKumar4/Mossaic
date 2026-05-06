import { describe, it, expect } from "vitest";
import { SELF, env, runInDurableObject } from "cloudflare:test";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * DELETE /api/auth/account — self-service account deletion.
 *
 * Pinned invariants:
 *   D1. After signup → write → DELETE /api/auth/account, the data DO
 *       has 0 file rows for the user, 0 folder rows, and quota.
 *       storage_used = 0.
 *   D2. After delete, login with the same credentials returns 401
 *       (auth row was wiped).
 *   D3. Idempotent: a second DELETE is a no-op (still 200, but
 *       reports 0 files removed and authRowRemoved=false).
 *   D4. Unauthenticated DELETE → 401.
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
}
const E = env as unknown as E;
const NS = "default";

async function signup(email: string): Promise<{
  userId: string;
  token: string;
}> {
  const res = await SELF.fetch("https://test/api/auth/signup", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password: "test-password-123" }),
  });
  expect(res.status).toBe(200);
  return (await res.json()) as { userId: string; token: string };
}

async function mintVfsBearer(sessionJwt: string): Promise<string> {
  const res = await SELF.fetch("https://test/api/auth/vfs-token", {
    method: "POST",
    headers: { Authorization: `Bearer ${sessionJwt}` },
  });
  expect(res.status).toBe(200);
  const { token } = (await res.json()) as { token: string };
  return token;
}

describe("DELETE /api/auth/account", () => {
  it("D1 — wipes file rows + folders + quota on the data DO", async () => {
    const email = "delete-d1@test.example";
    const { userId, token: sessionJwt } = await signup(email);

    // Write a small file via canonical VFS.
    const vfsToken = await mintVfsBearer(sessionJwt);
    const writeRes = await SELF.fetch("https://test/api/vfs/writeFile", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${vfsToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        path: "/seed.txt",
        // hello world (11 bytes) base64-encoded
        data: "aGVsbG8gd29ybGQ=",
      }),
    });
    expect(writeRes.status).toBe(200);

    // Confirm the data DO has at least one file row before delete.
    const dataStub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, userId))
    );
    const before = await runInDurableObject(dataStub, async (_inst, state) => {
      const rows = state.storage.sql
        .exec("SELECT COUNT(*) AS n FROM files WHERE user_id = ?", userId)
        .toArray()[0] as { n: number };
      return rows.n;
    });
    expect(before).toBeGreaterThanOrEqual(1);

    // Delete the account.
    const delRes = await SELF.fetch("https://test/api/auth/account", {
      method: "DELETE",
      headers: { Authorization: `Bearer ${sessionJwt}` },
    });
    expect(delRes.status).toBe(200);
    const report = (await delRes.json()) as {
      ok: true;
      data: {
        filesRemoved: number;
        foldersRemoved: number;
        chunksRemovedFromShards: number;
      };
      authRowRemoved: boolean;
    };
    expect(report.ok).toBe(true);
    expect(report.data.filesRemoved).toBeGreaterThanOrEqual(1);
    expect(report.authRowRemoved).toBe(true);

    // After-state: zero file rows + zero quota usage.
    const after = await runInDurableObject(dataStub, async (_inst, state) => {
      const files = state.storage.sql
        .exec("SELECT COUNT(*) AS n FROM files WHERE user_id = ?", userId)
        .toArray()[0] as { n: number };
      const quota = state.storage.sql
        .exec(
          "SELECT storage_used, file_count FROM quota WHERE user_id = ?",
          userId
        )
        .toArray()[0] as
        | { storage_used: number; file_count: number }
        | undefined;
      return {
        files: files.n,
        used: quota?.storage_used ?? 0,
        count: quota?.file_count ?? 0,
      };
    });
    expect(after.files).toBe(0);
    expect(after.used).toBe(0);
    expect(after.count).toBe(0);
  });

  it("D2 — login with the same credentials returns 401 after delete", async () => {
    const email = "delete-d2@test.example";
    const { token: sessionJwt } = await signup(email);

    const delRes = await SELF.fetch("https://test/api/auth/account", {
      method: "DELETE",
      headers: { Authorization: `Bearer ${sessionJwt}` },
    });
    expect(delRes.status).toBe(200);

    const loginRes = await SELF.fetch("https://test/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password: "test-password-123" }),
    });
    expect(loginRes.status).toBe(401);
  });

  it("D3 — idempotent on a second call", async () => {
    const email = "delete-d3@test.example";
    const { token: sessionJwt } = await signup(email);

    const first = await SELF.fetch("https://test/api/auth/account", {
      method: "DELETE",
      headers: { Authorization: `Bearer ${sessionJwt}` },
    });
    expect(first.status).toBe(200);

    const second = await SELF.fetch("https://test/api/auth/account", {
      method: "DELETE",
      headers: { Authorization: `Bearer ${sessionJwt}` },
    });
    expect(second.status).toBe(200);
    const r2 = (await second.json()) as {
      data: { filesRemoved: number };
      authRowRemoved: boolean;
    };
    expect(r2.data.filesRemoved).toBe(0);
    expect(r2.authRowRemoved).toBe(false);
  });

  it("D4 — unauthenticated DELETE returns 401", async () => {
    const res = await SELF.fetch("https://test/api/auth/account", {
      method: "DELETE",
    });
    expect(res.status).toBe(401);
  });
});
