import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";
import type { UserDO } from "@app/objects/user/user-do";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * App round-trip smoke.
 *
 * The App runs the SAME UserDO class as the canonical SDK; auth is
 * the App's only domain-specific surface. Round-trip:
 *
 *   1. signup → user (typed RPC `appHandleSignup`)
 *   2. canonical `vfsWriteFile(scope, path, bytes, {mimeType})`
 *   3. canonical `vfsExists` returns true
 *   4. canonical `vfsReadFile(scope, path)` echoes the bytes
 *   5. listFiles still works (legacy `files` table populated by
 *      vfsWriteFile through the unified schema)
 */

interface Env {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}

const E = env as unknown as Env;
const userStub = (tenant: string): DurableObjectStub<UserDO> =>
  E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
  );

describe("App round-trip via canonical VFS", () => {
  it("auth → vfsWriteFile → vfsReadFile echoes bytes", async () => {
    const tenant = "smoke-user";
    const stub = userStub(tenant);

    const { userId } = await stub.appHandleSignup(
      "smoke@example.com",
      "password123"
    );
    expect(userId).toBeTruthy();

    const scope = { ns: "default", tenant } as const;
    const payload = new TextEncoder().encode("hello world via canonical vfs");

    await stub.vfsWriteFile(scope, "/smoke.txt", payload, {
      mimeType: "text/plain",
    });

    expect(await stub.vfsExists(scope, "/smoke.txt")).toBe(true);

    const bytes = await stub.vfsReadFile(scope, "/smoke.txt");
    expect(new Uint8Array(bytes)).toEqual(payload);
  });

  it("vfsWriteFile populates the unified files table for gallery listFiles", async () => {
    const tenant = "list-smoke";
    const stub = userStub(tenant);

    const scope = { ns: "default", tenant } as const;
    for (const name of ["a.txt", "b.txt"]) {
      await stub.vfsWriteFile(
        scope,
        `/${name}`,
        new TextEncoder().encode("xxxxx"),
        { mimeType: "text/plain" }
      );
    }

    const list = await stub.appListFiles(tenant, null);
    expect(list.files.map((f) => f.fileName).sort()).toEqual([
      "a.txt",
      "b.txt",
    ]);
  });
});
