import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";
import type { UserDO } from "@app/objects/user/user-do";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * App path-resolution RPC integration tests.
 *
 * `appGetFilePath` (fileId → {path, mimeType}) is used by the App's
 * gallery + shared-album routes to translate the photo-library's
 * fileId-keyed URLs into the path that canonical `vfs.readFile()` /
 * `readPreview()` need.
 *
 * `appResolveFileByPath` (path → AppFileRow) is used by the
 * `/api/index/file` SPA-callback after upload-finalize to schedule
 * search indexing.
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
}
const E = env as unknown as E;

function userStub(tenant: string): DurableObjectStub<UserDO> {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
  );
}

describe("appGetFilePath — fileId → path", () => {
  it("returns path + mimeType for a root-level file", async () => {
    const tenant = "pathresolve-root";
    const stub = userStub(tenant);
    const scope = { ns: "default", tenant } as const;

    await stub.vfsWriteFile(
      scope,
      "/photo.png",
      new TextEncoder().encode("fake png bytes"),
      { mimeType: "image/png" }
    );

    // Look up the row to grab its file_id (the test couldn't get it
    // from vfsWriteFile directly).
    const resolved = await stub.appResolveFileByPath(tenant, "/photo.png");
    expect(resolved).not.toBeNull();
    const fileId = resolved!.file_id;

    const got = await stub.appGetFilePath(fileId);
    expect(got).not.toBeNull();
    expect(got!.path).toBe("/photo.png");
    expect(got!.mimeType).toBe("image/png");
  });

  it("walks the parent_id chain for a nested file", async () => {
    const tenant = "pathresolve-nested";
    const stub = userStub(tenant);
    const { userId } = await stub.appHandleSignup("nested@e.com", "abcd1234");
    const home = await stub.appCreateFolder(userId, "home", null);
    await stub.appCreateFolder(userId, "work", home.folderId);

    const scope = { ns: "default", tenant: userId } as const;
    await stub.vfsWriteFile(
      scope,
      "/home/work/notes.md",
      new TextEncoder().encode("hello"),
      { mimeType: "text/markdown" }
    );

    const resolved = await stub.appResolveFileByPath(
      userId,
      "/home/work/notes.md"
    );
    expect(resolved).not.toBeNull();

    const got = await stub.appGetFilePath(resolved!.file_id);
    expect(got).not.toBeNull();
    expect(got!.path).toBe("/home/work/notes.md");
    expect(got!.mimeType).toBe("text/markdown");
  });

  it("returns null for a missing file_id", async () => {
    const stub = userStub("pathresolve-missing");
    const got = await stub.appGetFilePath("nonexistent-id");
    expect(got).toBeNull();
  });
});

describe("appResolveFileByPath — path → AppFileRow", () => {
  it("returns the row for a root-level file", async () => {
    const tenant = "pathresolve-bypath";
    const stub = userStub(tenant);
    const scope = { ns: "default", tenant } as const;

    await stub.vfsWriteFile(
      scope,
      "/file.txt",
      new TextEncoder().encode("xx"),
      { mimeType: "text/plain" }
    );

    const row = await stub.appResolveFileByPath(tenant, "/file.txt");
    expect(row).not.toBeNull();
    expect(row!.file_name).toBe("file.txt");
    expect(row!.mime_type).toBe("text/plain");
    expect(row!.parent_id).toBeNull();
    expect(row!.status).toBe("complete");
  });

  it("returns null for a non-existent path", async () => {
    const stub = userStub("pathresolve-bypath-miss");
    const row = await stub.appResolveFileByPath(
      "pathresolve-bypath-miss",
      "/no-such-file.txt"
    );
    expect(row).toBeNull();
  });

  it("rejects malformed (non-absolute) paths with null", async () => {
    const stub = userStub("pathresolve-bypath-bad");
    const row = await stub.appResolveFileByPath(
      "pathresolve-bypath-bad",
      "relative/file.txt"
    );
    expect(row).toBeNull();
  });

  it("rejects malformed (non-absolute) paths with null", async () => {
    const stub = userStub("pathresolve-bypath-bad");
    const row = await stub.appResolveFileByPath(
      "pathresolve-bypath-bad",
      "relative/file.txt"
    );
    expect(row).toBeNull();
  });
});
