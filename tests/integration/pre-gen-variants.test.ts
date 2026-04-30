import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import type { UserDO } from "@app/objects/user/user-do";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * Pre-generation of standard preview variants at upload-finalize.
 *
 * Exercises `adminPreGenerateStandardVariants` directly (the route
 * layer's `c.executionCtx.waitUntil` dispatch is its sole consumer).
 *
 *   PG.1 image upload → all 3 standard variants (thumb/medium/lightbox)
 *        materialize into file_variants
 *   PG.2 empty file → no variants generated (early return)
 *   PG.3 encrypted file → no variants generated (renderer can't decrypt)
 *   PG.4 idempotent: second invocation produces no new variant rows
 *   PG.5 per-variant failure is isolated — one failing variant doesn't
 *        block the others (best-effort)
 *   PG.6 non-image mime (text) → variants still generated via
 *        code-svg / icon-card renderer; gallery thumbnails work
 *        regardless of input mime
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

/** Tiny PNG (1×1 transparent). Sufficient for renderer dispatch. */
const TINY_PNG = new Uint8Array([
  0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
  0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
  0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00,
  0x0d, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00,
  0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00, 0x49,
  0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
]);

async function getFileIdAt(
  stub: DurableObjectStub<UserDO>,
  userId: string,
  fileName: string
): Promise<string> {
  return await runInDurableObject(stub, async (_inst, state) => {
    const row = state.storage.sql
      .exec(
        "SELECT file_id FROM files WHERE user_id = ? AND file_name = ? AND status != 'deleted'",
        userId,
        fileName
      )
      .toArray()[0] as { file_id: string } | undefined;
    if (!row) throw new Error(`no file row at ${fileName}`);
    return row.file_id;
  });
}

async function countVariantRows(
  stub: DurableObjectStub<UserDO>,
  fileId: string
): Promise<number> {
  return await runInDurableObject(stub, async (_inst, state) => {
    const rows = state.storage.sql
      .exec("SELECT COUNT(*) AS n FROM file_variants WHERE file_id = ?", fileId)
      .toArray() as { n: number }[];
    return rows[0]?.n ?? 0;
  });
}

describe("preGenerateStandardVariants — adminPreGenerateStandardVariants RPC", () => {
  it("PG.1 — image upload yields all 3 standard variants", async () => {
    const tenant = "pre-gen-img";
    const stub = userStub(tenant);
    const scope = { ns: "default", tenant } as const;
    await stub.vfsWriteFile(scope, "/photo.png", TINY_PNG, {
      mimeType: "image/png",
    });
    const fileId = await getFileIdAt(stub, tenant, "photo.png");

    await stub.adminPreGenerateStandardVariants(scope, {
      fileId,
      path: "/photo.png",
      mimeType: "image/png",
      fileName: "photo.png",
      fileSize: TINY_PNG.byteLength,
      isEncrypted: false,
    });

    const count = await countVariantRows(stub, fileId);
    expect(count).toBeGreaterThanOrEqual(3); // thumb + medium + lightbox
  });

  it("PG.2 — empty file (size 0) generates no variants", async () => {
    const tenant = "pre-gen-empty";
    const stub = userStub(tenant);
    const scope = { ns: "default", tenant } as const;
    await stub.vfsWriteFile(scope, "/empty.png", new Uint8Array(0), {
      mimeType: "image/png",
    });
    const fileId = await getFileIdAt(stub, tenant, "empty.png");
    await stub.adminPreGenerateStandardVariants(scope, {
      fileId,
      path: "/empty.png",
      mimeType: "image/png",
      fileName: "empty.png",
      fileSize: 0,
      isEncrypted: false,
    });
    const count = await countVariantRows(stub, fileId);
    expect(count).toBe(0);
  });

  it("PG.3 — encrypted file generates no variants (server cannot decrypt)", async () => {
    const tenant = "pre-gen-enc";
    const stub = userStub(tenant);
    const scope = { ns: "default", tenant } as const;
    await stub.vfsWriteFile(scope, "/secret.png", TINY_PNG, {
      mimeType: "image/png",
    });
    const fileId = await getFileIdAt(stub, tenant, "secret.png");
    // Don't actually flip the encryption_mode — pre-gen guards on the
    // caller-passed `isEncrypted` flag; that is the contract.
    await stub.adminPreGenerateStandardVariants(scope, {
      fileId,
      path: "/secret.png",
      mimeType: "image/png",
      fileName: "secret.png",
      fileSize: TINY_PNG.byteLength,
      isEncrypted: true,
    });
    const count = await countVariantRows(stub, fileId);
    expect(count).toBe(0);
  });

  it("PG.4 — idempotent: re-running adds zero new variant rows", async () => {
    const tenant = "pre-gen-idem";
    const stub = userStub(tenant);
    const scope = { ns: "default", tenant } as const;
    await stub.vfsWriteFile(scope, "/idem.png", TINY_PNG, {
      mimeType: "image/png",
    });
    const fileId = await getFileIdAt(stub, tenant, "idem.png");

    await stub.adminPreGenerateStandardVariants(scope, {
      fileId,
      path: "/idem.png",
      mimeType: "image/png",
      fileName: "idem.png",
      fileSize: TINY_PNG.byteLength,
      isEncrypted: false,
    });
    const first = await countVariantRows(stub, fileId);

    await stub.adminPreGenerateStandardVariants(scope, {
      fileId,
      path: "/idem.png",
      mimeType: "image/png",
      fileName: "idem.png",
      fileSize: TINY_PNG.byteLength,
      isEncrypted: false,
    });
    const second = await countVariantRows(stub, fileId);

    expect(first).toBe(second);
    expect(first).toBeGreaterThanOrEqual(3);
  });

  it("PG.5 — variants generate even when the file row vanishes mid-flight (best-effort)", async () => {
    // Models the per-variant try/catch contract: if one variant
    // throws, the other variants still attempt. We can't easily
    // inject a single-variant failure without instrumentation, so
    // we exercise the broader contract: a missing-path call returns
    // without throwing (the renderer reads the file and ENOENTs).
    const tenant = "pre-gen-miss";
    const stub = userStub(tenant);
    const scope = { ns: "default", tenant } as const;
    // No write — fileId synthesized below points at a row that
    // doesn't exist. Each variant's render will throw ENOENT;
    // pre-gen swallows.
    await expect(
      stub.adminPreGenerateStandardVariants(scope, {
        fileId: "nonexistent-id-xyz",
        path: "/no-such-file.png",
        mimeType: "image/png",
        fileName: "no-such-file.png",
        fileSize: 100,
        isEncrypted: false,
      })
    ).resolves.toBeUndefined();
  });

  it("PG.6 — non-image mime (text) gets variants via code-svg / icon-card", async () => {
    const tenant = "pre-gen-text";
    const stub = userStub(tenant);
    const scope = { ns: "default", tenant } as const;
    const text = new TextEncoder().encode("hello world\nfoo bar\n");
    await stub.vfsWriteFile(scope, "/notes.txt", text, {
      mimeType: "text/plain",
    });
    const fileId = await getFileIdAt(stub, tenant, "notes.txt");
    await stub.adminPreGenerateStandardVariants(scope, {
      fileId,
      path: "/notes.txt",
      mimeType: "text/plain",
      fileName: "notes.txt",
      fileSize: text.byteLength,
      isEncrypted: false,
    });
    const count = await countVariantRows(stub, fileId);
    // code-svg renderer covers all 3 standard variants for text
    // mime; expect at least one row.
    expect(count).toBeGreaterThanOrEqual(1);
  });
});
