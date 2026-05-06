import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * vfsReadPreview integration tests. Drives the on-demand
 * preview pipeline through the typed UserDO RPC. Tests cover:
 *
 *  - renderer dispatch by MIME (code / waveform / icon-card)
 *  - variant-table caching (cold call → warm call)
 *  - encryption boundary (ENOTSUP)
 *  - error surface (ENOENT, EISDIR)
 *  - cross-tenant content-addressed dedup
 *  - custom-variant rendering (no cache row)
 *
 * The miniflare test environment intentionally OMITS the IMAGES
 * binding from `wrangler.test.jsonc`; the image renderer therefore
 * throws `EMOSSAIC_UNAVAILABLE` and the pipeline falls back to
 * icon-card. Tests assert on the renderer kind they expect to win.
 */

import type { UserDO } from "@app/objects/user/user-do";
import type { ShardDO } from "@core/objects/shard/shard-do";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}
const E = env as unknown as E;

async function seedUser(
  stub: DurableObjectStub<UserDO>,
  email: string
): Promise<string> {
  const { userId } = await stub.appHandleSignup(email, "abcd1234");
  return userId;
}

/** Inline-tier write through vfsWriteFile (≤16 KB lands in `files.inline_data`). */
async function writeInline(
  stub: DurableObjectStub<UserDO>,
  userId: string,
  path: string,
  bytes: Uint8Array,
  mimeType: string
): Promise<void> {
  const scope = { ns: "default", tenant: userId };
  await stub.vfsWriteFile(scope, path, bytes, { mimeType });
}

describe("vfsReadPreview — happy paths", () => {
  it("P1 — text file returns SVG via the code renderer fallback", async () => {
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName("preview:p1")
    );
    const userId = await seedUser(stub, "p1@e.com");
    const src = new TextEncoder().encode(
      "function hi() {\n  return 42;\n}\n"
    );
    await writeInline(stub, userId, "/hi.ts", src, "text/typescript");

    const scope = { ns: "default", tenant: userId };
    const out = await stub.vfsReadPreview(scope, "/hi.ts", {
      variant: "thumb",
    });

    expect(out.mimeType).toBe("image/svg+xml");
    expect(out.rendererKind).toBe("code-svg");
    expect(out.fromVariantTable).toBe(false);
    const text = new TextDecoder().decode(out.bytes);
    expect(text).toContain("<svg");
    expect(text).toContain("</svg>");
  });

  it("P2 — second call hits the variant cache (fromVariantTable=true)", async () => {
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName("preview:p2")
    );
    const userId = await seedUser(stub, "p2@e.com");
    await writeInline(
      stub,
      userId,
      "/note.txt",
      new TextEncoder().encode("hello world\n"),
      "text/plain"
    );
    const scope = { ns: "default", tenant: userId };

    const cold = await stub.vfsReadPreview(scope, "/note.txt", {
      variant: "thumb",
    });
    expect(cold.fromVariantTable).toBe(false);

    const warm = await stub.vfsReadPreview(scope, "/note.txt", {
      variant: "thumb",
    });
    expect(warm.fromVariantTable).toBe(true);
    // Same bytes → same hash → byte-identical SVG.
    expect(warm.bytes).toEqual(cold.bytes);
  });

  it("P3 — unknown binary MIME falls through to the icon-card renderer", async () => {
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName("preview:p3")
    );
    const userId = await seedUser(stub, "p3@e.com");
    await writeInline(
      stub,
      userId,
      "/blob.bin",
      new Uint8Array([0, 1, 2, 3, 4, 5]),
      "application/octet-stream"
    );
    const scope = { ns: "default", tenant: userId };

    const out = await stub.vfsReadPreview(scope, "/blob.bin", {
      variant: "thumb",
    });
    expect(out.rendererKind).toBe("icon-card");
    expect(out.mimeType).toBe("image/svg+xml");
    const text = new TextDecoder().decode(out.bytes);
    expect(text).toContain("blob.bin");
  });

  it("P4 — audio file dispatches to the waveform renderer", async () => {
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName("preview:p4")
    );
    const userId = await seedUser(stub, "p4@e.com");
    // Use 8 KB of pseudo-PCM (renderer doesn't decode; reads first
    // bytes deterministically).
    const audio = new Uint8Array(8192);
    for (let i = 0; i < audio.length; i++) audio[i] = (i * 7) & 0xff;
    await writeInline(stub, userId, "/song.mp3", audio, "audio/mpeg");
    const scope = { ns: "default", tenant: userId };

    const out = await stub.vfsReadPreview(scope, "/song.mp3", {
      variant: "thumb",
    });
    expect(out.rendererKind).toBe("waveform-svg");
    expect(out.mimeType).toBe("image/svg+xml");
  });
});

describe("vfsReadPreview — errors and gates", () => {
  it("P5 — ENOENT on missing path", async () => {
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName("preview:p5")
    );
    const userId = await seedUser(stub, "p5@e.com");
    const scope = { ns: "default", tenant: userId };
    await expect(
      stub.vfsReadPreview(scope, "/nope", { variant: "thumb" })
    ).rejects.toThrow(/ENOENT/);
  });

  it("P6 — EISDIR when path is a directory", async () => {
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName("preview:p6")
    );
    const userId = await seedUser(stub, "p6@e.com");
    await stub.appCreateFolder(userId, "docs", null);
    const scope = { ns: "default", tenant: userId };
    await expect(
      stub.vfsReadPreview(scope, "/docs", { variant: "thumb" })
    ).rejects.toThrow(/EISDIR/);
  });

  it("P7 — ENOTSUP on encrypted files", async () => {
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName("preview:p7")
    );
    const userId = await seedUser(stub, "p7@e.com");
    // Seed a row with encryption_mode set to simulate an encrypted
    // file. (We bypass the upload pipeline because this test only
    // exercises the readPreview gate.)
    await runInDurableObject(stub, async (_inst, state) => {
      const sql = state.storage.sql;
      const now = Date.now();
      sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name,
           file_size, file_hash, mime_type, chunk_size, chunk_count,
           pool_size, status, created_at, updated_at, mode, node_kind,
           encryption_mode, inline_data)
         VALUES ('enc-id', ?, NULL, 'secret.txt', 5, '', 'text/plain',
           5, 0, 32, 'complete', ?, ?, 420, 'file', 'random', X'68656c6c6f')`,
        userId,
        now,
        now
      );
    });
    const scope = { ns: "default", tenant: userId };
    await expect(
      stub.vfsReadPreview(scope, "/secret.txt", { variant: "thumb" })
    ).rejects.toThrow(/ENOTSUP/);
  });
});

describe("vfsReadPreview — caching + dedup", () => {
  it("P8 — variant_kind=medium and =thumb produce distinct rows for the same file", async () => {
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName("preview:p8")
    );
    const userId = await seedUser(stub, "p8@e.com");
    await writeInline(
      stub,
      userId,
      "/code.js",
      new TextEncoder().encode("const x = 1;\n"),
      "text/javascript"
    );
    const scope = { ns: "default", tenant: userId };

    await stub.vfsReadPreview(scope, "/code.js", { variant: "thumb" });
    await stub.vfsReadPreview(scope, "/code.js", { variant: "medium" });

    await runInDurableObject(stub, async (_inst, state) => {
      const rows = state.storage.sql
        .exec(
          "SELECT variant_kind FROM file_variants ORDER BY variant_kind"
        )
        .toArray();
      const kinds = rows.map((r) => (r as { variant_kind: string }).variant_kind);
      expect(kinds).toContain("thumb");
      expect(kinds).toContain("medium");
    });
  });

  it("P9 — identical content from different files dedupes to the same chunk_hash", async () => {
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName("preview:p9")
    );
    const userId = await seedUser(stub, "p9@e.com");
    const same = new TextEncoder().encode("identical content\n");
    await writeInline(stub, userId, "/a.txt", same, "text/plain");
    await writeInline(stub, userId, "/b.txt", same, "text/plain");
    const scope = { ns: "default", tenant: userId };

    const a = await stub.vfsReadPreview(scope, "/a.txt", {
      variant: "thumb",
    });
    const b = await stub.vfsReadPreview(scope, "/b.txt", {
      variant: "thumb",
    });

    // Different file_ids → distinct rows, but identical input bytes
    // → identical SVG output → identical chunk_hash.
    await runInDurableObject(stub, async (_inst, state) => {
      const rows = state.storage.sql
        .exec(
          "SELECT file_id, chunk_hash FROM file_variants WHERE variant_kind='thumb'"
        )
        .toArray() as { file_id: string; chunk_hash: string }[];
      expect(rows).toHaveLength(2);
      expect(rows[0].chunk_hash).toBe(rows[1].chunk_hash);
      expect(rows[0].file_id).not.toBe(rows[1].file_id);
    });
    // And the bytes returned are byte-identical.
    expect(a.bytes).toEqual(b.bytes);
  });

  it("P10 — custom variant {width:128} renders without leaving a `custom:*` row absent (uses encoded key)", async () => {
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName("preview:p10")
    );
    const userId = await seedUser(stub, "p10@e.com");
    await writeInline(
      stub,
      userId,
      "/c.txt",
      new TextEncoder().encode("custom variant test\n"),
      "text/plain"
    );
    const scope = { ns: "default", tenant: userId };

    const r1 = await stub.vfsReadPreview(scope, "/c.txt", {
      variant: { width: 128, height: 128, fit: "cover" },
    });
    expect(r1.fromVariantTable).toBe(false);

    // Second call — should hit the cache via the encoded variant key.
    const r2 = await stub.vfsReadPreview(scope, "/c.txt", {
      variant: { width: 128, height: 128, fit: "cover" },
    });
    expect(r2.fromVariantTable).toBe(true);

    // Confirm the row's variant_kind was encoded, not stringified-as-object.
    await runInDurableObject(stub, async (_inst, state) => {
      const rows = state.storage.sql
        .exec("SELECT variant_kind FROM file_variants WHERE file_id IN (SELECT file_id FROM files WHERE file_name='c.txt')")
        .toArray();
      const kinds = rows.map(
        (r) => (r as { variant_kind: string }).variant_kind
      );
      expect(kinds.some((k) => k.startsWith("custom:w128h128"))).toBe(true);
      expect(kinds.every((k) => k !== "[object Object]")).toBe(true);
    });
  });
});
