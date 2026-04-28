import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 1 — Refcount drift fix tests (sdk-impl-plan §0, §8.1).
 *
 * Before the fix: any retried PUT of the same chunk hash for the same
 * (file_id, chunk_index) bumped chunks.ref_count, even though
 * INSERT OR IGNORE INTO chunk_refs was a no-op. Result: ref_count drifted
 * upward, unlinks would never reach 0, blobs never freed.
 *
 * After the fix: ref_count only increments when changes() reports a new
 * row inserted into chunk_refs.
 */

interface Env {
  SHARD_DO: DurableObjectNamespace;
}

const E = env as unknown as Env;
const stubFor = (name: string) =>
  E.SHARD_DO.get(E.SHARD_DO.idFromName(name));

async function putChunk(
  stub: DurableObjectStub,
  hash: string,
  fileId: string,
  chunkIndex: number,
  body: string,
  userId = "u1"
): Promise<Response> {
  return stub.fetch(
    new Request("http://internal/chunk", {
      method: "PUT",
      headers: {
        "X-Chunk-Hash": hash,
        "X-File-Id": fileId,
        "X-Chunk-Index": String(chunkIndex),
        "X-User-Id": userId,
      },
      body,
    })
  );
}

async function readRefcount(
  stub: DurableObjectStub,
  hash: string
): Promise<number> {
  return runInDurableObject(stub, async (_instance, state) => {
    const sql = state.storage.sql;
    const row = sql
      .exec("SELECT ref_count FROM chunks WHERE hash = ?", hash)
      .toArray()[0] as { ref_count: number } | undefined;
    return row ? row.ref_count : -1;
  });
}

async function readRefRowCount(
  stub: DurableObjectStub,
  hash: string
): Promise<number> {
  return runInDurableObject(stub, async (_instance, state) => {
    const sql = state.storage.sql;
    const row = sql
      .exec(
        "SELECT COUNT(*) AS n FROM chunk_refs WHERE chunk_hash = ?",
        hash
      )
      .toArray()[0] as { n: number };
    return row.n;
  });
}

describe("ShardDO chunk refcount", () => {
  it("first PUT creates the chunk with ref_count=1 and one ref row", async () => {
    const stub = stubFor("refcount:first");
    const hash = "a".repeat(64);
    const r = await putChunk(stub, hash, "f1", 0, "hello world");
    expect(r.ok).toBe(true);
    const body = (await r.json()) as { status: string; bytesStored: number };
    expect(body.status).toBe("created");
    expect(body.bytesStored).toBe(11);

    expect(await readRefcount(stub, hash)).toBe(1);
    expect(await readRefRowCount(stub, hash)).toBe(1);
  });

  it("dedup PUT for a NEW (file_id, chunk_index) increments ref_count by 1 and inserts a ref row", async () => {
    const stub = stubFor("refcount:dedup-new");
    const hash = "b".repeat(64);
    await putChunk(stub, hash, "f1", 0, "payload");
    const r = await putChunk(stub, hash, "f2", 0, "payload");
    const body = (await r.json()) as { status: string; bytesStored: number };
    expect(body.status).toBe("deduplicated");
    expect(body.bytesStored).toBe(0);

    expect(await readRefcount(stub, hash)).toBe(2);
    expect(await readRefRowCount(stub, hash)).toBe(2);
  });

  it("retried PUT of SAME (file_id, chunk_index) does NOT drift ref_count (the fix)", async () => {
    const stub = stubFor("refcount:retry-same-slot");
    const hash = "c".repeat(64);
    await putChunk(stub, hash, "fX", 0, "payload");

    // First retry — same file_id + chunk_index. Pre-fix: this would have
    // bumped ref_count to 2 while leaving chunk_refs at 1 row.
    const r1 = await putChunk(stub, hash, "fX", 0, "payload");
    expect((await r1.json() as { status: string }).status).toBe("deduplicated");

    expect(await readRefcount(stub, hash)).toBe(1);
    expect(await readRefRowCount(stub, hash)).toBe(1);

    // Second retry — same again. Still must not drift.
    const r2 = await putChunk(stub, hash, "fX", 0, "payload");
    expect((await r2.json() as { status: string }).status).toBe("deduplicated");

    expect(await readRefcount(stub, hash)).toBe(1);
    expect(await readRefRowCount(stub, hash)).toBe(1);

    // Third retry — same. Still 1.
    const r3 = await putChunk(stub, hash, "fX", 0, "payload");
    expect((await r3.json() as { status: string }).status).toBe("deduplicated");

    expect(await readRefcount(stub, hash)).toBe(1);
    expect(await readRefRowCount(stub, hash)).toBe(1);
  });

  it("mixed retries + new slots: ref_count tracks distinct (file_id, chunk_index) pairs", async () => {
    const stub = stubFor("refcount:mixed");
    const hash = "d".repeat(64);

    // 3 distinct slots + 5 retries on the first slot
    await putChunk(stub, hash, "f1", 0, "data");
    for (let i = 0; i < 5; i++) await putChunk(stub, hash, "f1", 0, "data");
    await putChunk(stub, hash, "f1", 1, "data");
    await putChunk(stub, hash, "f2", 0, "data");

    expect(await readRefcount(stub, hash)).toBe(3);
    expect(await readRefRowCount(stub, hash)).toBe(3);
  });

  it("resurrection: a soft-marked chunk has deleted_at cleared on dedup PUT", async () => {
    const stub = stubFor("refcount:resurrect");
    const hash = "e".repeat(64);

    // Create.
    await putChunk(stub, hash, "f1", 0, "data");

    // Soft-mark via raw SQL (Phase 3 will wire `unlink` to do this; here
    // we simulate the state to confirm the dedup write path clears it).
    await runInDurableObject(stub, async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE chunks SET deleted_at = ? WHERE hash = ?",
        Date.now(),
        hash
      );
    });

    // A fresh slot dedups onto it — should clear deleted_at.
    await putChunk(stub, hash, "f2", 0, "data");

    const deletedAt = await runInDurableObject(stub, async (_instance, state) => {
      const row = state.storage.sql
        .exec("SELECT deleted_at FROM chunks WHERE hash = ?", hash)
        .toArray()[0] as { deleted_at: number | null };
      return row.deleted_at;
    });
    expect(deletedAt).toBeNull();

    // Sanity: ref_count went up by 1 (new slot inserted)
    expect(await readRefcount(stub, hash)).toBe(2);
  });
});
