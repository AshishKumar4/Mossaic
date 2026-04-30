import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * Phase 28 Fix 2 — yjs-purge skipped under versioning-on (latent
 * from Phase 25 $1000 bet).
 *
 * Pre-fix, `vfsUnlink` on a yjs-mode file under versioning-on:
 *   1. Hit the versioning fork early (mutations.ts:61).
 *   2. Wrote a tombstone version via `commitVersion(deleted: true)`.
 *   3. RETURNED — never reaching the yjs purge below.
 *
 * Result: head was tombstoned (list/stat/exists report ENOENT) but
 * the yjs runtime kept serving live bytes from `yjs_oplog` rows
 * that survived. `vfsReadFile` short-circuited to `readYjsAsBytes`
 * (Phase 25 added an explicit head_deleted gate to fix the byte-
 * read inconsistency, but the underlying op-log + WS state still
 * leaked indefinitely). Post-fix the versioning fork purges yjs
 * BEFORE writing the tombstone.
 *
 * Cases:
 *   YV1. Yjs file under versioning ON → unlink → yjs_oplog rows are
 *        gone. (Pre-fix they stayed; post-fix they're purged.)
 *   YV2. Yjs file under versioning ON → unlink → yjs_meta row is
 *        gone for the path.
 *   YV3. Yjs file under versioning OFF → unlink still purges (the
 *        original non-versioning code path is unchanged).
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
}
const E = env as unknown as E;
const NS = "default";

function userStub(tenant: string) {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}

const enc = new TextEncoder();

describe("Phase 28 Fix 2 — yjs purge under versioning ON", () => {
  it("YV1 — versioning ON: unlink purges yjs_oplog rows", async () => {
    const tenant = "yv1-oplog-purged";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    await (
      stub as unknown as {
        adminSetVersioning(t: string, e: boolean): Promise<unknown>;
      }
    ).adminSetVersioning(tenant, true);

    // Create a yjs-mode file with content (which writes oplog rows).
    await stub.vfsWriteFile(scope, "/notes.md", enc.encode(""));
    await stub.vfsSetYjsMode(scope, "/notes.md", true);
    await stub.vfsWriteFile(scope, "/notes.md", enc.encode("hello yjs"));

    // Find pathId.
    const pathId = await runInDurableObject(stub, async (_inst, state) => {
      const r = state.storage.sql
        .exec("SELECT file_id FROM files WHERE file_name = 'notes.md'")
        .toArray()[0] as { file_id: string };
      return r.file_id;
    });

    // Pre-unlink: oplog rows exist.
    const oplogBefore = await runInDurableObject(stub, async (_inst, state) => {
      return (
        state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM yjs_oplog WHERE path_id = ?",
            pathId
          )
          .toArray()[0] as { n: number }
      ).n;
    });
    expect(oplogBefore).toBeGreaterThanOrEqual(1);

    // Unlink under versioning-ON.
    await stub.vfsUnlink(scope, "/notes.md");

    // Phase 28 Fix 2 — oplog rows for this path are GONE.
    const oplogAfter = await runInDurableObject(stub, async (_inst, state) => {
      return (
        state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM yjs_oplog WHERE path_id = ?",
            pathId
          )
          .toArray()[0] as { n: number }
      ).n;
    });
    expect(oplogAfter).toBe(0);

    // The tombstone version row is also there (versioning intact).
    const tomb = await runInDurableObject(stub, async (_inst, state) => {
      return (
        state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM file_versions WHERE path_id = ? AND deleted = 1",
            pathId
          )
          .toArray()[0] as { n: number }
      ).n;
    });
    expect(tomb).toBe(1);
  });

  it("YV2 — versioning ON: unlink purges yjs_meta row", async () => {
    const tenant = "yv2-meta-purged";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    await (
      stub as unknown as {
        adminSetVersioning(t: string, e: boolean): Promise<unknown>;
      }
    ).adminSetVersioning(tenant, true);

    await stub.vfsWriteFile(scope, "/n.md", enc.encode(""));
    await stub.vfsSetYjsMode(scope, "/n.md", true);
    await stub.vfsWriteFile(scope, "/n.md", enc.encode("yjs body"));
    const pathId = await runInDurableObject(stub, async (_inst, state) => {
      const r = state.storage.sql
        .exec("SELECT file_id FROM files WHERE file_name = 'n.md'")
        .toArray()[0] as { file_id: string };
      return r.file_id;
    });
    const metaBefore = await runInDurableObject(stub, async (_inst, state) => {
      return (
        state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM yjs_meta WHERE path_id = ?",
            pathId
          )
          .toArray()[0] as { n: number }
      ).n;
    });
    expect(metaBefore).toBeGreaterThanOrEqual(1);

    await stub.vfsUnlink(scope, "/n.md");

    const metaAfter = await runInDurableObject(stub, async (_inst, state) => {
      return (
        state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM yjs_meta WHERE path_id = ?",
            pathId
          )
          .toArray()[0] as { n: number }
      ).n;
    });
    expect(metaAfter).toBe(0);
  });

  it("YV3 — versioning OFF: yjs purge still happens (regression guard)", async () => {
    const tenant = "yv3-nonversioned-still-purges";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    // Versioning OFF (default).

    await stub.vfsWriteFile(scope, "/n.md", enc.encode(""));
    await stub.vfsSetYjsMode(scope, "/n.md", true);
    await stub.vfsWriteFile(scope, "/n.md", enc.encode("yjs body"));
    const pathId = await runInDurableObject(stub, async (_inst, state) => {
      const r = state.storage.sql
        .exec("SELECT file_id FROM files WHERE file_name = 'n.md'")
        .toArray()[0] as { file_id: string };
      return r.file_id;
    });

    await stub.vfsUnlink(scope, "/n.md");

    const counts = await runInDurableObject(stub, async (_inst, state) => {
      const oplog = (
        state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM yjs_oplog WHERE path_id = ?",
            pathId
          )
          .toArray()[0] as { n: number }
      ).n;
      const meta = (
        state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM yjs_meta WHERE path_id = ?",
            pathId
          )
          .toArray()[0] as { n: number }
      ).n;
      const filesRow = (
        state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM files WHERE file_id = ?",
            pathId
          )
          .toArray()[0] as { n: number }
      ).n;
      return { oplog, meta, filesRow };
    });
    expect(counts.oplog).toBe(0);
    expect(counts.meta).toBe(0);
    // Versioning-OFF unlink hard-deletes the files row too.
    expect(counts.filesRow).toBe(0);
  });
});
