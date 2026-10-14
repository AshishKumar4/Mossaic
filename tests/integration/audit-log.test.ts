import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * Phase 42 \u2014 audit_log emission + retention.
 *
 * Every destructive op writes one row per call into the
 * per-tenant `audit_log` table. The insert that takes it past
 * AUDIT_LOG_MAX_ROWS (default 10K) trims the oldest rows.
 *
 * Cases:
 *   AL1  fresh tenant: audit_log table exists and is empty.
 *   AL2  unlink (versioning OFF) emits exactly one row.
 *   AL3  unlink (versioning ON) emits exactly one row with
 *        versioning:true payload.
 *   AL4  purge emits exactly one row.
 *   AL5  archive + unarchive each emit one row.
 *   AL6  rename emits one row with replacedFileId on overwrite.
 *   AL7  removeRecursive emits one row per call (paginated).
 *   AL8  dropVersions emits one row.
 *   AL9  restoreVersion emits one row.
 *   AL10 write/unlink cycles past the cap leave the log at or
 *        under it, newest rows kept, with no alarm armed.
 *   AL11 admin* RPCs emit audit-log rows.
 *   AL12 audit_log row carries actor + target + payload + ts +
 *        op fields populated.
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
}
const E = env as unknown as E;
const NS = "default";

interface AuditLogRow {
  id: string;
  ts: number;
  op: string;
  actor: string;
  target: string;
  payload: string | null;
  request_id: string | null;
}

async function readAuditLog(
  stub: DurableObjectStub,
  filter?: { op?: string }
): Promise<AuditLogRow[]> {
  return runInDurableObject(stub, async (_inst, state) => {
    if (filter?.op) {
      return state.storage.sql
        .exec("SELECT * FROM audit_log WHERE op = ? ORDER BY ts ASC", filter.op)
        .toArray() as AuditLogRow[];
    }
    return state.storage.sql
      .exec("SELECT * FROM audit_log ORDER BY ts ASC")
      .toArray() as AuditLogRow[];
  });
}

async function enableVersioning(
  stub: DurableObjectStub,
  userId: string
): Promise<void> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  await (stub as any).adminSetVersioning(userId, true);
}

describe("Phase 42 \u2014 audit_log emission", () => {
  it("AL1 \u2014 fresh tenant: audit_log exists, empty", async () => {
    const tenant = "al1-fresh";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await stub.vfsExists(scope, "/"); // ensureInit
    const rows = await readAuditLog(stub);
    expect(rows.length).toBe(0);
  });

  it("AL2 \u2014 unlink (versioning OFF) emits one row", async () => {
    const tenant = "al2-unlink-off";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await stub.vfsWriteFile(scope, "/u.bin", new Uint8Array(8).fill(1));
    await stub.vfsUnlink(scope, "/u.bin");
    const rows = await readAuditLog(stub, { op: "unlink" });
    expect(rows.length).toBe(1);
    expect(rows[0].op).toBe("unlink");
    expect(rows[0].actor).toBe(tenant);
    expect(rows[0].target).toBeTruthy();
    const payload = JSON.parse(rows[0].payload ?? "{}");
    expect(payload.versioning).toBe(false);
    expect(payload.path).toBe("/u.bin");
  });

  it("AL3 \u2014 unlink (versioning ON) records versioning:true", async () => {
    const tenant = "al3-unlink-on";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await enableVersioning(stub, tenant);
    await stub.vfsWriteFile(scope, "/v.bin", new Uint8Array(8).fill(2));
    await stub.vfsUnlink(scope, "/v.bin");
    const rows = await readAuditLog(stub, { op: "unlink" });
    expect(rows.length).toBe(1);
    const payload = JSON.parse(rows[0].payload ?? "{}");
    expect(payload.versioning).toBe(true);
    expect(typeof payload.tombId).toBe("string");
  });

  it("AL4 \u2014 purge emits one row", async () => {
    const tenant = "al4-purge";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await stub.vfsWriteFile(scope, "/p.bin", new Uint8Array(64).fill(3));
    await stub.vfsPurge(scope, "/p.bin");
    const rows = await readAuditLog(stub, { op: "purge" });
    expect(rows.length).toBe(1);
    const payload = JSON.parse(rows[0].payload ?? "{}");
    expect(payload.path).toBe("/p.bin");
  });

  it("AL5 \u2014 archive + unarchive each emit one row", async () => {
    const tenant = "al5-archive";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await stub.vfsWriteFile(scope, "/a.bin", new Uint8Array(8).fill(4));
    await stub.vfsArchive(scope, "/a.bin");
    await stub.vfsUnarchive(scope, "/a.bin");
    const archiveRows = await readAuditLog(stub, { op: "archive" });
    const unarchiveRows = await readAuditLog(stub, { op: "unarchive" });
    expect(archiveRows.length).toBe(1);
    expect(unarchiveRows.length).toBe(1);
    expect(archiveRows[0].target).toBe(unarchiveRows[0].target);
  });

  it("AL6 \u2014 rename overwrite records replacedFileId", async () => {
    const tenant = "al6-rename";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await stub.vfsWriteFile(scope, "/x.bin", new Uint8Array(8).fill(5));
    await stub.vfsWriteFile(scope, "/y.bin", new Uint8Array(8).fill(6));
    await stub.vfsRename(scope, "/x.bin", "/y.bin");
    const rows = await readAuditLog(stub, { op: "rename" });
    expect(rows.length).toBe(1);
    const payload = JSON.parse(rows[0].payload ?? "{}");
    expect(payload.src).toBe("/x.bin");
    expect(payload.dst).toBe("/y.bin");
    expect(payload.replacedFileId).toBeTruthy();
  });

  it("AL7 \u2014 removeRecursive emits one row per call", async () => {
    const tenant = "al7-rmrf";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await stub.vfsMkdir(scope, "/d");
    for (let i = 0; i < 3; i++) {
      await stub.vfsWriteFile(scope, `/d/f${i}.bin`, new Uint8Array([i]));
    }
    let safety = 50;
    while (!(await stub.vfsRemoveRecursive(scope, "/d")).done) {
      if (safety-- <= 0) throw new Error("rmrf did not terminate");
    }
    const rows = await readAuditLog(stub, { op: "removeRecursive" });
    expect(rows.length).toBeGreaterThanOrEqual(1);
    const lastRow = rows[rows.length - 1];
    const lastPayload = JSON.parse(lastRow.payload ?? "{}");
    expect(lastPayload.path).toBe("/d");
    expect(lastPayload.done).toBe(true);
  });

  it("AL8 \u2014 dropVersions emits one row", async () => {
    const tenant = "al8-drop-versions";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await enableVersioning(stub, tenant);
    for (const sz of [10, 20, 30]) {
      await stub.vfsWriteFile(scope, "/d.bin", new Uint8Array(sz).fill(sz));
    }
    const r = await stub.vfsDropVersions(scope, "/d.bin", { keepLast: 1 });
    expect(r.dropped).toBe(2);
    const rows = await readAuditLog(stub, { op: "dropVersions" });
    expect(rows.length).toBe(1);
    const payload = JSON.parse(rows[0].payload ?? "{}");
    expect(payload.dropped).toBe(2);
    expect(payload.kept).toBe(1);
  });

  it("AL9 \u2014 restoreVersion emits one row", async () => {
    const tenant = "al9-restore";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await enableVersioning(stub, tenant);
    await stub.vfsWriteFile(scope, "/r.bin", new Uint8Array(10).fill(1));
    await stub.vfsWriteFile(scope, "/r.bin", new Uint8Array(20).fill(2));
    const versions = await stub.vfsListVersions(scope, "/r.bin", {});
    const oldest = versions[versions.length - 1];
    await stub.vfsRestoreVersion(scope, "/r.bin", oldest.versionId);
    const rows = await readAuditLog(stub, { op: "restoreVersion" });
    expect(rows.length).toBe(1);
    const payload = JSON.parse(rows[0].payload ?? "{}");
    expect(payload.sourceVersionId).toBe(oldest.versionId);
    expect(typeof payload.newVersionId).toBe("string");
  });

  it("AL10 \u2014 write/unlink cycles past the cap stay under it, no alarm", async () => {
    const tenant = "al10-retention";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await stub.vfsExists(scope, "/"); // ensureInit
    // The smallest cap an operator can set; its trim floor is 98.
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO vfs_meta (key, value) VALUES ('audit_log_max_rows', '100')"
      );
    });
    // Each completed write and unlink takes back its alarm, so the
    // cap holds only if the insert crossing it trims.
    const cycles = 110;
    for (let i = 0; i < cycles; i++) {
      await stub.vfsWriteFile(scope, `/c${i}.bin`, new Uint8Array(8).fill(1));
      await stub.vfsUnlink(scope, `/c${i}.bin`);
    }
    // Insertion order: rows written in one millisecond tie on ts.
    const rows = await runInDurableObject(
      stub,
      (_inst, state) =>
        state.storage.sql
          .exec("SELECT * FROM audit_log ORDER BY rowid ASC")
          .toArray() as AuditLogRow[]
    );
    expect(rows.every((r) => r.op === "unlink")).toBe(true);
    expect(rows.length).toBeLessThanOrEqual(100);
    expect(rows.length).toBeGreaterThanOrEqual(98);
    const paths = rows.map((r) => JSON.parse(r.payload ?? "{}").path);
    expect(paths).toEqual(
      Array.from(
        { length: rows.length },
        (_, k) => `/c${cycles - rows.length + k}.bin`
      )
    );
    const alarm = await runInDurableObject(stub, (_inst, state) =>
      state.storage.getAlarm()
    );
    expect(alarm).toBeNull();
  });

  it("AL11 \u2014 admin* RPCs emit audit-log rows", async () => {
    const tenant = "al11-admin";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    // adminSetVersioning
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await (stub as any).adminSetVersioning(tenant, true);
    const setRows = await readAuditLog(stub, { op: "adminSetVersioning" });
    expect(setRows.length).toBe(1);
    expect(setRows[0].actor).toBe("operator");
    expect(setRows[0].target).toBe(tenant);
    const payload = JSON.parse(setRows[0].payload ?? "{}");
    expect(payload.enabled).toBe(true);
  });

  it("AL12 \u2014 audit_log row has all expected fields populated", async () => {
    const tenant = "al12-fields";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    const before = Date.now();
    await stub.vfsWriteFile(scope, "/f.bin", new Uint8Array(8).fill(1));
    await stub.vfsUnlink(scope, "/f.bin");
    const after = Date.now();
    const rows = await readAuditLog(stub, { op: "unlink" });
    expect(rows.length).toBe(1);
    const r = rows[0];
    expect(typeof r.id).toBe("string");
    expect(r.id.length).toBeGreaterThan(0);
    expect(r.ts).toBeGreaterThanOrEqual(before);
    expect(r.ts).toBeLessThanOrEqual(after);
    expect(r.op).toBe("unlink");
    expect(r.actor).toBe(tenant);
    expect(r.target).toBeTruthy();
    expect(r.payload).toBeTruthy();
  });
});
