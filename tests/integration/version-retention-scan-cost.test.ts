import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

import { vfsUserDOName } from "@core/lib/utils";
import type { UserDO } from "@app/objects/user/user-do";
import type { SqlMetrics } from "../bench/counting-sql-storage";

/**
 * What a retention step costs, a thousand versions in.
 *
 * The scan resumes with a row-value seek — `(mtime_ms, version_id) < (cursor…)`
 * against an index that covers exactly that ordering — so the SQL one step
 * issues does not grow with the depth already visited. The naive form of the
 * same resume, "everything newer than the cursor's mtime, minus what I already
 * saw", re-reads the visited prefix on every step and turns a deep history
 * quadratic. This measures the difference the only way that cannot be argued
 * with: the rows a late step reads against the rows the first step read.
 *
 * The policy keeps every version on purpose. A step that dropped what it
 * visited would leave a shallower history behind it, so nothing would have to
 * be seeked past — the cost of paging a deep history is only observable while
 * the pages stay in place.
 */

// A thousand-version history, seeded and then walked in full.
vi.setConfig({ testTimeout: 120_000 });

interface UserFaultControls {
  testResetSqlMetrics(): Promise<void>;
  testSqlMetrics(): Promise<SqlMetrics>;
}

type TestUserDO = UserDO & UserFaultControls;

const E = env as unknown as {
  MOSSAIC_USER: DurableObjectNamespace<TestUserDO>;
};
const NS = "default";
const TENANT = "retention-scan-cost";
const HISTORY_PATH = "/history.txt";

/** Versions one step visits, mirroring the server's own bound. */
const SCAN_LIMIT = 128;
const VERSIONS = 1_100;

function userStub(): DurableObjectStub<TestUserDO> {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, TENANT))
  );
}

function scope(): { ns: string; tenant: string } {
  return { ns: NS, tenant: TENANT };
}

async function seedHistory(): Promise<void> {
  const stub = userStub();
  await stub.adminSetVersioning(TENANT, true);
  await stub.vfsWriteFile(scope(), HISTORY_PATH, new Uint8Array([1]));
  await runInDurableObject(stub, (_instance, state) => {
    const sql = state.storage.sql;
    const file = sql
      .exec<{ file_id: string; updated_at: number }>(
        `SELECT file_id, updated_at FROM files
          WHERE file_name = 'history.txt' AND user_id = ?`,
        TENANT
      )
      .toArray()[0];
    if (file === undefined) throw new Error("seed: head version is missing");
    for (let index = 0; index < VERSIONS; index++) {
      sql.exec(
        `INSERT INTO file_versions
           (path_id, version_id, user_id, size, mode, mtime_ms, deleted,
            inline_data, chunk_size, chunk_count, file_hash, mime_type,
            user_visible)
         VALUES (?, ?, ?, 1, 420, ?, 0, NULL, 0, 0, '', 'text/plain', 1)`,
        file.file_id,
        `seed-${index.toString().padStart(6, "0")}`,
        TENANT,
        file.updated_at - 1 - index
      );
    }
  });
}

/** The rows one step reads, with the step's own SQL counted in isolation. */
async function measureStep(operationId: string): Promise<number> {
  const stub = userStub();
  await stub.testResetSqlMetrics();
  const step = await stub.vfsDropVersionsStep(
    scope(),
    HISTORY_PATH,
    { keepLast: VERSIONS + 1 },
    operationId
  );
  expect(step.done).toBe(false);
  return (await stub.testSqlMetrics()).rowsRead;
}

describe("version retention scan cost", () => {
  it("reads the same rows for a late page as for the first", async () => {
    await seedHistory();
    const operationId = "scan-cost";

    const firstPage = await measureStep(operationId);
    let latePage = firstPage;
    // Seven more pages, so the eighth resumes with ~900 versions behind it.
    for (let page = 2; page <= 8; page++) {
      latePage = await measureStep(operationId);
    }

    // A rescan would have read the visited prefix again here — seven pages of
    // it — so the bound is what proves the seek is a seek.
    expect(firstPage).toBeLessThan(SCAN_LIMIT * 4);
    expect(latePage).toBeLessThanOrEqual(firstPage + SCAN_LIMIT);

    // And the walk really did get that deep: every version was kept, so the
    // history is still whole and the operation is still running.
    const state = await runInDurableObject(userStub(), (_instance, durable) => {
      const sql = durable.storage.sql;
      const operation = sql
        .exec<{ status: string; kept: number; dropped: number }>(
          `SELECT status, kept, dropped FROM version_retention_operations
            WHERE operation_id = ?`,
          operationId
        )
        .toArray()[0];
      const versions = sql
        .exec<{ n: number }>("SELECT COUNT(*) AS n FROM file_versions")
        .toArray()[0];
      return { operation, versions: versions?.n };
    });
    expect(state.versions).toBe(VERSIONS + 1);
    expect(state.operation).toEqual({
      status: "running",
      dropped: 0,
      kept: SCAN_LIMIT * 8,
    });
  });
});
