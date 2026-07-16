import { SELF, env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import { signVFSToken } from "@core/lib/auth";
import { vfsUserDOName } from "@core/lib/utils";
import type { EnvCore } from "@shared/types";
import {
  createMossaicHttpClient,
  createVFS,
  MULTIPART_OPERATION_RPC_BUDGET,
  type MossaicEnv,
  type UserDO,
} from "../../sdk/src/index";

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
  JWT_SECRET?: string;
}

interface SeededHistory {
  vfs: ReturnType<typeof createVFS>;
  stub: DurableObjectStub<UserDO>;
  versionIds: string[];
  headVersionId: string;
}

const E = env as unknown as TestEnv;
const NS = "default";

function envFor(): MossaicEnv {
  return {
    MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD:
      E.MOSSAIC_SHARD as unknown as MossaicEnv["MOSSAIC_SHARD"],
  };
}

function userStub(tenant: string): DurableObjectStub<UserDO> {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}

async function seedHistory(
  tenant: string,
  versionCount: number
): Promise<SeededHistory> {
  const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
  await vfs.writeFile("/history.txt", "head");
  const stub = userStub(tenant);
  const seeded = await runInDurableObject(stub, async (_instance, state) => {
    const sql = state.storage.sql;
    const file = sql
      .exec(
        `SELECT file_id, head_version_id, updated_at
           FROM files WHERE file_name = 'history.txt' AND user_id = ?`,
        tenant
      )
      .toArray()[0] as {
      file_id: string;
      head_version_id: string;
      updated_at: number;
    };
    const versionIds: string[] = [];
    for (let i = 0; i < versionCount; i++) {
      const versionId = `seed-${i.toString().padStart(6, "0")}`;
      versionIds.push(versionId);
      sql.exec(
        `INSERT INTO file_versions
           (path_id, version_id, user_id, size, mode, mtime_ms, deleted,
            inline_data, chunk_size, chunk_count, file_hash, mime_type,
            user_visible)
         VALUES (?, ?, ?, 1, 420, ?, 0, ?, 0, 0, '', 'text/plain', 1)`,
        file.file_id,
        versionId,
        tenant,
        file.updated_at - i - 1,
        new Uint8Array([i & 0xff])
      );
    }
    sql.exec(
      `UPDATE quota
          SET storage_used = storage_used + ?,
              inline_bytes_used = inline_bytes_used + ?
        WHERE user_id = ?`,
      versionCount,
      versionCount,
      tenant
    );
    return {
      versionIds,
      headVersionId: file.head_version_id,
    };
  });
  return { vfs, stub, ...seeded };
}

async function versionCount(stub: DurableObjectStub<UserDO>): Promise<number> {
  return runInDurableObject(stub, async (_instance, state) => {
    return (
      state.storage.sql
        .exec("SELECT COUNT(*) AS n FROM file_versions")
        .toArray()[0] as { n: number }
    ).n;
  });
}

describe("bounded version retention", () => {
  it("keeps completion results stable and exposes bounded cancellation progress", async () => {
    let requests = 0;
    const progress: number[] = [];
    const operationIds: string[] = [];
    const boundedEnv: MossaicEnv = {
      MOSSAIC_USER: {
        idFromName: (name) => E.MOSSAIC_USER.idFromName(name),
        get: () => ({
          vfsDropVersionsStep: async (
            _scope: object,
            _path: string,
            _policy: object,
            operationId: string
          ) => {
            requests++;
            operationIds.push(operationId);
            if (requests % (MULTIPART_OPERATION_RPC_BUDGET + 1) === 0) {
              return { done: true, dropped: 2, kept: 1 };
            }
            return { done: false };
          },
        }),
      },
      MOSSAIC_SHARD: envFor().MOSSAIC_SHARD,
    };
    const vfs = createVFS(boundedEnv, { tenant: "retention-sdk-budget" });

    const pending = await vfs.startDropVersions("/history.txt", {}, {
      onProgress: (event) => progress.push(event.requestsUsed),
    });
    expect(pending).toMatchObject({
      operation: { kind: "drop-versions", operationId: expect.any(String) },
    });
    expect(requests).toBe(MULTIPART_OPERATION_RPC_BUDGET);
    expect(progress).toEqual(
      Array.from({ length: MULTIPART_OPERATION_RPC_BUDGET }, (_, index) => index + 1)
    );
    if (!("operation" in pending)) throw new Error("expected pending retention");
    await expect(
      vfs.stepDropVersions("/history.txt", {}, pending.operation)
    ).resolves.toEqual({ dropped: 2, kept: 1 });
    expect(new Set(operationIds).size).toBe(1);

    await expect(vfs.dropVersions("/history.txt", {})).rejects.toMatchObject({
      code: "EFBIG",
      checkpoint: {
        kind: "drop-versions",
        operationId: expect.any(String),
      },
    });

    const controller = new AbortController();
    controller.abort(new DOMException("cancelled", "AbortError"));
    await expect(
      vfs.dropVersions("/history.txt", {}, { signal: controller.signal })
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(requests).toBe(
      MULTIPART_OPERATION_RPC_BUDGET * 2 + 1
    );
  });

  it("maps errors from binding and HTTP legacy retention fallbacks", async () => {
    const legacyEnv: MossaicEnv = {
      MOSSAIC_USER: {
        idFromName: (name) => E.MOSSAIC_USER.idFromName(name),
        get: () => ({
          vfsDropVersionsStep: async () => {
            throw new Error('Durable Object does not implement "vfsDropVersionsStep"');
          },
          vfsDropVersions: async () => {
            throw new Error("VFSError: ENOENT: legacy history missing");
          },
        }),
      },
      MOSSAIC_SHARD: envFor().MOSSAIC_SHARD,
    };
    const binding = createVFS(legacyEnv, { tenant: "retention-legacy-errors" });
    await expect(binding.dropVersions("/missing.txt", {})).rejects.toMatchObject({
      code: "ENOENT",
    });

    const http = createMossaicHttpClient({
      url: "https://mossaic.test",
      apiKey: "token",
      fetcher: async (input) => {
        const url = typeof input === "string" ? input : input.toString();
        if (url.endsWith("/dropVersionsStep")) {
          return Response.json(
            { code: "ENOENT", message: "Unknown VFS method" },
            { status: 404 }
          );
        }
        return Response.json(
          { code: "ENOENT", message: "legacy history missing" },
          { status: 404 }
        );
      },
    });
    await expect(http.dropVersions("/missing.txt", {})).rejects.toMatchObject({
      code: "ENOENT",
    });
  });

  it("applies keepLast and additive exceptions across more than 128 versions", async () => {
    const seeded = await seedHistory("retention-large-policy", 300);
    const exceptions = [seeded.versionIds[150]!, seeded.versionIds[280]!];

    const result = await seeded.vfs.dropVersions("/history.txt", {
      keepLast: 3,
      exceptVersions: exceptions,
    });

    expect(result).toEqual({ dropped: 296, kept: 5 });
    const remaining = await seeded.vfs.listVersions("/history.txt", {
      limit: 400,
    });
    expect(remaining.map((version) => version.id).sort()).toEqual(
      [
        seeded.headVersionId,
        seeded.versionIds[0]!,
        seeded.versionIds[1]!,
        ...exceptions,
      ].sort()
    );
    const audit = await runInDurableObject(
      seeded.stub,
      async (_instance, state) =>
        state.storage.sql
          .exec(
            `SELECT payload FROM audit_log
              WHERE op = 'dropVersions' ORDER BY id`
          )
          .toArray() as { payload: string }[]
    );
    expect(audit).toHaveLength(1);
    expect(JSON.parse(audit[0]!.payload)).toMatchObject({
      dropped: 296,
      kept: 5,
    });
    const quota = await runInDurableObject(
      seeded.stub,
      async (_instance, state) =>
        state.storage.sql
          .exec(
            `SELECT storage_used, inline_bytes_used, file_count
               FROM quota WHERE user_id = 'retention-large-policy'`
          )
          .toArray()[0] as {
          storage_used: number;
          inline_bytes_used: number;
          file_count: number;
        }
    );
    expect(quota).toEqual({
      storage_used: 8,
      inline_bytes_used: 8,
      file_count: 1,
    });
  });

  it("deletes no more than 128 versions in one UserDO step", async () => {
    const seeded = await seedHistory("retention-step-bound", 300);
    await runInDurableObject(seeded.stub, async (_instance, state) => {
      state.storage.sql.exec(
        "CREATE TABLE retention_step_deletes (n INTEGER NOT NULL)"
      );
      state.storage.sql.exec("INSERT INTO retention_step_deletes VALUES (0)");
      state.storage.sql.exec(`
        CREATE TRIGGER retention_step_delete_bound
        BEFORE DELETE ON file_versions
        WHEN (SELECT n FROM retention_step_deletes) >= 128
        BEGIN
          SELECT RAISE(ABORT, 'retention step exceeded 128 deletes');
        END
      `);
      state.storage.sql.exec(`
        CREATE TRIGGER retention_step_delete_count
        AFTER DELETE ON file_versions
        BEGIN
          UPDATE retention_step_deletes SET n = n + 1;
        END
      `);
    });

    const scope = { ns: NS, tenant: "retention-step-bound" };
    const first = await seeded.stub.vfsDropVersionsStep(
      scope,
      "/history.txt",
      {},
      "bounded-step-operation"
    );
    expect(first).toEqual({ done: false });
    expect(await versionCount(seeded.stub)).toBe(174);
    const firstDeletes = await runInDurableObject(
      seeded.stub,
      async (_instance, state) => {
        const row = state.storage.sql
          .exec("SELECT n FROM retention_step_deletes")
          .toArray()[0] as { n: number };
        state.storage.sql.exec("UPDATE retention_step_deletes SET n = 0");
        return row.n;
      }
    );
    expect(firstDeletes).toBe(127);

    const second = await seeded.stub.vfsDropVersionsStep(
      scope,
      "/history.txt",
      {},
      "bounded-step-operation"
    );
    expect(second).toEqual({ done: false });
    const secondDeletes = await runInDurableObject(
      seeded.stub,
      async (_instance, state) =>
        (
          state.storage.sql
            .exec("SELECT n FROM retention_step_deletes")
            .toArray()[0] as { n: number }
        ).n
    );
    expect(secondDeletes).toBe(128);
  });

  it("keeps the legacy UserDO response compatible when work is bounded", async () => {
    const seeded = await seedHistory("retention-legacy-bound", 100);
    const scope = { ns: NS, tenant: "retention-legacy-bound" };
    const result = await seeded.stub.vfsDropVersions(scope, "/history.txt", {});

    expect(result).toEqual({ dropped: 100, kept: 1 });
    expect(await versionCount(seeded.stub)).toBe(1);
  });

  it("rejects legacy retention before mutating history that requires paging", async () => {
    const seeded = await seedHistory("retention-legacy-capability", 300);
    const scope = { ns: NS, tenant: "retention-legacy-capability" };

    await expect(
      seeded.stub.vfsDropVersions(scope, "/history.txt", {})
    ).rejects.toThrow(/EFBIG.*paged retention capability/);
    expect(await versionCount(seeded.stub)).toBe(301);
  });

  it("rejects a huge legacy manifest with a threshold probe before mutation", async () => {
    const seeded = await seedHistory("retention-legacy-huge-manifest", 2);
    await runInDurableObject(seeded.stub, async (_instance, state) => {
      for (let index = 0; index < 100; index++) {
        state.storage.sql.exec(
          `INSERT INTO version_chunks
             (version_id, chunk_index, chunk_hash, chunk_size, shard_index)
           VALUES (?, ?, ?, 1, 0)`,
          seeded.versionIds[1],
          index,
          index.toString(16).padStart(64, "0")
        );
      }
    });
    const scope = { ns: NS, tenant: "retention-legacy-huge-manifest" };

    await expect(
      seeded.stub.vfsDropVersions(scope, "/history.txt", {})
    ).rejects.toThrow(/EFBIG.*manifests require the paged retention capability/);
    expect(await versionCount(seeded.stub)).toBe(3);
  });

  it("restarts an equal-mtime cursor when a concurrent write advances the head", async () => {
    const seeded = await seedHistory("retention-concurrent-head", 260);
    const scope = { ns: NS, tenant: "retention-concurrent-head" };
    const operationId = "concurrent-head-operation";
    const sharedMtime = 1_700_000_000_000;
    await runInDurableObject(seeded.stub, async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE file_versions SET mtime_ms = ?",
        sharedMtime
      );
    });
    expect(
      await seeded.stub.vfsDropVersionsStep(
        scope,
        "/history.txt",
        { keepLast: 1 },
        operationId
      )
    ).toEqual({ done: false });
    await expect(
      runInDurableObject(seeded.stub, async (_instance, state) =>
        state.storage.sql
          .exec(
            `SELECT cursor_mtime_ms, cursor_version_id
               FROM version_retention_operations WHERE operation_id = ?`,
            operationId
          )
          .toArray()[0]
      )
    ).resolves.toEqual({
      cursor_mtime_ms: sharedMtime,
      cursor_version_id: "seed-000132",
    });

    await seeded.vfs.writeFile("/history.txt", "new-head");
    await runInDurableObject(seeded.stub, async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE file_versions SET mtime_ms = ?",
        sharedMtime
      );
    });
    let step = await seeded.stub.vfsDropVersionsStep(
      scope,
      "/history.txt",
      { keepLast: 1 },
      operationId
    );
    while (!step.done) {
      step = await seeded.stub.vfsDropVersionsStep(
        scope,
        "/history.txt",
        { keepLast: 1 },
        operationId
      );
    }

    expect(step).toEqual({ done: true, dropped: 261, kept: 1 });
    expect(
      await seeded.vfs.readFile("/history.txt", { encoding: "utf8" })
    ).toBe("new-head");
  });

  it("binding SDK retries a lost step response with the same operation", async () => {
    const tenant = "retention-binding-response-loss";
    const seeded = await seedHistory(tenant, 130);
    let loseResponse = true;
    const lossyEnv: MossaicEnv = {
      MOSSAIC_USER: {
        idFromName: (name) => E.MOSSAIC_USER.idFromName(name),
        get: () => ({
          vfsDropVersionsStep: async (
            ...args: Parameters<UserDO["vfsDropVersionsStep"]>
          ) => {
            const result = await seeded.stub.vfsDropVersionsStep(...args);
            if (loseResponse && result.done) {
              loseResponse = false;
              throw new Error("Network connection lost.");
            }
            return result;
          },
        }),
      },
      MOSSAIC_SHARD: envFor().MOSSAIC_SHARD,
    };
    const lossyVfs = createVFS(lossyEnv, { tenant });

    await expect(lossyVfs.dropVersions("/history.txt", {})).resolves.toEqual({
      dropped: 130,
      kept: 1,
    });
    const state = await runInDurableObject(
      seeded.stub,
      async (_instance, durableState) => ({
        operations: (
          durableState.storage.sql
            .exec("SELECT COUNT(*) AS n FROM version_retention_operations")
            .toArray()[0] as { n: number }
        ).n,
        audits: (
          durableState.storage.sql
            .exec(
              "SELECT COUNT(*) AS n FROM audit_log WHERE op = 'dropVersions'"
            )
            .toArray()[0] as { n: number }
        ).n,
      })
    );
    expect(state).toEqual({ operations: 1, audits: 1 });
  });

  it("HTTP SDK retries a lost step response and completes transparently", async () => {
    const tenant = "retention-http-response-loss";
    const seeded = await seedHistory(tenant, 130);
    const apiKey = await signVFSToken(E as unknown as EnvCore, {
      ns: NS,
      tenant,
    });
    let loseResponse = true;
    const fetcher: typeof fetch = async (input, init) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input.url;
      const response = await SELF.fetch(url, init);
      if (
        loseResponse &&
        url.endsWith("/api/vfs/dropVersionsStep") &&
        response.ok
      ) {
        const step = (await response.clone().json()) as { done?: boolean };
        if (step.done) {
          loseResponse = false;
          await response.arrayBuffer();
          throw new TypeError("fetch failed");
        }
      }
      return response;
    };
    const http = createMossaicHttpClient({
      url: "https://mossaic.test",
      apiKey,
      fetcher,
    });

    await expect(http.dropVersions("/history.txt", {})).resolves.toEqual({
      dropped: 130,
      kept: 1,
    });
    expect(await versionCount(seeded.stub)).toBe(1);
  });
});
