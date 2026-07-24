import {
  env,
  runDurableObjectAlarm,
  runInDurableObject,
} from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

import type { UserDO } from "@app/objects/user/user-do";
import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import {
  SHARD_CLEANUP_JOURNAL_TTL_MS,
  type ShardDO,
} from "@core/objects/shard/shard-do";
import {
  ChunkCleanupKind,
  stageChunkCleanupIntent,
} from "@core/objects/user/internal-storage";
import { drainChunkCleanupIntents } from "@core/objects/user/vfs/write-commit";
import { purgeYjs, YJS_CLEANUP_PAGE_SIZE } from "@core/objects/user/yjs";

interface FaultShardDO extends ShardDO {
  testConfigureDeleteChunksFailure(
    fileId: string,
    phase: "before" | "after",
    remaining: number | null
  ): Promise<void>;
}

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<FaultShardDO>;
}

function hasTestBindings(value: object): value is TestEnv {
  return "MOSSAIC_USER" in value && "MOSSAIC_SHARD" in value;
}

if (!hasTestBindings(env)) throw new Error("missing cleanup fault bindings");
const E = env;

describe("paged cleanup fault recovery", () => {
  it("restarts colliding chunk and multipart-staging cleanup as a fresh multipart generation", async () => {
    const tenant = "paged-cleanup-kind-collision";
    const scope = { ns: "default", tenant } as const;
    const user = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(scope.ns, tenant))
    );
    const shard = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(
        vfsShardDOName(scope.ns, tenant, undefined, 0)
      )
    );
    const refId = "cleanup-kind-collision-ref";
    const hash = "c".repeat(64);
    await user.vfsExists(scope, "/missing");
    await shard.putChunk(hash, new Uint8Array([1]), refId, 0, tenant);
    await runInDurableObject(user, async (instance) => {
      const now = Date.now();
      stageChunkCleanupIntent(
        instance,
        refId,
        0,
        now,
        now,
        ChunkCleanupKind.Chunks,
        true
      );
      await drainChunkCleanupIntents(instance, scope);
    });
    const firstGeneration = await runInDurableObject(
      user,
      (_instance, state) =>
        (
          state.storage.sql
            .exec(
              `SELECT cleanup_generation FROM chunk_cleanup_intents
                WHERE ref_id = ? AND shard_index = 0`,
              refId
            )
            .toArray()[0] as { cleanup_generation: string }
        ).cleanup_generation
    );

    await runInDurableObject(shard, (_instance, state) => {
      const sql = state.storage.sql;
      sql.exec(
        `INSERT INTO chunk_refs (chunk_hash, file_id, chunk_index, user_id)
         VALUES (?, ?, 0, ?)`,
        hash,
        refId,
        tenant
      );
      sql.exec(
        "UPDATE chunks SET ref_count = 1, deleted_at = NULL WHERE hash = ?",
        hash
      );
      sql.exec(
        `INSERT INTO upload_chunks
           (upload_id, chunk_index, chunk_hash, chunk_size, user_id, created_at)
         VALUES (?, 0, ?, 1, ?, ?)`,
        refId,
        hash,
        tenant,
        Date.now()
      );
    });
    await runInDurableObject(user, async (instance) => {
      const now = Date.now();
      stageChunkCleanupIntent(
        instance,
        refId,
        0,
        now,
        now,
        ChunkCleanupKind.MultipartStaging
      );
      const merged = instance.sql
        .exec(
          `SELECT cleanup_kind, cleanup_generation, cleanup_cursor, cleanup_phase
             FROM chunk_cleanup_intents WHERE ref_id = ? AND shard_index = 0`,
          refId
        )
        .toArray()[0] as {
        cleanup_kind: string;
        cleanup_generation: string;
        cleanup_cursor: number;
        cleanup_phase: string;
      };
      expect(merged).toMatchObject({
        cleanup_kind: "multipart",
        cleanup_cursor: 0,
        cleanup_phase: "chunks",
      });
      expect(merged.cleanup_generation).not.toBe(firstGeneration);
      await drainChunkCleanupIntents(instance, scope, refId);
    });

    await expect(
      runInDurableObject(shard, (_instance, state) => ({
        refs: (
          state.storage.sql
            .exec("SELECT COUNT(*) AS n FROM chunk_refs WHERE file_id = ?", refId)
            .toArray()[0] as { n: number }
        ).n,
        staging: (
          state.storage.sql
            .exec("SELECT COUNT(*) AS n FROM upload_chunks WHERE upload_id = ?", refId)
            .toArray()[0] as { n: number }
        ).n,
      }))
    ).resolves.toEqual({ refs: 0, staging: 0 });
  });

  it("replays a lost page response exactly and resumes from the persisted outbox cursor on alarms", async () => {
    const tenant = "paged-cleanup-alarm-response-loss";
    const scope = { ns: "default", tenant } as const;
    const user = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(scope.ns, tenant))
    );
    const shard = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(
        vfsShardDOName(scope.ns, tenant, undefined, 0)
      )
    );
    const fileId = "paged-outbox-ref";
    const survivor = "paged-outbox-survivor";
    const hash = "b".repeat(64);

    await user.vfsExists(scope, "/missing");
    await shard.putChunk(hash, new Uint8Array([1]), fileId, 0, tenant);
    await shard.putChunk(hash, new Uint8Array(0), survivor, 0, tenant);
    await runInDurableObject(shard, (_instance, state) => {
      state.storage.transactionSync(() => {
        for (let index = 1; index < 300; index++) {
          state.storage.sql.exec(
            `INSERT INTO chunk_refs (chunk_hash, file_id, chunk_index, user_id)
             VALUES (?, ?, ?, ?)`,
            hash,
            fileId,
            index,
            tenant
          );
        }
        state.storage.sql.exec(
          "UPDATE chunks SET ref_count = 301 WHERE hash = ?",
          hash
        );
      });
    });
    await runInDurableObject(user, async (_instance, state) => {
      const now = Date.now();
      state.storage.sql.exec(
        `INSERT INTO chunk_cleanup_intents
           (ref_id, shard_index, cleanup_kind, state, generation,
            cleanup_generation, cleanup_cursor, cleanup_phase, provisional,
            created_at, updated_at, next_attempt_at, attempts, last_error)
         VALUES (?, 0, 'chunks', 'pending', 0, ?, 0, 'chunks', 0,
                 ?, ?, 0, 0, NULL)`,
        fileId,
        "outbox-generation",
        now,
        now
      );
      await state.storage.setAlarm(Date.now() + 1_000);
    });
    await shard.testConfigureDeleteChunksFailure(fileId, "after", 1);

    expect(await runDurableObjectAlarm(user)).toBe(true);
    await expect(
      runInDurableObject(user, (_instance, state) =>
        state.storage.sql
          .exec(
            `SELECT state, cleanup_generation, cleanup_cursor, attempts
               FROM chunk_cleanup_intents WHERE ref_id = ?`,
            fileId
          )
          .toArray()[0]
      )
    ).resolves.toEqual({
      state: "pending",
      cleanup_generation: "outbox-generation",
      cleanup_cursor: 0,
      attempts: 1,
    });

    await runInDurableObject(user, async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE chunk_cleanup_intents SET next_attempt_at = 0 WHERE ref_id = ?",
        fileId
      );
      await state.storage.setAlarm(Date.now() + 1_000);
    });
    expect(await runDurableObjectAlarm(user)).toBe(true);
    await expect(
      runInDurableObject(user, (_instance, state) =>
        state.storage.sql
          .exec(
            `SELECT state, cleanup_generation, cleanup_cursor, attempts
               FROM chunk_cleanup_intents WHERE ref_id = ?`,
            fileId
          )
          .toArray()[0]
      )
    ).resolves.toEqual({
      state: "pending",
      cleanup_generation: "outbox-generation",
      cleanup_cursor: 256,
      attempts: 1,
    });
    await expect(
      runInDurableObject(shard, (_instance, state) => {
        const sql = state.storage.sql;
        return {
          targetRefs: (
            sql
              .exec("SELECT COUNT(*) AS n FROM chunk_refs WHERE file_id = ?", fileId)
              .toArray()[0] as { n: number }
          ).n,
          refCount: (
            sql.exec("SELECT ref_count FROM chunks WHERE hash = ?", hash)
              .toArray()[0] as { ref_count: number }
          ).ref_count,
          pages: (
            sql.exec("SELECT COUNT(*) AS n FROM shard_cleanup_pages").toArray()[0] as {
              n: number;
            }
          ).n,
        };
      })
    ).resolves.toEqual({ targetRefs: 44, refCount: 45, pages: 1 });

    await runInDurableObject(user, async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE chunk_cleanup_intents SET next_attempt_at = 0 WHERE ref_id = ?",
        fileId
      );
      await state.storage.setAlarm(Date.now() + 1_000);
    });
    expect(await runDurableObjectAlarm(user)).toBe(true);
    await expect(
      runInDurableObject(user, (_instance, state) =>
        state.storage.sql
          .exec("SELECT 1 FROM chunk_cleanup_intents WHERE ref_id = ?", fileId)
          .toArray()
      )
    ).resolves.toEqual([]);
    await expect(
      runInDurableObject(shard, (_instance, state) => {
        const sql = state.storage.sql;
        return {
          targetRefs: (
            sql
              .exec("SELECT COUNT(*) AS n FROM chunk_refs WHERE file_id = ?", fileId)
              .toArray()[0] as { n: number }
          ).n,
          survivorRefs: (
            sql
              .exec("SELECT COUNT(*) AS n FROM chunk_refs WHERE file_id = ?", survivor)
              .toArray()[0] as { n: number }
          ).n,
          refCount: (
            sql.exec("SELECT ref_count FROM chunks WHERE hash = ?", hash)
              .toArray()[0] as { ref_count: number }
          ).ref_count,
        };
      })
    ).resolves.toEqual({ targetRefs: 0, survivorRefs: 1, refCount: 1 });
  });

  it("retains partial refs and staging progress past replay TTL until UserDO completes", async () => {
    const tenant = "paged-cleanup-active-progress-ttl";
    const scope = { ns: "default", tenant } as const;
    const user = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(scope.ns, tenant))
    );
    const shard = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(
        vfsShardDOName(scope.ns, tenant, undefined, 0)
      )
    );
    const uploadId = "active-progress-upload";
    const survivor = "active-progress-survivor";
    const hash = "d".repeat(64);
    let now = Date.now();
    const dateNow = vi.spyOn(Date, "now").mockImplementation(() => now);

    try {
      await user.vfsExists(scope, "/missing");
      await shard.putChunk(hash, new Uint8Array([1]), uploadId, 0, tenant);
      await shard.putChunk(hash, new Uint8Array(0), survivor, 0, tenant);
      await runInDurableObject(shard, (_instance, state) => {
        state.storage.transactionSync(() => {
          for (let index = 1; index < 300; index++) {
            state.storage.sql.exec(
              `INSERT INTO chunk_refs (chunk_hash, file_id, chunk_index, user_id)
               VALUES (?, ?, ?, ?)`,
              hash,
              uploadId,
              index,
              tenant
            );
          }
          for (let index = 0; index < 300; index++) {
            state.storage.sql.exec(
              `INSERT INTO upload_chunks
                 (upload_id, chunk_index, chunk_hash, chunk_size, user_id, created_at)
               VALUES (?, ?, ?, 1, ?, ?)`,
              uploadId,
              index,
              hash,
              tenant,
              now
            );
          }
          state.storage.sql.exec(
            "UPDATE chunks SET ref_count = 301 WHERE hash = ?",
            hash
          );
        });
      });
      await runInDurableObject(user, async (instance) => {
        stageChunkCleanupIntent(
          instance,
          uploadId,
          0,
          now,
          now,
          ChunkCleanupKind.Multipart
        );
        await drainChunkCleanupIntents(instance, scope, uploadId);
      });

      await expect(
        runInDurableObject(user, (_instance, state) =>
          state.storage.sql
            .exec(
              `SELECT cleanup_cursor, cleanup_phase FROM chunk_cleanup_intents
                WHERE ref_id = ?`,
              uploadId
            )
            .toArray()[0]
        )
      ).resolves.toEqual({ cleanup_cursor: 256, cleanup_phase: "chunks" });

      now += SHARD_CLEANUP_JOURNAL_TTL_MS + 1;
      expect(await runDurableObjectAlarm(shard)).toBe(true);
      await expect(
        runInDurableObject(shard, (_instance, state) =>
          state.storage.sql
            .exec(
              `SELECT next_cursor, done FROM shard_cleanup_progress
                WHERE cleanup_kind = 'refs' AND ref_id = ?`,
              uploadId
            )
            .toArray()[0]
        )
      ).resolves.toEqual({ next_cursor: 256, done: 0 });

      await runInDurableObject(user, async (instance) => {
        await drainChunkCleanupIntents(instance, scope, uploadId);
      });
      await expect(
        runInDurableObject(user, (_instance, state) =>
          state.storage.sql
            .exec(
              `SELECT cleanup_cursor, cleanup_phase FROM chunk_cleanup_intents
                WHERE ref_id = ?`,
              uploadId
            )
            .toArray()[0]
        )
      ).resolves.toEqual({ cleanup_cursor: 256, cleanup_phase: "staging" });

      now += SHARD_CLEANUP_JOURNAL_TTL_MS + 1;
      expect(await runDurableObjectAlarm(shard)).toBe(true);
      await expect(
        runInDurableObject(shard, (_instance, state) =>
          state.storage.sql
            .exec(
              `SELECT cleanup_kind, next_cursor, done
                 FROM shard_cleanup_progress WHERE ref_id = ?
                 ORDER BY cleanup_kind`,
              uploadId
            )
            .toArray()
        )
      ).resolves.toEqual([
        { cleanup_kind: "staging", next_cursor: 256, done: 0 },
      ]);

      await runInDurableObject(user, async (instance) => {
        await drainChunkCleanupIntents(instance, scope, uploadId);
      });
      await expect(
        runInDurableObject(user, (_instance, state) =>
          state.storage.sql
            .exec("SELECT 1 FROM chunk_cleanup_intents WHERE ref_id = ?", uploadId)
            .toArray()
        )
      ).resolves.toEqual([]);
      await expect(
        runInDurableObject(shard, (_instance, state) => {
          const sql = state.storage.sql;
          return {
            refs: (
              sql
                .exec("SELECT COUNT(*) AS n FROM chunk_refs WHERE file_id = ?", uploadId)
                .toArray()[0] as { n: number }
            ).n,
            staging: (
              sql
                .exec("SELECT COUNT(*) AS n FROM upload_chunks WHERE upload_id = ?", uploadId)
                .toArray()[0] as { n: number }
            ).n,
            survivorRefs: (
              sql
                .exec("SELECT COUNT(*) AS n FROM chunk_refs WHERE file_id = ?", survivor)
                .toArray()[0] as { n: number }
            ).n,
            refCount: (
              sql.exec("SELECT ref_count FROM chunks WHERE hash = ?", hash)
                .toArray()[0] as { ref_count: number }
            ).ref_count,
          };
        })
      ).resolves.toEqual({ refs: 0, staging: 0, survivorRefs: 1, refCount: 1 });
    } finally {
      dateNow.mockRestore();
    }
  });

  it("replays a completed page until its bounded journal entry expires", async () => {
    const tenant = "paged-cleanup-journal-expiry";
    const shard = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(
        vfsShardDOName("default", tenant, undefined, 0)
      )
    );
    const fileId = "journal-expiry-ref";
    for (let index = 0; index < 3; index++) {
      const hash = (index + 1).toString(16).padStart(64, "0");
      await shard.putChunk(hash, new Uint8Array([index]), fileId, index, tenant);
    }

    const first = await shard.deleteChunksPage(fileId, 0, "journal-expiry-generation");
    const replay = await shard.deleteChunksPage(
      fileId,
      0,
      "journal-expiry-generation"
    );
    expect(replay).toEqual(first);
    expect(first).toMatchObject({ cursor: 3, done: true, processed: 3 });

    await runInDurableObject(shard, async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE shard_cleanup_page_expirations SET expires_at = 0"
      );
      await state.storage.setAlarm(Date.now() + 1_000);
    });
    expect(await runDurableObjectAlarm(shard)).toBe(true);

    await expect(
      runInDurableObject(shard, (_instance, state) => ({
        pages: (
          state.storage.sql
            .exec("SELECT COUNT(*) AS n FROM shard_cleanup_pages")
            .toArray()[0] as { n: number }
        ).n,
        progress: (
          state.storage.sql
            .exec("SELECT COUNT(*) AS n FROM shard_cleanup_progress")
            .toArray()[0] as { n: number }
        ).n,
      }))
    ).resolves.toEqual({ pages: 0, progress: 0 });
  });

  it("resumes a large encrypted Yjs purge after cleanup response loss", async () => {
    const tenant = "paged-encrypted-yjs-restart";
    const scope = { ns: "default", tenant } as const;
    const user = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(scope.ns, tenant))
    );
    const shard = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(
        vfsShardDOName(scope.ns, tenant, undefined, 0)
      )
    );
    await user.vfsWriteFile(scope, "/large.yjs", new Uint8Array(0));
    await shard.getStorageBytes();

    const seeded = await runInDurableObject(user, (_instance, state) => {
      const sql = state.storage.sql;
      const file = sql
        .exec(
          "SELECT file_id FROM files WHERE user_id = ? AND file_name = 'large.yjs'",
          tenant
        )
        .toArray()[0] as { file_id: string };
      sql.exec(
        `UPDATE files SET mode_yjs = 1, encryption_mode = 'random'
          WHERE file_id = ?`,
        file.file_id
      );
      sql.exec(
        `INSERT INTO yjs_meta
           (path_id, next_seq, last_checkpoint_seq, op_count_since_ckpt,
            last_compact_at, materialized_at, bytes_since_last_compact)
         VALUES (?, 600, -1, 600, 0, 0, 600)`,
        file.file_id
      );
      const hash = "e".repeat(64);
      for (let seq = 0; seq < 600; seq++) {
        sql.exec(
          `INSERT INTO yjs_oplog
             (path_id, seq, kind, chunk_hash, chunk_size, shard_index, created_at)
           VALUES (?, ?, 'op', ?, 1, 0, 0)`,
          file.file_id,
          seq,
          hash
        );
      }
      return { pathId: file.file_id, hash };
    });
    await runInDurableObject(shard, (_instance, state) => {
      const sql = state.storage.sql;
      sql.exec(
        `INSERT INTO chunks (hash, data, size, ref_count, created_at, deleted_at)
         VALUES (?, ?, 1, 600, 0, NULL)`,
        seeded.hash,
        new Uint8Array([1])
      );
      for (let seq = 0; seq < 600; seq++) {
        sql.exec(
          `INSERT INTO chunk_refs (chunk_hash, file_id, chunk_index, user_id)
           VALUES (?, ?, 0, ?)`,
          seeded.hash,
          `${seeded.pathId}#yjs#${seq}`,
          tenant
        );
      }
    });
    await shard.testConfigureDeleteChunksFailure(
      `${seeded.pathId}#yjs#0`,
      "after",
      1
    );

    await runInDurableObject(user, (instance) =>
      purgeYjs(instance, scope, seeded.pathId)
    );
    let remaining = await runInDurableObject(user, (_instance, state) =>
      (
        state.storage.sql
          .exec("SELECT COUNT(*) AS n FROM yjs_oplog WHERE path_id = ?", seeded.pathId)
          .toArray()[0] as { n: number }
      ).n
    );
    expect(remaining).toBe(600 - YJS_CLEANUP_PAGE_SIZE);

    for (let attempt = 0; attempt < 5 && remaining > 0; attempt++) {
      await runInDurableObject(user, async (_instance, state) => {
        await state.storage.setAlarm(Date.now() + 1_000);
      });
      expect(await runDurableObjectAlarm(user)).toBe(true);
      const next = await runInDurableObject(user, (_instance, state) =>
        (
          state.storage.sql
            .exec("SELECT COUNT(*) AS n FROM yjs_oplog WHERE path_id = ?", seeded.pathId)
            .toArray()[0] as { n: number }
        ).n
      );
      expect(remaining - next).toBeLessThanOrEqual(YJS_CLEANUP_PAGE_SIZE);
      remaining = next;
    }

    expect(remaining).toBe(0);
    await expect(
      runInDurableObject(user, (_instance, state) => ({
        meta: state.storage.sql
          .exec("SELECT 1 FROM yjs_meta WHERE path_id = ?", seeded.pathId)
          .toArray().length,
        operation: state.storage.sql
          .exec("SELECT 1 FROM yjs_cleanup_operations WHERE path_id = ?", seeded.pathId)
          .toArray().length,
      }))
    ).resolves.toEqual({ meta: 0, operation: 0 });
    await expect(
      runInDurableObject(shard, (_instance, state) =>
        (
          state.storage.sql
            .exec("SELECT COUNT(*) AS n FROM chunk_refs")
            .toArray()[0] as { n: number }
        ).n
      )
    ).resolves.toBe(0);
  });
});
