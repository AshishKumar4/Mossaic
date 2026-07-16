import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import type { UserDO } from "@app/objects/user/user-do";
import type { ShardDO } from "@core/objects/shard/shard-do";
import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import { verifyVFSMultipartToken } from "@core/lib/auth";
import { pruneTerminalMultipartSessions } from "@core/objects/user/multipart-upload";
import { hashChunk } from "@shared/crypto";
import {
  MULTIPART_MAX_OPEN_SESSIONS_PER_TENANT,
  MULTIPART_MAX_TTL_MS,
  MULTIPART_PLACEMENT_VERSION,
  MULTIPART_PROTOCOL_VERSION,
  MULTIPART_TERMINAL_RETENTION_MS,
} from "@shared/multipart";
import type { EnvCore } from "@shared/types";
import { placeChunk, placeMultipartChunk } from "@shared/placement";
import { createVFS, type MossaicEnv } from "../../sdk/src/index";

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}

const E = env as unknown as TestEnv;
const AUTH_ENV = env as unknown as EnvCore;
const hash = (value: number): string => value.toString(16).padStart(64, "0");

function stub(tenant: string): DurableObjectStub<UserDO> {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
  );
}

function shardStub(
  tenant: string,
  shardIndex: number
): DurableObjectStub<ShardDO> {
  return E.MOSSAIC_SHARD.get(
    E.MOSSAIC_SHARD.idFromName(
      vfsShardDOName("default", tenant, undefined, shardIndex)
    )
  );
}

function unusedShard(
  tenant: string,
  uploadId: string,
  poolSize: number,
  totalChunks: number,
  placementVersion?: number
): number {
  const used = new Set(
    Array.from({ length: totalChunks }, (_, index) =>
      placeMultipartChunk(
        tenant,
        uploadId,
        index,
        poolSize,
        placementVersion
      )
    )
  );
  for (let shardIndex = 0; shardIndex < poolSize; shardIndex++) {
    if (!used.has(shardIndex)) return shardIndex;
  }
  throw new Error("test upload touched every shard");
}

function bindingEnv(): MossaicEnv {
  return {
    MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD: E.MOSSAIC_SHARD as MossaicEnv["MOSSAIC_SHARD"],
  };
}

async function seedManifest(
  tenant: string,
  uploadId: string,
  poolSize: number,
  hashes: readonly string[],
  placementVersion?: number
): Promise<void> {
  const byShard = new Map<number, number[]>();
  for (let index = 0; index < hashes.length; index++) {
    const shardIndex = placeMultipartChunk(
      tenant,
      uploadId,
      index,
      poolSize,
      placementVersion
    );
    const indices = byShard.get(shardIndex) ?? [];
    indices.push(index);
    byShard.set(shardIndex, indices);
  }
  await Promise.all(
    Array.from(byShard, async ([shardIndex, indices]) => {
      const shard = E.MOSSAIC_SHARD.get(
        E.MOSSAIC_SHARD.idFromName(
          vfsShardDOName("default", tenant, undefined, shardIndex)
        )
      );
      await shard.getMultipartManifest(uploadId);
      await runInDurableObject(shard, (_instance, state) => {
        for (const index of indices) {
          state.storage.sql.exec(
            `INSERT INTO upload_chunks
               (upload_id, chunk_index, chunk_hash, chunk_size, user_id, created_at)
             VALUES (?, ?, ?, 1, ?, ?)`,
            uploadId,
            index,
            hashes[index],
            tenant,
            Date.now()
          );
        }
      });
    })
  );
}

async function readPublishedCounts(
  tenant: string,
  uploadId: string
): Promise<{
  fileChunks: number;
  versionChunks: number;
  sessionStatus: string;
  finalizeContext: string | null;
}> {
  return runInDurableObject(stub(tenant), (_instance, state) => {
    const sql = state.storage.sql;
    return {
      fileChunks: (
        sql
          .exec(
            "SELECT COUNT(*) AS n FROM file_chunks WHERE file_id = ?",
            uploadId
          )
          .toArray()[0] as { n: number }
      ).n,
      versionChunks: (
        sql.exec("SELECT COUNT(*) AS n FROM version_chunks").toArray()[0] as {
          n: number;
        }
      ).n,
      sessionStatus: (
        sql
          .exec(
            "SELECT status FROM upload_sessions WHERE upload_id = ?",
            uploadId
          )
          .toArray()[0] as { status: string }
      ).status,
      finalizeContext: (
        sql
          .exec(
            "SELECT finalize_context FROM upload_sessions WHERE upload_id = ?",
            uploadId
          )
          .toArray()[0] as { finalize_context: string | null }
      ).finalize_context,
    };
  });
}

describe("multipart expected hash pages", () => {
  it("counts open, finalizing, and aborting sessions atomically against the tenant cap", async () => {
    const tenant = "multipart-active-session-cap";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    await user.vfsExists(scope, "/missing");
    await runInDurableObject(user, (_instance, state) => {
      for (
        let index = 0;
        index < MULTIPART_MAX_OPEN_SESSIONS_PER_TENANT;
        index++
      ) {
        const status = ["open", "finalizing", "aborting"][index % 3]!;
        state.storage.sql.exec(
          `INSERT INTO upload_sessions
             (upload_id, user_id, parent_id, leaf, total_size, total_chunks,
              chunk_size, pool_size, expires_at, status, mode, mime_type,
              created_at)
           VALUES (?, ?, NULL, ?, 0, 0, 0, 1, ?, ?, 420,
                   'application/octet-stream', ?)`,
          `active-cap-${index}`,
          tenant,
          `active-cap-${index}.bin`,
          Date.now() + 60_000,
          status,
          Date.now()
        );
      }
    });

    await expect(
      user.vfsBeginMultipart(scope, "/over-cap.bin", { size: 0 })
    ).rejects.toThrow(/EBUSY.*64 active sessions/);
    await expect(
      runInDurableObject(user, (_instance, state) => ({
        sessions: (
          state.storage.sql
            .exec("SELECT COUNT(*) AS n FROM upload_sessions")
            .toArray()[0] as { n: number }
        ).n,
        tempFiles: (
          state.storage.sql
            .exec("SELECT COUNT(*) AS n FROM files WHERE status = 'uploading'")
            .toArray()[0] as { n: number }
        ).n,
      }))
    ).resolves.toEqual({ sessions: 64, tempFiles: 0 });
  });

  it("persists the exact clamped token expiry on begin and resume", async () => {
    const tenant = "multipart-persisted-ttl-clamp";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    const startedAt = Date.now();
    const begin = await user.vfsBeginMultipart(scope, "/ttl.bin", {
      size: 0,
      ttlMs: 1,
    });
    const token = await verifyVFSMultipartToken(AUTH_ENV, begin.sessionToken);
    if (token === null) throw new Error("multipart token did not verify");
    expect(begin.expiresAtMs - startedAt).toBeGreaterThanOrEqual(59_000);
    expect(begin.expiresAtMs - startedAt).toBeLessThanOrEqual(61_000);
    expect(token.exp * 1_000).toBeLessThanOrEqual(begin.expiresAtMs);
    expect(token.exp * 1_000).toBeGreaterThan(begin.expiresAtMs - 1_000);
    await expect(
      runInDurableObject(user, (_instance, state) =>
        (
          state.storage.sql
            .exec(
              "SELECT expires_at FROM upload_sessions WHERE upload_id = ?",
              begin.uploadId
            )
            .toArray()[0] as { expires_at: number }
        ).expires_at
      )
    ).resolves.toBe(begin.expiresAtMs);

    const resumedAt = Date.now();
    const resumed = await user.vfsBeginMultipart(scope, "/ignored.bin", {
      size: 0,
      resumeFrom: begin.uploadId,
      ttlMs: Number.POSITIVE_INFINITY,
    });
    const resumedToken = await verifyVFSMultipartToken(
      AUTH_ENV,
      resumed.sessionToken
    );
    if (resumedToken === null) throw new Error("resumed multipart token did not verify");
    expect(resumed.expiresAtMs - resumedAt).toBeGreaterThanOrEqual(
      MULTIPART_MAX_TTL_MS - 1_000
    );
    expect(resumed.expiresAtMs - resumedAt).toBeLessThanOrEqual(
      MULTIPART_MAX_TTL_MS + 1_000
    );
    expect(resumedToken.exp * 1_000).toBeLessThanOrEqual(resumed.expiresAtMs);
    await expect(
      runInDurableObject(user, (_instance, state) =>
        (
          state.storage.sql
            .exec(
              "SELECT expires_at FROM upload_sessions WHERE upload_id = ?",
              begin.uploadId
            )
            .toArray()[0] as { expires_at: number }
        ).expires_at
      )
    ).resolves.toBe(resumed.expiresAtMs);
  });

  it("compacts terminal payloads and prunes terminal rows after the idempotency window", async () => {
    const tenant = "multipart-terminal-retention";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    const finalized = await user.vfsBeginMultipart(scope, "/finalized.bin", {
      size: 0,
      metadata: { payload: "x".repeat(1_024) },
      tags: ["terminal"],
    });
    await user.vfsFinalizeMultipart(scope, finalized.uploadId, []);
    const aborted = await user.vfsBeginMultipart(scope, "/aborted.bin", {
      size: 0,
      metadata: { payload: "y".repeat(1_024) },
      tags: ["terminal"],
    });
    await user.vfsAbortMultipart(scope, aborted.uploadId);
    const poisonedId = "terminal-retention-poisoned";
    const cutoff = Date.now() - MULTIPART_TERMINAL_RETENTION_MS - 1;
    await runInDurableObject(user, (_instance, state) => {
      const sql = state.storage.sql;
      expect(
        sql
          .exec(
            `SELECT metadata_blob, tags_json, finalize_context,
                    finalize_sha_state, finalize_result, terminal_at
               FROM upload_sessions WHERE upload_id = ?`,
            finalized.uploadId
          )
          .toArray()[0]
      ).toMatchObject({
        metadata_blob: null,
        tags_json: null,
        finalize_context: null,
        finalize_sha_state: null,
        finalize_result: expect.any(String),
        terminal_at: expect.any(Number),
      });
      expect(
        sql
          .exec(
            `SELECT metadata_blob, tags_json, finalize_context,
                    finalize_sha_state, terminal_at
               FROM upload_sessions WHERE upload_id = ?`,
            aborted.uploadId
          )
          .toArray()[0]
      ).toEqual({
        metadata_blob: null,
        tags_json: null,
        finalize_context: null,
        finalize_sha_state: null,
        terminal_at: expect.any(Number),
      });
      sql.exec(
        `INSERT INTO upload_sessions
           (upload_id, user_id, parent_id, leaf, total_size, total_chunks,
            chunk_size, pool_size, expires_at, status, metadata_blob,
            tags_json, mode, mime_type, created_at, terminal_at)
         VALUES (?, ?, NULL, 'poisoned.bin', 0, 0, 0, 1, 0, 'poisoned',
                 ?, '["terminal"]', 420, 'application/octet-stream', ?, ?)`,
        poisonedId,
        tenant,
        new Uint8Array([1, 2, 3]),
        cutoff,
        cutoff
      );
      sql.exec(
        "UPDATE upload_sessions SET terminal_at = ? WHERE upload_id IN (?, ?)",
        cutoff,
        finalized.uploadId,
        aborted.uploadId
      );
    });

    await expect(
      runInDurableObject(user, (instance) =>
        pruneTerminalMultipartSessions(instance)
      )
    ).resolves.toEqual({ pruned: 3, remaining: false });
    await expect(
      runInDurableObject(user, (_instance, state) =>
        state.storage.sql
          .exec(
            "SELECT upload_id FROM upload_sessions WHERE upload_id IN (?, ?, ?)",
            finalized.uploadId,
            aborted.uploadId,
            poisonedId
          )
          .toArray()
      )
    ).resolves.toEqual([]);
  });

  it("advances terminal fencing in bounded 64-shard pages", async () => {
    const tenant = "hash-pages-fence-cursor";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    const begin = await user.vfsBeginMultipart(scope, "/fence.bin", {
      size: 1,
      chunkSize: 1,
    });
    await user.vfsStageMultipartHashes(scope, begin.uploadId, 0, [hash(1)]);
    await runInDurableObject(user, (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE upload_sessions SET pool_size = 130 WHERE upload_id = ?",
        begin.uploadId
      );
    });

    await expect(user.vfsFinalizeMultipartStep(scope, begin.uploadId)).resolves.toEqual({
      done: false,
      phase: "fencing",
      cursor: 64,
      total: 130,
    });
    await expect(user.vfsFinalizeMultipartStep(scope, begin.uploadId)).resolves.toEqual({
      done: false,
      phase: "fencing",
      cursor: 128,
      total: 130,
    });
    await expect(user.vfsFinalizeMultipartStep(scope, begin.uploadId)).resolves.toEqual({
      done: false,
      phase: "verifying",
      cursor: 0,
      total: 1,
    });
  });

  it("verifies and persists at most 256 chunks per step", async () => {
    const tenant = "hash-pages-verify-cursor";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    const begin = await user.vfsBeginMultipart(scope, "/verify.bin", {
      size: 300,
      chunkSize: 1,
      protocolVersion: MULTIPART_PROTOCOL_VERSION,
    });
    const hashes = Array.from({ length: 300 }, (_, i) => hash(i));
    await user.vfsStageMultipartHashes(scope, begin.uploadId, 0, hashes.slice(0, 256));
    await user.vfsStageMultipartHashes(scope, begin.uploadId, 256, hashes.slice(256));
    await runInDurableObject(user, (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE upload_sessions SET pool_size = 1 WHERE upload_id = ?",
        begin.uploadId
      );
    });
    const shard = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(vfsShardDOName("default", tenant, undefined, 0))
    );
    await shard.getMultipartManifest(begin.uploadId);
    await runInDurableObject(shard, (_instance, state) => {
      for (let index = 0; index < hashes.length; index++) {
        state.storage.sql.exec(
          `INSERT INTO upload_chunks
             (upload_id, chunk_index, chunk_hash, chunk_size, user_id, created_at)
           VALUES (?, ?, ?, 1, ?, ?)`,
          begin.uploadId,
          index,
          hashes[index],
          tenant,
          Date.now()
        );
      }
    });

    await expect(user.vfsFinalizeMultipartStep(scope, begin.uploadId)).resolves.toMatchObject({
      phase: "verifying",
      cursor: 0,
    });
    await expect(user.vfsFinalizeMultipartStep(scope, begin.uploadId)).resolves.toEqual({
      done: false,
      phase: "verifying",
      cursor: 256,
      total: 300,
    });
    await expect(user.vfsFinalizeMultipartStep(scope, begin.uploadId)).resolves.toEqual({
      done: false,
      phase: "publishing",
      cursor: 300,
      total: 300,
    });
    expect(
      await runInDurableObject(user, (_instance, state) =>
        state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM upload_verified_chunks WHERE upload_id = ?",
            begin.uploadId
          )
          .toArray()[0]
      )
    ).toMatchObject({ n: 300 });
  });

  it("persists contiguous pages and accepts identical replay", async () => {
    const tenant = "hash-pages-persist";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    const begin = await user.vfsBeginMultipart(scope, "/large.bin", {
      size: 300,
      chunkSize: 1,
      protocolVersion: MULTIPART_PROTOCOL_VERSION,
    });
    const first = Array.from({ length: 256 }, (_, i) => hash(i));
    const second = Array.from({ length: 44 }, (_, i) => hash(256 + i));

    await expect(
      user.vfsStageMultipartHashes(scope, begin.uploadId, 0, first)
    ).resolves.toEqual({ staged: 256, total: 300 });
    await expect(
      user.vfsStageMultipartHashes(scope, begin.uploadId, 256, second)
    ).resolves.toEqual({ staged: 300, total: 300 });
    await expect(
      user.vfsStageMultipartHashes(scope, begin.uploadId, 0, first)
    ).resolves.toEqual({ staged: 300, total: 300 });

    expect(
      await runInDurableObject(user, (_instance, state) =>
        state.storage.sql
          .exec(
            `SELECT s.staged_hash_cursor,
                    (SELECT COUNT(*) FROM upload_expected_chunks e
                      WHERE e.upload_id = s.upload_id) AS n
               FROM upload_sessions s WHERE s.upload_id = ?`,
            begin.uploadId
          )
          .toArray()[0]
      )
    ).toEqual({ staged_hash_cursor: 300, n: 300 });
  });

  it("rejects oversized, conflicting, invalid, and out-of-range pages", async () => {
    const tenant = "hash-pages-invalid";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    const begin = await user.vfsBeginMultipart(scope, "/invalid.bin", {
      size: 300,
      chunkSize: 1,
      protocolVersion: MULTIPART_PROTOCOL_VERSION,
    });
    await expect(
      user.vfsStageMultipartHashes(
        scope,
        begin.uploadId,
        0,
        Array.from({ length: 257 }, (_, i) => hash(i))
      )
    ).rejects.toThrow(/EINVAL/);
    await expect(
      user.vfsStageMultipartHashes(scope, begin.uploadId, 299, [hash(1), hash(2)])
    ).rejects.toThrow(/EINVAL/);
    await expect(
      user.vfsStageMultipartHashes(scope, begin.uploadId, 1, [hash(2)])
    ).rejects.toThrow(/contiguous cursor 0/);
    await expect(
      user.vfsStageMultipartHashes(scope, begin.uploadId, 0, ["not-a-hash"])
    ).rejects.toThrow(/EINVAL/);
    await user.vfsStageMultipartHashes(scope, begin.uploadId, 0, [hash(1)]);
    await expect(
      user.vfsStageMultipartHashes(scope, begin.uploadId, 0, [hash(2)])
    ).rejects.toThrow(/EBUSY/);
  });

  it("pages abort fencing, cleanup intents, and staged metadata deletion", async () => {
    const tenant = "hash-pages-abort-bounds";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    const begin = await user.vfsBeginMultipart(scope, "/abort.bin", {
      size: 300,
      chunkSize: 1,
      protocolVersion: MULTIPART_PROTOCOL_VERSION,
    });
    const hashes = Array.from({ length: 300 }, (_, index) => hash(10_000 + index));
    await user.vfsStageMultipartHashes(scope, begin.uploadId, 0, hashes.slice(0, 256));
    await user.vfsStageMultipartHashes(scope, begin.uploadId, 256, hashes.slice(256));
    const versionId = "abort-version";
    await runInDurableObject(user, (_instance, state) => {
      const sql = state.storage.sql;
      sql.exec(
        `UPDATE upload_sessions
            SET pool_size = 65, status = 'finalizing', finalize_phase = 'verifying',
                finalize_context = ?
          WHERE upload_id = ?`,
        JSON.stringify({
          schema: 1,
          versioning: true,
          pathId: begin.uploadId,
          versionId,
          expectedDestination: null,
          committedAt: Date.now(),
          metadataPresent: false,
          metadataBase64: null,
          tagsPresent: false,
          tags: [],
        }),
        begin.uploadId
      );
      for (let index = 0; index < hashes.length; index++) {
        sql.exec(
          `INSERT INTO upload_verified_chunks
             (upload_id, chunk_index, chunk_hash, chunk_size, shard_index)
           VALUES (?, ?, ?, 1, 0)`,
          begin.uploadId,
          index,
          hashes[index]
        );
        sql.exec(
          `INSERT INTO version_chunks
             (version_id, chunk_index, chunk_hash, chunk_size, shard_index)
           VALUES (?, ?, ?, 1, 0)`,
          versionId,
          index,
          hashes[index]
        );
      }
    });

    await expect(
      user.vfsAbortMultipartStep(scope, begin.uploadId)
    ).resolves.toEqual({
      done: false,
      phase: "fencing",
      cursor: 64,
      total: 65,
    });
    await expect(
      runInDurableObject(user, (_instance, state) =>
        state.storage.sql
          .exec(
            `SELECT status, abort_phase, abort_fence_cursor
               FROM upload_sessions WHERE upload_id = ?`,
            begin.uploadId
          )
          .toArray()[0]
      )
    ).resolves.toEqual({
      status: "aborting",
      abort_phase: "fencing",
      abort_fence_cursor: 64,
    });

    await expect(
      user.vfsAbortMultipart(scope, begin.uploadId)
    ).rejects.toThrow(/EBUSY.*cleanup is still in progress/);
    await expect(
      runInDurableObject(user, (_instance, state) => {
        const sql = state.storage.sql;
        return {
          session: sql
            .exec(
              `SELECT status, abort_phase, abort_cleanup_cursor
                 FROM upload_sessions WHERE upload_id = ?`,
              begin.uploadId
            )
            .toArray()[0],
          expected: (
            sql
              .exec(
                "SELECT COUNT(*) AS n FROM upload_expected_chunks WHERE upload_id = ?",
                begin.uploadId
              )
              .toArray()[0] as { n: number }
          ).n,
          verified: (
            sql
              .exec(
                "SELECT COUNT(*) AS n FROM upload_verified_chunks WHERE upload_id = ?",
                begin.uploadId
              )
              .toArray()[0] as { n: number }
          ).n,
          version: (
            sql
              .exec("SELECT COUNT(*) AS n FROM version_chunks WHERE version_id = ?", versionId)
              .toArray()[0] as { n: number }
          ).n,
        };
      })
    ).resolves.toEqual({
      session: {
        status: "aborting",
        abort_phase: "local",
        abort_cleanup_cursor: 300,
      },
      expected: 0,
      verified: 0,
      version: 0,
    });

    await expect(
      user.vfsAbortMultipartStep(scope, begin.uploadId)
    ).resolves.toEqual({ done: true });
    expect(await readPublishedCounts(tenant, begin.uploadId)).toMatchObject({
      fileChunks: 0,
      versionChunks: 0,
      sessionStatus: "aborted",
    });
  });

  it("alarms promptly finish unexpired aborting sessions", async () => {
    const tenant = "hash-pages-abort-alarm";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    const begin = await user.vfsBeginMultipart(scope, "/abort-alarm.bin", {
      size: 0,
      protocolVersion: MULTIPART_PROTOCOL_VERSION,
      ttlMs: 60 * 60 * 1000,
    });
    await runInDurableObject(user, (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE upload_sessions SET pool_size = 130 WHERE upload_id = ?",
        begin.uploadId
      );
    });
    await expect(
      user.vfsAbortMultipartStep(scope, begin.uploadId)
    ).resolves.toMatchObject({ done: false, phase: "fencing", cursor: 64 });

    await runInDurableObject(user, (instance) => instance.alarm());
    const afterAlarm = await runInDurableObject(
      user,
      (_instance, state) => ({
        session: state.storage.sql
          .exec(
            "SELECT status, abort_phase, expires_at FROM upload_sessions WHERE upload_id = ?",
            begin.uploadId
          )
          .toArray()[0] as {
          status: string;
          abort_phase: string;
          expires_at: number;
        },
      })
    );
    expect(afterAlarm.session).toMatchObject({
      status: "aborted",
      abort_phase: "done",
    });
    expect(afterAlarm.session.expires_at).toBeGreaterThan(Date.now());
    expect(await readPublishedCounts(tenant, begin.uploadId)).toMatchObject({
      sessionStatus: "aborted",
    });
  });

  it("binding SDK loops abort steps until terminal cleanup", async () => {
    const tenant = "hash-pages-abort-binding";
    const user = stub(tenant);
    const vfs = createVFS(bindingEnv(), { tenant });
    const handle = await vfs.beginMultipartUpload("/abort-binding.bin", {
      size: 0,
    });
    await runInDurableObject(user, (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE upload_sessions SET pool_size = 65 WHERE upload_id = ?",
        handle.uploadId
      );
    });

    await expect(vfs.abortMultipartUpload(handle)).resolves.toEqual({
      aborted: true,
    });
    expect(await readPublishedCounts(tenant, handle.uploadId)).toMatchObject({
      sessionStatus: "aborted",
    });
  });

  it("publishes more than 256 chunks through the public binding SDK with the exact file hash", async () => {
    const tenant = "hash-pages-public-nonversioned";
    const vfs = createVFS(bindingEnv(), { tenant });
    const handle = await vfs.beginMultipartUpload("/large-public.bin", {
      size: 300,
      chunkSize: 1,
    });
    const hashes = Array.from({ length: 300 }, (_, index) => hash(index));
    await seedManifest(
      tenant,
      handle.uploadId,
      handle.poolSize,
      hashes,
      MULTIPART_PLACEMENT_VERSION
    );

    const result = await vfs.finalizeMultipartUpload(handle, hashes);
    const expectedHash = await hashChunk(
      new TextEncoder().encode(hashes.join(""))
    );

    expect(result).toMatchObject({
      path: "/large-public.bin",
      pathId: handle.uploadId,
      versionId: "",
      size: 300,
      fileHash: expectedHash,
    });
    expect(await readPublishedCounts(tenant, handle.uploadId)).toMatchObject({
      fileChunks: 300,
      versionChunks: 0,
      sessionStatus: "finalized",
    });
    await expect(vfs.finalizeMultipartUpload(handle, hashes)).resolves.toEqual(
      result
    );
  });

  it("pre-stages overwrite cleanup and deletes the old manifest only after publication in pages", async () => {
    const tenant = "hash-pages-overwrite-cleanup";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    await user.vfsWriteFile(scope, "/overwrite.bin", new Uint8Array([1]));
    const oldFileId = await runInDurableObject(user, (_instance, state) => {
      const sql = state.storage.sql;
      const file = sql
        .exec(
          "SELECT file_id FROM files WHERE user_id = ? AND file_name = 'overwrite.bin'",
          tenant
        )
        .toArray()[0] as { file_id: string };
      sql.exec(
        `UPDATE files SET inline_data = NULL, file_size = 600, chunk_count = 600
          WHERE file_id = ?`,
        file.file_id
      );
      for (let index = 0; index < 600; index++) {
        sql.exec(
          `INSERT INTO file_chunks
             (file_id, chunk_index, chunk_hash, chunk_size, shard_index)
           VALUES (?, ?, ?, 1, ?)`,
          file.file_id,
          index,
          hash(20_000 + index),
          index % 65
        );
      }
      return file.file_id;
    });
    const begin = await user.vfsBeginMultipart(scope, "/overwrite.bin", {
      size: 1,
      chunkSize: 1,
      protocolVersion: MULTIPART_PROTOCOL_VERSION,
    });
    const hashes = [hash(30_000)];
    await seedManifest(
      tenant,
      begin.uploadId,
      begin.poolSize,
      hashes,
      MULTIPART_PLACEMENT_VERSION
    );
    await user.vfsStageMultipartHashes(scope, begin.uploadId, 0, hashes);

    await user.vfsFinalizeMultipartStep(scope, begin.uploadId);
    await expect(user.vfsFinalizeMultipartStep(scope, begin.uploadId)).resolves.toMatchObject({
      phase: "preparing",
    });
    await expect(user.vfsFinalizeMultipartStep(scope, begin.uploadId)).resolves.toMatchObject({
      phase: "preparing",
      cursor: 255,
    });
    await expect(user.vfsFinalizeMultipartStep(scope, begin.uploadId)).resolves.toMatchObject({
      phase: "preparing",
      cursor: 511,
    });
    await expect(user.vfsFinalizeMultipartStep(scope, begin.uploadId)).resolves.toMatchObject({
      phase: "publishing",
      cursor: 599,
    });
    expect(
      await runInDurableObject(user, (_instance, state) => {
        const sql = state.storage.sql;
        return {
          routes: (
            sql
              .exec(
                "SELECT COUNT(*) AS n FROM upload_cleanup_routes WHERE upload_id = ? AND cleanup_kind = 'chunks'",
                begin.uploadId
              )
              .toArray()[0] as { n: number }
          ).n,
          stagingRoutes: (
            sql
              .exec(
                "SELECT COUNT(*) AS n FROM upload_cleanup_routes WHERE upload_id = ? AND cleanup_kind = 'multipart_staging'",
                begin.uploadId
              )
              .toArray()[0] as { n: number }
          ).n,
          executableIntents: (
            sql
              .exec(
                "SELECT COUNT(*) AS n FROM chunk_cleanup_intents WHERE ref_id = ?",
                oldFileId
              )
              .toArray()[0] as { n: number }
          ).n,
          uploadExecutableIntents: (
            sql
              .exec(
                "SELECT COUNT(*) AS n FROM chunk_cleanup_intents WHERE ref_id = ?",
                begin.uploadId
              )
              .toArray()[0] as { n: number }
          ).n,
        };
      })
    ).toEqual({
      routes: 65,
      stagingRoutes: 1,
      executableIntents: 0,
      uploadExecutableIntents: 0,
    });
    await expect(user.vfsFinalizeMultipartStep(scope, begin.uploadId)).resolves.toMatchObject({
      phase: "cleaning",
    });

    const oldManifestCount = async (): Promise<number> =>
      runInDurableObject(user, (_instance, state) =>
        (
          state.storage.sql
            .exec("SELECT COUNT(*) AS n FROM file_chunks WHERE file_id = ?", oldFileId)
            .toArray()[0] as { n: number }
        ).n
      );
    expect(await oldManifestCount()).toBe(600);

    const counts = [600];
    let progress = await user.vfsFinalizeMultipartStep(scope, begin.uploadId);
    while (!progress.done) {
      counts.push(await oldManifestCount());
      progress = await user.vfsFinalizeMultipartStep(scope, begin.uploadId);
    }
    counts.push(await oldManifestCount());
    for (let index = 1; index < counts.length; index++) {
      expect(counts[index - 1]! - counts[index]!).toBeLessThanOrEqual(256);
    }
    expect(counts).toContain(344);
    expect(counts).toContain(88);
    expect(counts.at(-1)).toBe(0);
    expect(
      await runInDurableObject(user, (_instance, state) =>
        state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM upload_cleanup_routes WHERE upload_id = ?",
            begin.uploadId
          )
          .toArray()[0]
      )
    ).toMatchObject({ n: 0 });
    await expect(user.vfsStat(scope, "/overwrite.bin")).resolves.toMatchObject({ size: 1 });
  });

  it("freezes versioning and publishes more than 256 version chunks", async () => {
    const tenant = "hash-pages-public-versioned";
    const vfs = createVFS(bindingEnv(), {
      tenant,
      versioning: "enabled",
    });
    const handle = await vfs.beginMultipartUpload("/large-versioned.bin", {
      size: 300,
      chunkSize: 1,
    });
    const hashes = Array.from({ length: 300 }, (_, index) => hash(1_000 + index));
    await seedManifest(
      tenant,
      handle.uploadId,
      handle.poolSize,
      hashes,
      MULTIPART_PLACEMENT_VERSION
    );

    const result = await vfs.finalizeMultipartUpload(handle, hashes);
    expect(result).not.toHaveProperty("operation");
    expect(result.versionId).toMatch(/^[a-z0-9]+$/);
    expect(result.fileHash).toBe(
      await hashChunk(new TextEncoder().encode(hashes.join("")))
    );
    const published = await readPublishedCounts(tenant, handle.uploadId);
    expect(published).toMatchObject({
      fileChunks: 0,
      versionChunks: 300,
      sessionStatus: "finalized",
    });
    expect(published.finalizeContext).toBeNull();
  });

  it("rejects a destination changed after the finalize context was frozen", async () => {
    const tenant = "hash-pages-stale-destination";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    await user.vfsWriteFile(scope, "/stale.bin", new Uint8Array([1]));
    const begin = await user.vfsBeginMultipart(scope, "/stale.bin", {
      size: 1,
      chunkSize: 1,
    });
    const hashes = [hash(77)];
    await seedManifest(tenant, begin.uploadId, begin.poolSize, hashes);
    await user.vfsStageMultipartHashes(scope, begin.uploadId, 0, hashes);
    await expect(
      user.vfsFinalizeMultipartStep(scope, begin.uploadId)
    ).resolves.toMatchObject({ done: false, phase: "verifying" });

    await user.vfsWriteFile(scope, "/stale.bin", new Uint8Array([2]));
    await user.vfsFinalizeMultipartStep(scope, begin.uploadId);
    await expect(
      user.vfsFinalizeMultipartStep(scope, begin.uploadId)
    ).resolves.toMatchObject({ done: false, phase: "publishing" });
    await expect(
      user.vfsFinalizeMultipartStep(scope, begin.uploadId)
    ).rejects.toThrow(/destination changed/);
    await expect(user.vfsReadFile(scope, "/stale.bin")).resolves.toEqual(
      new Uint8Array([2])
    );
  });

  it("uses the versioning flag frozen before fencing", async () => {
    const tenant = "hash-pages-frozen-versioning";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    await user.adminSetVersioning(tenant, true);
    const begin = await user.vfsBeginMultipart(scope, "/frozen-version.bin", {
      size: 1,
      chunkSize: 1,
    });
    const hashes = [hash(91)];
    await seedManifest(tenant, begin.uploadId, begin.poolSize, hashes);
    await user.vfsStageMultipartHashes(scope, begin.uploadId, 0, hashes);
    await user.vfsFinalizeMultipartStep(scope, begin.uploadId);

    await user.adminSetVersioning(tenant, false);
    await user.vfsFinalizeMultipartStep(scope, begin.uploadId);
    await expect(
      user.vfsFinalizeMultipartStep(scope, begin.uploadId)
    ).resolves.toMatchObject({ done: false, phase: "cleaning" });
    await expect(
      user.vfsFinalizeMultipartStep(scope, begin.uploadId)
    ).resolves.toMatchObject({ done: false, phase: "cleaning" });
    const completed = await user.vfsFinalizeMultipartStep(scope, begin.uploadId);

    expect(completed).toMatchObject({
      done: true,
      result: { fileId: begin.uploadId },
    });
    expect(
      await runInDurableObject(user, (_instance, state) =>
        state.storage.sql
          .exec("SELECT COUNT(*) AS n FROM file_versions")
          .toArray()[0]
      )
    ).toMatchObject({ n: 1 });
  });

  it("publishes the verified stored size when it differs from the source size", async () => {
    const tenant = "hash-pages-size-mismatch";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    const begin = await user.vfsBeginMultipart(scope, "/wrong-size.bin", {
      size: 1,
      chunkSize: 1,
    });
    const hashes = [hash(88)];
    await seedManifest(tenant, begin.uploadId, begin.poolSize, hashes);
    const shardIndex = placeChunk(tenant, begin.uploadId, 0, begin.poolSize);
    const shard = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(
        vfsShardDOName("default", tenant, undefined, shardIndex)
      )
    );
    await runInDurableObject(shard, (_instance, state) => {
      state.storage.sql.exec(
        `UPDATE upload_chunks SET chunk_size = 2
          WHERE upload_id = ? AND chunk_index = 0`,
        begin.uploadId
      );
    });
    await user.vfsStageMultipartHashes(scope, begin.uploadId, 0, hashes);
    await user.vfsFinalizeMultipartStep(scope, begin.uploadId);
    await user.vfsFinalizeMultipartStep(scope, begin.uploadId);
    await expect(user.vfsFinalizeMultipartStep(scope, begin.uploadId)).resolves.toMatchObject({
      done: false,
      phase: "cleaning",
    });
    await expect(user.vfsFinalizeMultipartStep(scope, begin.uploadId)).resolves.toMatchObject({
      done: false,
      phase: "cleaning",
    });
    await expect(user.vfsFinalizeMultipartStep(scope, begin.uploadId)).resolves.toMatchObject({
      done: true,
      result: {
        size: 2,
        chunkCount: 1,
        fileHash: await hashChunk(new TextEncoder().encode(hashes.join(""))),
      },
    });
    await expect(user.vfsStat(scope, "/wrong-size.bin")).resolves.toMatchObject({
      size: 2,
    });
  });

  it("rejects a chunk written directly to a non-deterministic shard", async () => {
    const tenant = "hash-pages-wrong-direct-shard";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    const begin = await user.vfsBeginMultipart(scope, "/wrong-shard.bin", {
      size: 2,
      chunkSize: 1,
      protocolVersion: MULTIPART_PROTOCOL_VERSION,
    });
    const hashes = Array.from({ length: begin.totalChunks }, (_, index) =>
      hash(2_000 + index)
    );
    const misplacedIndex = 0;
    const wrongShard = unusedShard(
      tenant,
      begin.uploadId,
      begin.poolSize,
      begin.totalChunks,
      MULTIPART_PLACEMENT_VERSION
    );
    const misplacedBytes = new Uint8Array([201]);
    hashes[misplacedIndex] = await hashChunk(misplacedBytes);
    await seedManifest(
      tenant,
      begin.uploadId,
      begin.poolSize,
      hashes,
      MULTIPART_PLACEMENT_VERSION
    );
    const expectedShard = placeMultipartChunk(
      tenant,
      begin.uploadId,
      misplacedIndex,
      begin.poolSize,
      MULTIPART_PLACEMENT_VERSION
    );
    await runInDurableObject(shardStub(tenant, expectedShard), (_instance, state) => {
      state.storage.sql.exec(
        "DELETE FROM upload_chunks WHERE upload_id = ? AND chunk_index = ?",
        begin.uploadId,
        misplacedIndex
      );
    });
    await shardStub(tenant, wrongShard).putChunkMultipart(
      hashes[misplacedIndex]!,
      misplacedBytes,
      begin.uploadId,
      misplacedIndex,
      tenant,
      begin.sessionToken
    );
    await expect(
      user.vfsGetMultipartStatus(scope, begin.uploadId)
    ).resolves.toMatchObject({ landed: [1] });
    await user.vfsStageMultipartHashes(scope, begin.uploadId, 0, hashes);
    await user.vfsFinalizeMultipartStep(scope, begin.uploadId);

    await expect(
      user.vfsFinalizeMultipartStep(scope, begin.uploadId)
    ).rejects.toThrow(/chunk .* landed on shard .* expected shard/);
    expect(await readPublishedCounts(tenant, begin.uploadId)).toMatchObject({
      fileChunks: 0,
      versionChunks: 0,
      sessionStatus: "aborted",
    });
  });

  it("rejects duplicate chunk indices written directly across shards", async () => {
    const tenant = "hash-pages-duplicate-direct-shard";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    const begin = await user.vfsBeginMultipart(scope, "/duplicate-shard.bin", {
      size: 2,
      chunkSize: 1,
      protocolVersion: MULTIPART_PROTOCOL_VERSION,
    });
    const hashes = Array.from({ length: begin.totalChunks }, (_, index) =>
      hash(3_000 + index)
    );
    const duplicateIndex = 0;
    const wrongShard = unusedShard(
      tenant,
      begin.uploadId,
      begin.poolSize,
      begin.totalChunks,
      MULTIPART_PLACEMENT_VERSION
    );
    const duplicateBytes = new Uint8Array([202]);
    hashes[duplicateIndex] = await hashChunk(duplicateBytes);
    await seedManifest(
      tenant,
      begin.uploadId,
      begin.poolSize,
      hashes,
      MULTIPART_PLACEMENT_VERSION
    );
    await shardStub(tenant, wrongShard).putChunkMultipart(
      hashes[duplicateIndex]!,
      duplicateBytes,
      begin.uploadId,
      duplicateIndex,
      tenant,
      begin.sessionToken
    );
    await user.vfsStageMultipartHashes(scope, begin.uploadId, 0, hashes);
    await user.vfsFinalizeMultipartStep(scope, begin.uploadId);

    await expect(
      user.vfsFinalizeMultipartStep(scope, begin.uploadId)
    ).rejects.toThrow(/duplicate chunk index .* across shards/);
    expect(await readPublishedCounts(tenant, begin.uploadId)).toMatchObject({
      fileChunks: 0,
      versionChunks: 0,
      sessionStatus: "aborted",
    });
  });

  it("lazily reconstructs and persists a pre-upgrade finalized result", async () => {
    const tenant = "hash-pages-legacy-finalized";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    const begin = await user.vfsBeginMultipart(scope, "/legacy-finalized.bin", {
      size: 1,
      chunkSize: 1,
    });
    const hashes = [hash(4_001)];
    await seedManifest(tenant, begin.uploadId, begin.poolSize, hashes);
    const original = await user.vfsFinalizeMultipart(scope, begin.uploadId, hashes);
    await runInDurableObject(user, (_instance, state) => {
      state.storage.sql.exec(
        `UPDATE upload_sessions
            SET finalize_phase = NULL, finalize_context = NULL,
                finalize_sha_state = NULL, finalize_result = NULL
          WHERE upload_id = ?`,
        begin.uploadId
      );
    });

    await expect(
      user.vfsFinalizeMultipart(scope, begin.uploadId, hashes)
    ).resolves.toEqual(original);
    expect(
      await runInDurableObject(user, (_instance, state) =>
        state.storage.sql
          .exec(
            "SELECT finalize_phase, finalize_result FROM upload_sessions WHERE upload_id = ?",
            begin.uploadId
          )
          .toArray()[0]
      )
    ).toMatchObject({
      finalize_phase: null,
      finalize_result: JSON.stringify(original),
    });
  });

  it("lazily reconstructs a pre-upgrade versioned finalized result", async () => {
    const tenant = "hash-pages-legacy-versioned-finalized";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    await user.adminSetVersioning(tenant, true);
    const begin = await user.vfsBeginMultipart(
      scope,
      "/legacy-versioned-finalized.bin",
      { size: 1, chunkSize: 1 }
    );
    const hashes = [hash(4_002)];
    await seedManifest(tenant, begin.uploadId, begin.poolSize, hashes);
    const original = await user.vfsFinalizeMultipart(scope, begin.uploadId, hashes);
    await runInDurableObject(user, (_instance, state) => {
      state.storage.sql.exec(
        `UPDATE upload_sessions
            SET finalize_phase = NULL, finalize_context = NULL,
                finalize_sha_state = NULL, finalize_result = NULL
          WHERE upload_id = ?`,
        begin.uploadId
      );
    });

    await expect(
      user.vfsFinalizeMultipart(scope, begin.uploadId, hashes)
    ).resolves.toEqual(original);
    expect(original.versionId).not.toBe("");
    expect(
      await runInDurableObject(user, (_instance, state) =>
        state.storage.sql
          .exec(
            "SELECT finalize_result FROM upload_sessions WHERE upload_id = ?",
            begin.uploadId
          )
          .toArray()[0]
      )
    ).toMatchObject({ finalize_result: JSON.stringify(original) });
  });

  it("safely aborts a pre-upgrade finalizing session without frozen state", async () => {
    const tenant = "hash-pages-legacy-finalizing";
    const user = stub(tenant);
    const scope = { ns: "default", tenant } as const;
    const begin = await user.vfsBeginMultipart(scope, "/legacy-finalizing.bin", {
      size: 1,
      chunkSize: 1,
    });
    const bytes = new Uint8Array([203]);
    const chunkHash = await hashChunk(bytes);
    const shardIndex = placeChunk(tenant, begin.uploadId, 0, begin.poolSize);
    await shardStub(tenant, shardIndex).putChunkMultipart(
      chunkHash,
      bytes,
      begin.uploadId,
      0,
      tenant,
      begin.sessionToken
    );
    await runInDurableObject(user, (_instance, state) => {
      state.storage.sql.exec(
        `UPDATE upload_sessions
            SET status = 'finalizing', finalize_phase = NULL,
                finalize_context = NULL
          WHERE upload_id = ?`,
        begin.uploadId
      );
    });

    await expect(
      user.vfsFinalizeMultipartStep(scope, begin.uploadId)
    ).rejects.toThrow(/pre-upgrade finalizing session was aborted/);
    expect(await readPublishedCounts(tenant, begin.uploadId)).toMatchObject({
      fileChunks: 0,
      versionChunks: 0,
      sessionStatus: "aborted",
    });
    expect(
      await runInDurableObject(
        shardStub(tenant, shardIndex),
        (_instance, state) =>
          state.storage.sql
            .exec(
              "SELECT COUNT(*) AS n FROM chunk_refs WHERE file_id = ?",
              begin.uploadId
            )
            .toArray()[0]
      )
    ).toMatchObject({ n: 0 });
  });
});
