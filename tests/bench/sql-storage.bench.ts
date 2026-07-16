import { env, runInDurableObject } from "cloudflare:test";
import { expect, it } from "vitest";
import { hashChunk } from "@shared/crypto";
import { INLINE_LIMIT } from "@shared/inline";
import { MULTIPART_PROTOCOL_VERSION } from "@shared/multipart";
import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import type { MultipartBeginResponse } from "@shared/multipart";
import type { DropVersionsStepResult, VFSScope } from "@shared/vfs-types";
import type { SqlMetrics } from "./counting-sql-storage";
import type {
  BenchmarkMetrics,
  BenchmarkShardDO,
  BenchmarkUserDO,
} from "./test-worker";

interface BenchmarkEnv {
  MOSSAIC_USER: DurableObjectNamespace<BenchmarkUserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<BenchmarkShardDO>;
}

interface MetricsStub {
  benchmarkResetSqlMetrics(): Promise<void>;
  benchmarkMetrics(): Promise<BenchmarkMetrics>;
}

interface VfsHarness {
  scope: VFSScope;
  user: DurableObjectStub<BenchmarkUserDO>;
  shard: DurableObjectStub<BenchmarkShardDO>;
}

interface BenchmarkResult extends SqlMetrics {
  name: string;
  wallMs: number;
  rpcCalls: number;
  userRpcCalls: number;
  shardRpcCalls: number;
  user: SqlMetrics;
  shards: SqlMetrics;
}

interface PreparedBenchmark {
  user: MetricsStub | null;
  shards: MetricsStub[];
  operation: () => Promise<void>;
}

interface RetentionCursorBenchmark {
  first: BenchmarkResult;
  late: BenchmarkResult;
  plan: string[];
}

const E = env as unknown as BenchmarkEnv;
const encoder = new TextEncoder();

type SqlBudget = Omit<SqlMetrics, "other">;

interface ComponentBudget {
  user: SqlBudget;
  shards: SqlBudget | null;
}

interface BenchmarkBudget extends SqlBudget {
  rpcCalls: number;
}

const VFS_COMPONENT_BUDGETS = {
  "inline overwrite": {
    user: {
      statements: 45,
      reads: 22,
      writes: 25,
      rowsRead: 39,
      rowsWritten: 25,
    },
    shards: null,
  },
  "inline unlink": {
    user: {
      statements: 28,
      reads: 12,
      writes: 18,
      rowsRead: 27,
      rowsWritten: 15,
    },
    shards: null,
  },
  "one-chunk overwrite": {
    user: {
      statements: 58,
      reads: 27,
      writes: 33,
      rowsRead: 55,
      rowsWritten: 40,
    },
    shards: {
      statements: 20,
      reads: 10,
      writes: 10,
      rowsRead: 24,
      rowsWritten: 20,
    },
  },
  "one-chunk unlink": {
    user: {
      statements: 33,
      reads: 14,
      writes: 21,
      rowsRead: 37,
      rowsWritten: 22,
    },
    shards: {
      statements: 12,
      reads: 6,
      writes: 6,
      rowsRead: 15,
      rowsWritten: 12,
    },
  },
  "one-chunk rename-overwrite": {
    user: {
      statements: 38,
      reads: 17,
      writes: 23,
      rowsRead: 42,
      rowsWritten: 28,
    },
    shards: {
      statements: 12,
      reads: 6,
      writes: 6,
      rowsRead: 16,
      rowsWritten: 12,
    },
  },
} satisfies Record<string, ComponentBudget>;

function budget(
  statements: number,
  reads: number,
  writes: number,
  rowsRead: number,
  rowsWritten: number,
  rpcCalls: number
): BenchmarkBudget {
  return { statements, reads, writes, rowsRead, rowsWritten, rpcCalls };
}

const BENCHMARK_BUDGETS = {
  "inline overwrite": budget(45, 22, 25, 39, 25, 1),
  "inline unlink": budget(28, 12, 18, 27, 15, 1),
  "one-chunk overwrite": budget(80, 36, 44, 72, 60, 2),
  "one-chunk unlink": budget(48, 20, 28, 50, 32, 2),
  "one-chunk rename-overwrite": budget(52, 22, 30, 55, 40, 2),
  "multipart finalize fencing (1 shard)": budget(24, 14, 12, 24, 18, 2),
  "multipart finalize verifying (1 shard)": budget(20, 11, 11, 20, 14, 2),
  "multipart finalize preparing (256 old)": budget(14, 7, 8, 300, 10, 1),
  "multipart finalize preparing (4096 old)": budget(14, 7, 8, 300, 10, 1),
  "multipart finalize publishing (256 old)": budget(32, 14, 20, 30, 20, 1),
  "multipart finalize publishing (4096 old)": budget(32, 14, 20, 30, 20, 1),
  "multipart finalize cleaning (256 rows)": budget(14, 6, 9, 550, 540, 1),
  "multipart finalize cleaning (4096 rows)": budget(14, 6, 9, 550, 540, 1),
  "multipart finalize old cleanup (256 old)": budget(14, 7, 8, 550, 280, 1),
  "multipart finalize old cleanup (4096 old)": budget(14, 7, 8, 550, 280, 1),
  "multipart hash page (256 prior)": budget(270, 6, 264, 16, 530, 1),
  "multipart hash page (4096 prior)": budget(270, 6, 264, 16, 530, 1),
  "multipart PUT (first)": budget(14, 7, 9, 10, 20, 1),
  "multipart PUT (replay)": budget(12, 7, 7, 12, 10, 1),
  "multipart abort fencing (64 shards)": budget(280, 75, 205, 90, 470, 65),
  "multipart abort intents (1 backlog)": budget(50, 22, 30, 55, 40, 3),
  "multipart abort cleanup (256 backlog)": budget(16, 7, 10, 550, 540, 1),
  "multipart abort cleanup (4096 backlog)": budget(16, 7, 10, 550, 540, 1),
  "multipart abort old_intents (256 backlog)": budget(80, 8, 72, 150, 75, 1),
  "multipart abort old_intents (4096 backlog)": budget(80, 8, 72, 150, 75, 1),
  "multipart abort local (256 backlog)": budget(20, 9, 11, 16, 10, 1),
  "multipart status first page (256 landed)": budget(8, 5, 4, 270, 5, 2),
  "multipart status first page (4096 landed)": budget(8, 5, 4, 270, 5, 2),
  "multipart status resume page (4096 landed)": budget(8, 5, 4, 270, 5, 2),
  "multipart status sparse page (256 shards)": budget(72, 68, 4, 72, 5, 65),
  "multipart fence GC (256 backlog)": budget(525, 8, 517, 800, 520, 1),
  "multipart fence GC (4096 backlog)": budget(525, 8, 517, 800, 520, 1),
  "cleanup journal GC (256 backlog)": budget(780, 8, 772, 1_300, 780, 1),
  "cleanup journal GC (4096 backlog)": budget(780, 8, 772, 1_300, 780, 1),
  "bounded migration maintenance (256 backlog)": budget(264, 4, 260, 530, 270, 1),
  "bounded migration maintenance (4096 backlog)": budget(264, 4, 260, 530, 270, 1),
  "retention worst step (256 chunks)": budget(228, 14, 214, 240, 115, 1),
  "retention worst step (4096 chunks)": budget(228, 14, 214, 240, 115, 1),
  "retention cursor first page": budget(152, 140, 14, 290, 12, 1),
  "retention cursor late page": budget(142, 136, 8, 280, 10, 1),
  "ShardDO deleteChunksPage (1 refs)": budget(14, 7, 8, 16, 14, 1),
  "ShardDO deleteChunksPage (256 refs)": budget(270, 7, 264, 2_850, 1_050, 1),
  "ShardDO deleteChunksPage (4096 refs)": budget(270, 7, 264, 2_850, 1_050, 1),
  "ShardDO staging page (256 rows)": budget(10, 6, 6, 1_300, 270, 1),
  "ShardDO staging page (4096 rows)": budget(10, 6, 6, 1_300, 270, 1),
} satisfies Record<string, BenchmarkBudget>;

function zeroMetrics(): SqlMetrics {
  return {
    statements: 0,
    reads: 0,
    writes: 0,
    other: 0,
    rowsRead: 0,
    rowsWritten: 0,
  };
}

function addMetrics(left: SqlMetrics, right: SqlMetrics): SqlMetrics {
  return {
    statements: left.statements + right.statements,
    reads: left.reads + right.reads,
    writes: left.writes + right.writes,
    other: left.other + right.other,
    rowsRead: left.rowsRead + right.rowsRead,
    rowsWritten: left.rowsWritten + right.rowsWritten,
  };
}

function metricsOf(result: BenchmarkResult): SqlMetrics {
  return {
    statements: result.statements,
    reads: result.reads,
    writes: result.writes,
    other: result.other,
    rowsRead: result.rowsRead,
    rowsWritten: result.rowsWritten,
  };
}

function expectWithinBudget(
  metrics: SqlMetrics,
  budget: SqlBudget,
  label: string
): void {
  expect(metrics.statements, `${label} statements`).toBeGreaterThan(0);
  expect(metrics.writes, `${label} writes`).toBeGreaterThan(0);
  expect(metrics.other, `${label} unclassified statements`).toBe(0);
  expect(metrics.statements, `${label} statements`).toBeLessThanOrEqual(
    budget.statements
  );
  expect(metrics.reads, `${label} reads`).toBeLessThanOrEqual(budget.reads);
  expect(metrics.writes, `${label} writes`).toBeLessThanOrEqual(budget.writes);
  expect(metrics.rowsRead, `${label} rows read`).toBeLessThanOrEqual(
    budget.rowsRead
  );
  expect(metrics.rowsWritten, `${label} rows written`).toBeLessThanOrEqual(
    budget.rowsWritten
  );
}

function resultFor(
  results: BenchmarkResult[],
  name: string
): BenchmarkResult {
  const result = results.find((candidate) => candidate.name === name);
  if (result === undefined) throw new Error(`missing benchmark result: ${name}`);
  return result;
}

function expectConstantStep(
  results: BenchmarkResult[],
  pageName: string,
  backlogName: string,
  rowsReadTolerance = 0
): void {
  const page = resultFor(results, pageName);
  const backlog = resultFor(results, backlogName);
  expect(
    {
      statements: backlog.statements,
      reads: backlog.reads,
      writes: backlog.writes,
      rowsWritten: backlog.rowsWritten,
      rpcCalls: backlog.rpcCalls,
    },
    `${backlogName} per-step work`
  ).toEqual({
    statements: page.statements,
    reads: page.reads,
    writes: page.writes,
    rowsWritten: page.rowsWritten,
    rpcCalls: page.rpcCalls,
  });
  expect(backlog.rowsRead, `${backlogName} rows read`).toBeLessThanOrEqual(
    page.rowsRead + rowsReadTolerance
  );
}

async function createVfsHarness(name: string): Promise<VfsHarness> {
  const scope = { ns: "benchmark", tenant: `sql-${name}` };
  const user = E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName(scope.ns, scope.tenant))
  );
  const shard = E.MOSSAIC_SHARD.get(
    E.MOSSAIC_SHARD.idFromName(
      vfsShardDOName(scope.ns, scope.tenant, undefined, 0)
    )
  );

  await user.vfsExists(scope, "/");
  return { scope, user, shard };
}

async function prepareSingleShard(harness: VfsHarness): Promise<void> {
  await harness.shard.getStorageBytes();
  await runInDurableObject(
    harness.shard,
    (instance: BenchmarkShardDO): void => {
      instance.sql.exec(
        "INSERT OR IGNORE INTO shard_meta (key, value) VALUES ('capacity_used_bytes', 0)"
      );
    }
  );
  await pinSingleShard(harness.user, harness.scope.tenant);
}

async function pinSingleShard(
  user: DurableObjectStub<BenchmarkUserDO>,
  tenant: string
): Promise<void> {
  await runInDurableObject(user, (instance: BenchmarkUserDO): void => {
    instance.sql.exec(
      `INSERT OR IGNORE INTO quota
         (user_id, storage_used, storage_limit, file_count, pool_size)
       VALUES (?, 0, 107374182400, 0, 1)`,
      tenant
    );
    instance.sql.exec("UPDATE quota SET pool_size = 1 WHERE user_id = ?", tenant);
  });
}

function multipartShards(
  scope: VFSScope,
  count: number
): Array<DurableObjectStub<BenchmarkShardDO>> {
  return Array.from({ length: count }, (_, shardIndex) =>
    E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(
        vfsShardDOName(scope.ns, scope.tenant, scope.sub, shardIndex)
      )
    )
  );
}

async function measure(
  name: string,
  prepare: (suffix: string) => Promise<PreparedBenchmark>
): Promise<BenchmarkResult> {
  const timing = await prepare("timing");
  const started = performance.now();
  await timing.operation();
  const wallMs = performance.now() - started;

  const counted = await prepare("counted");
  const sources =
    counted.user === null
      ? counted.shards
      : [counted.user, ...counted.shards];
  await Promise.all(sources.map((source) => source.benchmarkResetSqlMetrics()));
  await counted.operation();

  const snapshots = await Promise.all(
    sources.map((source) => source.benchmarkMetrics())
  );
  const userMetrics =
    counted.user === null ? zeroMetrics() : snapshots[0]!.sql;
  const shardMetrics = snapshots
    .slice(counted.user === null ? 0 : 1)
    .map((snapshot) => snapshot.sql)
    .reduce(addMetrics, zeroMetrics());
  const userRpcCalls = counted.user === null ? 0 : snapshots[0]!.rpcCalls;
  const shardRpcCalls = snapshots
    .slice(counted.user === null ? 0 : 1)
    .reduce((total, snapshot) => total + snapshot.rpcCalls, 0);
  const total = addMetrics(userMetrics, shardMetrics);
  return {
    name,
    wallMs,
    rpcCalls: userRpcCalls + shardRpcCalls,
    userRpcCalls,
    shardRpcCalls,
    ...total,
    user: userMetrics,
    shards: shardMetrics,
  };
}

async function benchmarkInlineOverwrite(): Promise<BenchmarkResult> {
  return measure("inline overwrite", async (suffix) => {
    const harness = await createVfsHarness(`inline-overwrite-${suffix}`);
    await harness.user.vfsWriteFile(
      harness.scope,
      "/file.txt",
      encoder.encode("before")
    );
    return {
      user: harness.user,
      shards: [],
      operation: () =>
        harness.user.vfsWriteFile(
          harness.scope,
          "/file.txt",
          encoder.encode("after")
        ),
    };
  });
}

async function benchmarkInlineUnlink(): Promise<BenchmarkResult> {
  return measure("inline unlink", async (suffix) => {
    const harness = await createVfsHarness(`inline-unlink-${suffix}`);
    await harness.user.vfsWriteFile(
      harness.scope,
      "/file.txt",
      encoder.encode("remove me")
    );
    return {
      user: harness.user,
      shards: [],
      operation: () => harness.user.vfsUnlink(harness.scope, "/file.txt"),
    };
  });
}

async function benchmarkChunkOverwrite(): Promise<BenchmarkResult> {
  return measure("one-chunk overwrite", async (suffix) => {
    const harness = await createVfsHarness(`chunk-overwrite-${suffix}`);
    await prepareSingleShard(harness);
    await harness.user.vfsWriteFile(
      harness.scope,
      "/file.bin",
      new Uint8Array(INLINE_LIMIT + 1).fill(1)
    );
    await pinSingleShard(harness.user, harness.scope.tenant);
    return {
      user: harness.user,
      shards: [harness.shard],
      operation: () =>
        harness.user.vfsWriteFile(
          harness.scope,
          "/file.bin",
          new Uint8Array(INLINE_LIMIT + 1).fill(2)
        ),
    };
  });
}

async function benchmarkChunkUnlink(): Promise<BenchmarkResult> {
  return measure("one-chunk unlink", async (suffix) => {
    const harness = await createVfsHarness(`chunk-unlink-${suffix}`);
    await prepareSingleShard(harness);
    await harness.user.vfsWriteFile(
      harness.scope,
      "/file.bin",
      new Uint8Array(INLINE_LIMIT + 1).fill(3)
    );
    return {
      user: harness.user,
      shards: [harness.shard],
      operation: () => harness.user.vfsUnlink(harness.scope, "/file.bin"),
    };
  });
}

async function benchmarkRenameOverwrite(): Promise<BenchmarkResult> {
  return measure("one-chunk rename-overwrite", async (suffix) => {
    const harness = await createVfsHarness(`rename-overwrite-${suffix}`);
    await prepareSingleShard(harness);
    await harness.user.vfsWriteFile(
      harness.scope,
      "/source.bin",
      new Uint8Array(INLINE_LIMIT + 1).fill(4)
    );
    await pinSingleShard(harness.user, harness.scope.tenant);
    await harness.user.vfsWriteFile(
      harness.scope,
      "/destination.bin",
      new Uint8Array(INLINE_LIMIT + 1).fill(5)
    );
    return {
      user: harness.user,
      shards: [harness.shard],
      operation: () =>
        harness.user.vfsRename(
          harness.scope,
          "/source.bin",
          "/destination.bin"
        ),
    };
  });
}

interface MultipartBenchmarkFixture extends VfsHarness {
  begin: MultipartBeginResponse;
  bytes: Uint8Array;
  chunkHash: string;
}

async function prepareMultipartBenchmark(
  name: string,
  oldChunks = 0
): Promise<MultipartBenchmarkFixture> {
  const harness = await createVfsHarness(name);
  await prepareSingleShard(harness);
  const path = "/multipart.bin";
  if (oldChunks > 0) {
    await harness.user.vfsWriteFile(harness.scope, path, new Uint8Array([1]));
    await runInDurableObject(harness.user, (instance: BenchmarkUserDO): void => {
      const file = instance.sql
        .exec(
          "SELECT file_id FROM files WHERE user_id = ? AND file_name = 'multipart.bin'",
          harness.scope.tenant
        )
        .toArray()[0] as { file_id: string };
      instance.sql.exec(
        "UPDATE files SET inline_data = NULL, file_size = ?, chunk_count = ? WHERE file_id = ?",
        oldChunks,
        oldChunks,
        file.file_id
      );
      instance.sql.exec(
        `WITH RECURSIVE seq(i) AS (
           VALUES(0) UNION ALL SELECT i + 1 FROM seq WHERE i + 1 < ?
         )
         INSERT INTO file_chunks
           (file_id, chunk_index, chunk_hash, chunk_size, shard_index)
         SELECT ?, i, printf('%064x', i), 1, 0 FROM seq`,
        oldChunks,
        file.file_id
      );
    });
    await pinSingleShard(harness.user, harness.scope.tenant);
  }
  const begin = await harness.user.vfsBeginMultipart(harness.scope, path, {
    size: 1,
    chunkSize: 1,
    protocolVersion: MULTIPART_PROTOCOL_VERSION,
  });
  const bytes = new Uint8Array([7]);
  const chunkHash = await hashChunk(bytes);
  await harness.shard.putChunkMultipart(
    chunkHash,
    bytes,
    begin.uploadId,
    0,
    harness.scope.tenant,
    begin.sessionToken
  );
  await harness.user.vfsStageMultipartHashes(
    harness.scope,
    begin.uploadId,
    0,
    [chunkHash]
  );
  return { ...harness, begin, bytes, chunkHash };
}

async function driveToPublishing(
  fixture: MultipartBenchmarkFixture
): Promise<void> {
  for (let step = 0; step < 100; step++) {
    const progress = await fixture.user.vfsFinalizeMultipartStep(
      fixture.scope,
      fixture.begin.uploadId
    );
    if (!progress.done && progress.phase === "publishing") return;
  }
  throw new Error("multipart benchmark did not reach publishing");
}

async function driveToOldManifestCleanup(
  fixture: MultipartBenchmarkFixture
): Promise<void> {
  await driveToPublishing(fixture);
  const published = await fixture.user.vfsFinalizeMultipartStep(
    fixture.scope,
    fixture.begin.uploadId
  );
  if (published.done) throw new Error("expected multipart cleanup work");
  for (let step = 0; step < 100; step++) {
    const phase = await runInDurableObject(
      fixture.user,
      (instance: BenchmarkUserDO) =>
        (
          instance.sql
            .exec(
              "SELECT finalize_phase FROM upload_sessions WHERE upload_id = ?",
              fixture.begin.uploadId
            )
            .toArray()[0] as { finalize_phase: string }
        ).finalize_phase
    );
    if (phase === "cleaning_old_manifest") return;
    const progress = await fixture.user.vfsFinalizeMultipartStep(
      fixture.scope,
      fixture.begin.uploadId
    );
    if (progress.done) throw new Error("old manifest cleanup was skipped");
  }
  throw new Error("multipart benchmark did not reach old manifest cleanup");
}

async function benchmarkFinalizeFencing(poolSize: number): Promise<BenchmarkResult> {
  return measure(`multipart finalize fencing (${poolSize} shard${poolSize === 1 ? "" : "s"})`, async (suffix) => {
    const fixture = await prepareMultipartBenchmark(`finalize-fencing-${suffix}`);
    await runInDurableObject(fixture.user, (instance: BenchmarkUserDO): void => {
      instance.sql.exec(
        "UPDATE upload_sessions SET pool_size = ? WHERE upload_id = ?",
        poolSize,
        fixture.begin.uploadId
      );
    });
    const shards = multipartShards(fixture.scope, Math.min(poolSize, 64));
    await Promise.all(shards.map((shard) => shard.getStorageBytes()));
    return {
      user: fixture.user,
      shards,
      operation: async () => {
        await fixture.user.vfsFinalizeMultipartStep(
          fixture.scope,
          fixture.begin.uploadId
        );
      },
    };
  });
}

async function benchmarkFinalizeVerifying(poolSize: number): Promise<BenchmarkResult> {
  return measure(`multipart finalize verifying (${poolSize} shard${poolSize === 1 ? "" : "s"})`, async (suffix) => {
    const fixture = await prepareMultipartBenchmark(`finalize-verifying-${suffix}`);
    await fixture.user.vfsFinalizeMultipartStep(
      fixture.scope,
      fixture.begin.uploadId
    );
    await runInDurableObject(fixture.user, (instance: BenchmarkUserDO): void => {
      instance.sql.exec(
        `UPDATE upload_sessions
            SET pool_size = ?, finalize_phase = 'verifying',
                finalize_verify_shard_cursor = 0
          WHERE upload_id = ?`,
        poolSize,
        fixture.begin.uploadId
      );
    });
    const shards = multipartShards(fixture.scope, Math.min(poolSize, 64));
    await Promise.all(shards.map((shard) => shard.getStorageBytes()));
    return {
      user: fixture.user,
      shards,
      operation: async () => {
        await fixture.user.vfsFinalizeMultipartStep(
          fixture.scope,
          fixture.begin.uploadId
        );
      },
    };
  });
}

async function benchmarkFinalizePreparing(oldChunks: number): Promise<BenchmarkResult> {
  return measure(`multipart finalize preparing (${oldChunks} old)`, async (suffix) => {
    const fixture = await prepareMultipartBenchmark(
      `finalize-preparing-${suffix}`,
      oldChunks
    );
    await fixture.user.vfsFinalizeMultipartStep(
      fixture.scope,
      fixture.begin.uploadId
    );
    const progress = await fixture.user.vfsFinalizeMultipartStep(
      fixture.scope,
      fixture.begin.uploadId
    );
    if (progress.done || progress.phase !== "preparing") {
      throw new Error("multipart benchmark did not reach preparing");
    }
    return {
      user: fixture.user,
      shards: [],
      operation: async () => {
        await fixture.user.vfsFinalizeMultipartStep(
          fixture.scope,
          fixture.begin.uploadId
        );
      },
    };
  });
}

async function benchmarkFinalizePublishing(
  oldChunks: number
): Promise<BenchmarkResult> {
  return measure(`multipart finalize publishing (${oldChunks} old)`, async (suffix) => {
    const fixture = await prepareMultipartBenchmark(
      `finalize-publishing-${oldChunks}-${suffix}`,
      oldChunks
    );
    await driveToPublishing(fixture);
    return {
      user: fixture.user,
      shards: [],
      operation: async () => {
        await fixture.user.vfsFinalizeMultipartStep(
          fixture.scope,
          fixture.begin.uploadId
        );
      },
    };
  });
}

async function benchmarkFinalizeCleaning(totalChunks: number): Promise<BenchmarkResult> {
  return measure(`multipart finalize cleaning (${totalChunks} rows)`, async (suffix) => {
    const fixture = await prepareMultipartBenchmark(`finalize-cleaning-${suffix}`);
    await driveToPublishing(fixture);
    await fixture.user.vfsFinalizeMultipartStep(
      fixture.scope,
      fixture.begin.uploadId
    );
    await runInDurableObject(fixture.user, (instance: BenchmarkUserDO): void => {
      instance.sql.exec(
        `WITH RECURSIVE seq(i) AS (
           VALUES(1) UNION ALL SELECT i + 1 FROM seq WHERE i + 1 < ?
         )
         INSERT INTO upload_expected_chunks (upload_id, chunk_index, chunk_hash)
         SELECT ?, i, printf('%064x', i) FROM seq`,
        totalChunks,
        fixture.begin.uploadId
      );
      instance.sql.exec(
        `INSERT INTO upload_verified_chunks
           (upload_id, chunk_index, chunk_hash, chunk_size, shard_index)
         SELECT upload_id, chunk_index, chunk_hash, 1, 0
           FROM upload_expected_chunks
          WHERE upload_id = ? AND chunk_index > 0`,
        fixture.begin.uploadId
      );
      instance.sql.exec(
        `UPDATE upload_sessions
            SET total_chunks = ?, finalize_phase = 'cleaning',
                finalize_cleanup_cursor = 0
          WHERE upload_id = ?`,
        totalChunks,
        fixture.begin.uploadId
      );
    });
    return {
      user: fixture.user,
      shards: [],
      operation: async () => {
        await fixture.user.vfsFinalizeMultipartStep(
          fixture.scope,
          fixture.begin.uploadId
        );
      },
    };
  });
}

async function benchmarkFinalizeOldManifestCleanup(
  oldChunks: number
): Promise<BenchmarkResult> {
  return measure(`multipart finalize old cleanup (${oldChunks} old)`, async (suffix) => {
    const fixture = await prepareMultipartBenchmark(
      `finalize-old-cleanup-${suffix}`,
      oldChunks
    );
    await driveToOldManifestCleanup(fixture);
    return {
      user: fixture.user,
      shards: [],
      operation: async () => {
        await fixture.user.vfsFinalizeMultipartStep(
          fixture.scope,
          fixture.begin.uploadId
        );
      },
    };
  });
}

async function benchmarkStagedHashPage(priorRows: number): Promise<BenchmarkResult> {
  return measure(`multipart hash page (${priorRows} prior)`, async (suffix) => {
    const harness = await createVfsHarness(`hash-page-${priorRows}-${suffix}`);
    await prepareSingleShard(harness);
    const pageSize = 256;
    const priorPages = priorRows / pageSize;
    const begin = await harness.user.vfsBeginMultipart(harness.scope, "/hashes.bin", {
      size: (priorPages + 1) * pageSize,
      chunkSize: 1,
      protocolVersion: MULTIPART_PROTOCOL_VERSION,
    });
    for (let page = 0; page < priorPages; page++) {
      const start = page * pageSize;
      await harness.user.vfsStageMultipartHashes(
        harness.scope,
        begin.uploadId,
        start,
        Array.from({ length: pageSize }, (_, offset) =>
          (start + offset).toString(16).padStart(64, "0")
        )
      );
    }
    const start = priorPages * pageSize;
    const hashes = Array.from({ length: pageSize }, (_, offset) =>
      (start + offset).toString(16).padStart(64, "0")
    );
    return {
      user: harness.user,
      shards: [],
      operation: async () => {
        await harness.user.vfsStageMultipartHashes(
          harness.scope,
          begin.uploadId,
          start,
          hashes
        );
      },
    };
  });
}

async function benchmarkMultipartPut(replay: boolean): Promise<BenchmarkResult> {
  return measure(`multipart PUT (${replay ? "replay" : "first"})`, async (suffix) => {
    const fixture = await prepareMultipartBenchmark(`put-${replay}-${suffix}`);
    if (!replay) {
      await runInDurableObject(fixture.shard, (instance: BenchmarkShardDO): void => {
        instance.sql.exec("DELETE FROM upload_chunks WHERE upload_id = ?", fixture.begin.uploadId);
        instance.sql.exec(
          "DELETE FROM chunk_refs WHERE file_id = ?",
          fixture.begin.uploadId
        );
        instance.sql.exec("DELETE FROM chunks WHERE hash = ?", fixture.chunkHash);
        instance.sql.exec(
          "DELETE FROM multipart_fences WHERE upload_id = ?",
          fixture.begin.uploadId
        );
      });
    }
    return {
      user: null,
      shards: [fixture.shard],
      operation: async () => {
        await fixture.shard.putChunkMultipart(
          fixture.chunkHash,
          fixture.bytes,
          fixture.begin.uploadId,
          0,
          fixture.scope.tenant,
          fixture.begin.sessionToken
        );
      },
    };
  });
}

async function benchmarkMultipartAbortPage(poolSize: number): Promise<BenchmarkResult> {
  return measure(`multipart abort fencing (${poolSize} shards)`, async (suffix) => {
    const harness = await createVfsHarness(`abort-page-${suffix}`);
    await prepareSingleShard(harness);
    const begin = await harness.user.vfsBeginMultipart(harness.scope, "/abort.bin", {
      size: 1,
      chunkSize: 1,
      protocolVersion: MULTIPART_PROTOCOL_VERSION,
    });
    await runInDurableObject(harness.user, (instance: BenchmarkUserDO): void => {
      instance.sql.exec(
        "UPDATE upload_sessions SET pool_size = ? WHERE upload_id = ?",
        poolSize,
        begin.uploadId
      );
    });
    const shards = multipartShards(harness.scope, Math.min(poolSize, 64));
    await Promise.all(shards.map((shard) => shard.getStorageBytes()));
    return {
      user: harness.user,
      shards,
      operation: async () => {
        await harness.user.vfsAbortMultipartStep(harness.scope, begin.uploadId);
      },
    };
  });
}

type MultipartAbortBenchmarkPhase = "intents" | "cleanup" | "old_intents" | "local";

async function benchmarkMultipartAbortPhase(
  phase: MultipartAbortBenchmarkPhase,
  backlog: number
): Promise<BenchmarkResult> {
  return measure(`multipart abort ${phase} (${backlog} backlog)`, async (suffix) => {
    const fixture = await prepareMultipartBenchmark(
      `abort-${phase}-${backlog}-${suffix}`,
      phase === "old_intents" ? 1 : 0
    );
    await fixture.user.vfsFinalizeMultipartStep(
      fixture.scope,
      fixture.begin.uploadId
    );
    await runInDurableObject(fixture.user, (instance: BenchmarkUserDO): void => {
      if (phase === "cleanup") {
        instance.sql.exec(
          `WITH RECURSIVE seq(i) AS (
             VALUES(1) UNION ALL SELECT i + 1 FROM seq WHERE i + 1 < ?
           )
           INSERT INTO upload_expected_chunks (upload_id, chunk_index, chunk_hash)
           SELECT ?, i, printf('%064x', i) FROM seq`,
          backlog,
          fixture.begin.uploadId
        );
        instance.sql.exec(
          `INSERT INTO upload_verified_chunks
             (upload_id, chunk_index, chunk_hash, chunk_size, shard_index)
           SELECT upload_id, chunk_index, chunk_hash, 1, 0
             FROM upload_expected_chunks
            WHERE upload_id = ? AND chunk_index > 0`,
          fixture.begin.uploadId
        );
      }
      if (phase === "old_intents") {
        instance.sql.exec(
          `WITH RECURSIVE seq(i) AS (
             VALUES(0) UNION ALL SELECT i + 1 FROM seq WHERE i + 1 < ?
           )
           INSERT OR IGNORE INTO upload_cleanup_routes
             (upload_id, cleanup_kind, shard_index)
           SELECT ?, 'chunks', i FROM seq`,
          backlog,
          fixture.begin.uploadId
        );
      }
      instance.sql.exec(
        `UPDATE upload_sessions
            SET status = 'aborting', abort_phase = ?, pool_size = ?,
                total_chunks = ?, abort_fence_cursor = 0,
                abort_intent_cursor = 0, abort_cleanup_cursor = 0,
                abort_old_intent_cursor = -1
          WHERE upload_id = ?`,
        phase,
        backlog,
        phase === "cleanup" ? backlog : 1,
        fixture.begin.uploadId
      );
    });
    const shards =
      phase === "intents"
        ? multipartShards(fixture.scope, Math.min(backlog, 64))
        : [];
    await Promise.all(shards.map((shard) => shard.getStorageBytes()));
    return {
      user: fixture.user,
      shards,
      operation: async () => {
        await fixture.user.vfsAbortMultipartStep(
          fixture.scope,
          fixture.begin.uploadId
        );
      },
    };
  });
}

async function benchmarkMultipartStatusDensePage(
  backlog: number,
  resume: boolean
): Promise<BenchmarkResult> {
  return measure(
    `multipart status ${resume ? "resume" : "first"} page (${backlog} landed)`,
    async (suffix) => {
    const harness = await createVfsHarness(`status-dense-${suffix}`);
    await prepareSingleShard(harness);
    const begin = await harness.user.vfsBeginMultipart(
      harness.scope,
      "/status-dense.bin",
      {
        size: backlog,
        chunkSize: 1,
        protocolVersion: MULTIPART_PROTOCOL_VERSION,
      }
    );
    await runInDurableObject(harness.shard, (instance: BenchmarkShardDO): void => {
      instance.sql.exec(
        `WITH RECURSIVE seq(i) AS (
           VALUES(0) UNION ALL SELECT i + 1 FROM seq WHERE i + 1 < ?
         )
         INSERT INTO upload_chunks
           (upload_id, chunk_index, chunk_hash, chunk_size, user_id, created_at)
         SELECT ?, i, printf('%064x', i), 1, ?, ? FROM seq`,
        backlog,
        begin.uploadId,
        harness.scope.tenant,
        Date.now()
      );
    });
    const first = resume
      ? await harness.user.vfsGetMultipartStatus(harness.scope, begin.uploadId)
      : undefined;
    return {
      user: harness.user,
      shards: [harness.shard],
      operation: async () => {
        const page = await harness.user.vfsGetMultipartStatus(
          harness.scope,
          begin.uploadId,
          first?.continuation
        );
        expect(page.landed).toHaveLength(256);
        expect(page.continuation).toEqual(expect.any(String));
      },
    };
    }
  );
}

async function benchmarkMultipartStatusSparsePage(poolSize: number): Promise<BenchmarkResult> {
  return measure(`multipart status sparse page (${poolSize} shards)`, async (suffix) => {
    const harness = await createVfsHarness(`status-sparse-${suffix}`);
    const begin = await harness.user.vfsBeginMultipart(
      harness.scope,
      "/status-sparse.bin",
      {
        size: 0,
        chunkSize: 1,
        protocolVersion: MULTIPART_PROTOCOL_VERSION,
      }
    );
    await runInDurableObject(harness.user, (instance: BenchmarkUserDO): void => {
      instance.sql.exec(
        "UPDATE upload_sessions SET pool_size = ? WHERE upload_id = ?",
        poolSize,
        begin.uploadId
      );
    });
    const shards = multipartShards(harness.scope, 64);
    await Promise.all(shards.map((shard) => shard.getStorageBytes()));
    return {
      user: harness.user,
      shards,
      operation: async () => {
        const page = await harness.user.vfsGetMultipartStatus(
          harness.scope,
          begin.uploadId
        );
        expect(page.landed).toEqual([]);
        expect(page.continuation).toEqual(expect.any(String));
      },
    };
  });
}

async function benchmarkFenceGc(backlog: number): Promise<BenchmarkResult> {
  return measure(`multipart fence GC (${backlog} backlog)`, async (suffix) => {
    const shard = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(`fence-gc-${backlog}-${suffix}`)
    );
    await shard.getStorageBytes();
    await runInDurableObject(shard, (instance: BenchmarkShardDO): void => {
      const expiredAt = Date.now() - 2 * 60 * 60 * 1000;
      instance.sql.exec(
        `WITH RECURSIVE seq(i) AS (
           VALUES(0) UNION ALL SELECT i + 1 FROM seq WHERE i + 1 < ?
         )
         INSERT INTO multipart_fences
           (upload_id, fence_id, state, updated_at, expires_at)
         SELECT 'expired-fence-' || i, 'fence-' || i, 'finalizing', ?, ? FROM seq`,
        backlog,
        expiredAt,
        expiredAt
      );
    });
    return {
      user: null,
      shards: [shard],
      operation: () => shard.benchmarkRunAlarm(),
    };
  });
}

async function benchmarkCleanupJournalGc(backlog: number): Promise<BenchmarkResult> {
  return measure(`cleanup journal GC (${backlog} backlog)`, async (suffix) => {
    const shard = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(`journal-gc-${backlog}-${suffix}`)
    );
    await shard.getStorageBytes();
    await runInDurableObject(shard, (instance: BenchmarkShardDO): void => {
      const expiredAt = Date.now() - 1;
      instance.sql.exec(
        `WITH RECURSIVE seq(i) AS (
           VALUES(0) UNION ALL SELECT i + 1 FROM seq WHERE i + 1 < ?
         )
         INSERT INTO shard_cleanup_progress
           (cleanup_kind, ref_id, cleanup_generation, next_cursor, done, updated_at)
         SELECT 'refs', 'journal-ref-' || i, 'generation-' || i, 1, 1, ? FROM seq`,
        backlog,
        expiredAt
      );
      instance.sql.exec(
        `INSERT INTO shard_cleanup_pages
           (cleanup_kind, ref_id, cleanup_generation, request_cursor,
            next_cursor, processed, marked, done, created_at)
         SELECT cleanup_kind, ref_id, cleanup_generation, 0, 1, 1, 1, 1, ?
           FROM shard_cleanup_progress`,
        expiredAt
      );
      instance.sql.exec(
        `INSERT INTO shard_cleanup_page_expirations
           (expires_at, cleanup_kind, ref_id, cleanup_generation, request_cursor)
         SELECT ?, cleanup_kind, ref_id, cleanup_generation, 0
           FROM shard_cleanup_progress`,
        expiredAt
      );
    });
    return {
      user: null,
      shards: [shard],
      operation: () => shard.benchmarkRunAlarm(),
    };
  });
}

async function benchmarkMigrationMaintenance(
  backlog: number
): Promise<BenchmarkResult> {
  return measure(`bounded migration maintenance (${backlog} backlog)`, async (suffix) => {
    const harness = await createVfsHarness(`migration-${backlog}-${suffix}`);
    await runInDurableObject(harness.user, (instance: BenchmarkUserDO): void => {
      instance.sql.exec("DROP TRIGGER version_retention_order_insert");
      instance.sql.exec("DROP TRIGGER version_retention_order_update");
      instance.sql.exec("DROP TRIGGER version_retention_order_delete");
      instance.sql.exec(
        "DELETE FROM schema_maintenance WHERE name = 'version_retention_order_v1'"
      );
      instance.sql.exec(
        `WITH RECURSIVE seq(i) AS (
           VALUES(0) UNION ALL SELECT i + 1 FROM seq WHERE i + 1 < ?
         )
         INSERT INTO file_versions
           (path_id, version_id, user_id, size, mode, mtime_ms, deleted,
            inline_data, chunk_size, chunk_count, file_hash, mime_type,
            user_visible)
         SELECT 'migration-path', 'migration-' || i, ?, 0, 420, i, 0, NULL,
                0, 0, '', 'application/octet-stream', 1 FROM seq`,
        backlog,
        harness.scope.tenant
      );
    });
    return {
      user: harness.user,
      shards: [],
      operation: async () => {
        await harness.user.benchmarkMaintainVersionRetentionOrder();
      },
    };
  });
}

async function benchmarkRetentionWorstStep(
  chunkCount: number
): Promise<BenchmarkResult> {
  return measure(`retention worst step (${chunkCount} chunks)`, async (suffix) => {
    const harness = await createVfsHarness(`retention-${chunkCount}-${suffix}`);
    await harness.user.adminSetVersioning(harness.scope.tenant, true);
    await harness.user.vfsWriteFile(
      harness.scope,
      "/history.txt",
      encoder.encode("head")
    );
    await runInDurableObject(harness.user, (instance: BenchmarkUserDO): void => {
      const file = instance.sql
        .exec(
          "SELECT file_id, updated_at FROM files WHERE user_id = ? AND file_name = 'history.txt'",
          harness.scope.tenant
        )
        .toArray()[0] as { file_id: string; updated_at: number };
      const versionId = `old-${chunkCount}`;
      instance.sql.exec(
        `INSERT INTO file_versions
           (path_id, version_id, user_id, size, mode, mtime_ms, deleted,
            inline_data, chunk_size, chunk_count, file_hash, mime_type,
            user_visible, shard_ref_id)
         VALUES (?, ?, ?, ?, 420, ?, 0, NULL, 1, ?, '',
                 'application/octet-stream', 1, ?)`,
        file.file_id,
        versionId,
        harness.scope.tenant,
        chunkCount,
        file.updated_at - 1,
        chunkCount,
        `retention-ref-${chunkCount}`
      );
      instance.sql.exec(
        `WITH RECURSIVE seq(i) AS (
           VALUES(0) UNION ALL SELECT i + 1 FROM seq WHERE i + 1 < ?
         )
         INSERT INTO version_chunks
           (version_id, chunk_index, chunk_hash, chunk_size, shard_index)
         SELECT ?, i, printf('%064x', i), 1, i % 32 FROM seq`,
        chunkCount,
        versionId
      );
    });
    return {
      user: harness.user,
      shards: [],
      operation: async () => {
        await harness.user.vfsDropVersionsStep(
          harness.scope,
          "/history.txt",
          {},
          `retention-operation-${chunkCount}`
        );
      },
    };
  });
}

async function benchmarkRetentionCursorDepth(): Promise<RetentionCursorBenchmark> {
  const harness = await createVfsHarness("retention-cursor-depth");
  await harness.user.adminSetVersioning(harness.scope.tenant, true);
  await harness.user.vfsWriteFile(
    harness.scope,
    "/cursor.txt",
    encoder.encode("head")
  );
  const historySize = 1_280;
  const sharedMtime = await runInDurableObject(
    harness.user,
    (instance: BenchmarkUserDO): number => {
      const file = instance.sql
        .exec(
          "SELECT file_id, updated_at FROM files WHERE user_id = ? AND file_name = 'cursor.txt'",
          harness.scope.tenant
        )
        .toArray()[0] as { file_id: string; updated_at: number };
      const mtime = file.updated_at - 1;
      instance.sql.exec(
        `WITH RECURSIVE seq(i) AS (
           VALUES(0) UNION ALL SELECT i + 1 FROM seq WHERE i + 1 < ?
         )
         INSERT INTO file_versions
           (path_id, version_id, user_id, size, mode, mtime_ms, deleted,
            inline_data, chunk_size, chunk_count, file_hash, mime_type,
            user_visible)
         SELECT ?, printf('cursor-%06d', i), ?, 0, 420, ?, 0, NULL, 0, 0, '',
                'text/plain', 1 FROM seq`,
        historySize,
        file.file_id,
        harness.scope.tenant,
        mtime
      );
      return mtime;
    }
  );
  const plan = await runInDurableObject(
    harness.user,
    (instance: BenchmarkUserDO): string[] =>
      (
        instance.sql
          .exec(
             `EXPLAIN QUERY PLAN
              SELECT version_id, mtime_ms FROM version_retention_order
               WHERE path_id = ?
                 AND (mtime_ms, version_id) < (?, ?)
               ORDER BY mtime_ms DESC, version_id DESC LIMIT 1`,
             "unused-path",
             sharedMtime,
            "cursor-000257"
          )
          .toArray() as { detail: string }[]
      ).map((row) => row.detail)
  );
  const operationId = "retention-cursor-depth-operation";
  const policy = { olderThan: 0 };

  await harness.user.benchmarkResetSqlMetrics();
  let started = performance.now();
  const firstStep = await harness.user.vfsDropVersionsStep(
    harness.scope,
    "/cursor.txt",
    policy,
    operationId
  );
  const firstWallMs = performance.now() - started;
  if (firstStep.done) throw new Error("deep retention completed on its first page");
  const firstMetrics = (await harness.user.benchmarkMetrics()).sql;

  for (let page = 1; page < 8; page++) {
    const step = await harness.user.vfsDropVersionsStep(
      harness.scope,
      "/cursor.txt",
      policy,
      operationId
    );
    if (step.done) throw new Error("deep retention completed before its late page");
  }
  const cursorBeforeLate = await runInDurableObject(
    harness.user,
    (instance: BenchmarkUserDO) =>
      instance.sql
        .exec(
          `SELECT cursor_mtime_ms, cursor_version_id
             FROM version_retention_operations WHERE operation_id = ?`,
          operationId
        )
        .toArray()[0] as {
        cursor_mtime_ms: number;
        cursor_version_id: string;
      }
  );

  await harness.user.benchmarkResetSqlMetrics();
  started = performance.now();
  const lateStep = await harness.user.vfsDropVersionsStep(
    harness.scope,
    "/cursor.txt",
    policy,
    operationId
  );
  const lateWallMs = performance.now() - started;
  if (lateStep.done) throw new Error("deep retention completed on its late page");
  const lateMetrics = (await harness.user.benchmarkMetrics()).sql;
  const cursorAfterLate = await runInDurableObject(
    harness.user,
    (instance: BenchmarkUserDO) =>
      instance.sql
        .exec(
          `SELECT cursor_mtime_ms, cursor_version_id
             FROM version_retention_operations WHERE operation_id = ?`,
          operationId
        )
        .toArray()[0] as {
        cursor_mtime_ms: number;
        cursor_version_id: string;
      }
  );

  expect(cursorBeforeLate).toEqual({
    cursor_mtime_ms: sharedMtime,
    cursor_version_id: "cursor-000257",
  });
  expect(cursorAfterLate).toEqual({
    cursor_mtime_ms: sharedMtime,
    cursor_version_id: "cursor-000129",
  });

  let final: DropVersionsStepResult = lateStep;
  while (!final.done) {
    final = await harness.user.vfsDropVersionsStep(
      harness.scope,
      "/cursor.txt",
      policy,
      operationId
    );
  }
  expect(final).toEqual({ done: true, dropped: 0, kept: historySize + 1 });

  const asResult = (
    name: string,
    wallMs: number,
    user: SqlMetrics
  ): BenchmarkResult => ({
    name,
    wallMs,
    ...user,
    rpcCalls: 1,
    userRpcCalls: 1,
    shardRpcCalls: 0,
    user,
    shards: zeroMetrics(),
  });
  return {
    first: asResult("retention cursor first page", firstWallMs, firstMetrics),
    late: asResult("retention cursor late page", lateWallMs, lateMetrics),
    plan,
  };
}

async function benchmarkShardRefDeletion(refs: number): Promise<BenchmarkResult> {
  return measure(`ShardDO deleteChunksPage (${refs} refs)`, async (suffix) => {
    const shard = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(`sql-ref-delete-${refs}-${suffix}`)
    );
    const fileId = `file-${refs}`;
    await shard.getStorageBytes();
    await runInDurableObject(shard, (instance: BenchmarkShardDO, state): void => {
      state.storage.transactionSync(() => {
        instance.sql.exec(
          `WITH RECURSIVE seq(i) AS (
             VALUES(0) UNION ALL SELECT i + 1 FROM seq WHERE i + 1 < ?
           )
           INSERT INTO chunks (hash, data, size, ref_count, created_at)
           SELECT printf('%064x', i + 1), X'00', 1, 1, ? FROM seq`,
          refs,
          Date.now()
        );
        instance.sql.exec(
          `WITH RECURSIVE seq(i) AS (
             VALUES(0) UNION ALL SELECT i + 1 FROM seq WHERE i + 1 < ?
           )
           INSERT INTO chunk_refs (chunk_hash, file_id, chunk_index, user_id)
           SELECT printf('%064x', i + 1), ?, i, 'benchmark' FROM seq`,
          refs,
          fileId
        );
      });
    });
    return {
      user: null,
      shards: [shard],
      operation: async () => {
        await shard.deleteChunksPage(fileId, 0, `benchmark-${suffix}`);
      },
    };
  });
}

async function benchmarkShardStagingDeletion(
  rows: number
): Promise<BenchmarkResult> {
  return measure(`ShardDO staging page (${rows} rows)`, async (suffix) => {
    const shard = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(`sql-staging-delete-${rows}-${suffix}`)
    );
    const uploadId = `upload-${rows}`;
    await shard.getStorageBytes();
    await runInDurableObject(shard, (instance: BenchmarkShardDO, state): void => {
      state.storage.transactionSync(() => {
        instance.sql.exec(
          `WITH RECURSIVE seq(i) AS (
             VALUES(0) UNION ALL SELECT i + 1 FROM seq WHERE i + 1 < ?
           )
           INSERT INTO upload_chunks
             (upload_id, chunk_index, chunk_hash, chunk_size, user_id, created_at)
           SELECT ?, i, printf('%064x', i + 1), 1, 'benchmark', ? FROM seq`,
          rows,
          uploadId,
          Date.now()
        );
      });
    });
    return {
      user: null,
      shards: [shard],
      operation: async () => {
        await shard.clearMultipartStagingPage(
          uploadId,
          0,
          `benchmark-${suffix}`
        );
      },
    };
  });
}

function report(results: BenchmarkResult[]): void {
  const header = [
    "operation".padEnd(36),
    "wall ms".padStart(10),
    "statements".padStart(12),
    "reads".padStart(8),
    "writes".padStart(8),
    "rows read".padStart(12),
    "rows written".padStart(15),
    "RPCs".padStart(8),
  ].join("");
  const lines = [
    "Mossaic real Durable Object SqlStorage benchmark",
    "Wall time is informational; SQL counters are the deterministic signal.",
    header,
    "-".repeat(header.length),
    ...results.map((result) =>
      [
        result.name.padEnd(36),
        result.wallMs.toFixed(3).padStart(10),
        String(result.statements).padStart(12),
        String(result.reads).padStart(8),
        String(result.writes).padStart(8),
        String(result.rowsRead).padStart(12),
        String(result.rowsWritten).padStart(15),
        String(result.rpcCalls).padStart(8),
      ].join("")
    ),
    `MOSSAIC_SQL_BENCHMARK=${JSON.stringify({
      backend: "durable-object-sqlstorage",
      results,
    })}`,
  ];
  console.log(`\n${lines.join("\n")}\n`);
}

it("enforces component SQL budgets on real Durable Objects", async () => {
  const retentionCursor = await benchmarkRetentionCursorDepth();
  const results = [
    await benchmarkInlineOverwrite(),
    await benchmarkInlineUnlink(),
    await benchmarkChunkOverwrite(),
    await benchmarkChunkUnlink(),
    await benchmarkRenameOverwrite(),
    await benchmarkFinalizeFencing(1),
    await benchmarkFinalizeVerifying(1),
    await benchmarkFinalizePreparing(256),
    await benchmarkFinalizePreparing(4_096),
    await benchmarkFinalizePublishing(256),
    await benchmarkFinalizePublishing(4_096),
    await benchmarkFinalizeCleaning(256),
    await benchmarkFinalizeCleaning(4_096),
    await benchmarkFinalizeOldManifestCleanup(256),
    await benchmarkFinalizeOldManifestCleanup(4_096),
    await benchmarkStagedHashPage(256),
    await benchmarkStagedHashPage(4_096),
    await benchmarkMultipartPut(false),
    await benchmarkMultipartPut(true),
    await benchmarkMultipartAbortPage(64),
    await benchmarkMultipartAbortPhase("intents", 1),
    await benchmarkMultipartAbortPhase("cleanup", 256),
    await benchmarkMultipartAbortPhase("cleanup", 4_096),
    await benchmarkMultipartAbortPhase("old_intents", 256),
    await benchmarkMultipartAbortPhase("old_intents", 4_096),
    await benchmarkMultipartAbortPhase("local", 256),
    await benchmarkMultipartStatusDensePage(256, false),
    await benchmarkMultipartStatusDensePage(4_096, false),
    await benchmarkMultipartStatusDensePage(4_096, true),
    await benchmarkMultipartStatusSparsePage(256),
    await benchmarkFenceGc(256),
    await benchmarkFenceGc(4_096),
    await benchmarkCleanupJournalGc(256),
    await benchmarkCleanupJournalGc(4_096),
    await benchmarkMigrationMaintenance(256),
    await benchmarkMigrationMaintenance(4_096),
    await benchmarkRetentionWorstStep(256),
    await benchmarkRetentionWorstStep(4_096),
    retentionCursor.first,
    retentionCursor.late,
    await benchmarkShardRefDeletion(1),
    await benchmarkShardRefDeletion(256),
    await benchmarkShardRefDeletion(4_096),
    await benchmarkShardStagingDeletion(256),
    await benchmarkShardStagingDeletion(4_096),
  ];

  report(results);

  expect(Object.keys(BENCHMARK_BUDGETS)).toHaveLength(results.length);
  for (const [name, budget] of Object.entries(BENCHMARK_BUDGETS)) {
    const result = resultFor(results, name);
    expectWithinBudget(metricsOf(result), budget, name);
    expect(result.rpcCalls, `${name} RPCs`).toBeLessThanOrEqual(budget.rpcCalls);
  }

  for (const [name, budget] of Object.entries(VFS_COMPONENT_BUDGETS)) {
    const result = resultFor(results, name);
    expect(metricsOf(result), `${name} aggregate`).toEqual(
      addMetrics(result.user, result.shards)
    );
    expectWithinBudget(result.user, budget.user, `${name} UserDO`);
    if (budget.shards === null) {
      expect(result.shards, `${name} must stay inline`).toEqual(zeroMetrics());
    } else {
      expectWithinBudget(result.shards, budget.shards, `${name} ShardDO`);
    }
  }

  for (const [pageName, backlogName, rowsReadTolerance] of [
    ["multipart finalize preparing (256 old)", "multipart finalize preparing (4096 old)", 0],
    ["multipart finalize publishing (256 old)", "multipart finalize publishing (4096 old)", 0],
    ["multipart finalize cleaning (256 rows)", "multipart finalize cleaning (4096 rows)", 2],
    ["multipart finalize old cleanup (256 old)", "multipart finalize old cleanup (4096 old)", 0],
    ["multipart hash page (256 prior)", "multipart hash page (4096 prior)", 0],
    ["multipart abort cleanup (256 backlog)", "multipart abort cleanup (4096 backlog)", 2],
    ["multipart abort old_intents (256 backlog)", "multipart abort old_intents (4096 backlog)", 0],
    ["multipart status first page (256 landed)", "multipart status first page (4096 landed)", 0],
    ["multipart fence GC (256 backlog)", "multipart fence GC (4096 backlog)", 0],
    ["cleanup journal GC (256 backlog)", "cleanup journal GC (4096 backlog)", 0],
    ["bounded migration maintenance (256 backlog)", "bounded migration maintenance (4096 backlog)", 0],
    ["retention worst step (256 chunks)", "retention worst step (4096 chunks)", 0],
    ["ShardDO deleteChunksPage (256 refs)", "ShardDO deleteChunksPage (4096 refs)", 0],
    ["ShardDO staging page (256 rows)", "ShardDO staging page (4096 rows)", 0],
  ] as const) {
    expectConstantStep(results, pageName, backlogName, rowsReadTolerance);
  }

  expect(metricsOf(resultFor(results, "multipart status resume page (4096 landed)"))).toEqual(
    metricsOf(resultFor(results, "multipart status first page (4096 landed)"))
  );
  expect(resultFor(results, "multipart status resume page (4096 landed)").rpcCalls).toBe(
    resultFor(results, "multipart status first page (4096 landed)").rpcCalls
  );

  const retentionCursorPlan = retentionCursor.plan.join("\n");
  expect(retentionCursorPlan).toContain("version_retention_order");
  expect(retentionCursorPlan).toContain("(mtime_ms,version_id)<(?,?)");
  expect(retentionCursor.late.user.rowsRead).toBeLessThanOrEqual(
    retentionCursor.first.user.rowsRead
  );
  expect(retentionCursor.late.user.rowsRead).toBeLessThan(1_000);
});
