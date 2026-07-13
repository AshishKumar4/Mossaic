import { env, runInDurableObject } from "cloudflare:test";
import { expect, it } from "vitest";
import { INLINE_LIMIT } from "@shared/inline";
import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import type { VFSScope } from "@shared/vfs-types";
import type { SqlMetrics } from "./counting-sql-storage";
import type { BenchmarkShardDO, BenchmarkUserDO } from "./test-worker";

interface BenchmarkEnv {
  MOSSAIC_USER: DurableObjectNamespace<BenchmarkUserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<BenchmarkShardDO>;
}

interface MetricsStub {
  benchmarkResetSqlMetrics(): Promise<void>;
  benchmarkSqlMetrics(): Promise<SqlMetrics>;
}

interface VfsHarness {
  scope: VFSScope;
  user: DurableObjectStub<BenchmarkUserDO>;
  shard: DurableObjectStub<BenchmarkShardDO>;
}

interface BenchmarkResult extends SqlMetrics {
  name: string;
  wallMs: number;
  user: SqlMetrics;
  shards: SqlMetrics;
}

interface PreparedBenchmark {
  user: MetricsStub | null;
  shards: MetricsStub[];
  operation: () => Promise<void>;
}

const E = env as unknown as BenchmarkEnv;
const encoder = new TextEncoder();

type SqlBudget = Omit<SqlMetrics, "other">;

interface ComponentBudget {
  user: SqlBudget;
  shards: SqlBudget | null;
}

const VFS_COMPONENT_BUDGETS = {
  "inline overwrite": {
    user: {
      statements: 45,
      reads: 22,
      writes: 25,
      rowsRead: 32,
      rowsWritten: 25,
    },
    shards: null,
  },
  "inline unlink": {
    user: {
      statements: 28,
      reads: 12,
      writes: 18,
      rowsRead: 22,
      rowsWritten: 15,
    },
    shards: null,
  },
  "one-chunk overwrite": {
    user: {
      statements: 58,
      reads: 27,
      writes: 33,
      rowsRead: 48,
      rowsWritten: 40,
    },
    shards: {
      statements: 12,
      reads: 4,
      writes: 8,
      rowsRead: 24,
      rowsWritten: 14,
    },
  },
  "one-chunk unlink": {
    user: {
      statements: 33,
      reads: 14,
      writes: 21,
      rowsRead: 30,
      rowsWritten: 22,
    },
    shards: {
      statements: 6,
      reads: 2,
      writes: 4,
      rowsRead: 15,
      rowsWritten: 6,
    },
  },
  "one-chunk rename-overwrite": {
    user: {
      statements: 38,
      reads: 17,
      writes: 23,
      rowsRead: 35,
      rowsWritten: 28,
    },
    shards: {
      statements: 6,
      reads: 2,
      writes: 4,
      rowsRead: 16,
      rowsWritten: 6,
    },
  },
} satisfies Record<string, ComponentBudget>;

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
    sources.map((source) => source.benchmarkSqlMetrics())
  );
  const userMetrics = counted.user === null ? zeroMetrics() : snapshots[0];
  const shardMetrics = snapshots
    .slice(counted.user === null ? 0 : 1)
    .reduce(addMetrics, zeroMetrics());
  const total = addMetrics(userMetrics, shardMetrics);
  return { name, wallMs, ...total, user: userMetrics, shards: shardMetrics };
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

async function benchmarkShardRefDeletion(refs: number): Promise<BenchmarkResult> {
  return measure(`ShardDO deleteChunks (${refs} refs)`, async (suffix) => {
    const shard = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(`sql-ref-delete-${refs}-${suffix}`)
    );
    const fileId = `file-${refs}`;
    await shard.getStorageBytes();
    for (let index = 0; index < refs; index++) {
      await shard.putChunk(
        index.toString(16).padStart(64, "0"),
        new Uint8Array([index % 256]),
        fileId,
        index,
        "benchmark"
      );
    }
    return {
      user: null,
      shards: [shard],
      operation: async () => {
        await shard.deleteChunks(fileId);
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
  const results = [
    await benchmarkInlineOverwrite(),
    await benchmarkInlineUnlink(),
    await benchmarkChunkOverwrite(),
    await benchmarkChunkUnlink(),
    await benchmarkRenameOverwrite(),
    await benchmarkShardRefDeletion(1),
    await benchmarkShardRefDeletion(16),
    await benchmarkShardRefDeletion(64),
  ];

  report(results);

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

  const deleteStatementCounts: number[] = [];
  for (const refs of [1, 16, 64]) {
    const result = resultFor(
      results,
      `ShardDO deleteChunks (${refs} refs)`
    );
    deleteStatementCounts.push(result.statements);
    expect(result.statements).toBe(4);
    expect(result.reads).toBe(1);
    expect(result.writes).toBe(3);
    expect(result.other).toBe(0);
    expect(result.rowsRead).toBeGreaterThanOrEqual(12 * refs);
    expect(result.rowsRead).toBeLessThanOrEqual(13 * refs);
    expect(result.rowsWritten).toBe(4 * refs);
  }
  expect(deleteStatementCounts).toEqual([4, 4, 4]);
});
