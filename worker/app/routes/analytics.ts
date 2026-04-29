import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import type { ShardStats, AnalyticsOverview } from "../types";
import { authMiddleware } from "@core/lib/auth";
import { legacyAppPlacement } from "@shared/placement";
import { userStub } from "../lib/user-stub";

const analytics = new Hono<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>();

analytics.use("*", authMiddleware());

/**
 * GET /api/analytics/overview
 * Returns aggregated analytics: user stats + per-shard breakdown +
 * totals.
 *
 * Phase 17: typed RPCs `appGetUserStats` + `appGetQuota` replace the
 * legacy fetch indirection. ShardDO `/stats` is still HTTP because
 * ShardDO doesn't expose a typed RPC for stats.
 */
analytics.get("/overview", async (c) => {
  const userId = c.get("userId");
  const stub = userStub(c.env, userId);

  const [userStats, quota] = await Promise.all([
    stub.appGetUserStats(userId),
    stub.appGetQuota(userId),
  ]);
  const poolSize = quota.poolSize;

  // Only query ShardDOs if the user actually has files — avoids
  // waking up all 32 DOs for a brand-new user with nothing stored.
  let shards: ShardStats[] = [];
  const hasFiles =
    userStats.totalFiles > 0 || userStats.shardDistribution.length > 0;

  if (hasFiles) {
    const activeShardIndices = new Set(
      userStats.shardDistribution.map((s) => s.shardIndex)
    );
    const indicesToQuery =
      activeShardIndices.size > 0
        ? Array.from(activeShardIndices)
        : Array.from({ length: poolSize }, (_, i) => i);

    const shardPromises: Promise<ShardStats | null>[] = indicesToQuery.map(
      (i) => {
        const doName = legacyAppPlacement.shardDOName(
          { ns: "default", tenant: userId },
          i
        );
        const shardId = c.env.MOSSAIC_SHARD.idFromName(doName);
        const shardStub = c.env.MOSSAIC_SHARD.get(shardId);

        return shardStub
          .fetch(new Request("http://internal/stats", { method: "GET" }))
          .then(async (res) => {
            if (!res.ok) return null;
            const stats = (await res.json()) as {
              totalChunks: number;
              totalBytes: number;
              uniqueChunks: number;
              totalRefs: number;
              capacityUsed: number;
            };
            if (stats.totalChunks === 0 && stats.totalBytes === 0) return null;
            const dedupRatio =
              stats.totalRefs > 0
                ? 1 - stats.uniqueChunks / stats.totalRefs
                : 0;
            return {
              shardIndex: i,
              ...stats,
              dedupRatio: Math.round(dedupRatio * 10000) / 10000,
            } as ShardStats;
          })
          .catch(() => null);
      }
    );

    const shardResults = await Promise.all(shardPromises);
    shards = shardResults.filter((s): s is ShardStats => s !== null);
  }

  // Compute totals.
  let totalChunksAcrossShards = 0;
  let totalBytesAcrossShards = 0;
  let totalUniqueChunks = 0;
  let totalRefs = 0;

  for (const shard of shards) {
    totalChunksAcrossShards += shard.totalChunks;
    totalBytesAcrossShards += shard.totalBytes;
    totalUniqueChunks += shard.uniqueChunks;
    totalRefs += shard.totalRefs;
  }

  const averageDedupRatio =
    totalRefs > 0
      ? Math.round((1 - totalUniqueChunks / totalRefs) * 10000) / 10000
      : 0;

  const overview: AnalyticsOverview = {
    user: userStats,
    shards,
    totals: {
      totalChunksAcrossShards,
      totalBytesAcrossShards,
      totalUniqueChunks,
      totalRefs,
      averageDedupRatio,
    },
  };

  return c.json(overview);
});

export default analytics;
