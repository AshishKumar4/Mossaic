import { Hono } from "hono";
import type {
  Env,
  ShardStats,
  UserStats,
  AnalyticsOverview,
} from "@shared/types";
import { authMiddleware } from "../lib/auth";
import { userDOName, shardDOName } from "../lib/utils";

const analytics = new Hono<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>();

analytics.use("*", authMiddleware());

/**
 * GET /api/analytics/overview
 * Returns aggregated analytics: user stats + per-shard breakdown + totals.
 */
analytics.get("/overview", async (c) => {
  const userId = c.get("userId");

  // 1. Get user stats from UserDO
  const userDoId = c.env.USER_DO.idFromName(userDOName(userId));
  const userStub = c.env.USER_DO.get(userDoId);

  const userRes = await userStub.fetch(
    new Request("http://internal/stats", {
      method: "POST",
      body: JSON.stringify({ userId }),
    })
  );

  if (!userRes.ok) {
    return c.json({ error: "Failed to fetch user stats" }, 500);
  }

  const userStats = (await userRes.json()) as UserStats;

  // 2. Determine pool size from user's quota
  const quotaRes = await userStub.fetch(
    new Request("http://internal/quota", {
      method: "POST",
      body: JSON.stringify({ userId }),
    })
  );

  const quota = (await quotaRes.json()) as { poolSize: number };
  const poolSize = quota.poolSize;

  // 3. Query all ShardDOs in parallel for their stats
  const shardPromises: Promise<ShardStats | null>[] = [];

  for (let i = 0; i < poolSize; i++) {
    const doName = shardDOName(userId, i);
    const shardId = c.env.SHARD_DO.idFromName(doName);
    const shardStub = c.env.SHARD_DO.get(shardId);

    shardPromises.push(
      shardStub
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
        .catch(() => null)
    );
  }

  const shardResults = await Promise.all(shardPromises);
  const shards = shardResults.filter((s): s is ShardStats => s !== null);

  // 4. Compute totals
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
