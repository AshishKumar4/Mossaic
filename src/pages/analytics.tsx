import { motion } from "framer-motion";
import {
  HardDrive,
  Files,
  Database,
  Copy,
  RefreshCw,
  TrendingUp,
  Server,
  BarChart3,
  Zap,
  Upload,
  Download,
  Info,
} from "lucide-react";
import { useAnalytics } from "@/hooks/use-analytics";
import { useTransferStats } from "@/hooks/use-transfer-stats";
import { cn, formatBytes, formatDate, formatDuration } from "@/lib/utils";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Spinner } from "@/components/ui/spinner";
import {
  Tooltip,
  TooltipTrigger,
  TooltipContent,
} from "@/components/ui/tooltip";
import type { ShardStats, CompletedTransferStats } from "@shared/types";

const container = {
  hidden: { opacity: 0 },
  show: {
    opacity: 1,
    transition: { staggerChildren: 0.05 },
  },
};

const item = {
  hidden: { opacity: 0, y: 10 },
  show: { opacity: 1, y: 0, transition: { duration: 0.3, ease: "easeOut" as const } },
};

export function AnalyticsPage() {
  const { data, loading, error, refresh } = useAnalytics();
  const transferStats = useTransferStats();

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center h-full">
        <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-secondary/80">
          <Spinner className="h-5 w-5 text-primary" />
        </div>
        <p className="mt-3 text-sm text-muted-foreground">
          Loading analytics...
        </p>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="flex flex-col items-center justify-center h-full">
        <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-destructive/10">
          <BarChart3 className="h-6 w-6 text-destructive" />
        </div>
        <p className="mt-4 text-sm font-medium text-foreground">
          Failed to load analytics
        </p>
        <p className="mt-1 text-xs text-muted-foreground">
          {error || "No data available"}
        </p>
        <Button
          variant="outline"
          size="sm"
          className="mt-4 rounded-lg"
          onClick={refresh}
        >
          <RefreshCw className="h-3.5 w-3.5 mr-1.5" />
          Try again
        </Button>
      </div>
    );
  }

  const { user, shards, totals } = data;
  const quotaPct =
    user.quotaLimit > 0
      ? Math.round((user.totalStorageUsed / user.quotaLimit) * 100)
      : 0;

  const activeShards = shards.filter((s) => s.totalChunks > 0);

  // Compute chunk context info
  const totalChunks = totals.totalChunksAcrossShards;
  const chunkSub =
    totalChunks === 0
      ? "no data yet"
      : `${totals.totalUniqueChunks} unique across ${activeShards.length} ${activeShards.length === 1 ? "shard" : "shards"}`;

  // Latest transfer stats for the speed card
  const lastUpload = transferStats.find((s) => s.direction === "upload");
  const lastDownload = transferStats.find((s) => s.direction === "download");

  return (
    <div className="h-full overflow-y-auto">
      <div className="flex flex-col gap-3 border-b border-border px-6 py-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-lg font-semibold tracking-tight text-heading">Analytics</h1>
          <p className="text-sm text-muted-foreground">
            Storage, performance, and shard distribution overview
          </p>
        </div>
        <Button
          variant="ghost"
          size="icon"
          onClick={refresh}
          className="h-8 w-8 shrink-0 transition-all duration-200"
        >
          <RefreshCw className="h-4 w-4" />
        </Button>
      </div>

      <motion.div
        variants={container}
        initial="hidden"
        animate="show"
        className="p-4 space-y-6 sm:p-6"
      >
        {/* Stat cards */}
        <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
          <StatCard
            icon={HardDrive}
            label="Storage Used"
            value={formatBytes(user.totalStorageUsed)}
            sub={`of ${formatBytes(user.quotaLimit)}`}
          />
          <StatCard
            icon={Files}
            label="Total Files"
            value={user.totalFiles.toString()}
            sub={`${user.filesByStatus.complete} complete`}
          />
          <StatCard
            icon={Database}
            label="Total Chunks"
            value={totalChunks.toString()}
            sub={chunkSub}
            tooltip="Files are split into 1 MB chunks distributed across Durable Object shards for parallel storage and retrieval"
          />
          <StatCard
            icon={Copy}
            label="Dedup Ratio"
            value={`${(totals.averageDedupRatio * 100).toFixed(1)}%`}
            sub={`${totals.totalRefs} total refs`}
            tooltip="Percentage of duplicate chunks eliminated via content-addressed SHA-256 deduplication"
          />
        </div>

        {/* Storage quota */}
        <motion.div variants={item}>
          <Card>
            <CardHeader>
              <CardTitle className="text-sm font-medium">
                Storage Quota
              </CardTitle>
            </CardHeader>
            <CardContent>
              <Progress
                value={quotaPct}
                className="h-3"
                indicatorClassName={
                  quotaPct > 90
                    ? "bg-destructive"
                    : quotaPct > 70
                      ? "bg-warning"
                      : undefined
                }
              />
              <div className="mt-2.5 flex justify-between text-xs text-muted-foreground">
                <span>{formatBytes(user.totalStorageUsed)} used</span>
                <span className="font-medium text-foreground">{quotaPct}%</span>
                <span>{formatBytes(user.quotaLimit)} limit</span>
              </div>
              {user.totalStorageUsed > 0 && (
                <div className="mt-3 text-[11px] text-muted-foreground">
                  {formatBytes(user.quotaLimit - user.totalStorageUsed)} remaining
                </div>
              )}
            </CardContent>
          </Card>
        </motion.div>

        {/* Transfer Performance */}
        <motion.div variants={item}>
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-sm font-medium">
                <Zap className="h-4 w-4 text-muted-foreground" />
                Transfer Performance
              </CardTitle>
            </CardHeader>
            <CardContent>
              {!lastUpload && !lastDownload ? (
                <div className="flex flex-col items-center py-6 text-muted-foreground">
                  <Zap className="h-8 w-8 opacity-30" />
                  <p className="mt-2 text-sm">No transfer data yet</p>
                  <p className="mt-1 text-xs">Upload or download a file to see speed stats</p>
                </div>
              ) : (
                <div className="grid gap-4 sm:grid-cols-2">
                  {lastUpload && (
                    <SpeedCard
                      icon={Upload}
                      label="Last Upload"
                      stats={lastUpload}
                    />
                  )}
                  {lastDownload && (
                    <SpeedCard
                      icon={Download}
                      label="Last Download"
                      stats={lastDownload}
                    />
                  )}
                </div>
              )}
            </CardContent>
          </Card>
        </motion.div>

        <div className="grid gap-4 lg:grid-cols-2">
          {/* File status breakdown */}
          <motion.div variants={item}>
            <Card className="h-full">
              <CardHeader>
                <CardTitle className="text-sm font-medium">
                  File Status
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  <StatusBar
                    label="Complete"
                    count={user.filesByStatus.complete}
                    total={user.totalFiles}
                    color="bg-success"
                  />
                  <StatusBar
                    label="Uploading"
                    count={user.filesByStatus.uploading}
                    total={user.totalFiles}
                    color="bg-primary"
                  />
                  <StatusBar
                    label="Failed"
                    count={user.filesByStatus.failed}
                    total={user.totalFiles}
                    color="bg-destructive"
                  />
                  <StatusBar
                    label="Deleted"
                    count={user.filesByStatus.deleted}
                    total={user.totalFiles}
                    color="bg-muted-foreground"
                  />
                </div>
              </CardContent>
            </Card>
          </motion.div>

          {/* MIME distribution */}
          <motion.div variants={item}>
            <Card className="h-full">
              <CardHeader>
                <CardTitle className="text-sm font-medium">
                  File Types
                </CardTitle>
              </CardHeader>
              <CardContent>
                {user.mimeDistribution.length === 0 ? (
                  <div className="flex flex-col items-center py-6 text-muted-foreground">
                    <Files className="h-8 w-8 opacity-30" />
                    <p className="mt-2 text-sm">No files yet</p>
                  </div>
                ) : (
                  <div className="space-y-2.5">
                    {user.mimeDistribution.slice(0, 6).map((m) => (
                      <div
                        key={m.mimeType}
                        className="flex items-center justify-between text-sm"
                      >
                        <span className="truncate text-muted-foreground">
                          {m.mimeType}
                        </span>
                        <div className="flex shrink-0 items-center gap-3 ml-3">
                          <Badge variant="secondary" className="text-[10px]">
                            {m.count}
                          </Badge>
                          <span className="text-xs text-muted-foreground w-16 text-right tabular-nums">
                            {formatBytes(m.totalSize)}
                          </span>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          </motion.div>
        </div>

        {/* Upload History */}
        {user.recentUploads.length > 0 && (
          <motion.div variants={item}>
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-sm font-medium">
                  <BarChart3 className="h-4 w-4 text-muted-foreground" />
                  Upload History
                </CardTitle>
              </CardHeader>
              <CardContent>
                <UploadHistoryChart
                  uploads={user.recentUploads.filter(
                    (u) => u.status === "complete"
                  )}
                />
              </CardContent>
            </Card>
          </motion.div>
        )}

        {/* Shard distribution */}
        <motion.div variants={item}>
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle className="flex items-center gap-2 text-sm font-medium">
                  <Server className="h-4 w-4 text-muted-foreground" />
                  Shard Distribution
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <Info className="h-3.5 w-3.5 text-muted-foreground cursor-help" />
                    </TooltipTrigger>
                    <TooltipContent>
                      <p className="max-w-[220px]">
                        Active Durable Object shards storing file chunks.
                        Files are split into 1 MB chunks and distributed via rendezvous hashing.
                      </p>
                    </TooltipContent>
                  </Tooltip>
                </CardTitle>
                <Badge variant="secondary">
                  {activeShards.length} active {activeShards.length === 1 ? "shard" : "shards"}
                </Badge>
              </div>
            </CardHeader>
            <CardContent>
              {activeShards.length === 0 ? (
                <div className="flex flex-col items-center py-6 text-muted-foreground">
                  <Server className="h-8 w-8 opacity-30" />
                  <p className="mt-2 text-sm">No active shards</p>
                  <p className="mt-1 text-xs">Upload a file to see chunk distribution across shards</p>
                </div>
              ) : (
                <>
                  {/* Chunk distribution bar */}
                  <ChunkDistributionBar shards={activeShards} />
                  <div className="mt-4 grid gap-3 grid-cols-2 lg:grid-cols-4">
                    {activeShards
                      .sort((a, b) => b.totalBytes - a.totalBytes)
                      .slice(0, 16)
                      .map((shard) => (
                        <ShardCard key={shard.shardIndex} shard={shard} />
                      ))}
                  </div>
                </>
              )}
            </CardContent>
          </Card>
        </motion.div>

        {/* Recent uploads */}
        <motion.div variants={item}>
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-sm font-medium">
                <TrendingUp className="h-4 w-4 text-muted-foreground" />
                Recent Uploads
              </CardTitle>
            </CardHeader>
            <CardContent>
              {user.recentUploads.length === 0 ? (
                <div className="flex flex-col items-center py-6 text-muted-foreground">
                  <TrendingUp className="h-8 w-8 opacity-30" />
                  <p className="mt-2 text-sm">No uploads yet</p>
                  <p className="mt-1 text-xs">Your upload history will appear here</p>
                </div>
              ) : (
                <div className="space-y-2">
                  {user.recentUploads.map((upload) => (
                    <div
                      key={upload.fileId}
                      className="flex items-center justify-between rounded-lg bg-white/[0.03] px-3.5 py-2.5 transition-colors duration-200 hover:bg-white/[0.05]"
                    >
                      <div className="min-w-0 flex-1">
                        <span className="block truncate text-sm font-medium">
                          {upload.fileName}
                        </span>
                        <span className="text-xs text-muted-foreground">
                          {upload.mimeType} &middot; {formatDate(upload.createdAt)}
                        </span>
                      </div>
                      <div className="flex shrink-0 items-center gap-2 ml-4">
                        <span className="text-sm tabular-nums">
                          {formatBytes(upload.fileSize)}
                        </span>
                        <Badge
                          variant={
                            upload.status === "complete"
                              ? "success"
                              : upload.status === "failed"
                                ? "destructive"
                                : "default"
                          }
                          className="text-[10px]"
                        >
                          {upload.status}
                        </Badge>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </motion.div>
      </motion.div>
    </div>
  );
}

// ── Sub-components ──

function StatCard({
  icon: Icon,
  label,
  value,
  sub,
  tooltip,
}: {
  icon: typeof HardDrive;
  label: string;
  value: string;
  sub: string;
  tooltip?: string;
}) {
  return (
    <motion.div variants={item}>
      <Card className="transition-shadow duration-200 hover:shadow-md">
        <CardContent className="p-4 sm:p-5">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-primary/10">
              <Icon className="h-[18px] w-[18px] text-primary" />
            </div>
            <div className="min-w-0">
              <div className="flex items-center gap-1">
                <p className="text-xs text-muted-foreground">{label}</p>
                {tooltip && (
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <Info className="h-3 w-3 text-muted-foreground/50 cursor-help" />
                    </TooltipTrigger>
                    <TooltipContent>
                      <p className="max-w-[220px]">{tooltip}</p>
                    </TooltipContent>
                  </Tooltip>
                )}
              </div>
              <p className="text-lg font-semibold tracking-tight tabular-nums">
                {value}
              </p>
              <p className="text-[11px] text-muted-foreground">{sub}</p>
            </div>
          </div>
        </CardContent>
      </Card>
    </motion.div>
  );
}

function StatusBar({
  label,
  count,
  total,
  color,
}: {
  label: string;
  count: number;
  total: number;
  color: string;
}) {
  const pct = total > 0 ? Math.round((count / total) * 100) : 0;
  return (
    <div className="flex items-center gap-3">
      <span className="w-20 text-xs text-muted-foreground">{label}</span>
      <div className="flex-1 h-2 rounded-full bg-secondary overflow-hidden">
        <div
          className={cn(
            "h-full rounded-full transition-all duration-500 ease-out",
            color
          )}
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className="w-8 text-right text-xs tabular-nums text-muted-foreground">
        {count}
      </span>
    </div>
  );
}

function SpeedCard({
  icon: Icon,
  label,
  stats,
}: {
  icon: typeof Upload;
  label: string;
  stats: CompletedTransferStats;
}) {
  return (
    <div className="rounded-xl border border-white/[0.06] bg-white/[0.03] p-4">
      <div className="flex items-center gap-2 mb-3">
        <div className="flex h-7 w-7 items-center justify-center rounded-lg bg-primary/10">
          <Icon className="h-3.5 w-3.5 text-primary" />
        </div>
        <span className="text-xs font-medium">{label}</span>
      </div>
      <div className="space-y-2 text-[11px]">
        <div className="flex justify-between text-muted-foreground">
          <span>File</span>
          <span className="truncate max-w-[140px] text-right font-medium text-foreground">
            {stats.fileName}
          </span>
        </div>
        <div className="flex justify-between text-muted-foreground">
          <span>Size</span>
          <span className="tabular-nums">{formatBytes(stats.fileSize)}</span>
        </div>
        <div className="flex justify-between text-muted-foreground">
          <span>Duration</span>
          <span className="tabular-nums">{formatDuration(stats.durationMs)}</span>
        </div>
        <div className="flex justify-between text-muted-foreground">
          <span>Avg Speed</span>
          <span className="tabular-nums font-medium text-foreground">
            {formatBytes(stats.averageThroughputBps)}/s
          </span>
        </div>
        <div className="flex justify-between text-muted-foreground">
          <span>Peak Speed</span>
          <span className="tabular-nums">
            {formatBytes(stats.peakThroughputBps)}/s
          </span>
        </div>
      </div>
    </div>
  );
}

function ShardCard({ shard }: { shard: ShardStats }) {
  return (
    <div className="rounded-xl border border-white/[0.06] bg-white/[0.03] p-3 transition-all duration-200 hover:bg-white/[0.05]">
      <div className="flex items-center justify-between">
        <span className="text-xs font-semibold">Shard {shard.shardIndex}</span>
        <Badge
          variant={shard.dedupRatio > 0.1 ? "success" : "secondary"}
          className="text-[10px]"
        >
          {(shard.dedupRatio * 100).toFixed(0)}% dedup
        </Badge>
      </div>
      <div className="mt-2.5 space-y-1.5 text-[11px] text-muted-foreground">
        <div className="flex justify-between">
          <span>Chunks</span>
          <span className="tabular-nums">{shard.totalChunks}</span>
        </div>
        <div className="flex justify-between">
          <span>Size</span>
          <span className="tabular-nums">{formatBytes(shard.totalBytes)}</span>
        </div>
        <div className="flex justify-between">
          <span>Unique</span>
          <span className="tabular-nums">{shard.uniqueChunks}</span>
        </div>
      </div>
    </div>
  );
}

/** Simple bar chart of recent upload sizes */
function UploadHistoryChart({
  uploads,
}: {
  uploads: { fileId: string; fileName: string; fileSize: number; createdAt: number }[];
}) {
  if (uploads.length === 0) return null;

  const maxSize = Math.max(...uploads.map((u) => u.fileSize));

  return (
    <div className="flex items-end gap-1.5 h-24">
      {uploads
        .slice(0, 10)
        .reverse()
        .map((upload) => {
          const heightPct = maxSize > 0 ? (upload.fileSize / maxSize) * 100 : 0;
          return (
            <Tooltip key={upload.fileId}>
              <TooltipTrigger asChild>
                <div className="flex-1 flex flex-col items-center justify-end h-full">
                  <div
                    className="w-full rounded-t bg-primary/60 transition-all duration-300 hover:bg-primary/80 cursor-default min-h-[2px]"
                    style={{ height: `${Math.max(heightPct, 2)}%` }}
                  />
                </div>
              </TooltipTrigger>
              <TooltipContent>
                <p className="font-medium">{upload.fileName}</p>
                <p className="text-muted-foreground">
                  {formatBytes(upload.fileSize)} &middot; {formatDate(upload.createdAt)}
                </p>
              </TooltipContent>
            </Tooltip>
          );
        })}
    </div>
  );
}

/** Stacked bar showing chunk distribution across shards */
function ChunkDistributionBar({ shards }: { shards: ShardStats[] }) {
  const totalChunks = shards.reduce((sum, s) => sum + s.totalChunks, 0);
  if (totalChunks === 0) return null;

  // Color palette for shards
  const colors = [
    "bg-primary/70",
    "bg-success/70",
    "bg-warning/70",
    "bg-destructive/50",
    "bg-primary/40",
    "bg-success/40",
    "bg-warning/40",
    "bg-destructive/30",
  ];

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-1 h-6 rounded-lg overflow-hidden">
        {shards
          .filter((s) => s.totalChunks > 0)
          .sort((a, b) => b.totalChunks - a.totalChunks)
          .map((shard, i) => {
            const widthPct = (shard.totalChunks / totalChunks) * 100;
            return (
              <Tooltip key={shard.shardIndex}>
                <TooltipTrigger asChild>
                  <div
                    className={cn(
                      "h-full transition-all duration-300 cursor-default first:rounded-l-lg last:rounded-r-lg",
                      colors[i % colors.length]
                    )}
                    style={{ width: `${Math.max(widthPct, 1)}%` }}
                  />
                </TooltipTrigger>
                <TooltipContent>
                  <p className="font-medium">Shard {shard.shardIndex}</p>
                  <p className="text-muted-foreground">
                    {shard.totalChunks} chunks ({formatBytes(shard.totalBytes)})
                  </p>
                </TooltipContent>
              </Tooltip>
            );
          })}
      </div>
      <p className="text-[11px] text-muted-foreground">
        {totalChunks} chunks across {shards.length} shards
      </p>
    </div>
  );
}
