import { useState, useEffect, useMemo } from "react";
import {
  ChartBar,
  HardDrives,
  Database,
  CloudArrowUp,
  CloudArrowDown,
  FileText,
  Image,
  VideoCamera,
  MusicNote,
  FileArchive,
  File,
  Heartbeat,
  Clock,
  ArrowUp,
  ArrowDown,
  Lightning,
  WarningCircle,
  CheckCircle,
  Warning,
} from "@phosphor-icons/react";
import {
  Text,
  Badge,
  Loader,
  Banner,
  Meter,
  Table,
} from "@cloudflare/kumo";
import type {
  AnalyticsOverview,
  ShardStats,
  MimeDistribution,
  RecentUpload,
} from "@shared/types";
import { api } from "../lib/api";
import { formatBytes, formatDate, cn } from "../lib/utils";
import { useTransferStats } from "../hooks/useTransferStats";

// ── Helpers ──

function pct(used: number, total: number): number {
  if (total === 0) return 0;
  return Math.min(Math.round((used / total) * 10000) / 100, 100);
}

function mimeCategory(mime: string): {
  label: string;
  icon: typeof File;
  color: string;
} {
  if (mime.startsWith("image/"))
    return { label: "Images", icon: Image, color: "bg-violet-500" };
  if (mime.startsWith("video/"))
    return { label: "Videos", icon: VideoCamera, color: "bg-rose-500" };
  if (mime.startsWith("audio/"))
    return { label: "Audio", icon: MusicNote, color: "bg-amber-500" };
  if (
    mime.startsWith("text/") ||
    mime.includes("pdf") ||
    mime.includes("document") ||
    mime.includes("spreadsheet")
  )
    return { label: "Documents", icon: FileText, color: "bg-sky-500" };
  if (
    mime.includes("zip") ||
    mime.includes("tar") ||
    mime.includes("gzip") ||
    mime.includes("rar")
  )
    return { label: "Archives", icon: FileArchive, color: "bg-emerald-500" };
  return { label: "Other", icon: File, color: "bg-kumo-fill" };
}

function shardHealthColor(capacityUsed: number): {
  bg: string;
  text: string;
  label: string;
} {
  if (capacityUsed < 0.6)
    return { bg: "bg-emerald-500", text: "text-emerald-400", label: "Healthy" };
  if (capacityUsed < 0.85)
    return { bg: "bg-amber-500", text: "text-amber-400", label: "Moderate" };
  return { bg: "bg-rose-500", text: "text-rose-400", label: "Critical" };
}

function shardHeatColor(capacityUsed: number): string {
  if (capacityUsed === 0) return "bg-kumo-fill/30";
  if (capacityUsed < 0.2) return "bg-emerald-500/30";
  if (capacityUsed < 0.4) return "bg-emerald-500/50";
  if (capacityUsed < 0.6) return "bg-emerald-500/70";
  if (capacityUsed < 0.75) return "bg-amber-500/50";
  if (capacityUsed < 0.85) return "bg-amber-500/70";
  if (capacityUsed < 0.95) return "bg-rose-500/50";
  return "bg-rose-500/80";
}

// ── Sparkline Component ──

function Sparkline({
  data,
  width = 200,
  height = 40,
  color = "#8b5cf6",
}: {
  data: number[];
  width?: number;
  height?: number;
  color?: string;
}) {
  if (data.length < 2) {
    return (
      <div
        className="flex items-center justify-center rounded-lg bg-kumo-fill/20"
        style={{ width, height }}
      >
        <Text size="xs" variant="secondary">
          Waiting for data...
        </Text>
      </div>
    );
  }

  const max = Math.max(...data, 1);
  const points = data.map((v, i) => {
    const x = (i / (data.length - 1)) * width;
    const y = height - (v / max) * (height - 4) - 2;
    return `${x},${y}`;
  });

  const areaPoints = [
    `0,${height}`,
    ...points,
    `${width},${height}`,
  ].join(" ");

  return (
    <svg width={width} height={height} className="overflow-visible">
      <defs>
        <linearGradient id={`sparkGrad-${color.replace("#", "")}`} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.3" />
          <stop offset="100%" stopColor={color} stopOpacity="0" />
        </linearGradient>
      </defs>
      <polygon
        points={areaPoints}
        fill={`url(#sparkGrad-${color.replace("#", "")})`}
      />
      <polyline
        points={points.join(" ")}
        fill="none"
        stroke={color}
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      {/* Current value dot */}
      {data.length > 0 && (
        <circle
          cx={width}
          cy={
            height -
            (data[data.length - 1] / max) * (height - 4) -
            2
          }
          r="3"
          fill={color}
        />
      )}
    </svg>
  );
}

// ── Donut Chart Component ──

function DonutChart({
  segments,
  size = 160,
}: {
  segments: { label: string; value: number; color: string }[];
  size?: number;
}) {
  const total = segments.reduce((s, seg) => s + seg.value, 0);
  if (total === 0) {
    return (
      <div
        className="flex items-center justify-center rounded-full border-8 border-kumo-fill/30"
        style={{ width: size, height: size }}
      >
        <Text size="xs" variant="secondary">
          No data
        </Text>
      </div>
    );
  }

  const radius = (size - 20) / 2;
  const cx = size / 2;
  const cy = size / 2;
  const circumference = 2 * Math.PI * radius;

  let offset = 0;
  const arcs = segments.map((seg) => {
    const pctVal = seg.value / total;
    const dashLen = pctVal * circumference;
    const dashOffset = -offset;
    offset += dashLen;
    return { ...seg, dashLen, dashOffset };
  });

  return (
    <div className="relative" style={{ width: size, height: size }}>
      <svg width={size} height={size}>
        <circle
          cx={cx}
          cy={cy}
          r={radius}
          fill="none"
          strokeWidth="10"
          className="stroke-kumo-fill/20"
        />
        {arcs.map((arc, i) => (
          <circle
            key={i}
            cx={cx}
            cy={cy}
            r={radius}
            fill="none"
            stroke="currentColor"
            strokeWidth="10"
            strokeDasharray={`${arc.dashLen} ${circumference - arc.dashLen}`}
            strokeDashoffset={arc.dashOffset}
            strokeLinecap="round"
            className={arc.color.replace("bg-", "text-")}
            transform={`rotate(-90 ${cx} ${cy})`}
          />
        ))}
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <Text size="lg" bold>
          {segments.length}
        </Text>
        <Text size="xs" variant="secondary">
          types
        </Text>
      </div>
    </div>
  );
}

// ── Card wrapper ──

function DashCard({
  children,
  className,
}: {
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "rounded-xl border border-kumo-line bg-kumo-elevated p-5",
        className
      )}
    >
      {children}
    </div>
  );
}

function CardHeader({
  icon: Icon,
  title,
  action,
}: {
  icon: typeof ChartBar;
  title: string;
  action?: React.ReactNode;
}) {
  return (
    <div className="mb-4 flex items-center justify-between">
      <div className="flex items-center gap-2.5">
        <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-kumo-fill/40">
          <Icon size={16} weight="duotone" className="text-kumo-brand" />
        </div>
        <Text size="sm" bold>
          {title}
        </Text>
      </div>
      {action}
    </div>
  );
}

// ── Main Page ──

export function AnalyticsPage() {
  const [data, setData] = useState<AnalyticsOverview | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const { stats } = useTransferStats();

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        setIsLoading(true);
        const overview = await api.getAnalyticsOverview();
        if (!cancelled) {
          setData(overview);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError(
            err instanceof Error ? err.message : "Failed to load analytics"
          );
        }
      } finally {
        if (!cancelled) setIsLoading(false);
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, []);

  if (isLoading) {
    return (
      <div className="flex flex-col items-center justify-center gap-4 py-20">
        <Loader size="lg" />
        <Text variant="secondary">Loading analytics...</Text>
      </div>
    );
  }

  if (error || !data) {
    return (
      <Banner
        variant="error"
        icon={<WarningCircle weight="fill" />}
        title={error || "No analytics data available"}
      />
    );
  }

  const { user, shards, totals } = data;
  const storagePercent = pct(user.totalStorageUsed, user.quotaLimit);

  // Aggregate mime distribution into categories
  const mimeGroups = new Map<
    string,
    { label: string; icon: typeof File; color: string; count: number; size: number }
  >();
  for (const md of user.mimeDistribution) {
    const cat = mimeCategory(md.mimeType);
    const existing = mimeGroups.get(cat.label);
    if (existing) {
      existing.count += md.count;
      existing.size += md.totalSize;
    } else {
      mimeGroups.set(cat.label, { ...cat, count: md.count, size: md.totalSize });
    }
  }
  const mimeEntries = [...mimeGroups.values()].sort((a, b) => b.size - a.size);

  // Shard health summary
  const healthyShardsCount = shards.filter(
    (s) => s.capacityUsed < 0.6
  ).length;
  const warnShardsCount = shards.filter(
    (s) => s.capacityUsed >= 0.6 && s.capacityUsed < 0.85
  ).length;
  const critShardsCount = shards.filter(
    (s) => s.capacityUsed >= 0.85
  ).length;

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div className="flex items-center justify-between">
        <div>
          <Text variant="heading3" as="h1">
            Analytics
          </Text>
          <Text size="sm" variant="secondary">
            Infrastructure monitoring &amp; storage intelligence
          </Text>
        </div>
        <Badge variant="secondary">
          {shards.length} shard{shards.length !== 1 ? "s" : ""} active
        </Badge>
      </div>

      {/* ── Row 1: Storage Overview + Speed Tracking ── */}
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        {/* Storage Overview */}
        <DashCard>
          <CardHeader icon={Database} title="Storage Overview" />

          <div className="space-y-4">
            {/* Kumo Meter visualization */}
            <div className="rounded-lg bg-kumo-base p-4">
              <div className="mb-3 flex items-end justify-between">
                <div>
                  <Text size="2xl" bold>
                    {formatBytes(user.totalStorageUsed)}
                  </Text>
                  <Text size="xs" variant="secondary">
                    of {formatBytes(user.quotaLimit)} quota
                  </Text>
                </div>
                <Text
                  size="sm"
                  bold
                  className={cn(
                    storagePercent > 85
                      ? "text-rose-400"
                      : storagePercent > 60
                        ? "text-amber-400"
                        : "text-emerald-400"
                  )}
                >
                  {storagePercent}%
                </Text>
              </div>

              <Meter value={storagePercent} />

              {/* Stats row */}
              <div className="mt-4 grid grid-cols-3 gap-4">
                <div>
                  <Text size="xs" variant="secondary">
                    Total Files
                  </Text>
                  <Text size="lg" bold>
                    {user.totalFiles}
                  </Text>
                </div>
                <div>
                  <Text size="xs" variant="secondary">
                    Dedup Ratio
                  </Text>
                  <Text size="lg" bold>
                    {(totals.averageDedupRatio * 100).toFixed(1)}%
                  </Text>
                </div>
                <div>
                  <Text size="xs" variant="secondary">
                    Unique Chunks
                  </Text>
                  <Text size="lg" bold>
                    {totals.totalUniqueChunks.toLocaleString()}
                  </Text>
                </div>
              </div>
            </div>

            {/* File status badges */}
            <div className="flex flex-wrap gap-2">
              <Badge variant="success">
                <CheckCircle size={12} weight="fill" />
                {user.filesByStatus.complete} complete
              </Badge>
              {user.filesByStatus.uploading > 0 && (
                <Badge variant="warning">
                  <CloudArrowUp size={12} weight="fill" />
                  {user.filesByStatus.uploading} uploading
                </Badge>
              )}
              {user.filesByStatus.failed > 0 && (
                <Badge variant="error">
                  <WarningCircle size={12} weight="fill" />
                  {user.filesByStatus.failed} failed
                </Badge>
              )}
              {user.filesByStatus.deleted > 0 && (
                <Badge variant="secondary">
                  {user.filesByStatus.deleted} deleted
                </Badge>
              )}
            </div>
          </div>
        </DashCard>

        {/* Upload/Download Speed Tracking */}
        <DashCard>
          <CardHeader icon={Lightning} title="Transfer Speed" />

          <div className="space-y-5">
            {/* Upload sparkline */}
            <div>
              <div className="mb-2 flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <ArrowUp
                    size={14}
                    weight="bold"
                    className="text-violet-400"
                  />
                  <Text size="xs" variant="secondary">
                    Upload
                  </Text>
                </div>
                <Text size="sm" bold>
                  {stats.currentUploadSpeed > 0
                    ? formatBytes(stats.currentUploadSpeed) + "/s"
                    : "Idle"}
                </Text>
              </div>
              <Sparkline
                data={stats.uploadSamples.map((s) => s.bytesPerSecond)}
                width={360}
                height={48}
                color="#8b5cf6"
              />
              <div className="mt-1.5 flex gap-4">
                <Text size="xs" variant="secondary">
                  Peak: {formatBytes(stats.peakUploadSpeed)}/s
                </Text>
                <Text size="xs" variant="secondary">
                  Total: {formatBytes(stats.totalBytesUploaded)}
                </Text>
              </div>
            </div>

            {/* Download sparkline */}
            <div>
              <div className="mb-2 flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <ArrowDown
                    size={14}
                    weight="bold"
                    className="text-sky-400"
                  />
                  <Text size="xs" variant="secondary">
                    Download
                  </Text>
                </div>
                <Text size="sm" bold>
                  {stats.currentDownloadSpeed > 0
                    ? formatBytes(stats.currentDownloadSpeed) + "/s"
                    : "Idle"}
                </Text>
              </div>
              <Sparkline
                data={stats.downloadSamples.map((s) => s.bytesPerSecond)}
                width={360}
                height={48}
                color="#38bdf8"
              />
              <div className="mt-1.5 flex gap-4">
                <Text size="xs" variant="secondary">
                  Peak: {formatBytes(stats.peakDownloadSpeed)}/s
                </Text>
                <Text size="xs" variant="secondary">
                  Total: {formatBytes(stats.totalBytesDownloaded)}
                </Text>
              </div>
            </div>
          </div>
        </DashCard>
      </div>

      {/* ── Row 2: Shard Distribution (showpiece) ── */}
      <DashCard>
        <CardHeader
          icon={HardDrives}
          title="Shard Distribution"
          action={
            <div className="flex items-center gap-3">
              <div className="flex items-center gap-1.5">
                <div className="h-2.5 w-2.5 rounded-full bg-emerald-500" />
                <Text size="xs" variant="secondary">
                  Healthy ({healthyShardsCount})
                </Text>
              </div>
              <div className="flex items-center gap-1.5">
                <div className="h-2.5 w-2.5 rounded-full bg-amber-500" />
                <Text size="xs" variant="secondary">
                  Moderate ({warnShardsCount})
                </Text>
              </div>
              <div className="flex items-center gap-1.5">
                <div className="h-2.5 w-2.5 rounded-full bg-rose-500" />
                <Text size="xs" variant="secondary">
                  Critical ({critShardsCount})
                </Text>
              </div>
            </div>
          }
        />

        {/* Shard heat map grid */}
        <div className="rounded-lg bg-kumo-base p-4">
          {shards.length === 0 ? (
            <div className="flex items-center justify-center py-8">
              <Text variant="secondary">No shard data available</Text>
            </div>
          ) : (
            <>
              <div className="grid grid-cols-4 gap-2 sm:grid-cols-6 md:grid-cols-8 lg:grid-cols-10 xl:grid-cols-12">
                {shards.map((shard) => (
                  <ShardCell key={shard.shardIndex} shard={shard} />
                ))}
              </div>

              {/* Detailed shard table */}
              <div className="mt-5">
                <Table>
                  <Table.Header>
                    <Table.Row>
                      <Table.HeaderCell>Shard</Table.HeaderCell>
                      <Table.HeaderCell>Chunks</Table.HeaderCell>
                      <Table.HeaderCell>Size</Table.HeaderCell>
                      <Table.HeaderCell>Unique</Table.HeaderCell>
                      <Table.HeaderCell>Dedup</Table.HeaderCell>
                      <Table.HeaderCell>Capacity</Table.HeaderCell>
                      <Table.HeaderCell>Status</Table.HeaderCell>
                    </Table.Row>
                  </Table.Header>
                  <Table.Body>
                    {shards.map((shard) => {
                      const health = shardHealthColor(shard.capacityUsed);
                      return (
                        <Table.Row key={shard.shardIndex}>
                          <Table.Cell>
                            <div className="flex items-center gap-2">
                              <div
                                className={cn(
                                  "h-2 w-2 rounded-full",
                                  health.bg
                                )}
                              />
                              <Text size="sm" bold>
                                #{shard.shardIndex}
                              </Text>
                            </div>
                          </Table.Cell>
                          <Table.Cell>
                            <Text size="sm">
                              {shard.totalChunks.toLocaleString()}
                            </Text>
                          </Table.Cell>
                          <Table.Cell>
                            <Text size="sm">
                              {formatBytes(shard.totalBytes)}
                            </Text>
                          </Table.Cell>
                          <Table.Cell>
                            <Text size="sm">
                              {shard.uniqueChunks.toLocaleString()}
                            </Text>
                          </Table.Cell>
                          <Table.Cell>
                            <Text size="sm">
                              {(shard.dedupRatio * 100).toFixed(1)}%
                            </Text>
                          </Table.Cell>
                          <Table.Cell>
                            <div className="flex items-center gap-2">
                              <div className="h-1.5 w-16 overflow-hidden rounded-full bg-kumo-fill/30">
                                <div
                                  className={cn(
                                    "h-full rounded-full transition-all",
                                    health.bg
                                  )}
                                  style={{
                                    width: `${Math.max(shard.capacityUsed * 100, 2)}%`,
                                  }}
                                />
                              </div>
                              <Text size="xs" variant="secondary">
                                {(shard.capacityUsed * 100).toFixed(0)}%
                              </Text>
                            </div>
                          </Table.Cell>
                          <Table.Cell>
                            <Badge
                              variant={
                                shard.capacityUsed < 0.6
                                  ? "success"
                                  : shard.capacityUsed < 0.85
                                    ? "warning"
                                    : "error"
                              }
                            >
                              {health.label}
                            </Badge>
                          </Table.Cell>
                        </Table.Row>
                      );
                    })}
                  </Table.Body>
                </Table>
              </div>
            </>
          )}
        </div>
      </DashCard>

      {/* ── Row 3: File Types + Recent Activity + System Health ── */}
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        {/* File Type Distribution */}
        <DashCard>
          <CardHeader icon={FileText} title="File Types" />
          <div className="flex items-center gap-6">
            <DonutChart
              segments={mimeEntries.map((e) => ({
                label: e.label,
                value: e.size,
                color: e.color,
              }))}
              size={140}
            />
            <div className="flex-1 space-y-2.5">
              {mimeEntries.map((entry) => {
                const Icon = entry.icon;
                return (
                  <div key={entry.label} className="flex items-center gap-2.5">
                    <div
                      className={cn(
                        "flex h-6 w-6 items-center justify-center rounded",
                        entry.color
                      )}
                    >
                      <Icon size={12} weight="bold" className="text-white" />
                    </div>
                    <div className="flex-1">
                      <Text size="xs" bold>
                        {entry.label}
                      </Text>
                      <Text size="xs" variant="secondary">
                        {entry.count} file{entry.count !== 1 ? "s" : ""} &middot;{" "}
                        {formatBytes(entry.size)}
                      </Text>
                    </div>
                  </div>
                );
              })}
              {mimeEntries.length === 0 && (
                <Text size="xs" variant="secondary">
                  No files uploaded yet
                </Text>
              )}
            </div>
          </div>
        </DashCard>

        {/* Recent Activity */}
        <DashCard>
          <CardHeader icon={Clock} title="Recent Activity" />
          <div className="space-y-2">
            {user.recentUploads.length === 0 ? (
              <Text size="xs" variant="secondary">
                No recent uploads
              </Text>
            ) : (
              user.recentUploads.map((upload: RecentUpload) => (
                <div
                  key={upload.fileId}
                  className="flex items-center gap-3 rounded-lg bg-kumo-base px-3 py-2"
                >
                  <div className="flex h-7 w-7 items-center justify-center rounded bg-kumo-fill/40">
                    <RecentFileIcon mimeType={upload.mimeType} />
                  </div>
                  <div className="min-w-0 flex-1">
                    <Text size="xs" bold className="truncate">
                      {upload.fileName}
                    </Text>
                    <Text size="xs" variant="secondary">
                      {formatBytes(upload.fileSize)} &middot;{" "}
                      {formatDate(upload.createdAt)}
                    </Text>
                  </div>
                  <Badge
                    variant={
                      upload.status === "complete"
                        ? "success"
                        : upload.status === "uploading"
                          ? "warning"
                          : "error"
                    }
                  >
                    {upload.status}
                  </Badge>
                </div>
              ))
            )}
          </div>
        </DashCard>

        {/* System Health */}
        <DashCard>
          <CardHeader icon={Heartbeat} title="System Health" />

          <div className="space-y-4">
            {/* Health overview */}
            <div className="grid grid-cols-3 gap-3">
              <HealthBadge
                count={healthyShardsCount}
                label="Healthy"
                icon={CheckCircle}
                color="text-emerald-400"
                bg="bg-emerald-500/10"
              />
              <HealthBadge
                count={warnShardsCount}
                label="Moderate"
                icon={Warning}
                color="text-amber-400"
                bg="bg-amber-500/10"
              />
              <HealthBadge
                count={critShardsCount}
                label="Critical"
                icon={WarningCircle}
                color="text-rose-400"
                bg="bg-rose-500/10"
              />
            </div>

            {/* Per-shard health bars */}
            <div className="space-y-1.5">
              {shards.slice(0, 10).map((shard) => {
                const health = shardHealthColor(shard.capacityUsed);
                return (
                  <div key={shard.shardIndex} className="flex items-center gap-2">
                    <Text
                      size="xs"
                      variant="secondary"
                      className="w-6 text-right"
                    >
                      {shard.shardIndex}
                    </Text>
                    <div className="h-2 flex-1 overflow-hidden rounded-full bg-kumo-fill/20">
                      <div
                        className={cn(
                          "h-full rounded-full transition-all",
                          health.bg
                        )}
                        style={{
                          width: `${Math.max(shard.capacityUsed * 100, 1)}%`,
                        }}
                      />
                    </div>
                  </div>
                );
              })}
              {shards.length > 10 && (
                <Text size="xs" variant="secondary" className="pl-8">
                  +{shards.length - 10} more shards...
                </Text>
              )}
            </div>

            {/* Totals */}
            <div className="rounded-lg bg-kumo-base p-3">
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <Text size="xs" variant="secondary">
                    Total Chunks
                  </Text>
                  <Text size="sm" bold>
                    {totals.totalChunksAcrossShards.toLocaleString()}
                  </Text>
                </div>
                <div>
                  <Text size="xs" variant="secondary">
                    Total Refs
                  </Text>
                  <Text size="sm" bold>
                    {totals.totalRefs.toLocaleString()}
                  </Text>
                </div>
                <div>
                  <Text size="xs" variant="secondary">
                    Raw Storage
                  </Text>
                  <Text size="sm" bold>
                    {formatBytes(totals.totalBytesAcrossShards)}
                  </Text>
                </div>
                <div>
                  <Text size="xs" variant="secondary">
                    Avg Dedup
                  </Text>
                  <Text size="sm" bold>
                    {(totals.averageDedupRatio * 100).toFixed(1)}%
                  </Text>
                </div>
              </div>
            </div>
          </div>
        </DashCard>
      </div>
    </div>
  );
}

// ── Sub-components ──

function ShardCell({ shard }: { shard: ShardStats }) {
  const [hovered, setHovered] = useState(false);

  return (
    <div
      className="relative"
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
      <div
        className={cn(
          "flex aspect-square cursor-default items-center justify-center rounded-lg border border-kumo-line/50 text-xs font-medium transition-all",
          shardHeatColor(shard.capacityUsed),
          hovered && "scale-110 border-kumo-fill shadow-lg"
        )}
      >
        {shard.shardIndex}
      </div>

      {/* Tooltip */}
      {hovered && (
        <div className="absolute bottom-full left-1/2 z-20 mb-2 -translate-x-1/2 rounded-lg border border-kumo-line bg-kumo-elevated px-3 py-2 shadow-xl">
          <div className="whitespace-nowrap text-center">
            <Text size="xs" bold>
              Shard #{shard.shardIndex}
            </Text>
            <div className="mt-1 space-y-0.5">
              <Text size="xs" variant="secondary">
                {shard.totalChunks} chunks &middot; {formatBytes(shard.totalBytes)}
              </Text>
              <Text size="xs" variant="secondary">
                Dedup: {(shard.dedupRatio * 100).toFixed(1)}% &middot; Cap:{" "}
                {(shard.capacityUsed * 100).toFixed(0)}%
              </Text>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function HealthBadge({
  count,
  label,
  icon: Icon,
  color,
  bg,
}: {
  count: number;
  label: string;
  icon: typeof CheckCircle;
  color: string;
  bg: string;
}) {
  return (
    <div
      className={cn(
        "flex flex-col items-center gap-1 rounded-lg py-3",
        bg
      )}
    >
      <Icon size={18} weight="fill" className={color} />
      <Text size="lg" bold>
        {count}
      </Text>
      <Text size="xs" variant="secondary">
        {label}
      </Text>
    </div>
  );
}

function RecentFileIcon({ mimeType }: { mimeType: string }) {
  const cat = mimeCategory(mimeType);
  const Icon = cat.icon;
  return <Icon size={14} weight="duotone" className="text-kumo-default" />;
}
