import type { TransferProgress, ChunkProgress } from "@shared/types";
import {
  formatBytes,
  formatThroughput,
  formatTimeRemaining,
  cn,
} from "../../lib/utils";
import { Surface, Text, Badge } from "@cloudflare/kumo";
import {
  UploadSimple,
  DownloadSimple,
  Lightning,
  Clock,
  Cube,
  ArrowsDownUp,
} from "@phosphor-icons/react";

interface UploadProgressProps {
  transfers: Map<string, TransferProgress>;
}

export function UploadProgress({ transfers }: UploadProgressProps) {
  if (transfers.size === 0) return null;

  return (
    <div className="space-y-3">
      {Array.from(transfers.values()).map((transfer) => (
        <TransferCard key={transfer.fileId} transfer={transfer} />
      ))}
    </div>
  );
}

function TransferCard({ transfer }: { transfer: TransferProgress }) {
  const percent =
    transfer.bytesTotal > 0
      ? (transfer.bytesTransferred / transfer.bytesTotal) * 100
      : 0;

  const isUpload = transfer.direction === "upload";
  const Icon = isUpload ? UploadSimple : DownloadSimple;
  const isComplete = percent >= 100;

  return (
    <Surface className="rounded-xl p-5 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3 min-w-0">
          <div
            className={cn(
              "flex h-8 w-8 items-center justify-center rounded-lg",
              isComplete ? "bg-green-500/10" : "bg-kumo-brand/10"
            )}
          >
            <Icon
              size={16}
              weight="bold"
              className={isComplete ? "text-green-500" : "text-kumo-brand"}
            />
          </div>
          <div className="min-w-0">
            <Text bold className="truncate">
              {transfer.fileName}
            </Text>
            <Text size="xs" variant="secondary">
              {formatBytes(transfer.bytesTransferred)} /{" "}
              {formatBytes(transfer.bytesTotal)}
            </Text>
          </div>
        </div>
        <Badge variant={isComplete ? "success" : "primary"}>
          {Math.round(percent)}%
        </Badge>
      </div>

      {/* Progress bar */}
      <div className="w-full overflow-hidden rounded-full bg-kumo-fill h-2">
        <div
          className={cn(
            "h-full rounded-full transition-all duration-300",
            isComplete ? "bg-green-500" : "bg-kumo-brand"
          )}
          style={{ width: `${Math.min(100, percent)}%` }}
        />
      </div>

      {/* Stats row */}
      <div className="flex flex-wrap gap-x-5 gap-y-2">
        <Stat
          icon={Cube}
          label="Chunks"
          value={`${transfer.completedChunks}/${transfer.totalChunks}`}
        />
        <Stat
          icon={Lightning}
          label="Speed"
          value={formatThroughput(transfer.throughputBps)}
          highlight
        />
        <Stat
          icon={ArrowsDownUp}
          label="Streams"
          value={String(transfer.activeConcurrency)}
        />
        <Stat
          icon={Clock}
          label="Remaining"
          value={formatTimeRemaining(transfer.estimatedRemainingMs)}
        />
      </div>

      {/* Chunk visualization grid -- the showpiece */}
      {transfer.chunks.length > 0 && transfer.chunks.length <= 2000 && (
        <ChunkGrid chunks={transfer.chunks} />
      )}
    </Surface>
  );
}

function Stat({
  icon: Icon,
  label,
  value,
  highlight,
}: {
  icon: React.ComponentType<{
    size?: number;
    className?: string;
    weight?: string;
  }>;
  label: string;
  value: string;
  highlight?: boolean;
}) {
  return (
    <div className="flex items-center gap-1.5">
      <Icon
        size={13}
        className={highlight ? "text-amber-500" : "text-kumo-subtle"}
      />
      <Text size="xs" variant="secondary">
        {label}
      </Text>
      <Text size="xs" bold className={highlight ? "text-amber-500" : undefined}>
        {value}
      </Text>
    </div>
  );
}

interface ChunkGridProps {
  chunks: Array<ChunkProgress>;
}

/**
 * Visual per-chunk status grid -- the showpiece upload visualization.
 * Each tiny square represents one chunk with color-coded status.
 */
function ChunkGrid({ chunks }: ChunkGridProps) {
  const size = chunks.length > 500 ? 3 : chunks.length > 100 ? 4 : 5;
  const gap = chunks.length > 500 ? 1 : 2;

  return (
    <div className="rounded-lg bg-kumo-elevated p-3">
      <div className="mb-2 flex items-center justify-between">
        <Text size="xs" variant="secondary">
          Chunk Status
        </Text>
        <div className="flex items-center gap-3">
          <ChunkLegend color="bg-kumo-fill" label="Pending" />
          <ChunkLegend
            color="bg-kumo-brand chunk-uploading"
            label="Active"
          />
          <ChunkLegend color="bg-green-500" label="Done" />
          <ChunkLegend color="bg-red-500" label="Failed" />
        </div>
      </div>
      <div
        className="flex flex-wrap"
        style={{ gap: `${gap}px` }}
        title={`${chunks.filter((c) => c.status === "complete").length}/${chunks.length} chunks`}
      >
        {chunks.map((chunk, i) => (
          <div
            key={i}
            className={cn(
              "rounded-[1px] transition-colors duration-200",
              chunk.status === "pending" && "bg-kumo-fill",
              chunk.status === "uploading" && "bg-kumo-brand chunk-uploading",
              chunk.status === "complete" && "bg-green-500",
              chunk.status === "failed" && "bg-red-500"
            )}
            style={{ width: `${size}px`, height: `${size}px` }}
          />
        ))}
      </div>
    </div>
  );
}

function ChunkLegend({ color, label }: { color: string; label: string }) {
  return (
    <div className="flex items-center gap-1">
      <div className={cn("h-2 w-2 rounded-[1px]", color)} />
      <Text size="xs" variant="secondary">
        {label}
      </Text>
    </div>
  );
}
