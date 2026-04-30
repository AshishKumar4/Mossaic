import { motion, AnimatePresence } from "framer-motion";
import { X, Upload, Download, CheckCircle2, AlertCircle } from "lucide-react";
import type { TransferProgress } from "@app/types";
import { cn, formatBytes } from "@/lib/utils";
import { Progress } from "@/components/ui/progress";
import { Button } from "@/components/ui/button";

interface TransferPanelProps {
  uploads: Map<string, TransferProgress>;
  downloads: Map<string, TransferProgress>;
  onClearUpload: (fileId: string) => void;
  onClearDownload: (fileId: string) => void;
}

export function TransferPanel({
  uploads,
  downloads,
  onClearUpload,
  onClearDownload,
}: TransferPanelProps) {
  const allTransfers = [
    ...Array.from(uploads.values()),
    ...Array.from(downloads.values()),
  ];

  if (allTransfers.length === 0) return null;

  return (
    <motion.div
      initial={{ y: 100, opacity: 0 }}
      animate={{ y: 0, opacity: 1 }}
      exit={{ y: 100, opacity: 0 }}
      transition={{ type: "spring", stiffness: 300, damping: 30 }}
      className="fixed bottom-4 right-4 z-50 w-80 overflow-hidden rounded-xl border border-white/[0.06] bg-[#2d2d2d]/95 shadow-2xl backdrop-blur-xl sm:w-96"
    >
      <div className="flex items-center justify-between border-b border-white/[0.06] px-4 py-3">
        <span className="text-sm font-semibold">
          Transfers ({allTransfers.length})
        </span>
        <div className="flex h-5 w-5 items-center justify-center rounded-full bg-primary/15">
          <span className="text-[10px] font-bold text-primary tabular-nums">
            {allTransfers.length}
          </span>
        </div>
      </div>
      <div className="max-h-64 overflow-y-auto">
        <AnimatePresence>
          {allTransfers.map((t) => (
            <TransferRow
              key={t.fileId}
              transfer={t}
              onClear={() =>
                t.direction === "upload"
                  ? onClearUpload(t.fileId)
                  : onClearDownload(t.fileId)
              }
            />
          ))}
        </AnimatePresence>
      </div>
    </motion.div>
  );
}

function TransferRow({
  transfer,
  onClear,
}: {
  transfer: TransferProgress;
  onClear: () => void;
}) {
  const pct =
    transfer.bytesTotal > 0
      ? Math.round((transfer.bytesTransferred / transfer.bytesTotal) * 100)
      : 0;

  // Terminal failure (transfer unrecoverable) takes priority over the
  // partial-failed-chunks UI: the X button must appear so the user can
  // dismiss + retry. `completedAt` keeps marking success.
  const hasFailed =
    transfer.failedAt !== undefined || transfer.failedChunks > 0;
  const isTerminal =
    transfer.completedAt !== undefined || transfer.failedAt !== undefined;
  // Preserve the legacy "Complete" label only on actual success.
  const isDone =
    transfer.completedAt !== undefined && transfer.failedAt === undefined;

  return (
    <motion.div
      layout
      initial={{ opacity: 0, height: 0 }}
      animate={{ opacity: 1, height: "auto" }}
      exit={{ opacity: 0, height: 0 }}
      transition={{ duration: 0.2 }}
      className="border-b border-white/[0.06] px-4 py-3 last:border-0"
    >
      <div className="flex items-start gap-3">
        <div
          className={cn(
            "mt-0.5 flex h-7 w-7 shrink-0 items-center justify-center rounded-lg transition-colors duration-200",
            isDone && !hasFailed
              ? "bg-success/15 text-success"
              : hasFailed
                ? "bg-destructive/15 text-destructive"
                : "bg-primary/15 text-primary"
          )}
        >
          {isDone ? (
            <CheckCircle2 className="h-3.5 w-3.5" />
          ) : hasFailed ? (
            <AlertCircle className="h-3.5 w-3.5" />
          ) : transfer.direction === "upload" ? (
            <Upload className="h-3.5 w-3.5" />
          ) : (
            <Download className="h-3.5 w-3.5" />
          )}
        </div>

        <div className="flex-1 min-w-0">
          <div className="flex items-center justify-between gap-2">
            <span className="truncate text-sm font-medium">
              {transfer.fileName}
            </span>
            {isTerminal && (
              <Button
                variant="ghost"
                size="icon"
                className="h-5 w-5 shrink-0 transition-all duration-200"
                onClick={onClear}
              >
                <X className="h-3 w-3" />
              </Button>
            )}
          </div>

          <Progress value={pct} className="mt-2 h-1.5" />

          <div className="mt-1.5 flex items-center justify-between text-[11px] text-muted-foreground">
            <span className="tabular-nums">
              {formatBytes(transfer.bytesTransferred)} /{" "}
              {formatBytes(transfer.bytesTotal)}
            </span>
            <span className={transfer.failedAt !== undefined ? "text-destructive" : undefined}>
              {isDone
                ? "Complete"
                : transfer.failedAt !== undefined
                  ? (transfer.error ?? "Failed")
                  : hasFailed
                    ? `${transfer.failedChunks} failed`
                    : `${formatBytes(transfer.throughputBps)}/s`}
            </span>
          </div>
        </div>
      </div>
    </motion.div>
  );
}
