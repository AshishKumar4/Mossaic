import { useState, useCallback } from "react";
import { api } from "@/lib/api";
import { MAX_RETRIES, RETRY_BASE_DELAY } from "@shared/constants";
import { addTransferStats } from "@/lib/transfer-stats";
import type { TransferProgress, ChunkProgress, ChunkStatus, CompletedTransferStats } from "@shared/types";

export function useDownload() {
  const [transfers, setTransfers] = useState<Map<string, TransferProgress>>(
    new Map()
  );
  const [completedStats, setCompletedStats] = useState<CompletedTransferStats[]>([]);

  const updateTransfer = useCallback(
    (fileId: string, updater: (p: TransferProgress) => TransferProgress) => {
      setTransfers((prev) => {
        const next = new Map(prev);
        const current = next.get(fileId);
        if (current) {
          next.set(fileId, updater(current));
        }
        return next;
      });
    },
    []
  );

  const downloadFile = useCallback(
    async (fileId: string, fileName: string) => {
      const manifest = await api.getManifest(fileId);
      const chunkCount = manifest.chunks.length;

      const chunks: ChunkProgress[] = manifest.chunks.map((c) => ({
        index: c.index,
        status: "pending" as ChunkStatus,
        bytesTransferred: 0,
        size: c.size,
      }));

      const progress: TransferProgress = {
        fileId,
        fileName,
        direction: "download",
        totalChunks: chunkCount,
        completedChunks: 0,
        failedChunks: 0,
        bytesTransferred: 0,
        bytesTotal: manifest.fileSize,
        activeConcurrency: 0,
        throughputBps: 0,
        estimatedRemainingMs: 0,
        chunks,
        startedAt: Date.now(),
      };

      setTransfers((prev) => new Map(prev).set(fileId, progress));

      const buffers: ArrayBuffer[] = new Array(chunkCount);
      const concurrency = Math.min(6, chunkCount);
      let nextChunk = 0;
      let completedCount = 0;
      let totalBytes = 0;
      let peakBps = 0;

      const downloadChunk = async (): Promise<void> => {
        while (nextChunk < chunkCount) {
          const idx = nextChunk++;

          updateTransfer(fileId, (p) => {
            const c = [...p.chunks];
            c[idx] = { ...c[idx], status: "uploading" };
            return {
              ...p,
              chunks: c,
              activeConcurrency: p.activeConcurrency + 1,
            };
          });

          let success = false;
          for (let attempt = 0; attempt < MAX_RETRIES && !success; attempt++) {
            try {
              buffers[idx] = await api.downloadChunk(fileId, idx);
              success = true;
            } catch {
              if (attempt < MAX_RETRIES - 1) {
                await new Promise((r) =>
                  setTimeout(r, RETRY_BASE_DELAY * Math.pow(2, attempt))
                );
              }
            }
          }

          if (success) {
            completedCount++;
            totalBytes += buffers[idx].byteLength;
            const elapsed = Date.now() - progress.startedAt;
            const bps = elapsed > 0 ? (totalBytes / elapsed) * 1000 : 0;
            const remaining =
              bps > 0
                ? ((manifest.fileSize - totalBytes) / bps) * 1000
                : 0;

            if (bps > peakBps) peakBps = bps;

            updateTransfer(fileId, (p) => {
              const c = [...p.chunks];
              c[idx] = {
                ...c[idx],
                status: "complete",
                bytesTransferred: buffers[idx].byteLength,
              };
              return {
                ...p,
                chunks: c,
                completedChunks: completedCount,
                bytesTransferred: totalBytes,
                activeConcurrency: Math.max(0, p.activeConcurrency - 1),
                throughputBps: bps,
                peakThroughputBps: peakBps,
                estimatedRemainingMs: remaining,
              };
            });
          } else {
            updateTransfer(fileId, (p) => {
              const c = [...p.chunks];
              c[idx] = { ...c[idx], status: "failed" };
              return {
                ...p,
                chunks: c,
                failedChunks: p.failedChunks + 1,
                activeConcurrency: Math.max(0, p.activeConcurrency - 1),
              };
            });
          }
        }
      };

      const workers = Array.from({ length: concurrency }, () =>
        downloadChunk()
      );
      await Promise.all(workers);

      // Reassemble and download
      if (completedCount === chunkCount) {
        const completedAt = Date.now();
        const durationMs = completedAt - progress.startedAt;
        const avgBps = durationMs > 0 ? (manifest.fileSize / durationMs) * 1000 : 0;

        updateTransfer(fileId, (p) => ({
          ...p,
          completedAt,
          averageThroughputBps: avgBps,
          peakThroughputBps: peakBps,
        }));

        const statsEntry: CompletedTransferStats = {
          fileId,
          fileName,
          direction: "download" as const,
          fileSize: manifest.fileSize,
          durationMs,
          averageThroughputBps: avgBps,
          peakThroughputBps: peakBps,
          completedAt,
        };

        setCompletedStats((prev) => [statsEntry, ...prev.slice(0, 19)]);
        addTransferStats(statsEntry);

        const blob = new Blob(buffers, {
          type: manifest.mimeType,
        });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = fileName;
        a.click();
        URL.revokeObjectURL(url);
      }
    },
    [updateTransfer]
  );

  const clearTransfer = useCallback((fileId: string) => {
    setTransfers((prev) => {
      const next = new Map(prev);
      next.delete(fileId);
      return next;
    });
  }, []);

  return { transfers, downloadFile, clearTransfer, completedStats };
}
