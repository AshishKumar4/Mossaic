import { useState, useCallback } from "react";
import { api } from "@/lib/api";
import { MAX_RETRIES, RETRY_BASE_DELAY } from "@shared/constants";
import { AIMDController } from "@shared/aimd";
import { addTransferStats } from "@/lib/transfer-stats";
import type { TransferProgress, ChunkProgress, ChunkStatus, CompletedTransferStats } from "@app/types";

export function useDownload() {
  const [transfers, setTransfers] = useState<Map<string, TransferProgress>>(
    new Map()
  );
  const [completedStats, setCompletedStats] = useState<
    CompletedTransferStats[]
  >([]);

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

      const startedAt = Date.now();
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
        startedAt,
      };

      setTransfers((prev) => new Map(prev).set(fileId, progress));

      // -- AIMD-controlled download --
      const controller = new AIMDController();
      const buffers: ArrayBuffer[] = new Array(chunkCount);
      const chunkDone: boolean[] = new Array(chunkCount).fill(false);
      let nextChunk = 0;
      let completedCount = 0;
      let failedCount = 0;
      let totalBytes = 0;
      let peakBps = 0;
      let activeWorkers = 0;
      let endgameActive = false;

      /**
       * Process a single chunk: download with retry, feed RTT to AIMD.
       */
      const processChunk = async (idx: number): Promise<boolean> => {
        if (chunkDone[idx]) return true;

        updateTransfer(fileId, (p) => {
          const c = [...p.chunks];
          c[idx] = { ...c[idx], status: "uploading" }; // "uploading" = "transferring" in the progress UI
          return {
            ...p,
            chunks: c,
            activeConcurrency: p.activeConcurrency + 1,
          };
        });

        let success = false;
        for (let attempt = 0; attempt < MAX_RETRIES && !success; attempt++) {
          if (chunkDone[idx]) {
            updateTransfer(fileId, (p) => ({
              ...p,
              activeConcurrency: Math.max(0, p.activeConcurrency - 1),
            }));
            return true;
          }

          const t0 = performance.now();
          try {
            buffers[idx] = await api.downloadChunk(fileId, idx);
            const rtt = performance.now() - t0;
            controller.onSuccess(rtt);
            success = true;
          } catch {
            controller.onFailure();
            if (attempt < MAX_RETRIES - 1) {
              const backoff = Math.min(
                RETRY_BASE_DELAY * Math.pow(2, attempt),
                controller.getRTO()
              );
              await new Promise((r) => setTimeout(r, backoff));
            }
          }
        }

        if (success && !chunkDone[idx]) {
          chunkDone[idx] = true;
          completedCount++;
          totalBytes += buffers[idx].byteLength;
          const elapsed = Date.now() - startedAt;
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
          return true;
        } else if (chunkDone[idx]) {
          updateTransfer(fileId, (p) => ({
            ...p,
            activeConcurrency: Math.max(0, p.activeConcurrency - 1),
          }));
          return true;
        } else {
          failedCount++;
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
          return false;
        }
      };

      /**
       * Dynamic worker: pulls chunks, adjusts to AIMD window,
       * enters endgame mode for the tail.
       */
      const worker = async (): Promise<void> => {
        activeWorkers++;
        try {
          while (nextChunk < chunkCount || endgameActive) {
            if (nextChunk < chunkCount) {
              const idx = nextChunk++;
              await processChunk(idx);
            } else if (endgameActive) {
              const remaining = chunkDone
                .map((done, i) => (done ? -1 : i))
                .filter((i) => i >= 0);
              if (remaining.length === 0) break;
              const idx =
                remaining[Math.floor(Math.random() * remaining.length)];
              await processChunk(idx);
            } else {
              break;
            }

            if (completedCount + failedCount >= chunkCount) break;
          }
        } finally {
          activeWorkers--;
        }
      };

      // Start initial workers
      const initialConcurrency = Math.min(
        controller.getMaxConcurrency(),
        chunkCount
      );
      const workerPromises: Promise<void>[] = [];

      for (let i = 0; i < initialConcurrency; i++) {
        workerPromises.push(worker());
      }

      // Scale workers as AIMD window grows
      const scaleInterval = setInterval(() => {
        if (completedCount + failedCount >= chunkCount) {
          clearInterval(scaleInterval);
          return;
        }

        const target = Math.min(
          controller.getMaxConcurrency(),
          chunkCount - completedCount - failedCount
        );

        // Endgame: >90% done AND total > 10 chunks
        if (
          !endgameActive &&
          chunkCount > 10 &&
          completedCount > chunkCount * 0.9
        ) {
          endgameActive = true;
        }

        while (activeWorkers < target) {
          workerPromises.push(worker());
        }
      }, 100);

      await Promise.all(workerPromises);
      clearInterval(scaleInterval);

      // -- Reassemble and trigger browser download --
      if (completedCount === chunkCount) {
        const completedAt = Date.now();
        const durationMs = completedAt - startedAt;
        const avgBps =
          durationMs > 0
            ? (manifest.fileSize / durationMs) * 1000
            : 0;

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
