import { useState, useCallback, useRef } from "react";
import { api } from "@/lib/api";
// Phase 17: import the chunk hashing + AIMD primitives from the
// `@mossaic/sdk/http` browser-safe entry instead of reaching into
// `@shared/*` directly. The `/http` entry omits the Worker-only
// DO class re-exports (UserDO, ShardDO) so it tree-shakes cleanly
// in the browser bundle. Behaviour is bit-identical to the
// pre-Phase-17 `@shared/crypto` + `@shared/aimd` imports — the SDK
// re-exports the SAME implementations.
import {
  hashChunk,
  computeFileHash,
  AIMDController,
} from "@mossaic/sdk/http";
import { MAX_RETRIES, RETRY_BASE_DELAY } from "@shared/constants";
import { addTransferStats } from "@/lib/transfer-stats";
import type { TransferProgress, ChunkProgress, ChunkStatus, CompletedTransferStats } from "@app/types";

export function useUpload(onComplete?: () => void) {
  const [transfers, setTransfers] = useState<Map<string, TransferProgress>>(
    new Map()
  );
  const [completedStats, setCompletedStats] = useState<
    CompletedTransferStats[]
  >([]);
  const activeRef = useRef(false);

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

  const uploadFile = useCallback(
    async (file: File, parentId: string | null = null) => {
      // Initialize — server computes adaptive chunk size
      const initRes = await api.uploadInit({
        fileName: file.name,
        fileSize: file.size,
        mimeType: file.type || "application/octet-stream",
        parentId,
      });

      const { fileId, chunkSize, chunkCount, poolSize } = initRes;

      // Build initial chunk progress array
      const chunks: ChunkProgress[] = Array.from(
        { length: chunkCount },
        (_, i) => ({
          index: i,
          status: "pending" as ChunkStatus,
          bytesTransferred: 0,
          size:
            i === chunkCount - 1 ? file.size - i * chunkSize : chunkSize,
        })
      );

      const startedAt = Date.now();
      const progress: TransferProgress = {
        fileId,
        fileName: file.name,
        direction: "upload",
        totalChunks: chunkCount,
        completedChunks: 0,
        failedChunks: 0,
        bytesTransferred: 0,
        bytesTotal: file.size,
        activeConcurrency: 0,
        throughputBps: 0,
        estimatedRemainingMs: 0,
        chunks,
        startedAt,
      };

      setTransfers((prev) => new Map(prev).set(fileId, progress));

      // -- AIMD-controlled upload --
      const controller = new AIMDController();
      const chunkHashes: string[] = new Array(chunkCount);
      const chunkDone: boolean[] = new Array(chunkCount).fill(false);
      let nextChunk = 0;
      let completedCount = 0;
      let failedCount = 0;
      let totalBytes = 0;
      let peakBps = 0;
      let activeWorkers = 0;
      let endgameActive = false;

      /**
       * Process a single chunk index: hash, upload with retry, update progress.
       * Returns true if the chunk was successfully uploaded.
       */
      const processChunk = async (idx: number): Promise<boolean> => {
        // Skip if already completed (possible in endgame mode)
        if (chunkDone[idx]) return true;

        const start = idx * chunkSize;
        const end = Math.min(start + chunkSize, file.size);
        const blob = file.slice(start, end);
        const buffer = await blob.arrayBuffer();
        const data = new Uint8Array(buffer);
        const hash = await hashChunk(data);
        chunkHashes[idx] = hash;

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
          // Another worker may have completed this chunk (endgame)
          if (chunkDone[idx]) {
            updateTransfer(fileId, (p) => ({
              ...p,
              activeConcurrency: Math.max(0, p.activeConcurrency - 1),
            }));
            return true;
          }

          const t0 = performance.now();
          try {
            await api.uploadChunk(fileId, idx, buffer, hash, poolSize);
            const rtt = performance.now() - t0;
            controller.onSuccess(rtt);
            success = true;
          } catch {
            const rtt = performance.now() - t0;
            controller.onFailure();
            if (attempt < MAX_RETRIES - 1) {
              // Backoff using AIMD-informed RTO, capped by exponential backoff
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
          totalBytes += data.byteLength;
          const elapsed = Date.now() - startedAt;
          const bps = elapsed > 0 ? (totalBytes / elapsed) * 1000 : 0;
          const remaining =
            bps > 0 ? ((file.size - totalBytes) / bps) * 1000 : 0;
          if (bps > peakBps) peakBps = bps;

          updateTransfer(fileId, (p) => {
            const c = [...p.chunks];
            c[idx] = {
              ...c[idx],
              status: "complete",
              bytesTransferred: data.byteLength,
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
          // Completed by another worker (endgame duplicate)
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
              failedChunks: failedCount,
              activeConcurrency: Math.max(0, p.activeConcurrency - 1),
            };
          });
          return false;
        }
      };

      /**
       * Dynamic worker: pulls chunks from the queue, adjusts to AIMD window.
       * When endgame mode activates, workers also duplicate remaining chunks.
       */
      const worker = async (): Promise<void> => {
        activeWorkers++;
        try {
          while (nextChunk < chunkCount || endgameActive) {
            // Normal mode: grab next chunk from queue
            if (nextChunk < chunkCount) {
              const idx = nextChunk++;
              await processChunk(idx);
            } else if (endgameActive) {
              // Endgame: find any incomplete chunks and send duplicate requests
              const remaining = chunkDone
                .map((done, i) => (done ? -1 : i))
                .filter((i) => i >= 0);
              if (remaining.length === 0) break;
              // Pick a random remaining chunk to avoid thundering herd
              const idx =
                remaining[Math.floor(Math.random() * remaining.length)];
              await processChunk(idx);
            } else {
              break;
            }

            // Check if all done
            if (completedCount + failedCount >= chunkCount) break;
          }
        } finally {
          activeWorkers--;
        }
      };

      // Start initial workers up to AIMD window
      const initialConcurrency = Math.min(
        controller.getMaxConcurrency(),
        chunkCount
      );
      const workerPromises: Promise<void>[] = [];

      for (let i = 0; i < initialConcurrency; i++) {
        workerPromises.push(worker());
      }

      // Spawn additional workers as AIMD window grows
      const scaleInterval = setInterval(() => {
        if (completedCount + failedCount >= chunkCount) {
          clearInterval(scaleInterval);
          return;
        }

        const target = Math.min(
          controller.getMaxConcurrency(),
          chunkCount - completedCount - failedCount
        );

        // Activate endgame mode: >90% done AND total > 10 chunks
        if (
          !endgameActive &&
          chunkCount > 10 &&
          completedCount > chunkCount * 0.9
        ) {
          endgameActive = true;
        }

        // Spawn more workers if needed
        while (activeWorkers < target) {
          workerPromises.push(worker());
        }
      }, 100);

      await Promise.all(workerPromises);
      clearInterval(scaleInterval);

      // -- Finalize --
      if (failedCount === 0) {
        const fileHash = await computeFileHash(chunkHashes);
        await api.uploadComplete(fileId, fileHash);

        const completedAt = Date.now();
        const durationMs = completedAt - startedAt;
        const avgBps =
          durationMs > 0 ? (file.size / durationMs) * 1000 : 0;

        updateTransfer(fileId, (p) => ({
          ...p,
          completedAt,
          averageThroughputBps: avgBps,
          peakThroughputBps: peakBps,
        }));

        const statsEntry: CompletedTransferStats = {
          fileId,
          fileName: file.name,
          direction: "upload" as const,
          fileSize: file.size,
          durationMs,
          averageThroughputBps: avgBps,
          peakThroughputBps: peakBps,
          completedAt,
        };

        setCompletedStats((prev) => [statsEntry, ...prev.slice(0, 19)]);
        addTransferStats(statsEntry);

        onComplete?.();
      }

      return { fileId, failedCount };
    },
    [updateTransfer, onComplete]
  );

  const clearTransfer = useCallback((fileId: string) => {
    setTransfers((prev) => {
      const next = new Map(prev);
      next.delete(fileId);
      return next;
    });
  }, []);

  return {
    transfers,
    uploadFile,
    clearTransfer,
    completedStats,
  };
}
