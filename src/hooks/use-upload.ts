import { useState, useCallback, useRef } from "react";
import { api, ApiError } from "@/lib/api";
import { hashChunk, computeFileHash } from "@shared/crypto";
import { CHUNK_SIZE, MAX_RETRIES, RETRY_BASE_DELAY } from "@shared/constants";
import { addTransferStats } from "@/lib/transfer-stats";
import type { TransferProgress, ChunkProgress, ChunkStatus, CompletedTransferStats } from "@shared/types";

interface UploadJob {
  file: File;
  fileId: string;
  chunkCount: number;
  chunkSize: number;
  poolSize: number;
  parentId: string | null;
}

export function useUpload(onComplete?: () => void) {
  const [transfers, setTransfers] = useState<Map<string, TransferProgress>>(
    new Map()
  );
  const [completedStats, setCompletedStats] = useState<CompletedTransferStats[]>([]);
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
      // Initialize
      const initRes = await api.uploadInit({
        fileName: file.name,
        fileSize: file.size,
        mimeType: file.type || "application/octet-stream",
        parentId,
      });

      const { fileId, chunkSize, chunkCount, poolSize } = initRes;

      const chunks: ChunkProgress[] = Array.from(
        { length: chunkCount },
        (_, i) => ({
          index: i,
          status: "pending" as ChunkStatus,
          bytesTransferred: 0,
          size:
            i === chunkCount - 1
              ? file.size - i * chunkSize
              : chunkSize,
        })
      );

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
        startedAt: Date.now(),
      };

      setTransfers((prev) => new Map(prev).set(fileId, progress));

      // Upload chunks with concurrency
      const concurrency = Math.min(6, chunkCount);
      const chunkHashes: string[] = new Array(chunkCount);
      let nextChunk = 0;
      let completedCount = 0;
      let failedCount = 0;
      let totalBytes = 0;
      let peakBps = 0;

      const uploadChunk = async (): Promise<void> => {
        while (nextChunk < chunkCount) {
          const idx = nextChunk++;
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
            try {
              await api.uploadChunk(fileId, idx, buffer, hash, poolSize);
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
            totalBytes += data.byteLength;
            const elapsed = Date.now() - progress.startedAt;
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
          }
        }
      };

      // Run workers
      const workers = Array.from({ length: concurrency }, () => uploadChunk());
      await Promise.all(workers);

      if (failedCount === 0) {
        // Complete the upload
        const fileHash = await computeFileHash(chunkHashes);
        await api.uploadComplete(fileId, fileHash);

        const completedAt = Date.now();
        const durationMs = completedAt - progress.startedAt;
        const avgBps = durationMs > 0 ? (file.size / durationMs) * 1000 : 0;

        // Record final speed stats
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
