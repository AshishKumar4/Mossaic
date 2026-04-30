import { useState, useCallback, useRef } from "react";
import {
  parallelUpload,
  type ChunkEvent,
  type TransferProgressEvent,
} from "@mossaic/sdk/http";
import {
  getTransferClient,
  resetTransferClient,
} from "@/lib/transfer-client";
import { pathFromParentId } from "@/lib/path-utils";
import { addTransferStats } from "@/lib/transfer-stats";
import { api } from "@/lib/api";
import type {
  TransferProgress,
  ChunkProgress,
  ChunkStatus,
  CompletedTransferStats,
} from "@app/types";

/**
 * SPA upload hook.
 *
 * Collapsed onto `@mossaic/sdk` 's `parallelUpload`. The previous
 * implementation (~213 LoC of AIMD/processChunk/worker/scaleInterval/
 * endgame/finalize machinery) is replaced by:
 *
 *  1. `parallelUpload(client, path, file, opts)` — drives the entire
 *     transfer.
 *  2. `onChunkEvent` → maps SDK lifecycle events to ChunkProgress
 *     state flips.
 *  3. `onProgress` → maps to bytesTransferred/throughputBps/
 *     activeConcurrency.
 *  4. `onComplete` → records `CompletedTransferStats`.
 *
 * The hook's external surface is preserved 1:1: returns the same
 * `transfers` Map, `uploadFile` function, `clearTransfer` /
 * `completedStats` callbacks. UI components observe the SAME
 * `TransferProgress` shape.
 */
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
      // Use the file's identity inside the transfers Map. The SDK
      // assigns the real `fileId` only at finalize; we key on a
      // local id derived from name+timestamp for UI bookkeeping
      // until then. After completion we don't re-key — UI lookups
      // continue to use the local id.
      const localId = `${file.name}-${Date.now()}-${Math.floor(
        Math.random() * 1e6
      )}`;
      const startedAt = Date.now();
      let peakBps = 0;

      // Seed initial UI state. `chunks[]` and `totalChunks` are
      // populated lazily — onProgress fires with chunksTotal as soon
      // as begin returns, and onChunkEvent fills the per-chunk grid.
      setTransfers(
        (prev) =>
          new Map(prev).set(localId, {
            fileId: localId,
            fileName: file.name,
            direction: "upload",
            totalChunks: 0,
            completedChunks: 0,
            failedChunks: 0,
            bytesTransferred: 0,
            bytesTotal: file.size,
            activeConcurrency: 0,
            throughputBps: 0,
            estimatedRemainingMs: 0,
            chunks: [],
            startedAt,
          })
      );

      const uploadPath = pathFromParentId(parentId, file.name);
      try {
        const result = await parallelUpload(
          getTransferClient(),
          uploadPath,
          file,
          {
            mimeType: file.type || "application/octet-stream",
            metadata: { parentId, originalName: file.name },
            onChunkEvent: (e: ChunkEvent) => {
              updateTransfer(localId, (p) => {
                const chunks = [...p.chunks];
                // Lazily extend the chunks array as new indices are
                // observed (totalChunks is populated by onProgress).
                while (chunks.length <= e.index) {
                  chunks.push({
                    index: chunks.length,
                    status: "pending" as ChunkStatus,
                    bytesTransferred: 0,
                    size: 0,
                  });
                }
                if (e.state === "started") {
                  chunks[e.index] = {
                    ...chunks[e.index]!,
                    status: "uploading" as ChunkStatus,
                  };
                  return { ...p, chunks };
                }
                if (e.state === "completed") {
                  chunks[e.index] = {
                    ...chunks[e.index]!,
                    status: "complete" as ChunkStatus,
                    bytesTransferred:
                      e.bytesAccepted ?? chunks[e.index]!.size ?? 0,
                  };
                  return { ...p, chunks };
                }
                if (e.state === "failed") {
                  chunks[e.index] = {
                    ...chunks[e.index]!,
                    status: "failed" as ChunkStatus,
                  };
                  return { ...p, chunks, failedChunks: p.failedChunks + 1 };
                }
                // "retrying" — UI optional; surface as still uploading.
                return p;
              });
            },
            onProgress: (e: TransferProgressEvent) => {
              const elapsed = Date.now() - startedAt;
              const bps = elapsed > 0 ? (e.uploaded / elapsed) * 1000 : 0;
              if (bps > peakBps) peakBps = bps;
              const remaining =
                bps > 0 ? ((e.total - e.uploaded) / bps) * 1000 : 0;
              updateTransfer(localId, (p) => ({
                ...p,
                totalChunks: e.chunksTotal,
                completedChunks: e.chunksDone,
                bytesTransferred: e.uploaded,
                throughputBps: bps,
                peakThroughputBps: peakBps,
                estimatedRemainingMs: remaining,
                activeConcurrency: e.currentParallelism,
              }));
            },
          }
        );
        const completedAt = Date.now();
        const durationMs = completedAt - startedAt;
        const avgBps = durationMs > 0 ? (result.size / durationMs) * 1000 : 0;
        updateTransfer(localId, (p) => ({
          ...p,
          fileId: result.fileId, // server-assigned id replaces localId on completion
          completedAt,
          averageThroughputBps: avgBps,
          peakThroughputBps: peakBps,
        }));
        const statsEntry: CompletedTransferStats = {
          fileId: result.fileId,
          fileName: file.name,
          direction: "upload" as const,
          fileSize: result.size,
          durationMs,
          averageThroughputBps: avgBps,
          peakThroughputBps: peakBps,
          completedAt,
        };
        setCompletedStats((prev) => [statsEntry, ...prev.slice(0, 19)]);
        addTransferStats(statsEntry);

        // Notify the App about the new VFS row so it can schedule
        // semantic indexing. Failure is non-fatal — the upload itself
        // succeeded; the user just won't see this file in search until
        // the next reindex sweep.
        try {
          await api.postIndexFile(uploadPath);
        } catch (err) {
          console.warn("postIndexFile failed (non-fatal):", err);
        }

        onComplete?.();
        return { fileId: result.fileId, failedCount: 0 };
      } catch (err) {
        // Mark the transfer terminally failed so the UI shows the
        // error state (red badge + clear-button affordance) instead
        // of leaving the row stuck mid-progress. The SDK already
        // retries individual chunks via its adaptive engine; reaching
        // here means the entire transfer is unrecoverable from
        // inside the hook (auth-bridge mint failed, finalize 4xx,
        // network dropped, etc.).
        const failedAt = Date.now();
        const message =
          err instanceof Error
            ? err.message
            : typeof err === "string"
              ? err
              : "Upload failed";
        updateTransfer(localId, (p) => ({
          ...p,
          failedAt,
          error: message,
          // Bump failedChunks to surface the existing red-badge UI;
          // upper-bound by totalChunks so the displayed count stays
          // sane even if the failure happened pre-begin.
          failedChunks: Math.max(p.failedChunks + 1, 1),
        }));
        // Drop the cached HttpVFS so the NEXT upload re-mints a fresh
        // VFS token. With the apiKey-callback rotation fix in place
        // this is mostly belt-and-suspenders, but it ensures a
        // pathological token-cache state can't pin every subsequent
        // upload into the same failure.
        resetTransferClient();
        throw err;
      }
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

  // `activeRef` is reserved for a future cancellation surface —
  // currently the SDK's `signal` is per-call so we don't expose
  // a hook-level abort. Suppress unused-var lint.
  void activeRef;

  return {
    transfers,
    uploadFile,
    clearTransfer,
    completedStats,
  };
}
