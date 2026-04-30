import { useState, useCallback } from "react";
import {
  parallelDownload,
  type ChunkEvent,
  type ManifestEvent,
  type TransferProgressEvent,
} from "@mossaic/sdk/http";
import { getTransferClient } from "@/lib/transfer-client";
import { pathFromFileId } from "@/lib/path-utils";
import { addTransferStats } from "@/lib/transfer-stats";
import type {
  TransferProgress,
  ChunkProgress,
  ChunkStatus,
  CompletedTransferStats,
} from "@app/types";

/**
 * SPA download hook.
 *
 * Collapsed onto `@mossaic/sdk` 's `parallelDownload`. The previous
 * implementation (~205 LoC of AIMD/processChunk/worker/scaleInterval/
 * endgame/blob-reassembly machinery) is replaced by:
 *
 *  1. `parallelDownload(client, fileId, opts)` — drives the entire
 *     transfer; returns a `Uint8Array` of the full file content.
 *  2. `onManifest` → seeds `ChunkProgress[]` UI state + captures
 *     `mimeType` for the eventual `Blob` construction.
 *  3. `onChunkEvent` → maps SDK lifecycle events to `ChunkProgress`
 *     state flips (per-chunk grid).
 *  4. `onProgress` → maps to bytesTransferred/throughputBps/
 *     activeConcurrency aggregate counters.
 *
 * The hook's external surface is preserved 1:1.
 */
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
      const startedAt = Date.now();
      let peakBps = 0;
      let mimeType = "application/octet-stream";

      // Seed initial UI state. `chunks[]` and `bytesTotal` are
      // populated by `onManifest`.
      setTransfers(
        (prev) =>
          new Map(prev).set(fileId, {
            fileId,
            fileName,
            direction: "download",
            totalChunks: 0,
            completedChunks: 0,
            failedChunks: 0,
            bytesTransferred: 0,
            bytesTotal: 0,
            activeConcurrency: 0,
            throughputBps: 0,
            estimatedRemainingMs: 0,
            chunks: [],
            startedAt,
          })
      );

      try {
        const bytes = await parallelDownload(
          await getTransferClient(),
          pathFromFileId(fileId),
          {
            onManifest: (m: ManifestEvent) => {
              mimeType = m.mimeType;
              const chunks: ChunkProgress[] = m.chunks.map((c) => ({
                index: c.index,
                status: "pending" as ChunkStatus,
                bytesTransferred: 0,
                size: c.size,
              }));
              updateTransfer(fileId, (p) => ({
                ...p,
                totalChunks: m.chunkCount,
                bytesTotal: m.size,
                chunks,
              }));
            },
            onChunkEvent: (e: ChunkEvent) => {
              updateTransfer(fileId, (p) => {
                const chunks = [...p.chunks];
                if (chunks[e.index] === undefined) {
                  chunks[e.index] = {
                    index: e.index,
                    status: "pending" as ChunkStatus,
                    bytesTransferred: 0,
                    size: 0,
                  };
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
                return p;
              });
            },
            onProgress: (e: TransferProgressEvent) => {
              const elapsed = Date.now() - startedAt;
              const bps = elapsed > 0 ? (e.uploaded / elapsed) * 1000 : 0;
              if (bps > peakBps) peakBps = bps;
              const remaining =
                bps > 0 ? ((e.total - e.uploaded) / bps) * 1000 : 0;
              updateTransfer(fileId, (p) => ({
                ...p,
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
        const avgBps = durationMs > 0 ? (bytes.byteLength / durationMs) * 1000 : 0;
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
          fileSize: bytes.byteLength,
          durationMs,
          averageThroughputBps: avgBps,
          peakThroughputBps: peakBps,
          completedAt,
        };
        setCompletedStats((prev) => [statsEntry, ...prev.slice(0, 19)]);
        addTransferStats(statsEntry);

        // Trigger browser download via Blob + anchor click. The cast
        // narrows `Uint8Array<ArrayBufferLike>` to a `BlobPart`-compatible
        // shape under strict workers-types — same pattern as
        // `shared/crypto.ts:hashChunk`'s buffer narrowing.
        const blob = new Blob(
          [bytes as Uint8Array<ArrayBuffer>],
          { type: mimeType }
        );
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = fileName;
        a.click();
        URL.revokeObjectURL(url);
      } catch (err) {
        updateTransfer(fileId, (p) => ({
          ...p,
          failedChunks: p.failedChunks + 1,
        }));
        throw err;
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
