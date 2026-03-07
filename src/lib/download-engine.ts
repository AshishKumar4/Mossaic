import { hashChunk } from "@shared/crypto";
import type { FileManifest, TransferProgress, ChunkProgress } from "@shared/types";
import {
  MAX_DOWNLOAD_CONCURRENCY,
  MIN_UPLOAD_CONCURRENCY,
} from "@shared/constants";
import { api } from "./api";

interface DownloadCallbacks {
  onProgress: (progress: TransferProgress) => void;
  onComplete: (blob: Blob, fileName: string) => void;
  onError: (error: Error) => void;
}

/**
 * Download engine with parallel chunked download and reassembly.
 */
export async function downloadFile(
  fileId: string,
  callbacks: DownloadCallbacks
): Promise<void> {
  try {
    // Phase 1: Get manifest
    const manifest = await api.getManifest(fileId);

    // Phase 2: Parallel chunk download
    const chunkProgresses: ChunkProgress[] = manifest.chunks.map((c) => ({
      index: c.index,
      status: "pending" as const,
      bytesTransferred: 0,
      size: c.size,
    }));

    const progress: TransferProgress = {
      fileId: manifest.fileId,
      fileName: manifest.fileName,
      direction: "download",
      totalChunks: manifest.chunkCount,
      completedChunks: 0,
      failedChunks: 0,
      bytesTransferred: 0,
      bytesTotal: manifest.fileSize,
      activeConcurrency: 0,
      throughputBps: 0,
      estimatedRemainingMs: 0,
      chunks: chunkProgresses,
      startedAt: Date.now(),
    };

    callbacks.onProgress({ ...progress });

    const chunkBuffers = new Map<number, ArrayBuffer>();
    let concurrency = Math.min(30, manifest.chunkCount);
    const queue = [...manifest.chunks];
    const inFlight = new Set<Promise<void>>();
    const recentThroughputs: number[] = [];

    while (queue.length > 0 || inFlight.size > 0) {
      while (inFlight.size < concurrency && queue.length > 0) {
        const chunk = queue.shift()!;
        chunkProgresses[chunk.index].status = "uploading"; // reusing for "downloading"
        progress.activeConcurrency = inFlight.size + 1;
        callbacks.onProgress({ ...progress, chunks: [...chunkProgresses] });

        const startMs = performance.now();
        const promise = api
          .downloadChunk(manifest.fileId, chunk.index)
          .then(async (data) => {
            const elapsedMs = performance.now() - startMs;
            const throughput = data.byteLength / (elapsedMs / 1000);
            recentThroughputs.push(throughput);
            if (recentThroughputs.length > 10) recentThroughputs.shift();

            // Verify integrity
            const actualHash = await hashChunk(new Uint8Array(data));
            if (actualHash !== chunk.hash) {
              throw new Error(`Chunk ${chunk.index} integrity failed`);
            }

            chunkBuffers.set(chunk.index, data);
            chunkProgresses[chunk.index].status = "complete";
            chunkProgresses[chunk.index].bytesTransferred = data.byteLength;
            progress.completedChunks++;
            progress.bytesTransferred += data.byteLength;
            progress.throughputBps = ewma(progress.throughputBps, throughput, 0.3);

            const remaining = progress.bytesTotal - progress.bytesTransferred;
            progress.estimatedRemainingMs =
              progress.throughputBps > 0
                ? (remaining / progress.throughputBps) * 1000
                : 0;

            inFlight.delete(promise);
            progress.activeConcurrency = inFlight.size;
            callbacks.onProgress({ ...progress, chunks: [...chunkProgresses] });
          })
          .catch(() => {
            chunkProgresses[chunk.index].status = "failed";
            progress.failedChunks++;
            queue.push(chunk);
            concurrency = Math.max(
              Math.floor(concurrency * 0.75),
              MIN_UPLOAD_CONCURRENCY
            );
            inFlight.delete(promise);
          });

        inFlight.add(promise);
      }

      if (inFlight.size > 0) {
        await Promise.race(inFlight);
      }
    }

    // Phase 3: Reassemble in order
    const parts: ArrayBuffer[] = [];
    for (let i = 0; i < manifest.chunkCount; i++) {
      const buf = chunkBuffers.get(i);
      if (!buf) throw new Error(`Missing chunk ${i}`);
      parts.push(buf);
    }

    const blob = new Blob(parts, { type: manifest.mimeType });
    callbacks.onComplete(blob, manifest.fileName);
  } catch (err) {
    callbacks.onError(err instanceof Error ? err : new Error(String(err)));
  }
}

function ewma(current: number, sample: number, alpha: number): number {
  if (current === 0) return sample;
  return alpha * sample + (1 - alpha) * current;
}

/**
 * Trigger a browser download from a Blob.
 */
export function saveBlobAs(blob: Blob, fileName: string): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}
