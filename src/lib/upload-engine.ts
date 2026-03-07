import { computeChunkSpec } from "@shared/chunking";
import { hashChunk, computeFileHash } from "@shared/crypto";
import { placeChunk } from "@shared/placement";
import {
  CHUNK_SIZE,
  MAX_UPLOAD_CONCURRENCY,
  INITIAL_UPLOAD_CONCURRENCY,
  MIN_UPLOAD_CONCURRENCY,
} from "@shared/constants";
import type { TransferProgress, ChunkProgress, ChunkStatus } from "@shared/types";
import { api } from "./api";

interface UploadCallbacks {
  onProgress: (progress: TransferProgress) => void;
  onComplete: (fileId: string) => void;
  onError: (error: Error) => void;
}

interface ChunkPlan {
  index: number;
  hash: string;
  data: ArrayBuffer;
  size: number;
  shardIndex: number;
}

/**
 * Upload engine with parallel chunked uploads and adaptive concurrency.
 * This is the showpiece feature — per-chunk status, throughput, active connections.
 */
export async function uploadFile(
  file: File,
  parentId: string | null,
  userId: string,
  callbacks: UploadCallbacks
): Promise<void> {
  try {
    // Phase 1: Init upload on server
    const initResult = await api.initUpload({
      fileName: file.name,
      fileSize: file.size,
      mimeType: file.type || "application/octet-stream",
      parentId,
    });

    const { fileId, chunkSize, chunkCount, poolSize } = initResult;

    // Phase 2: Slice, hash, and plan chunks
    const chunkPlans: ChunkPlan[] = [];
    const chunkHashes: string[] = [];
    const chunkProgresses: ChunkProgress[] = [];

    for (let i = 0; i < chunkCount; i++) {
      const offset = i * chunkSize;
      const size = Math.min(chunkSize, file.size - offset);
      const data = await file.slice(offset, offset + size).arrayBuffer();
      const hash = await hashChunk(new Uint8Array(data));
      const shardIndex = placeChunk(userId, fileId, i, poolSize);

      chunkPlans.push({ index: i, hash, data, size, shardIndex });
      chunkHashes.push(hash);
      chunkProgresses.push({
        index: i,
        status: "pending",
        bytesTransferred: 0,
        size,
      });
    }

    // Phase 3: Adaptive parallel upload
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
      chunks: chunkProgresses,
      startedAt: Date.now(),
    };

    callbacks.onProgress({ ...progress });

    let concurrency = Math.min(INITIAL_UPLOAD_CONCURRENCY, chunkCount);
    const queue = [...chunkPlans];
    const inFlight = new Set<Promise<void>>();
    const recentThroughputs: number[] = [];

    while (queue.length > 0 || inFlight.size > 0) {
      while (inFlight.size < concurrency && queue.length > 0) {
        const chunk = queue.shift()!;
        chunkProgresses[chunk.index].status = "uploading";
        progress.activeConcurrency = inFlight.size + 1;
        callbacks.onProgress({ ...progress, chunks: [...chunkProgresses] });

        const startMs = performance.now();
        const promise = api
          .uploadChunk(fileId, chunk.index, chunk.hash, poolSize, chunk.data)
          .then(() => {
            const elapsedMs = performance.now() - startMs;
            const throughput = chunk.size / (elapsedMs / 1000);
            recentThroughputs.push(throughput);
            if (recentThroughputs.length > 10) recentThroughputs.shift();

            chunkProgresses[chunk.index].status = "complete";
            chunkProgresses[chunk.index].bytesTransferred = chunk.size;
            progress.completedChunks++;
            progress.bytesTransferred += chunk.size;
            progress.throughputBps = ewma(progress.throughputBps, throughput, 0.3);

            const remaining = progress.bytesTotal - progress.bytesTransferred;
            progress.estimatedRemainingMs =
              progress.throughputBps > 0
                ? (remaining / progress.throughputBps) * 1000
                : 0;

            // AIMD concurrency adjustment
            if (recentThroughputs.length >= 10) {
              const avgRecent = avg(recentThroughputs.slice(-5));
              const avgOlder = avg(recentThroughputs.slice(0, 5));
              if (avgRecent >= avgOlder * 0.9) {
                concurrency = Math.min(concurrency + 1, MAX_UPLOAD_CONCURRENCY);
              } else if (avgRecent < avgOlder * 0.7) {
                concurrency = Math.max(
                  Math.floor(concurrency * 0.5),
                  MIN_UPLOAD_CONCURRENCY
                );
              }
            }

            inFlight.delete(promise);
            progress.activeConcurrency = inFlight.size;
            callbacks.onProgress({ ...progress, chunks: [...chunkProgresses] });
          })
          .catch(() => {
            chunkProgresses[chunk.index].status = "failed";
            progress.failedChunks++;
            queue.push(chunk); // retry
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

    // Phase 4: Compute file hash and finalize
    const fileHash = await computeFileHash(chunkHashes);
    await api.completeUpload(fileId, fileHash);

    callbacks.onComplete(fileId);
  } catch (err) {
    callbacks.onError(err instanceof Error ? err : new Error(String(err)));
  }
}

function ewma(current: number, sample: number, alpha: number): number {
  if (current === 0) return sample;
  return alpha * sample + (1 - alpha) * current;
}

function avg(arr: number[]): number {
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}
