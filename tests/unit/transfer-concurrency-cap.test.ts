import { describe, it, expect } from "vitest";
import { parallelDownload } from "../../sdk/src/transfer";
import type { HttpVFS } from "../../sdk/src/http";

/**
 * Phase 41 Fix 4 — transfer.ts state.active under-reports concurrency
 * during endgame fanout (audit 40C top-1).
 *
 * Background: parallelDownload (and parallelUpload) drive a fixed
 * pool of `lanes` plus an "endgame" mechanism that, once ≥
 * endgameThreshold of chunks have completed AND ≤ endgameMaxFanout
 * remain pending, spawns ADDITIONAL lanes that race the same
 * pending chunks (idempotent re-PUTs / re-fetches). The intent is
 * tail-latency hedging: a slow shard no longer blocks the whole
 * download.
 *
 * Pre-Phase-41 bug: `state.active` was set ONCE at line 524/821 to
 * `Math.min(state.target, pending.size)` and never updated when
 * endgame fired. The `currentParallelism` field reported via
 * `onProgress` therefore lied — the metric showed (e.g.) 4 while
 * the engine was actually running 4 + 8 = 12 in-flight requests.
 *
 * Real-world impact:
 *  - The test suite's "respects concurrency.max bound" assertion
 *    saw a stale 4/8/etc. and passed even though the engine
 *    over-spawned.
 *  - Consumers wiring `currentParallelism` into adaptive-rate UIs
 *    or backend rate limiters got the wrong number.
 *
 * Fix: when endgame fanout fires, `state.active += extra` so the
 * reported metric reflects the real fanout for the duration of
 * the endgame window.
 *
 * Three pinning tests:
 *   TC1 — Pre-endgame: currentParallelism never exceeds initial
 *         lane count (= state.target after adaptive ramp) by the
 *         time the first onProgress fires.
 *   TC2 — During/after endgame: max observed currentParallelism
 *         equals initial lanes + endgameMaxFanout (bounded). This
 *         is the load-bearing post-fix behaviour: the metric
 *         catches up to reality.
 *   TC3 — Endgame-off (single-chunk file): metric remains constant
 *         at lane count; endgame never fires (no fanout headroom).
 */

interface RequestRow {
  idx: number;
  delayMs: number;
}

/**
 * Build a stub HTTP client serving N chunks of S bytes. Each chunk's
 * fetch optionally simulates latency so we can shape the pending
 * tail and force endgame to fire deterministically.
 *
 * `delays[idx]` controls the per-chunk latency. Default 0.
 */
function makeClient(
  totalChunks: number,
  chunkSize: number,
  delays: Map<number, number>
): { client: HttpVFS; requests: RequestRow[] } {
  const requests: RequestRow[] = [];
  const client = {
    multipartDownloadToken: async (_p: string) => {
      const chunks: { index: number; hash: string; size: number }[] = [];
      // Synthesise stable per-index hashes so verifyHash agrees with
      // the bytes we serve from fetchChunkByHash.
      for (let i = 0; i < totalChunks; i++) {
        const bytes = new Uint8Array(chunkSize).fill(i & 0xff);
        const digest = await crypto.subtle.digest("SHA-256", bytes);
        const hash = Array.from(new Uint8Array(digest))
          .map((b) => b.toString(16).padStart(2, "0"))
          .join("");
        chunks.push({ index: i, hash, size: chunkSize });
      }
      return {
        token: "tok",
        expiresAtMs: Date.now() + 60_000,
        manifest: {
          fileId: "stub-file",
          size: totalChunks * chunkSize,
          chunkSize,
          chunkCount: totalChunks,
          chunks,
          inlined: false,
        },
      };
    },
    fetchChunkByHash: async (
      _fileId: string,
      idx: number,
      _hash: string,
      _token: string,
      _path: string,
      _signal?: AbortSignal
    ) => {
      const delayMs = delays.get(idx) ?? 0;
      requests.push({ idx, delayMs });
      if (delayMs > 0) {
        await new Promise((r) => setTimeout(r, delayMs));
      }
      return new Uint8Array(chunkSize).fill(idx & 0xff);
    },
    readFile: async (_p: string) => new Uint8Array(0),
  } as unknown as HttpVFS;
  return { client, requests };
}

describe("Phase 41 Fix 4 — transfer state.active reflects real fanout", () => {
  it("TC1 — pre-endgame, currentParallelism ≤ initial lane count", async () => {
    // Small fast file: 4 chunks all served immediately. Endgame
    // threshold is 0.9 by default — we'll set it to a value the
    // download never reaches so endgame stays off, isolating the
    // pre-endgame metric.
    const { client } = makeClient(4, 4, new Map());
    const samples: number[] = [];
    await parallelDownload(client, "/a.bin", {
      concurrency: { initial: 4, max: 4, min: 1 },
      // Endgame off (threshold > 1.0 means it can never fire).
      endgameThreshold: 2.0,
      onProgress: (e) => samples.push(e.currentParallelism),
    });
    // Every observed currentParallelism must respect the cap.
    for (const v of samples) {
      expect(v).toBeLessThanOrEqual(4);
    }
  });

  it("TC2 — during endgame fanout, currentParallelism is bumped by `extra`", async () => {
    // 6 chunks. Lanes start at 2, max 2. endgameThreshold=0.5,
    // endgameMaxFanout=4. Workflow:
    //   - Chunks 0..2 complete fast → chunksDone=3, totalChunks=6
    //     → 3/6 = 0.5 ≥ threshold → endgame fires.
    //   - At endgame fire time, pending.size = 3 (chunks 3, 4, 5).
    //     extra = min(3, 4) = 3. state.active += 3 → 2 + 3 = 5.
    //   - Chunks 3..5 are slow so the metric is observed during
    //     the endgame window before they all drain.
    //
    // We seed delays so chunks 3..5 take long enough for an
    // onProgress tick to fire mid-endgame. The throttling is
    // "100ms or chunksDone===total"; we set delays to 250ms each
    // so a tick fires after the 3rd chunk completes.
    const delays = new Map<number, number>();
    delays.set(3, 250);
    delays.set(4, 260);
    delays.set(5, 270);
    const { client } = makeClient(6, 4, delays);
    const samples: number[] = [];
    let endgameWasReported = false;
    await parallelDownload(client, "/b.bin", {
      concurrency: { initial: 2, max: 2, min: 1 },
      endgameThreshold: 0.5,
      endgameMaxFanout: 4,
      onProgress: (e) => {
        samples.push(e.currentParallelism);
        if (e.endgameActive) endgameWasReported = true;
      },
    });
    // The bug-state metric would have stayed at 2 throughout. The
    // post-fix metric reports the bumped count. We assert at LEAST
    // ONE sample exceeds the initial lane count of 2 — that's the
    // proof that state.active was updated.
    const peak = samples.length > 0 ? Math.max(...samples) : 0;
    expect(peak).toBeGreaterThan(2);
    // And the bump never exceeds (initial + endgameMaxFanout).
    expect(peak).toBeLessThanOrEqual(2 + 4);
    // Endgame DID fire — sanity check that we exercised the branch.
    expect(endgameWasReported).toBe(true);
  });

  it("TC3 — endgame never fires for a 1-chunk file; metric stays at 1", async () => {
    const { client } = makeClient(1, 8, new Map());
    const samples: number[] = [];
    let endgameWasReported = false;
    await parallelDownload(client, "/c.bin", {
      concurrency: { initial: 4, max: 4, min: 1 },
      endgameThreshold: 0.0, // would fire on any progress
      endgameMaxFanout: 8,
      onProgress: (e) => {
        samples.push(e.currentParallelism);
        if (e.endgameActive) endgameWasReported = true;
      },
    });
    // Single-chunk: only 1 lane spawned (Math.min(target=4, pending=1)).
    // endgameLane sees pending.size === 0 immediately after the
    // chunk completes, so the if-branch never enters.
    for (const v of samples) {
      expect(v).toBe(1);
    }
    expect(endgameWasReported).toBe(false);
  });
});
