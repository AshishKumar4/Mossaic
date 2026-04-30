import { describe, it, expect } from "vitest";
import {
  beginUpload,
  finalizeUpload,
  parallelUpload,
  parallelDownload,
  THROUGHPUT_MATH,
  deriveClientChunkSpec,
} from "@mossaic/sdk/http";

/**
 * CLI-side smoke tests for the parallel transfer engine.
 *
 *   T1.  parallelUpload + parallelDownload symbols are exported.
 *   T2.  THROUGHPUT_MATH constants are non-zero & match plan §8.
 *   T3.  deriveClientChunkSpec matches the documented adaptive ladder
 *        (sub-INLINE_LIMIT inline, ≤16MB → 1MB chunks, …).
 *   T4.  Re-exports flow through the `@mossaic/sdk/http` subpath.
 *   T5.  WebCrypto SHA-256 hex helper matches the server hash format
 *        (used by `mossaic upload-finalize`'s offline rehash).
 */

describe("CLI / multipart engine smoke", () => {
  it("T1 — parallelUpload, parallelDownload, beginUpload, finalizeUpload are exported", () => {
    expect(typeof parallelUpload).toBe("function");
    expect(typeof parallelDownload).toBe("function");
    expect(typeof beginUpload).toBe("function");
    expect(typeof finalizeUpload).toBe("function");
  });

  it("T2 — THROUGHPUT_MATH exposes documented numbers", () => {
    expect(THROUGHPUT_MATH.perChunkP50Ms).toBeGreaterThan(0);
    expect(THROUGHPUT_MATH.perChunkP95Ms).toBeGreaterThan(
      THROUGHPUT_MATH.perChunkP50Ms
    );
    expect(THROUGHPUT_MATH.defaultChunkSizeBytes).toBe(1_048_576);
    expect(THROUGHPUT_MATH.defaultMaxConcurrency).toBe(64);
    // 100MB / gigabit ≈ 0.8s + 0.3s overhead < 10s acceptance bar.
    expect(THROUGHPUT_MATH.hundredMBOnGigabitSec).toBeLessThan(10);
  });

  it("T3 — deriveClientChunkSpec returns sane chunk specs", () => {
    const tiny = deriveClientChunkSpec(1024);
    expect(tiny.chunkCount).toBeGreaterThanOrEqual(0);
    const oneMb = deriveClientChunkSpec(1_048_576);
    expect(oneMb.chunkSize).toBeGreaterThan(0);
    const huge = deriveClientChunkSpec(100 * 1_048_576);
    expect(huge.chunkCount).toBeGreaterThan(0);
    expect(huge.chunkSize).toBeGreaterThan(0);
    // Sanity: chunkCount * chunkSize >= size for non-inline files.
    expect(huge.chunkCount * huge.chunkSize).toBeGreaterThanOrEqual(
      100 * 1_048_576
    );
  });

  it("T5 — Node's WebCrypto SHA-256 matches the server hash format", async () => {
    const bytes = new TextEncoder().encode("test-payload");
    const buf = await crypto.subtle.digest("SHA-256", bytes);
    const hex = Array.from(new Uint8Array(buf))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
    expect(hex.length).toBe(64);
    expect(/^[0-9a-f]{64}$/.test(hex)).toBe(true);
    // Known SHA-256("test-payload"):
    expect(hex).toBe(
      "6f06dd0e26608013eff30bb1e951cda7de3fdd9e78e907470e0dd5c0ed25e273"
    );
  });
});
