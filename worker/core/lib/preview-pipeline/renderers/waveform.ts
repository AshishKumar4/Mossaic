/**
 * Waveform renderer — produces a deterministic SVG sparkline of audio
 * peak amplitudes. Pure compute; no binding required.
 *
 * **Implementation strategy.** Native audio decoding (MP3 / Opus /
 * AAC) requires DSP infrastructure that's heavyweight to ship in a
 * Worker bundle. The current renderer takes a structural shortcut:
 * hash the input bytes into a deterministic peak buffer. The
 * result is a stable visual silhouette per file (different files
 * → different silhouettes; same file → identical bytes-out). It
 * IS NOT the acoustic envelope — a real PCM peak extractor would
 * require vendoring a small WASM decoder. The current renderer is
 * usefully diagnostic ("this file is X" with stable visual hash)
 * and content-deterministic (the load-bearing property).
 *
 * Determinism: output is a deterministic function of the first ~16 KB
 * of `input.bytes` plus the resolved variant dimensions.
 */

import type { Renderer } from "../types";
import type {
  RenderInput,
  RenderOpts,
  RenderResult,
  StandardVariant,
} from "../../../../../shared/preview-types";
import { STANDARD_VARIANT_DIMS } from "../../../../../shared/preview-types";

const MAX_INPUT_BYTES = 16 * 1024;
const PEAK_COUNT = 96;

function resolveDims(opts: RenderOpts): { width: number; height: number } {
  if (typeof opts.variant === "string") {
    const std = STANDARD_VARIANT_DIMS[opts.variant as StandardVariant];
    return { width: std.width, height: std.height };
  }
  return {
    width: opts.variant.width,
    height: opts.variant.height ?? Math.floor(opts.variant.width / 4),
  };
}

/**
 * Reduce N input bytes to PEAK_COUNT peak samples in [0, 1]. Each
 * peak is the mean magnitude (|x - 128| / 128) of its byte bucket,
 * giving a smooth sparkline that's stable per input file.
 */
function reduceToPeaks(bytes: Uint8Array): Float32Array {
  const peaks = new Float32Array(PEAK_COUNT);
  if (bytes.byteLength === 0) return peaks;
  const bucketSize = bytes.byteLength / PEAK_COUNT;
  for (let i = 0; i < PEAK_COUNT; i++) {
    const start = Math.floor(i * bucketSize);
    const end = Math.min(
      bytes.byteLength,
      Math.floor((i + 1) * bucketSize)
    );
    let sum = 0;
    let n = 0;
    for (let j = start; j < end; j++) {
      sum += Math.abs((bytes[j]! - 128) / 128);
      n++;
    }
    peaks[i] = n > 0 ? sum / n : 0;
  }
  return peaks;
}

export const waveformRenderer: Renderer = {
  kind: "waveform-svg",

  canRender(mimeType) {
    return mimeType.startsWith("audio/");
  },

  async render(
    input: RenderInput,
    _env,
    opts: RenderOpts
  ): Promise<RenderResult> {
    const reader = input.bytes.getReader();
    const parts: Uint8Array[] = [];
    let total = 0;
    try {
      while (total < MAX_INPUT_BYTES) {
        const r = await reader.read();
        if (r.done) break;
        parts.push(r.value);
        total += r.value.byteLength;
      }
    } finally {
      reader.releaseLock();
    }
    const merged = new Uint8Array(total);
    let offset = 0;
    for (const p of parts) {
      merged.set(p, offset);
      offset += p.byteLength;
    }
    const peaks = reduceToPeaks(
      merged.byteLength > MAX_INPUT_BYTES
        ? merged.subarray(0, MAX_INPUT_BYTES)
        : merged
    );

    const { width, height } = resolveDims(opts);
    const midY = height / 2;
    const barWidth = width / PEAK_COUNT;

    const bars: string[] = [];
    for (let i = 0; i < PEAK_COUNT; i++) {
      const peak = peaks[i]!;
      const h = Math.max(1, Math.floor(peak * (height * 0.85)));
      const x = i * barWidth;
      const y = midY - h / 2;
      const w = Math.max(1, barWidth - 1);
      bars.push(
        `<rect x="${x.toFixed(2)}" y="${y.toFixed(2)}" width="${w.toFixed(2)}" height="${h}" fill="#60a5fa" rx="1"/>`
      );
    }

    const svg = [
      `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">`,
      `<rect width="${width}" height="${height}" fill="#0f172a"/>`,
      ...bars,
      `</svg>`,
    ].join("");

    return {
      bytes: new TextEncoder().encode(svg),
      mimeType: "image/svg+xml",
      width,
      height,
    };
  },
};
