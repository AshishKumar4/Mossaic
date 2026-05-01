import { describe, it, expect } from "vitest";
import {
  parseRange,
  rangeResponse,
  rangeNotSatisfiableResponse,
  serveBytesWithRange,
  type ByteRange,
} from "@core/lib/http-range";

/**
 * Unit tests for the HTTP Range helper. Pure functions; no DO /
 * env / network dependencies. Covers the cases browsers actually
 * issue against `<video>` / `<audio>`:
 *
 *   R1 explicit range  bytes=START-END
 *   R2 open-ended      bytes=START-
 *   R3 suffix          bytes=-N
 *   R4 unsatisfiable   bytes=START >= total → 416
 *   R5 malformed       returns null (caller falls through to 200)
 *   R6 multi-range     returns null (we don't support multipart)
 *   R7 zero-byte file  any range is unsatisfiable
 *   R8 serveBytesWithRange shapes the response correctly (200 / 206 / 416)
 */
describe("http-range parseRange", () => {
  it("R1 — bytes=0-99 → {start:0, end:99}", () => {
    const r = parseRange("bytes=0-99", 1000);
    expect(r).toEqual({ start: 0, end: 99 } as ByteRange);
  });

  it("R1 — bytes=100-200 → {start:100, end:200}", () => {
    const r = parseRange("bytes=100-200", 1000);
    expect(r).toEqual({ start: 100, end: 200 } as ByteRange);
  });

  it("R1 — explicit end past size clamps to total-1", () => {
    const r = parseRange("bytes=900-9999", 1000);
    expect(r).toEqual({ start: 900, end: 999 } as ByteRange);
  });

  it("R2 — bytes=500- → {start:500, end:total-1}", () => {
    const r = parseRange("bytes=500-", 1000);
    expect(r).toEqual({ start: 500, end: 999 } as ByteRange);
  });

  it("R2 — bytes=0- (full file, open-ended) → whole file", () => {
    const r = parseRange("bytes=0-", 1000);
    expect(r).toEqual({ start: 0, end: 999 } as ByteRange);
  });

  it("R3 — bytes=-100 → last 100 bytes", () => {
    const r = parseRange("bytes=-100", 1000);
    expect(r).toEqual({ start: 900, end: 999 } as ByteRange);
  });

  it("R3 — suffix larger than file clamps to start=0", () => {
    const r = parseRange("bytes=-9999", 1000);
    expect(r).toEqual({ start: 0, end: 999 } as ByteRange);
  });

  it("R4 — start at total → unsatisfiable", () => {
    const r = parseRange("bytes=1000-1099", 1000);
    expect(r).toBe("unsatisfiable");
  });

  it("R4 — start past total → unsatisfiable", () => {
    const r = parseRange("bytes=2000-", 1000);
    expect(r).toBe("unsatisfiable");
  });

  it("R5 — empty header → null (no range)", () => {
    expect(parseRange("", 1000)).toBeNull();
    expect(parseRange(null, 1000)).toBeNull();
    expect(parseRange(undefined, 1000)).toBeNull();
  });

  it("R5 — wrong unit → null", () => {
    expect(parseRange("items=0-99", 1000)).toBeNull();
  });

  it("R5 — no dash → null", () => {
    expect(parseRange("bytes=100", 1000)).toBeNull();
  });

  it("R5 — non-numeric → null", () => {
    expect(parseRange("bytes=abc-def", 1000)).toBeNull();
    expect(parseRange("bytes=10-abc", 1000)).toBeNull();
  });

  it("R5 — negative start → null", () => {
    expect(parseRange("bytes=-10-100", 1000)).toBeNull();
  });

  it("R5 — end < start → null", () => {
    expect(parseRange("bytes=200-100", 1000)).toBeNull();
  });

  it("R6 — multi-range → null (graceful degrade to 200)", () => {
    expect(parseRange("bytes=0-99,200-299", 1000)).toBeNull();
  });

  it("R7 — zero-byte resource → any range unsatisfiable", () => {
    expect(parseRange("bytes=0-99", 0)).toBe("unsatisfiable");
    expect(parseRange("bytes=-10", 0)).toBe("unsatisfiable");
  });

  it("R7 — case-insensitive `BYTES=`", () => {
    const r = parseRange("BYTES=0-99", 1000);
    expect(r).toEqual({ start: 0, end: 99 } as ByteRange);
  });

  it("R7 — single-byte read bytes=5-5", () => {
    const r = parseRange("bytes=5-5", 1000);
    expect(r).toEqual({ start: 5, end: 5 } as ByteRange);
  });
});

describe("http-range rangeResponse", () => {
  it("RR1 — emits 206 with Content-Range, Content-Length, Accept-Ranges", () => {
    const bytes = new Uint8Array(1000).fill(42);
    const res = rangeResponse(bytes, { start: 100, end: 199 }, 1000, {
      "Content-Type": "video/mp4",
    });
    expect(res.status).toBe(206);
    expect(res.headers.get("Content-Range")).toBe("bytes 100-199/1000");
    expect(res.headers.get("Content-Length")).toBe("100");
    expect(res.headers.get("Accept-Ranges")).toBe("bytes");
    expect(res.headers.get("Vary")).toBe("Range");
    expect(res.headers.get("Content-Type")).toBe("video/mp4");
  });

  it("RR2 — appends Range to existing Vary header without duplicating", () => {
    const bytes = new Uint8Array(100);
    const res = rangeResponse(bytes, { start: 0, end: 49 }, 100, {
      Vary: "Authorization",
    });
    expect(res.headers.get("Vary")).toBe("Authorization, Range");

    const res2 = rangeResponse(bytes, { start: 0, end: 49 }, 100, {
      Vary: "Authorization, Range",
    });
    expect(res2.headers.get("Vary")).toBe("Authorization, Range");
  });

  it("RR3 — returns the correct sliced bytes", async () => {
    const bytes = new Uint8Array(1000);
    for (let i = 0; i < 1000; i++) bytes[i] = i & 0xff;
    const res = rangeResponse(bytes, { start: 50, end: 59 }, 1000);
    const out = new Uint8Array(await res.arrayBuffer());
    expect(out.byteLength).toBe(10);
    expect(out[0]).toBe(50);
    expect(out[9]).toBe(59);
  });
});

describe("http-range rangeNotSatisfiableResponse", () => {
  it("RU1 — emits 416 with Content-Range bytes <asterisk>/<total>", () => {
    const res = rangeNotSatisfiableResponse(1000, "video/mp4");
    expect(res.status).toBe(416);
    expect(res.headers.get("Content-Range")).toBe("bytes */1000");
    expect(res.headers.get("Accept-Ranges")).toBe("bytes");
    expect(res.headers.get("Content-Type")).toBe("video/mp4");
  });

  it("RU2 — omits Content-Type when not supplied", () => {
    const res = rangeNotSatisfiableResponse(0);
    expect(res.status).toBe(416);
    expect(res.headers.get("Content-Type")).toBeNull();
  });
});

describe("http-range serveBytesWithRange", () => {
  it("R8 — no Range header → 200 with full body + Accept-Ranges", async () => {
    const bytes = new Uint8Array(500).fill(7);
    const res = serveBytesWithRange(bytes, null, {
      "Content-Type": "image/jpeg",
    });
    expect(res.status).toBe(200);
    expect(res.headers.get("Content-Length")).toBe("500");
    expect(res.headers.get("Accept-Ranges")).toBe("bytes");
    const body = new Uint8Array(await res.arrayBuffer());
    expect(body.byteLength).toBe(500);
    expect(body[0]).toBe(7);
  });

  it("R8 — valid Range → 206 with sliced body", async () => {
    const bytes = new Uint8Array(500);
    for (let i = 0; i < 500; i++) bytes[i] = i % 256;
    const res = serveBytesWithRange(bytes, "bytes=100-199", {
      "Content-Type": "video/mp4",
    });
    expect(res.status).toBe(206);
    expect(res.headers.get("Content-Range")).toBe("bytes 100-199/500");
    const body = new Uint8Array(await res.arrayBuffer());
    expect(body.byteLength).toBe(100);
    expect(body[0]).toBe(100);
  });

  it("R8 — unsatisfiable Range → 416", () => {
    const bytes = new Uint8Array(100);
    const res = serveBytesWithRange(bytes, "bytes=200-300", {
      "Content-Type": "image/png",
    });
    expect(res.status).toBe(416);
    expect(res.headers.get("Content-Range")).toBe("bytes */100");
  });

  it("R8 — malformed Range → 200 (graceful)", async () => {
    const bytes = new Uint8Array(100);
    const res = serveBytesWithRange(bytes, "garbage", {
      "Content-Type": "text/plain",
    });
    expect(res.status).toBe(200);
    expect(res.headers.get("Accept-Ranges")).toBe("bytes");
  });

  it("R8 — suffix Range bytes=-N → 206 last N bytes", async () => {
    const bytes = new Uint8Array(1000);
    for (let i = 0; i < 1000; i++) bytes[i] = i & 0xff;
    const res = serveBytesWithRange(bytes, "bytes=-50");
    expect(res.status).toBe(206);
    expect(res.headers.get("Content-Range")).toBe("bytes 950-999/1000");
    const body = new Uint8Array(await res.arrayBuffer());
    expect(body.byteLength).toBe(50);
    expect(body[0]).toBe(950 & 0xff);
  });
});
