import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";

/**
 * Phase 39 A3 — image-passthrough fallback.
 *
 * Audit Phase 35 P3 finding: when the IMAGES binding is absent
 * (test miniflare, service-mode deployments without it), the
 * preview pipeline previously returned an icon-card SVG stub for
 * every MIME — including images. That's strictly worse than just
 * returning the original image bytes; the consumer's <img> tag
 * scales them on display.
 *
 * Phase 39 A3 fix: on EMOSSAIC_UNAVAILABLE (renderer needs a
 * binding it doesn't have), we now branch on source MIME:
 *   - image/* sources → image-passthrough renderer (returns
 *     original bytes with original MIME).
 *   - non-image sources → icon-card universal fallback (an MP4
 *     or PDF returned at full size in lieu of a thumbnail would
 *     dwarf the response budget).
 *
 * The miniflare test harness intentionally OMITS the IMAGES
 * binding (see tests/wrangler.test.jsonc) which is exactly the
 * EMOSSAIC_UNAVAILABLE case we need.
 *
 * Three pinning tests:
 *   PF1 — image/* without IMAGES → original bytes, original MIME,
 *         renderer_kind = "image-passthrough", NEVER icon-card
 *   PF2 — non-image without IMAGES → icon-card SVG (unchanged
 *         behaviour for non-image MIMEs)
 *   PF3 — passthrough is cached: second read returns
 *         fromVariantTable=true with byte-identical bytes
 */

import type { UserDO } from "@app/objects/user/user-do";
import type { ShardDO } from "@core/objects/shard/shard-do";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}
const E = env as unknown as E;

async function seedUser(
  stub: DurableObjectStub<UserDO>,
  email: string
): Promise<string> {
  const { userId } = await stub.appHandleSignup(email, "abcd1234");
  return userId;
}

describe("Phase 39 A3 — preview fallback when IMAGES is unbound", () => {
  it("PF1 — image/jpeg without IMAGES returns the ORIGINAL bytes (passthrough), not icon-card", async () => {
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName("preview:pf1")
    );
    const userId = await seedUser(stub, "pf1@e.com");
    const scope = { ns: "default", tenant: userId };

    // Plant a small "image" — the renderer doesn't decode the
    // bytes, only their MIME drives dispatch. 8 KB of synthetic
    // payload keeps us in the inline tier so the test isolates
    // the preview path from the chunked-fan-out path.
    const src = new Uint8Array(8192);
    for (let i = 0; i < src.length; i++) src[i] = (i * 31) & 0xff;
    await stub.vfsWriteFile(scope, "/photo.jpg", src, {
      mimeType: "image/jpeg",
    });

    const out = await stub.vfsReadPreview(scope, "/photo.jpg", {
      variant: "thumb",
    });

    // Renderer kind reports the actual code path. Audit's $1000-bet
    // outcome: NEVER icon-card for an image MIME when the binding
    // is unavailable.
    expect(out.rendererKind).toBe("image-passthrough");
    expect(out.rendererKind).not.toBe("icon-card");

    // Output MIME is the source MIME (passthrough doesn't transcode).
    expect(out.mimeType).toBe("image/jpeg");
    expect(out.sourceMimeType).toBe("image/jpeg");

    // Bytes are byte-identical to the source — that's the whole
    // point of "return the original instead of a stub".
    expect(out.bytes.byteLength).toBe(src.byteLength);
    expect(Array.from(out.bytes)).toEqual(Array.from(src));
  });

  it("PF2 — non-image MIME without IMAGES still falls through to icon-card", async () => {
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName("preview:pf2")
    );
    const userId = await seedUser(stub, "pf2@e.com");
    const scope = { ns: "default", tenant: userId };

    // PDF (non-image). The video-poster renderer doesn't claim
    // application/pdf via canRender, so dispatch falls through to
    // icon-card directly — but more importantly, even renderers
    // that DO claim non-image MIMEs but raise EMOSSAIC_UNAVAILABLE
    // (e.g. video-poster when BROWSER was wired) MUST still land
    // on icon-card, NOT passthrough. We test the dispatch outcome
    // here.
    const pdf = new Uint8Array(1024);
    await stub.vfsWriteFile(scope, "/doc.pdf", pdf, {
      mimeType: "application/pdf",
    });

    const out = await stub.vfsReadPreview(scope, "/doc.pdf", {
      variant: "thumb",
    });

    expect(out.rendererKind).toBe("icon-card");
    expect(out.mimeType).toBe("image/svg+xml");
    // The icon-card SVG includes the filename; verify we got the
    // stub, not the original PDF bytes.
    const text = new TextDecoder().decode(out.bytes);
    expect(text).toContain("<svg");
    expect(text).toContain("doc.pdf");
  });

  it("PF3 — passthrough variant is cached: second read hits fromVariantTable=true with identical bytes", async () => {
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName("preview:pf3")
    );
    const userId = await seedUser(stub, "pf3@e.com");
    const scope = { ns: "default", tenant: userId };

    // Use image/png to vary the source MIME from PF1 — confirms
    // the cache key (file_id, variant_kind, renderer_kind) doesn't
    // accidentally collide across MIME boundaries.
    const src = new Uint8Array(4096);
    for (let i = 0; i < src.length; i++) src[i] = (i * 17 + 3) & 0xff;
    await stub.vfsWriteFile(scope, "/icon.png", src, {
      mimeType: "image/png",
    });

    const cold = await stub.vfsReadPreview(scope, "/icon.png", {
      variant: "thumb",
    });
    expect(cold.fromVariantTable).toBe(false);
    expect(cold.rendererKind).toBe("image-passthrough");
    expect(cold.mimeType).toBe("image/png");

    const warm = await stub.vfsReadPreview(scope, "/icon.png", {
      variant: "thumb",
    });
    expect(warm.fromVariantTable).toBe(true);
    expect(warm.rendererKind).toBe("image-passthrough");
    expect(warm.mimeType).toBe("image/png");
    // Content-addressed dedup ⇒ byte-identical bytes across calls.
    expect(Array.from(warm.bytes)).toEqual(Array.from(cold.bytes));
    // And byte-identical to the source.
    expect(Array.from(warm.bytes)).toEqual(Array.from(src));
  });
});
