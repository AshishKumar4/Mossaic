import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * A successful `image-resize` that produces webp is persisted under
 * renderer_kind "image", but every reader looks the variant up under the
 * renderer's canonical kind ("image-resize") and then falls back to
 * "image-passthrough"/"icon-card". None of those match, so the row a
 * production resize just wrote can never be found again.
 *
 * The suite has never caught this because `tests/wrangler.test.jsonc`
 * omits the IMAGES binding, so the resize path always fails here and the
 * fallback renderers — which stamp their canonical kind — are what run.
 * These tests seed the row exactly as a live resize writes it.
 */

import type { UserDO } from "@app/objects/user/user-do";
import type { ShardDO } from "@core/objects/shard/shard-do";

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_USER_IMAGES: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}
const E = env as unknown as TestEnv;

async function seedUser(
  stub: DurableObjectStub<UserDO>,
  email: string
): Promise<string> {
  const { userId } = await stub.appHandleSignup(email, "abcd1234");
  return userId;
}

/**
 * Rewrite the cached variant row to look like the output of a real
 * IMAGES-backed resize: renderer_kind "image", mime image/webp.
 */
async function restampAsResizedWebp(
  stub: DurableObjectStub<UserDO>
): Promise<number> {
  return runInDurableObject(stub, (_instance, state) => {
    state.storage.sql.exec(
      `UPDATE file_variants
          SET renderer_kind = 'image', mime_type = 'image/webp'
        WHERE renderer_kind = 'image-passthrough'`
    );
    return (
      state.storage.sql
        .exec<{ n: number }>(
          "SELECT COUNT(*) AS n FROM file_variants WHERE renderer_kind = 'image'"
        )
        .toArray()[0]?.n ?? 0
    );
  });
}

describe("preview renderer-kind round-trip", () => {
  it("finds the variant row a successful webp resize persisted", async () => {
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName("preview:kind-readpreview")
    );
    const userId = await seedUser(stub, "kind-readpreview@e.com");
    const scope = { ns: "default", tenant: userId };

    const src = new Uint8Array(4096);
    for (let i = 0; i < src.length; i++) src[i] = (i * 17) & 0xff;
    await stub.vfsWriteFile(scope, "/photo.jpg", src, {
      mimeType: "image/jpeg",
    });

    await stub.vfsReadPreview(scope, "/photo.jpg", { variant: "thumb" });
    expect(await restampAsResizedWebp(stub)).toBe(1);

    const cached = await stub.vfsReadPreview(scope, "/photo.jpg", {
      variant: "thumb",
    });

    // The row is right there under renderer_kind 'image'. Before the fix the
    // readers never ask for that kind, so this comes back as a fresh render.
    expect(cached.fromVariantTable).toBe(true);
    expect(cached.rendererKind).toBe("image");
  });

  it("caches a genuinely successful resize instead of re-rendering it", async () => {
    // Runs against a UserDO with a working IMAGES binding, so image-resize
    // actually succeeds and persists under renderer_kind "image" — the exact
    // production shape no other test reaches.
    const stub = E.MOSSAIC_USER_IMAGES.get(
      E.MOSSAIC_USER_IMAGES.idFromName("preview:kind-live-resize")
    );
    const userId = await seedUser(stub, "kind-live-resize@e.com");
    const scope = { ns: "default", tenant: userId };

    const src = new Uint8Array(4096);
    for (let i = 0; i < src.length; i++) src[i] = (i * 29) & 0xff;
    await stub.vfsWriteFile(scope, "/live.jpg", src, {
      mimeType: "image/jpeg",
    });

    const first = await stub.vfsReadPreview(scope, "/live.jpg", {
      variant: "thumb",
    });
    expect(first.rendererKind).toBe("image");
    expect(first.fromVariantTable).toBe(false);

    const second = await stub.vfsReadPreview(scope, "/live.jpg", {
      variant: "thumb",
    });
    expect(second.fromVariantTable).toBe(true);
    expect(second.rendererKind).toBe("image");

    const minted = await stub.vfsMintPreviewToken(scope, "/live.jpg", {
      variant: "thumb",
    });
    expect(minted.rendererKind).toBe("image");
  });

  it("mints a signed preview url against that same row", async () => {
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName("preview:kind-previewurl")
    );
    const userId = await seedUser(stub, "kind-previewurl@e.com");
    const scope = { ns: "default", tenant: userId };

    const src = new Uint8Array(4096);
    for (let i = 0; i < src.length; i++) src[i] = (i * 23) & 0xff;
    await stub.vfsWriteFile(scope, "/shot.jpg", src, {
      mimeType: "image/jpeg",
    });

    await stub.vfsReadPreview(scope, "/shot.jpg", { variant: "thumb" });
    expect(await restampAsResizedWebp(stub)).toBe(1);

    // Before the fix this throws EBUSY "variant row missing after render":
    // the mint re-renders, stores under 'image', then fails to find it.
    const minted = await stub.vfsMintPreviewToken(scope, "/shot.jpg", {
      variant: "thumb",
    });
    expect(minted.rendererKind).toBe("image");
  });
});
