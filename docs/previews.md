# Universal preview pipeline

A single `vfs.readPreview()` call returns rendered preview bytes for any file the VFS holds — image, code, audio, video, or anything else. Variants are content-addressed, refcount-shared, and immutable-cached.

This document is the canonical reference for the preview surface. The integration guide's "Previews" section is a pointer here.

---

## 1. The renderer matrix

| Renderer       | `kind`            | Accepts                                                | Output         | Notes |
|----------------|-------------------|--------------------------------------------------------|----------------|-------|
| `image`        | `image`           | `image/jpeg`, `image/png`, `image/webp`, `image/avif`, `image/gif`, `image/heic` | `image/webp`   | Cloudflare Images binding. Falls through to `icon-card` when the binding is absent. |
| `code-svg`     | `code-svg`        | `text/*`, `application/json`, `application/javascript`, `application/typescript`, `application/x-yaml`, `application/x-sh` | `image/svg+xml` | First 1024 B / 28 lines. Token-class colouring (keyword/string/comment). Deterministic in input bytes. |
| `waveform-svg` | `waveform-svg`    | `audio/*`                                              | `image/svg+xml` | First 16 KB → 96 peak buckets → SVG sparkline. Deterministic in input bytes. |
| `video-poster` | `video-poster`    | `video/*`                                              | `image/svg+xml` | the preview pipeline stub: delegates to `icon-card`. a future enhancement swaps to a Browser-Run page-1 capture without changing the registry contract. |
| `icon-card`    | `icon-card`       | universal fallback (`canRender` is `true`)             | `image/svg+xml` | Renders a deterministic SVG with the file extension, name, and human-readable size. |

The dispatch order is: `image → code-svg → waveform-svg → video-poster → icon-card`. The first renderer whose `canRender(mimeType)` returns `true` wins.

### Determinism

Every renderer is content-deterministic: the same input bytes + variant + format produce the same output bytes (and therefore the same SHA-256). This is what makes content-addressed storage possible — two users uploading the same image share variant chunks via `chunk_refs`.

The only renderer with a runtime dependency is `image`: it requires `env.IMAGES` to be bound (Cloudflare Images). When the binding is absent (e.g. local `wrangler dev` without account), the renderer throws `RenderError("EMOSSAIC_UNAVAILABLE")` and the pipeline falls back to `icon-card`. The fallback renders a deterministic placeholder so the UI never breaks.

---

## 2. SDK surface

### 2.1 `vfs.readPreview()`

```ts
const result = await vfs.readPreview("/photos/sunset.jpg", { variant: "thumb" });
// {
//   bytes: Uint8Array,           // image/webp or image/svg+xml bytes
//   mimeType: "image/webp",
//   width: 256, height: 256,
//   sourceMimeType: "image/jpeg",
//   rendererKind: "image",
//   fromVariantTable: false,     // true on cache hit
// }
```

Variant shapes:

```ts
type Variant =
  | "thumb"      // 256² cover
  | "medium"     // 768² contain
  | "lightbox"   // 1920² contain
  | { width: number; height?: number; fit?: "cover" | "contain" | "scale-down" };
```

Custom variants encode as `custom:w<W>h<H><fit>` in the `file_variants` table — two requests for `{width: 512, fit: "cover"}` from different users share storage.

### 2.2 `vfs.openManifests()` (batched manifests)

Galleries fetching N thumbnails would otherwise pay N round-trips for manifests + N for chunks. The batched call cuts the manifest leg to one:

```ts
const results = await vfs.openManifests(["/a.jpg", "/b.jpg", "/c.jpg"]);
// [
//   { ok: true,  manifest: { fileId, size, chunkSize, chunkCount, chunks, inlined } },
//   { ok: false, code: "ENOENT", message: "..." },
//   { ok: true,  manifest: ... },
// ]
```

Per-path errors come back as `{ ok: false }` rather than throwing — a single bad path doesn't tank a gallery. Server-enforced cap is 256 paths per call.

### 2.3 HTTP wire shape

`POST /api/vfs/readPreview` with body `{ path, variant, format?, renderer? }` returns:

| Header                       | Example                          | Meaning |
|------------------------------|----------------------------------|---------|
| `Content-Type`               | `image/webp` or `image/svg+xml`  | Output MIME |
| `Cache-Control`              | `public, max-age=31536000, immutable` | Variant bytes are content-addressed |
| `ETag`                       | `W/"<sha256-hex>"`               | Weak ETag over the rendered bytes |
| `X-Mossaic-Renderer`         | `code-svg`                       | Which renderer produced this |
| `X-Mossaic-Variant-Cache`    | `hit` or `miss`                  | Whether the result came from `file_variants` |
| `X-Mossaic-Source-Mime`      | `text/typescript`                | Original file's MIME |
| `X-Mossaic-Width` / `Height` | `256`                            | Output pixel dimensions |

Conditional GET: clients sending `If-None-Match: W/"<hash>"` get a `304 Not Modified` with no body. Combined with the immutable cache header, this lets browsers and intermediaries dedup variant fetches indefinitely — the ETag only changes when the underlying file changes (which yields new content → new hash → new ETag).

`POST /api/vfs/manifests` with body `{ paths: string[] }` (max 256) returns `{ manifests: [...] }`.

---

## 3. Encryption boundary

Files written with per-file encryption (`encryption: { mode: "convergent" | "random", keyId? }`) **cannot** have server-side previews. The renderer would need to decrypt the bytes, which Mossaic explicitly does not do — the worker holds no plaintext key material for encrypted files.

`vfs.readPreview()` on an encrypted file throws `ENOTSUP`. Two paths forward:

1. **Don't preview encrypted files.** Acceptable for sensitive documents where a thumbnail would itself be sensitive. The SPA / SDK surface this as a generic file-icon placeholder.
2. **Render client-side** ((future)). The client calls `vfs.readFile()` (which transparently decrypts), pipes the plaintext into a client-side renderer (e.g. `createImageBitmap` for images, the `code-svg` renderer reimplemented in WASM, etc.), and uses the result locally. Bytes never leave the client.

the preview pipeline ships only the server-side path. The encryption boundary is enforced in `vfs/preview.ts:vfsReadPreview` — the gate runs **before** any renderer dispatch, so a misconfigured deploy cannot accidentally expose preview bytes for an encrypted file.

---

## 4. Cost model

Variant chunks are stored on ShardDOs the same way primary file chunks are — refcount-aware, content-addressed, dedup-friendly across users. Three additive costs:

1. **Storage.** Each cached variant is one row in `file_variants` + one row in `chunks` (if not already deduped). Typical sizes:
   - Thumb (256² WebP): 5–15 KB
   - Medium (768² WebP): 30–80 KB
   - Lightbox (1920² WebP): 100–300 KB
   - SVG renderers (code/waveform/icon-card): 0.5–4 KB
2. **Compute.** Renderer cost per variant:
   - `image`: one Cloudflare Images request per variant. Pricing per [CF Images docs](https://developers.cloudflare.com/images/transform-images/).
   - `code-svg` / `waveform-svg` / `icon-card`: pure compute, ~5–30 ms on the worker. No external dep.
3. **Egress.** Standard Workers egress for the variant bytes. The immutable cache header means subsequent fetches hit the CF cache or the browser cache — origin egress only on the first miss per (variant, ETag).

Typical 100-photo gallery first-load cost (uncached): 100 × thumb image transforms + 100 × ~10 KB egress = ~1 MB egress + 100 image transforms. Subsequent loads: ≤5 origin hits (only photos changed since last visit).

---

## 5. Custom renderer registration

The default registry is `buildDefaultRegistry()` in `worker/core/lib/preview-pipeline/index.ts`. To register a custom renderer (consumer Workers in library mode):

```ts
import { RendererRegistry, type Renderer } from "@mossaic/sdk";
// (export to be added in 20.1; currently the registry is internal to the worker)

const myRenderer: Renderer = {
  kind: "my-pdf-renderer",
  canRender: (mime) => mime === "application/pdf",
  async render(input, env, opts) {
    // env.BROWSER → page-1 capture → return { bytes, mimeType, width, height }
    // ...
  },
};

const registry = new RendererRegistry();
registry.register(myRenderer);
registry.register(iconCardRenderer); // ALWAYS register a fallback last.
```

The `RendererRegistry` API is currently internal; a future enhancement will lift it into the SDK surface so consumers can register PDF / DICOM / domain-specific renderers without forking the worker.

---

## 6. Backfill runbook

Existing files written before the preview pipeline have no `file_variants` rows. Two options to populate them:

### 6.1 Lazy backfill (default)

Do nothing. The first `vfs.readPreview()` call for any (file, variant) populates the row on cache miss. Galleries warm the cache organically as users browse. No operator action required.

### 6.2 Active backfill

For predictable first-render latency (e.g. an export that needs all thumbs immediately), iterate the file list and call `readPreview` once per (file, variant):

```ts
const files = await vfs.listFiles({ tags: ["photo"] });
for (const f of files.items) {
  // The serial loop matches the DO's single-threaded execution
  // model. Promise.all() over the same UserDO doesn't gain
  // throughput; the requests serialize at the DO regardless.
  await vfs.readPreview(f.path, { variant: "thumb" }).catch(() => {});
  await vfs.readPreview(f.path, { variant: "medium" }).catch(() => {});
}
```

Backfill is idempotent (`INSERT OR IGNORE` on the composite PK). Re-running it on a partially-warmed library only fills missing rows.

### 6.3 Cleanup of stale variants

When a file changes (overwrite, rename, encryption toggle), the file's `file_variants` rows go stale. Hard-delete cleans up automatically via `ON DELETE CASCADE` on the `files` foreign key. For overwrites, the new file gets a new `file_id`, so old variant rows become orphans. The `vfsReadPreview` dangling-row recovery path detects an orphan when the chunk fetch returns 404 and re-renders inline; no operator action required.

---

## 7. Test coverage

| Suite                                                | Tests | What it gates |
|------------------------------------------------------|-------|----------------|
| `tests/unit/file-variants-schema.test.ts`            | 4     | DDL idempotence, composite PK, ON DELETE CASCADE, indexes |
| `tests/unit/renderer-registry.test.ts`               | 7     | Registry order, fallback dispatch, kind dispatch, duplicate guard |
| `tests/integration/renderers/*.test.ts`              | 19    | Per-renderer determinism + edge cases (empty stream, escaped filenames, MIME boundaries) |
| `tests/integration/vfs-read-preview.test.ts`         | 10    | Cache hit/miss, ENOENT/EISDIR/ENOTSUP, dedup, custom variants |
| `tests/integration/preview-http-routes.test.ts`      | 8     | ETag + immutable cache, 304 revalidation, error mapping, batched manifests |
| `tests/integration/sdk-read-preview.test.ts`         | 6     | createVFS + createMossaicHttpClient parity, openManifests batching |
| `tests/integration/preview-perf.test.ts`             | 1     | 100-file cold render <3s; warm <500ms |
| `cli/tests/unit/preview.test.ts`                     | 2     | CLI command registration |
| `cli/tests/e2e/m-preview.test.ts`                    | 4     | Live: text → svg, cache hit, ENOENT, batched manifests |
| **Total**                                            | **61** | |

Run gates: `pnpm test` (root, miniflare-isolated), `pnpm -F @mossaic/cli test` (CLI unit), `pnpm -F @mossaic/cli test:e2e` (live, requires `MOSSAIC_E2E_JWT_SECRET`).
