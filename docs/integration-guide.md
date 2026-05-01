# Mossaic integration guide

The single source of truth for Mossaic's external surface. README and `sdk/README.md` link here; if a claim in those documents drifts from this file, this file is correct.

> **Feature surface** — DO bindings prefixed (`MOSSAIC_USER` / `MOSSAIC_SHARD`); `createWriteStream` accepts metadata, tags, and version options; HTTP `writeFile` uses a multipart envelope; Yjs awareness relays presence/cursors; opt-in end-to-end encryption (`createVFS({ encryption: { masterKey, tenantSalt, mode } })`); parallel multipart transfer via `parallelUpload` / `parallelDownload`; universal preview pipeline (`vfs.readPreview()` + batched `vfs.openManifests()`). The integration-guide is the canonical reference for every feature listed here.

---

## 1. Library mode (the supported path)

The consumer's Worker re-exports Mossaic's two Durable Object classes from `@mossaic/sdk` and uses `createVFS(env, opts)` for every request. One DO RPC subrequest per VFS call regardless of internal chunk fan-out.

### 1.1 `wrangler.jsonc` shape

```jsonc
{
  "name": "my-app",
  "main": "src/index.ts",
  "compatibility_date": "2026-03-01",
  "compatibility_flags": ["nodejs_compat"],

  "durable_objects": {
    "bindings": [
      { "name": "MOSSAIC_USER",  "class_name": "UserDO" },
      { "name": "MOSSAIC_SHARD", "class_name": "ShardDO" }
    ]
  },
  "migrations": [
    { "tag": "mossaic-v1", "new_sqlite_classes": ["UserDO", "ShardDO"] }
  ]
}
```

Both bindings are **required**. The SDK reads `env.MOSSAIC_USER` directly; `MOSSAIC_SHARD` is consumed by the bundled UserDO code from inside its own env. There is **no** `MOSSAIC_SEARCH` binding — vector search (SearchDO) is App-mode internal and not part of the SDK contract.

> **Migration safety.** If you previously had this Worker bound under different names (e.g. `USER_DO` / `SHARD_DO`), renaming the binding `name` while keeping `class_name` unchanged is a **data-safe** operation per [CF docs](https://developers.cloudflare.com/durable-objects/reference/durable-objects-migrations/) — DO storage is keyed by `(class_name, idFromName(name))`, never by binding name. No migration entry is required.

### 1.2 The `MossaicEnv` interface

```ts
import type { UserDO, ShardDO, MossaicEnv } from "@mossaic/sdk";

export interface Env extends MossaicEnv {
  // ...your own bindings...
}
```

`MossaicEnv` requires:

```ts
interface MossaicEnv {
  MOSSAIC_USER:  DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}
```

### 1.3 Worker entry point

```ts
// src/index.ts
import { UserDO, ShardDO, createVFS } from "@mossaic/sdk";

// wrangler discovers DO classes from the Worker's main-module exports.
export { UserDO, ShardDO };

export default {
  async fetch(req: Request, env: Env) {
    const vfs = createVFS(env, { tenant: "acme-corp" });
    await vfs.writeFile("/hello.txt", "world");
    return new Response(await vfs.readFile("/hello.txt", { encoding: "utf8" }));
  },
};
```

That is the entire integration. Multi-tenant via `vfs:${ns}:${tenant}[:${sub}]` DO naming; per-tenant rate limits; isomorphic-git plug-in via `vfs.promises === vfs`.

---

## 2. `writeFile` and `createWriteStream` parity

Both calls accept the same `WriteFileOpts`:

```ts
interface WriteFileOpts {
  mode?: number;
  mimeType?: string;

  // undefined (default) → keep existing
  // null               → CLEAR (UPDATE files SET metadata = NULL)
  // {...}              → SET (deep-validated against caps)
  metadata?: Record<string, unknown> | null;

  // undefined → keep; [] → drop all; [...] → REPLACE
  tags?: readonly string[];

  // Per-version flags. Silently no-ops on non-versioning tenants.
  version?: {
    label?: string;
    userVisible?: boolean; // default true
  };
}
```

### 2.1 `writeFile`

```ts
await vfs.writeFile("/photos/hike.jpg", bytes, {
  mimeType: "image/jpeg",
  metadata: { camera: "Pixel 8", iso: 200 },
  tags: ["nature", "2026"],
  version: { label: "trail-summit" },
});
```

### 2.2 `createWriteStream` / `createWriteStreamWithHandle`

The same opts apply at **commit time** (the rename-from-tmp-id step). Validation happens at `begin` so the caller fails fast on cap violations — no orphan tmp row is left behind.

```ts
const stream = await vfs.createWriteStream("/big.bin", {
  metadata: { source: "ingest-pipeline" },
  tags: ["batch-2026-04"],
});
const writer = stream.getWriter();
await writer.write(chunk1);
await writer.write(chunk2);
await writer.close();
// Metadata + tags applied at commit; version row created if versioning is enabled.
```

The `WithHandle` variant additionally surfaces the underlying write handle:

```ts
const { stream, handle } = await vfs.createWriteStreamWithHandle("/big.bin", {
  version: { label: "ingest-2026-04-06" },
});
console.log(handle.tmpId, handle.chunkSize, handle.poolSize);
// `handle.tmpId` is stable for resumable / progress-tracking use cases.
```

---

## 3. HTTP fallback envelope

For non-Worker consumers (native apps, scripts, browsers without a worker hop), the HTTP fallback at `/api/vfs` reaches **parity** with binding mode. `writeFile` accepts three body shapes; the SDK's `createMossaicHttpClient` picks the right one for you.

| Body shape | Used when | Carries metadata/tags/version |
|------------|-----------|-------------------------------|
| `application/json` | `data` is a string | yes |
| `application/octet-stream` | `data` is bytes AND no opts | no (legacy / fast path) |
| `multipart/form-data` | `data` is bytes AND opts present | yes |

Multipart shape:

```
POST /api/vfs/writeFile?path=/photo.bin
Content-Type: multipart/form-data; boundary=...
Authorization: Bearer <vfs-token>

--boundary
Content-Disposition: form-data; name="bytes"; filename="blob"

<binary file content>
--boundary
Content-Disposition: form-data; name="meta"

{"mimeType":"image/jpeg","metadata":{"camera":"x100"},"tags":["draft"],"version":{"label":"first"}}
--boundary--
```

The SDK builds this for you transparently:

```ts
import { createMossaicHttpClient } from "@mossaic/sdk";

const vfs = createMossaicHttpClient({
  url: "https://mossaic.example.com",
  apiKey: vfsToken,
});
await vfs.writeFile("/photo.bin", bytes, {
  metadata: { camera: "x100" },
  tags: ["draft"],
});
```

---

## 4. Live editing with Yjs

### 4.1 Promote a file to yjs-mode

Yjs-mode is **opt-in per-file** and uses POSIX mode bit `0o4000` (`VFS_MODE_YJS_BIT`):

```ts
await vfs.writeFile("/notes/today.md", "# Today\n");
await vfs.setYjsMode("/notes/today.md", true);
// or equivalently:
await vfs.chmod("/notes/today.md", { yjs: true });
```

Demoting back is **rejected** with `EINVAL` — losing CRDT history is never silent.

### 4.2 The `YDocHandle` shape

This is the **canonical** shape — every claim in any doc must match this:

```ts
interface YDocHandle {
  /** The Y.Doc for this file. Standard Yjs API; subscribe via doc.on(...). */
  readonly doc: Y.Doc;

  /**
   * y-protocols/awareness instance for cursors / selections / presence.
   * Set local state via awareness.setLocalState({...}); subscribe via
   * awareness.on("change", cb) or awareness.on("update", cb). Server
   * relays frames between editors but NEVER persists them.
   */
  readonly awareness: Awareness;

  /** Resolves once the initial sync round-trip with the server completes. */
  readonly synced: Promise<void>;

  /** Close the WebSocket; detach protocol listeners; destroy awareness. */
  close(): Promise<void>;

  /**
   * Trigger a Yjs compaction whose checkpoint emits a user-visible
   * Mossaic version row (when versioning is enabled). Optionally label.
   */
  flush(opts?: { label?: string }): Promise<{
    versionId: string | null;
    checkpointSeq: number;
  }>;

  /** Register a callback for the underlying socket close. */
  onClose(cb: (event: { code: number; reason: string }) => void): void;

  /** Register a callback for socket errors. */
  onError(cb: (err: unknown) => void): void;
}
```

There is **no** `handle.on("sync", cb)`. There is **no** `handle.on("update", cb)`. Those events live on the `doc` and `awareness` instances:

```ts
handle.doc.on("update", (update, origin) => { /* ... */ });
handle.awareness.on("change", () => render(handle.awareness.getStates()));
```

### 4.3 Two-client co-editing example

```ts
import { openYDoc } from "@mossaic/sdk/yjs";

const handle = await openYDoc(vfs, "/notes/today.md");
await handle.synced;

handle.doc.getText("content").insert(0, "DRAFT — ");
handle.awareness.setLocalState({ name: "alice", cursor: 0 });

handle.awareness.on("change", () => {
  for (const [clientID, state] of handle.awareness.getStates()) {
    // render presence: name, cursor, selection...
  }
});

// Later: request an explicit, user-visible save point.
const { versionId } = await handle.flush({ label: "morning save" });

await handle.close();
```

### 4.4 Wire protocol

Single-byte tag + payload, binary WebSocket frames:

| Tag | Meaning | Payload |
|-----|---------|---------|
| `0` | `sync_step_1` | state vector |
| `1` | `sync_step_2` | doc update bytes |
| `2` | `update` | doc update bytes |
| `3` | `awareness` | `encodeAwarenessUpdate(awareness, [...clientIDs])` from `y-protocols/awareness` |

Sync frames (`0`/`1`/`2`) are persisted as `yjs_oplog` rows. Awareness frames (`3`) are **never** persisted — the server holds awareness only in DO memory and resets on eviction. Clients re-broadcast their state on reconnect (standard y-websocket semantics).

### 4.5 Peer dependencies

`yjs` and `y-protocols` are **optional peer dependencies** of `@mossaic/sdk`. Install both in your consumer Worker if you use the `@mossaic/sdk/yjs` subpath. Mossaic itself does not bundle either.

```jsonc
{
  "dependencies": {
    "@mossaic/sdk": "^0.1.0",
    "yjs": "^13.6.0",
    "y-protocols": "^1.0.6"
  }
}
```

---

## 5. Command-line interface (`@mossaic/cli`)

adds a Node 20+ CLI (`mossaic` / `mscli`) that drives a deployed Mossaic Service worker over HTTP + WSS. It is intended for operators and scripting workflows that don't run inside a Cloudflare Worker.

### 5.1 Auth

`~/.mossaic/config.json` (mode `0600`) stores `{ endpoint, jwtSecret, scope }` per profile. The CLI mints VFS tokens locally using `jose.SignJWT` with the same wire shape as `worker/core/lib/auth.ts:signVFSToken` (`{ scope: "vfs", ns, tn, sub? }`, HS256, iat/exp). Justification: this is an operator tool and the secret already exists on the operator's machine via `wrangler secret put`; storing it under `0600` locally is no weaker than what wrangler already does.

```bash
mossaic auth setup --endpoint https://mossaic-core.example.workers.dev --secret "$JWT_SECRET" --tenant team-acme
mossaic auth whoami
```

### 5.2 Public Yjs WebSocket route

To make Yjs reachable from external clients (Node CLI, browsers, third-party Workers), the Service worker mounts a public WebSocket upgrade route at:

```
GET /api/vfs/yjs/ws?path=<encoded path>
Authorization: Bearer <vfs-token>
# ...or, for browsers (which can't set Authorization on WebSocket):
Sec-WebSocket-Protocol: bearer.<vfs-token>
Upgrade: websocket
```

The route validates the Bearer (matching `verifyVFSToken`'s `scope === "vfs"` check) and forwards the upgrade to the per-tenant `UserDOCore` via `stub.fetch()` against the synthetic `/yjs/ws?path=...&ns=...&tenant=...[&sub=...]` URL. The DO's existing `_fetchWebSocketUpgrade` then runs the Yjs handshake.

The same Service worker also exposes:

- `POST /api/vfs/setYjsMode { path, enabled }` — flip the per-file yjs-mode bit (binding-mode `vfs.setYjsMode`).
- `POST /api/vfs/flushYjs { path, label? }` — explicit compaction → user-visible version row (binding-mode `YDocHandle.flush`).
- `POST /api/vfs/admin/setVersioning { enabled }` — operator-class lazy enable of per-tenant versioning.
- `POST /api/vfs/readFile { path, versionId? }` — extended to honor `versionId` so external clients can read historical bytes.

### 5.3 Verbs

Every public SDK method is exposed as a CLI verb. See [`cli/README.md`](../cli/README.md) for the full table. Highlights:

- File ops: `ls`, `cat`, `write`, `put`, `get`, `stream-put`, `stream-get`, `rm`, `mv`, `cp`, `mkdir`, `rmdir`, `rm-rf`, `stat`, `ln`, `readlink`, `chmod`, `exists`.
- `meta patch`, `find` (`listFiles`).
- Versioning: `versions ls | restore | drop | mark`.
- Yjs: `yjs init | edit | awareness | flush`.
- Token utility: `token mint`.

`--json` is supported on every list-style command. Exit codes: `1` for any `VFSFsError`; `2` for `MossaicUnavailableError` (transport-level failures).

### 5.4 Coverage gates

The CLI ships with `≥58` live E2E test cases (categories A–I) plus `≥10` functional tests (execa-driven invocations of the binary) — all run against the live Service worker, not a local mock. Each test creates a fresh ULID-suffixed tenant for isolation.

---

## 6. Previews

The full preview surface lives in [`docs/previews.md`](./previews.md). The SDK exposes two complementary shapes:

### 6.1 Signed-URL caching (browser-direct, CDN-cached)

The right shape when the consumer is going to ship the bytes to a browser (gallery thumbnails, embedded images, public-share endpoints). Mint a signed URL via the auth-gated RPC; the browser fetches it directly:

```ts
const url = await vfs.previewUrl("/photos/sunset.jpg", { variant: "thumb" });
// "/api/vfs/preview-variant/eyJhbGciOi..."

// Or get the URL plus the metadata bundle in one mint:
const info = await vfs.previewInfo("/photos/sunset.jpg", { variant: "thumb" });
// {
//   token, url,
//   etag: "W/\"<contentHash>\"",
//   mimeType: "image/webp", width, height,
//   rendererKind: "image",
//   versionId,
//   cacheControl: "public, max-age=31536000, immutable",
//   contentHash,
//   expiresAtMs,
// }

// For galleries, batch the mint (cap 256 paths per call):
const results = await vfs.previewInfoMany(["/a.jpg", "/b.jpg", "/c.jpg"]);
// [{ ok: true, info } | { ok: false, code, message }, ...]
```

Bytes are content-addressed by `contentHash`; the response carries `Cache-Control: public, max-age=31536000, immutable` (no `Vary: Authorization`) so a CDN edge tier caches across all clients. After the first warmup, subsequent loads bypass the Worker entirely.

Token security:

- HMAC-signed (HS256) with the same `JWT_SECRET` as the rest of the auth surface; multi-secret rotation aware.
- Scope-bound (`scope: "vfs-pv"`): VFS / multipart / download / share tokens replayed at the preview-variant route are rejected.
- Default TTL 24h; clamped to `[60s, 30d]` via `opts.ttlMs`.
- Stale-after-write impossible: a write changes `headVersionId`, which mints a fresh `contentHash`, which is a different URL.

If the variant referenced by a token has been re-rendered since mint (its `chunk_hash` no longer matches the token's `contentHash`), the route returns 410 Gone &mdash; SPA recovery is to re-mint the token via `previewInfo` and retry.

### 6.2 Inline reads (bytes-through-RPC)

The right shape when the consumer processes bytes server-side (image manipulation, hashing, re-encoding):

```ts
const preview = await vfs.readPreview("/photos/sunset.jpg", { variant: "thumb" });
// preview.bytes is image/webp (or image/svg+xml for non-image MIMEs).

// Batched manifests for galleries &mdash; one round-trip for N paths:
const manifests = await vfs.openManifests(["/a.jpg", "/b.jpg", "/c.jpg"]);
```

Five built-in renderers dispatch by MIME (`image`, `code-svg`, `waveform-svg`, `video-poster`, `icon-card`). Variant bytes are content-addressed and shared across users via the existing `chunks` refcount table; cache header is `public, max-age=31536000, immutable`. The encryption boundary returns `ENOTSUP` for encrypted files &mdash; server-side rendering would require plaintext that the worker doesn't hold.

CLI: `mossaic preview <path> [--variant=thumb|medium|lightbox] [--width=<px>] [--out=<local>]`.

---

## 7. HTTP Range support for media

The gallery and shared-album image endpoints honor HTTP Range requests so the browser's native `<video>` / `<audio>` element can scrub without re-downloading. Two routes in scope:

```
GET /api/gallery/image/:fileId
GET /api/shared/:token/image/:fileId
```

Both honor `Range: bytes=N-M` with:

- **206 Partial Content** + `Content-Range: bytes N-M/total` + `Accept-Ranges: bytes` for valid ranges.
- **416 Range Not Satisfiable** + `Content-Range: bytes */total` for out-of-bounds.
- **200** with `Accept-Ranges: bytes` (advertising support) when no `Range` header is sent.

Range requests bypass the Workers Cache wrapper because the cached full response is the upstream of any range slice; the Worker streams the requested byte slice from the cached or freshly-fetched bytes.

The SDK-level `vfs.createReadStream(path, { start, end })` is a separate primitive &mdash; it slices across one or more 1 MB chunks to produce a memory-bounded `ReadableStream` and is suitable for in-Worker byte processing. The HTTP Range support above is for **browser-direct** seeking on the App routes; SDK consumers should use `createReadStream` for in-Worker workflows.

---

## 8. Folder revision counter (directory ETags)

Each folder row carries a monotonic `revision` column that bumps on every direct-child mutation (`writeFile`, `unlink`, `rename`, `mkdir`, `rmdir` in that folder). Nested-tree changes do **not** bump the parent &mdash; the counter is direct-children-only.

Two surfaces expose the counter:

### 8.1 `vfs.listChildren` returns it

```ts
const page = await vfs.listChildren("/photos/2026", { limit: 50 });
page.revision;     // monotonic per-folder counter
page.entries;      // VFSChild[] (discriminated union by `kind`)
page.cursor;       // optional next-page cursor
```

When `revision` is unchanged across two reads, the directory contents are guaranteed identical &mdash; you can skip diffing `entries` entirely. This is the SDK-side equivalent of an HTTP ETag for directory listings.

### 8.2 SPA-side ETags for tree views

A consumer Worker that surfaces a directory listing as an HTTP response can derive an ETag from `(folder_id, revision)` and serve `If-None-Match` with 304:

```ts
const page = await vfs.listChildren(folderPath);
const etag = `W/"folder-${page.revision}"`;
const ifNoneMatch = req.headers.get("If-None-Match");
if (ifNoneMatch === etag) return new Response(null, { status: 304, headers: { ETag: etag } });
return new Response(JSON.stringify(page), {
  headers: { ETag: etag, "Cache-Control": "private, max-age=60" },
});
```

The counter is monotonic within a single tenant DO; cross-tenant counters are independent (different DO instances).

---

## 9. `listChildren` &mdash; one-RPC enumeration of a folder's direct children

`readdir(path)` returns names only. `listFiles({prefix})` is for indexed queries across the entire tenant. `listChildren` answers the SPA-shaped question "what's directly inside this folder right now?" in a single DO RPC with optional `stat` / `metadata` / `tags` / `contentHash` hydration.

```ts
const page = await vfs.listChildren("/photos/2026", {
  orderBy: "mtime",            // 'mtime' (default) | 'name' | 'size'
  direction: "desc",           // 'desc' default for mtime/size, 'asc' for name
  limit: 50,                   // default 50, max 1000
  includeStat: true,           // default true
  includeMetadata: false,      // default false
  includeContentHash: false,   // default false; adds SHA-256 to file entries
  includeTombstones: false,    // default false; admin/recovery surfaces
  includeArchived: false,      // default false
});
```

Result entries are a discriminated union by `kind`:

```ts
type VFSChild =
  | { kind: "folder"; path: string; pathId: string; name: string; stat?: VFSStat }
  | {
      kind: "file";
      path: string; pathId: string; name: string;
      stat?: VFSStat;
      metadata?: Record<string, unknown> | null;
      tags: string[];
      contentHash?: string;
    }
  | { kind: "symlink"; path: string; pathId: string; name: string; target: string; stat?: VFSStat };
```

`name` is the leaf segment without leading slash; `path` is the absolute path with leading slash. Both pre-computed by the server.

The same `includeContentHash: true` knob exists on `listFiles` since both surfaces share the underlying hydration path.

---

## 10. Operations checklist for a deploy

1. `pnpm typecheck` &mdash; exit 0.
2. `pnpm ci:check` &mdash; chained typecheck + DTS-strict SDK build + no-Phase-tag lint gate; exit 0.
3. `pnpm test` &mdash; full suite green (~929 cases across unit, integration, CLI, browser e2e).
4. `npx wrangler deploy --dry-run` (App mode) &mdash; bindings list shows `MOSSAIC_USER`, `MOSSAIC_SHARD`, `SEARCH_DO`.
5. `npx wrangler deploy --dry-run -c deployments/service/wrangler.jsonc` (Service mode) &mdash; bindings show `MOSSAIC_USER`, `MOSSAIC_SHARD` only.
6. App-mode contract suite green: `pnpm test tests/integration/app-smoke.test.ts tests/integration/multipart-routes.test.ts` &mdash; these pin the legacy photo-app HTTP wire shape that the SPA still consumes via `stub.appXxx(...)` typed RPCs.
7. `pnpm verify:proofs` &mdash; Lean proofs green; 226 theorems, 0 axioms, 0 sorrys; no xref drift.
8. Final grep for the legacy binding-name tokens (whole-word match) returns zero hits outside `local/` (plans), `lean/` (proofs), and audit/history files such as `OPERATIONS.md` and this guide's migration-safety callout.

That is the full contract.
