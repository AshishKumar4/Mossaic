# Mossaic integration guide

The single source of truth for Mossaic's external surface. README and `sdk/README.md` link here; if a claim in those documents drifts from this file, this file is correct.

> **Phase 13** — DO bindings prefixed (`MOSSAIC_USER` / `MOSSAIC_SHARD`); `createWriteStream` accepts metadata/tags/version; HTTP `writeFile` multipart envelope; Yjs awareness relay; YDocHandle shape locked.

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

## 2. `writeFile` and `createWriteStream` parity (Phase 13)

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

## 3. HTTP fallback envelope (Phase 13)

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

## 4. Live editing with Yjs (Phase 13)

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

## 5. Operations checklist for a Phase 13 deploy

1. `npx tsc -b` — exit 0.
2. `pnpm test` — all green (≥ 290 cases at Phase 13).
3. `npx wrangler deploy --dry-run` (App mode) — bindings list shows `MOSSAIC_USER`, `MOSSAIC_SHARD`, `SEARCH_DO`.
4. `npx wrangler deploy --dry-run -c deployments/service/wrangler.jsonc` (Service mode) — bindings show `MOSSAIC_USER`, `MOSSAIC_SHARD` only.
5. Verify the legacy fetch hash on `worker/app/objects/user/user-do.ts:70..263` is unchanged (`4c6eb84925cd8b34298aa92a5201c6e8074defb4527c3bbb1d2c677f9f2c8e70`).
6. `pnpm lean:build` — proofs green; no new `sorry`s, no new project-level axioms.
7. Final grep for the legacy binding-name tokens (whole-word match) returns zero hits outside `local/` (plans), `lean/` (proofs), and audit/history files such as `OPERATIONS.md` and this guide's migration-safety callout.

That is the full Phase 13 contract.
