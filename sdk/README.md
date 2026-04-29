# @mossaic/sdk

Cloudflare-Worker-native VFS over Mossaic. fs/promises-shaped, isomorphic-git compatible, multi-tenant, content-addressed, deduplicating, streaming-aware.

> For the canonical shape of every public API (`MossaicEnv`, `WriteFileOpts`, `YDocHandle`, HTTP envelope), see the **[integration guide](../docs/integration-guide.md)**. This README is a DX walkthrough; the integration guide is the source of truth.

```
                                          Architecture
                                  ─────────────────────────
┌────────────────┐  1 DO RPC      ┌─────────────────────────┐   N internal    ┌─────────────────┐
│ Consumer       │ ─────────────► │  MOSSAIC_USER  (Core)   │ ──────────────► │ MOSSAIC_SHARD   │
│ Worker         │ ◄───────────── │  UserDOCore (per-tenant)│ ◄────────────── │ DO instances    │
│  (createVFS)   │                │                         │   chunk RPCs    │ (per shardIdx)  │
└──┬─────────────┘                │  ┌───────────────────┐  │                 └─────────────────┘
   │                              │  │ vfs_meta          │  │                  content-addressed
   │  WS upgrade                  │  │ files + file_tags │  │                  chunks, refcounted
   │ (Yjs binary sync)            │  │ file_versions     │  │                  partial unique idx
   └──── Hibernation API ─────────┤  │ yjs_oplog/yjs_meta│  │                  ⌃ shared by tenants
                                  │  │ idx_files_*       │  │                    ONLY when same DO
                                  │  └───────────────────┘  │                    binding (= same
                                  │  + listFiles cursor     │                    tenant by name)
                                  │    (HMAC w/ JWT_SECRET) │
                                  └────────────┬────────────┘
                                               │ (App vs Core split)
                                               │ App: UserDO extends UserDOCore
                                               │       └─ + _legacyFetch (byte-pinned)
                                               │       └─ + SearchDO (App-only, not in SDK)
                                               │ Service-mode: binds UserDOCore directly
                                               ▼
                                  ┌──────────────────────────┐
                                  │ HTTP fallback transport  │
                                  │ (non-Worker consumers)   │
                                  │ /api/vfs/*  Bearer VFS-  │
                                  │ scoped JWT (jose HS256)  │
                                  └──────────────────────────┘
```

The runtime split:

| Mode          | DO bindings                    | Routes mounted                  | Use case |
|---------------|--------------------------------|---------------------------------|----------|
| App mode      | `UserDO` (subclass of `UserDOCore`) + `ShardDO` + `SearchDO` | legacy `/api/upload`, `/api/download`, photo-app, `/api/vfs/*` | the existing `mossaic.ashishkumarsingh.com` deployment — the App's `UserDO.fetch` delegates non-WS HTTP to a byte-pinned `_legacyFetch` and forwards WS upgrades to `super.fetch` for Yjs |
| Service mode  | `UserDOCore` + `ShardDO` (no SearchDO) | `/api/vfs/*` only | a new fresh deployment for SDK consumers — see `deployments/service/wrangler.jsonc` |
| Library mode  | consumer's own `MOSSAIC_USER` + `MOSSAIC_SHARD` (re-exported `UserDO`/`ShardDO` from `@mossaic/sdk`) | consumer-defined | the recommended path: `import { UserDO, ShardDO, createVFS }` and bind in your own Worker — 1 DO RPC per VFS call, no HTTP hop |

Per-DO-instance state pinned to a `(ns, tenant, sub?)` tuple:

- **VFS metadata**: `files`, `folders`, `file_versions`, `chunk_refs`, `file_tags`, plus indexes `idx_files_parent_mtime`, `idx_files_parent_name`, `idx_files_parent_size`, `idx_file_tags_tag_mtime`.
- **Yjs**: `yjs_oplog`, `yjs_meta`. WebSocket transport uses Cloudflare's [Hibernation API](https://developers.cloudflare.com/durable-objects/api/websockets/#hibernation-api) — idle sockets cost $0; per-socket state survives hibernation via `serializeAttachment`.
- **listFiles cursor**: opaque base64 payload `{v,ob,d,ov,pid,sig}`. `sig` is HMAC-SHA256 truncated to 128 bits, keyed off `env.JWT_SECRET` (the same Workers secret used for VFS tokens — there is **no** dev fallback string in source). Tampered or wrong-secret cursors throw `EINVAL`.
- **Tenant boundary**: DO instances are named `vfs:${ns}:${tenant}[:${sub}]`. Different triples → different DO instances → different SQLite databases. Cross-tenant Yjs broadcast and cross-tenant listFiles cursors are structurally impossible (each DO has its own `YjsRuntime` and its own SQLite).

---

## Install

```bash
pnpm add @mossaic/sdk
```

The package re-exports the Mossaic Durable Object classes; consumer Workers re-export them in their own entry module so wrangler can discover them at deploy time.

### Workspace (vendored) install — `workspace:*`

For monorepos vendoring the SDK as TypeScript source (no prebuilt `dist/` required), declare the SDK as a workspace dependency and opt into the `workspace` exports condition in your tsconfig:

```jsonc
// consumer/package.json
{ "dependencies": { "@mossaic/sdk": "workspace:*" } }
```

```jsonc
// consumer/tsconfig.json
{
  "compilerOptions": {
    "moduleResolution": "bundler",     // required for customConditions
    "customConditions": ["workspace"], // resolves @mossaic/sdk → src/*.ts
    "types": ["@cloudflare/workers-types"]
  }
}
```

When `customConditions: ["workspace"]` is set, the SDK's `package.json` exports map resolves each subpath to `./src/*.ts` (TS source) instead of `./dist/*.js`. Consumers without the condition fall through to the prebuilt `dist/` artifacts — same behavior as published-on-npm consumers.

Use the canonical `MossaicUserDO` / `MossaicShardDO` names for new workspace consumers (legacy `UserDO` / `ShardDO` re-exports stay for backward compatibility):

```ts
// consumer/src/index.ts
import { createVFS, MossaicUserDO, MossaicShardDO, type MossaicEnv } from "@mossaic/sdk";
export { MossaicUserDO, MossaicShardDO };
```

```jsonc
// consumer/wrangler.jsonc
{
  "durable_objects": {
    "bindings": [
      { "name": "MOSSAIC_USER",  "class_name": "MossaicUserDO" },
      { "name": "MOSSAIC_SHARD", "class_name": "MossaicShardDO" }
    ]
  },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["MossaicUserDO", "MossaicShardDO"] }]
}
```

Yjs / collab editing is loaded lazily — consumers who don't import from `@mossaic/sdk/yjs` and don't set `mode_yjs = 1` on any file pay zero size cost for `yjs` + `y-protocols`. Both peer deps are optional; install only if your tenants use yjs-mode files.

---

## Setup

### 1. Re-export the DO classes from your Worker entry

```ts
// src/index.ts
import { UserDO, ShardDO, createVFS } from "@mossaic/sdk";

// wrangler discovers DO classes from the Worker's main-module exports.
export { UserDO, ShardDO };

export interface Env {
  MOSSAIC_USER:  DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}

export default {
  async fetch(req: Request, env: Env) {
    const vfs = createVFS(env, { tenant: "acme-corp" });
    await vfs.writeFile("/hello.txt", "world");
    const back = await vfs.readFile("/hello.txt", { encoding: "utf8" });
    return new Response(back); // → "world"
  },
};
```

### 2. Wire the wrangler bindings

Copy the [`templates/wrangler.jsonc`](./templates/wrangler.jsonc) snippet into your `wrangler.jsonc`:

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

That's the entire integration. No build step, no service binding, no deployment ceremony.

---

## fs/promises usage

The `VFS` instance returned by `createVFS(env, opts)` exposes the full Node `fs/promises` shape — every method you'd expect, with the same signatures:

```ts
const vfs = createVFS(env, { tenant: "acme" });

// Read / write
await vfs.writeFile("/notes.txt", "hello");
const buf = await vfs.readFile("/notes.txt");                  // → Uint8Array
const txt = await vfs.readFile("/notes.txt", { encoding: "utf8" }); // → string

// Directory ops
await vfs.mkdir("/work", { recursive: true });
const entries = await vfs.readdir("/work");                    // → string[]
await vfs.rmdir("/work");                                      // ENOTEMPTY if children
await vfs.removeRecursive("/big-tree");                        // paginated rm -rf

// Metadata
const stat = await vfs.stat("/notes.txt");
stat.isFile();        // true
stat.isDirectory();   // false
stat.size;            // bytes
stat.mtimeMs;         // ms-since-epoch
stat.mode;            // POSIX mode
stat.ino;             // stable 53-bit safe int

// Lifecycle
await vfs.unlink("/notes.txt");
await vfs.rename("/a.txt", "/b.txt");
await vfs.chmod("/b.txt", 0o600);

// Symlinks
await vfs.symlink("/b.txt", "/link-to-b");
await vfs.readlink("/link-to-b");                              // → "/b.txt"
const lstat = await vfs.lstat("/link-to-b");
lstat.isSymbolicLink();                                        // true

// Existence checks
await vfs.exists("/anything");                                 // → boolean
```

### Batched stat for git-style workloads

`git status` on a large tree calls `lstat` thousands of times. The SDK exposes a batched form:

```ts
const stats = await vfs.readManyStat(["/a", "/b", "/c", "/missing"]);
// → [VFSStat, VFSStat, VFSStat, null]
// Each null is a miss; no throw bubbles for a single ENOENT.
```

This is **one** outbound DO RPC regardless of how many paths you batch.

---

## Streaming for large files

The default `readFile` / `writeFile` cap at **100 MB** (configurable). Above that, use the streaming API:

```ts
// Read: ReadableStream<Uint8Array>, memory-bounded to one chunk
const stream = await vfs.createReadStream("/big.bin");
for await (const chunk of stream) {
  // process chunk
}

// Range reads
const range = await vfs.createReadStream("/big.bin", { start: 1024, end: 2048 });

// Write: WritableStream<Uint8Array>
const out = await vfs.createWriteStream("/big.bin");
const writer = out.getWriter();
await writer.write(chunk1);
await writer.write(chunk2);
await writer.close(); // commit-renames atomically; partial writes never visible

// Resumable writes via the handle-based primitive
const { stream: writeStream, handle } = await vfs.createWriteStreamWithHandle("/big.bin");
// `handle.tmpId` is stable for the duration of the write — useful for
// progress tracking or pause/resume across consumer invocations.
```

### Caller-orchestrated multi-invocation reads (multi-GB files)

For files larger than what one Worker invocation can fan out (~1,000 chunks paid tier), drive the chunk fetches yourself across separate consumer invocations:

```ts
const m = await vfs.openManifest("/huge.tar");
// → { fileId, size, chunkSize, chunkCount, chunks: [{index, hash, size}], inlined }

// Each readChunk call is one consumer-side subrequest.
// Mossaic's UserDO does just one ShardDO fetch per call.
const chunk0 = await vfs.readChunk("/huge.tar", 0);
const chunk1 = await vfs.readChunk("/huge.tar", 1);
// ...
```

Note: `chunks[].shardIndex` is intentionally hidden — it's an internal placement detail, not a stable identifier.

---

## isomorphic-git

`vfs.promises === vfs`, so the VFS instance is a valid isomorphic-git `fs` plugin directly:

```ts
import git from "isomorphic-git";
import { createVFS, createIgitFs } from "@mossaic/sdk/fs";

export default {
  async fetch(req: Request, env: Env) {
    const vfs = createVFS(env, { tenant: "acme" });
    const fs = createIgitFs(vfs); // pass-through; signals intent

    const dir = "/repo";
    await git.init({ fs, dir, defaultBranch: "main" });
    await vfs.writeFile(`${dir}/README.md`, "# hello");
    await git.add({ fs, dir, filepath: "README.md" });
    const oid = await git.commit({
      fs, dir,
      message: "first",
      author: { name: "Tester", email: "test@e.com" },
    });
    return new Response(oid);
  },
};
```

The SDK is verified end-to-end against `isomorphic-git@1.37.x` in `tests/integration/igit-smoke.test.ts`. `git.init`, `add`, `commit`, `log` round-trip cleanly. Symlinks, `chmod`, batched `lstat` (for `git status`) are all supported.

### Auto-batched lstat (opt-in)

`git status` on a large tree fires hundreds of `lstat` calls in tight bursts. Without batching, that's hundreds of DO RPCs against Mossaic's per-invocation budget. With `{ batchLstat: true }`, the SDK coalesces concurrent lstats within a small window into one `readManyStat` RPC:

```ts
const fs = createIgitFs(vfs, { batchLstat: true, batchWindowMs: 10 });
// Now: N concurrent fs.lstat() calls → 1 underlying readManyStat
```

Batched mode preserves the `lstat` contract — successful resolutions return identical `VFSStat` instances, missing paths throw `ENOENT`, transport errors reject every pending caller in the batch identically. Sequential awaited calls still hit one RPC each (the batching is a concurrency optimization, not a serial coalescer). Default off; opt-in for performance-sensitive workloads.

---

## Multi-tenancy

DO instances are named `vfs:${ns}:${tenant}[:${sub}]`. Different triples → different DO instances → different SQLite databases. No cross-tenant data is reachable, ever. Cross-tenant chunk dedup is impossible by construction (chunks live on per-tenant `vfs:...:s${idx}` ShardDOs).

> **Formal proofs.** Mossaic ships Lean 4 formal proofs of the refcount well-formedness, tenant isolation, and GC safety invariants. See [`lean/`](../lean/) for theorem names and `lean/README.md` for what is and isn't proved (zero `sorry` in the must-have set; one declared axiom for the numerical refcount equality, documented in `Gc.lean`). Run `pnpm lean:build` to verify.

```ts
// Production
const acme = createVFS(env, { tenant: "acme-corp", namespace: "prod" });
const wayne = createVFS(env, { tenant: "wayne-enterprises", namespace: "prod" });

// Per-end-user sub-tenancy
const alice = createVFS(env, { tenant: "acme-corp", sub: "alice" });
const bob = createVFS(env, { tenant: "acme-corp", sub: "bob" });

// Staging + production isolated
const staging = createVFS(env, { tenant: "acme-corp", namespace: "staging" });
```

Each component must match `[A-Za-z0-9._-]{1,128}`. `:` is rejected so a malicious tenant cannot inject characters that would collide with another tenant's DO name.

### Operator tokens

For deployments where you want to issue scoped tokens to downstream consumers (so they can talk to Mossaic without holding the operator's API key), use:

```ts
import { issueVFSToken, verifyVFSToken } from "@mossaic/sdk";

// Operator side, holding env.JWT_SECRET:
const token = await issueVFSToken(env, {
  ns: "prod",
  tenant: "acme-corp",
  sub: "alice", // optional
});
// Hand `token` to the downstream consumer. They send it to the
// Mossaic Worker; verifyVFSToken(env, token) validates the scope
// claim before any VFS op runs.
```

The verifier strictly requires `scope === "vfs"`. Legacy login JWTs (with `email` claim, no `scope`) are rejected; VFS tokens are rejected by the legacy verifier. The two surfaces never cross-pollute even though both share `JWT_SECRET`.

---

## File-level versioning (S3-style, opt-in)

Per-tenant versioning is **opt-in** and **off by default** — when off, behavior is byte-equivalent to a non-versioning Mossaic deployment. When on, every `writeFile` and `unlink` creates a historical version row; `readFile` resolves the head; explicit version IDs read history; and a retention API keeps storage bounded.

Enable per call:

```ts
const vfs = createVFS(env, { tenant: "acme-corp", versioning: "enabled" });

await vfs.writeFile("/notes.md", "v1");
await vfs.writeFile("/notes.md", "v2");
await vfs.writeFile("/notes.md", "v3");

// Head reads the newest non-tombstoned version.
await vfs.readFile("/notes.md", { encoding: "utf8" }); // → "v3"

// listVersions: newest-first; tombstones included with deleted=true.
const versions = await vfs.listVersions("/notes.md");
// [
//   { id: "01J...c", mtimeMs: 1735689600100, size: 2, mode: 420, deleted: false },
//   { id: "01J...b", mtimeMs: 1735689600050, size: 2, mode: 420, deleted: false },
//   { id: "01J...a", mtimeMs: 1735689600000, size: 2, mode: 420, deleted: false },
// ]

// Read a specific historical version.
const v1 = await vfs.readFile("/notes.md", {
  version: versions[2].id,
  encoding: "utf8",
});
// v1 === "v1"
```

### Tombstones

`unlink` writes a tombstone version (deleted: true, no chunks) instead of hard-deleting:

```ts
await vfs.unlink("/notes.md");

await vfs.exists("/notes.md");                       // → false
await vfs.readFile("/notes.md");                     // → throws ENOENT
await vfs.listVersions("/notes.md");                 // ← still returns full history
                                                     //   (newest is the tombstone)
```

The previous content's chunks are NOT decremented; they remain reachable via the historical version IDs until you explicitly drop them. This matches S3's delete-marker semantics.

### Restoring a version

`restoreVersion(path, sourceId)` creates a NEW version row whose content matches the source (chunks dedupe automatically — no bytes re-uploaded):

```ts
const versions = await vfs.listVersions("/notes.md");
const v1 = versions.find(v => !v.deleted)!;          // pick a live source
const { id: newId } = await vfs.restoreVersion("/notes.md", v1.id);

await vfs.readFile("/notes.md", { encoding: "utf8" }); // → v1's content
```

The restore is a copy, not a pointer — the source version remains in the history list, and the new version becomes the head. Restoring a tombstone is rejected with `EINVAL`.

### Retention via dropVersions

`dropVersions(path, policy)` reaps versions per a retention policy. The current head is always preserved (S3 invariant — you can't accidentally delete the live content via retention):

```ts
// Keep the head + 9 newest. Drop everything older.
await vfs.dropVersions("/notes.md", { keepLast: 9 });

// Drop versions older than a cutoff (ms epoch).
await vfs.dropVersions("/notes.md", { olderThan: Date.now() - 30 * 86400_000 });

// Combine: keep head + 5 newest + an explicit allowlist.
await vfs.dropVersions("/notes.md", {
  keepLast: 5,
  exceptVersions: ["01J...important", "01J...also-important"],
});

// Drop everything except the head.
await vfs.dropVersions("/notes.md", {});
```

Returns `{ dropped, kept }`. Chunks whose last reference was dropped become eligible for the alarm sweeper after its 30s grace.

### Cross-version dedup

Identical content across versions costs storage exactly once. Two writes of the same payload land on the same content-addressed shard, hit the dedup branch, and share a single chunk row whose `ref_count` equals the number of versions referencing it:

```ts
const payload = new Uint8Array(20 * 1024).fill(0xab);
await vfs.writeFile("/big.bin", payload);            // chunk stored: ref=1
await vfs.writeFile("/big.bin", payload);            // dedup hit:    ref=2
await vfs.writeFile("/big.bin", payload);            // dedup hit:    ref=3

// Storage cost: ONE chunk, three version rows pointing at it.
// dropVersions({ keepLast: 1 }) → ref=1, two chunk_refs decremented.
```

### Mossaic vs Cloudflare Artifacts (versioning vs Git)

Mossaic's versioning is **per-file** and **flat**: every overwrite creates a new immutable version_id; you can list, restore, and drop them. There are no branches, no commit graph, no merge resolution. It's the right fit for object-storage workloads (photos, datasets, attachments, ML weights) where users want "go back to yesterday's file" without buying into Git semantics.

Cloudflare Artifacts is **per-tree** and **versioned-as-Git**: every commit is a snapshot of the entire tree, branches and tags name commit graphs, and Git-protocol clients clone/push/pull. It's the right fit for source-shaped workloads where the consumer wants real Git semantics + Git tooling integration.

The two products **compose**. A Worker can hold both bindings: Artifacts for source repos, Mossaic for blob storage with per-file history. Use whichever shape matches the data.

## Live editing with Yjs (per-file CRDT mode)

adds **native Yjs** as a per-file mode. A regular file can be promoted to "yjs-mode" with a one-line `setYjsMode` call; from then on, every `writeFile` becomes a CRDT transaction, `readFile` materialises the current state, and any number of clients can co-edit live over a WebSocket.

### Mental model

- **Per-file**, not per-tenant. Mix CRDT and plain files freely; only files you toggle become CRDTs.
- **Storage**: one `yjs_oplog` row per Yjs update, keyed by `(path_id, seq)`. Updates are content-hashed and pushed into Mossaic's existing chunk fabric — they share the rendezvous-hashed shard placement, refcounted GC, and per-tenant isolation that ordinary blobs use. **Compaction** periodically emits a `Y.Doc` state snapshot and reaps prior op rows.
- **Wire protocol**: standard Yjs sync protocol (sync_step_1 / sync_step_2 / update) tagged with a single byte. **Binary frames** end-to-end — no JSON, no base64, no envelope overhead.
- **WebSocket transport**: Cloudflare's [Hibernation API](https://developers.cloudflare.com/durable-objects/api/websockets/#hibernation-api). **Idle connections cost $0** — workerd evicts the DO between frames and rehydrates per message. Per-socket state survives via `serializeAttachment`.
- **Versioning interop**: when both versioning and yjs-mode are on, **compaction snapshots** create Mossaic version rows. Live ops between snapshots are NOT versioned — the Yjs op log IS the live history.
- **`yjs` and `y-protocols` are optional peer dependencies** — bring your own versions. Importing from `@mossaic/sdk/yjs` is the opt-in (the main bundle stays Yjs-free).

### Example

```ts
import { createVFS, VFS_MODE_YJS_BIT } from "@mossaic/sdk";
import { openYDoc } from "@mossaic/sdk/yjs";

export default {
  async fetch(req: Request, env: Env) {
    const vfs = createVFS(env, { tenant: "acme" });

    // 1. Promote a file to yjs-mode (one-time per file).
    await vfs.writeFile("/notes/today.md", "# Today\n");
    await vfs.setYjsMode("/notes/today.md", true);
    // Equivalent overload:
    // await vfs.chmod("/notes/today.md", { yjs: true });

    // 2. Subsequent writeFile calls become CRDT transactions.
    await vfs.writeFile("/notes/today.md", "# Today\n- [ ] ship phase 10\n");

    // 3. Clients co-edit live over a WebSocket.
    const handle = await openYDoc(vfs, "/notes/today.md");
    await handle.synced; // initial round-trip complete

    handle.doc.getText("content").insert(0, "DRAFT — ");
    // ... edits propagate to every other open client ...

    // Presence / cursors / selections via y-protocols/awareness.
    handle.awareness.setLocalState({ name: "alice", cursor: 7 });
    handle.awareness.on("change", () => {
      // remote awareness states arrived — render them
      for (const [clientID, state] of handle.awareness.getStates()) {
        // ...
      }
    });

    await handle.close();
    return new Response("ok");
  },
};
```

### Detecting yjs-mode

Stat surfaces the bit on `mode`:

```ts
import { VFS_MODE_YJS_BIT } from "@mossaic/sdk";

const stat = await vfs.stat("/notes/today.md");
if ((stat.mode & VFS_MODE_YJS_BIT) !== 0) {
  // yjs-mode file — readFile returns the materialised content,
  // writeFile becomes a CRDT replacement of Y.Text("content").
}
```

### Caveats

- **Demoting back to plain mode is rejected** (`EINVAL`) — it would silently lose CRDT history. If you need a plain copy, do `readFile` and `writeFile` to a different path.
- **`writeFile` semantics on yjs-mode files**: the new bytes **replace** the value of `Y.Text("content")` inside a single `doc.transact`. If two writers race, both transactions commit and merge in CRDT order — neither is lost. To make finer-grained edits, open a `Y.Doc` via `openYDoc` and mutate it directly.
- **isomorphic-git interop**: a tracked file can be promoted in-place. Once promoted, blob hashes change every transaction (the underlying chunks are Yjs updates, not the file content), so don't expect Git-friendly diffs against earlier commits — promote a file when you want CRDT semantics, not on the source you want Git to track.
- **`yjs` and `y-protocols` peer dependencies**: install both in your consumer Worker. Mossaic does NOT bundle them. Tested against `yjs >=13.6.0`, `y-protocols >=1.0.6`.
- **Awareness is relay-only and never persisted.** The server forwards awareness frames between connected editors but never writes them to SQLite. On DO eviction the per-pathId Awareness instance resets; clients re-broadcast their state on reconnect (this is the same behavior as a vanilla y-websocket server).
- **`YDocHandle` shape.** `{ doc: Y.Doc, awareness: Awareness, synced: Promise<void>, close(): Promise<void>, flush({ label? }): Promise<{ versionId, checkpointSeq }>, onClose(cb), onError(cb) }`. Note: there is no `handle.on("sync")` or `handle.on("update")` event-emitter surface. Subscribe to `doc` and `awareness` instances directly via their respective `on(...)` methods.

## End-to-end encryption (opt-in, )

Mossaic optionally encrypts file content with AES-GCM-256 before it leaves the consumer Worker. The Mossaic server NEVER decrypts user data — it stores opaque envelopes and the per-file `(encryption_mode, encryption_key_id)` columns. Loss of the master key = permanent data loss; there is no recovery path.

### Quickstart

```ts
import {
  createVFS,
  type EncryptionConfig,
} from "@mossaic/sdk";
import { deriveMasterFromPassword } from "@mossaic/sdk/encryption";

const tenantSalt = /* 32 stable random bytes per tenant; treat as data, not a secret */;
const masterKey = await deriveMasterFromPassword("user-password", tenantSalt);
// Or: get raw 32-byte key bytes from your KMS / OS-keychain.

const vfs = createVFS(env, {
  tenant: "alice",
  encryption: { masterKey, tenantSalt /*, mode: "convergent" by default */ },
});

await vfs.writeFile("/secret.txt", "private data", { encrypted: true });
const back = await vfs.readFile("/secret.txt"); // auto-decrypted
```

### Modes

| Mode | When to use | Storage savings |
|---|---|---|
| `convergent` (default) | When dedup matters (image libraries, document repositories). Identical plaintexts under the same `(masterKey, tenantSalt)` produce identical envelopes — cross-file dedup works as before. **Within-tenant equality oracle is the documented cost**: an attacker holding two ciphertexts can determine `pt(a) = pt(b)` but recovers no plaintext bytes. Cross-tenant leak is impossible by salt-distinct construction. |  Full dedup preserved |
| `random` | High-secrecy tenants. Fresh 96-bit IV + per-chunk DEK wrapped under the master via AES-KW. No determinism, no dedup, no equality oracle. IND-CPA secure. | Dedup lost; ~2× storage |

Pick at the VFS level (`encryption.mode`) or per-call (`writeFile(p, d, { encrypted: { mode: "random" } })`). Mode-history is monotonic per path: once a path is encrypted with mode X, all future writes must be mode X (server enforces with `EBADF`).

### What's encrypted

| Surface | Encrypted? | AAD tag |
|---|---|---|
| File content (writeFile / readFile) | YES when `{ encrypted: true }` | `ck` |
| Yjs sync_step_2 / update / awareness payloads (per-file E2E) | YES when the file is encrypted | `yj` / `aw` |
| File metadata, tags, mode, mtime, path | NO (plaintext at the server) | — |
| Yjs state vectors (sync_step_1) | NO (vectors don't reveal content) | — |

### Yjs interaction (encrypted CRDT editing)

Encrypted Yjs files preserve the standard `openYDoc` API:

```ts
import { openYDoc } from "@mossaic/sdk/yjs";

const handle = await openYDoc(vfs, "/notes.md");
// handle.encrypted === true when the file is encrypted
handle.doc.getText("content").insert(0, "secret edit");
```

**Server-side compaction is disabled for encrypted Yjs** (the server can't materialize the doc). The client compacts via `vfs.compactYjs(path)`:

```ts
// React to the server's tag-4 compact-please advisory:
handle.onCompactNeeded(async (seqCount) => {
  await vfs.compactYjs("/notes.md");
});
```

Or call `vfs.compactYjs(path)` on a timer. Backpressure: at 500 envelopes + 100 MB + 7 days inactivity, server rejects further writes with `EBUSY` until the consumer compacts.

### Key custody — your responsibility

Mossaic NEVER stores master keys. Recommendations:

| Environment | Storage |
|---|---|
| Browser | WebCrypto + IndexedDB non-extractable `CryptoKey` |
| Node.js / Worker | KMS (AWS KMS, GCP KMS, etc.) — unwrap on cold start |
| CLI | PBKDF2 over an interactive password (use `deriveMasterFromPassword`) |

PBKDF2 cost (~250–400 ms at 600k iterations, OWASP 2024) is paid once per session.

### Migrating existing plaintext files

```ts
import { migrateEncrypt } from "@mossaic/sdk/encryption";

await migrateEncrypt(vfs, "/path/to/file");        // single file (atomic per file)
// or, in bulk via the CLI:
//   $ mossaic encrypt /path/to/file --mode=convergent
//   $ mossaic encrypt --prefix=/photos/ --mode=convergent
```

Mossaic NEVER auto-encrypts. Pre-Phase-15 files remain plaintext until the consumer opts in.

### Limits & known gotchas

- **Master-key loss is permanent.** Mossaic cannot recover. Document this prominently to your end-users.
- **Convergent mode leaks plaintext-equality within a tenant** (documented; switch to `random` if undesirable). Cross-tenant leak is structurally impossible.
- **Encrypted Yjs cold-open starts blank** when no peer with the master key is connected (the server can't bootstrap state). Surviving peers re-broadcast on reconnect.
- **Re-keying convergent files breaks dedup transitively.** Post-rotation, the chunks table size temporarily ~doubles until the alarm sweeper reaps the old envelopes.
- **`vfs.destroy()` zeroes the in-memory master key.** Call this when you're done with the VFS instance (best-effort; defense-in-depth).

### See also

- [`local/phase-15-plan.md`](../local/phase-15-plan.md) — full design doc (envelope layout, IND-CPA + dedup-oracle theorems, race-rule analysis).
- [`lean/Mossaic/Vfs/Encryption.lean`](../lean/Mossaic/Vfs/Encryption.lean) — formal proof obligations + the single new `AES_GCM_IND_CPA` axiom.
- [`@mossaic/cli`](../cli/README.md) — `mossaic encrypt`, `mossaic decrypt-readback`, `mossaic rotate-key` for command-line use.

## copy, metadata, tags, indexed listFiles, version marks

extends the surface with five additive primitives. None change byte-equivalence: a tenant that never calls these methods sees no behavior change. All caps are enforced server-side and live in `shared/metadata-caps.ts`.

| Cap | Default | Purpose |
|---|---|---|
| `METADATA_MAX_BYTES` | 64 KB | per-file metadata blob (UTF-8 JSON) |
| `TAGS_MAX_PER_FILE` | 32 | distinct tags per file |
| `TAG_MAX_LEN` | 128 chars | each tag, charset `[A-Za-z0-9._:/-]` |
| `TAGS_MAX_PER_LIST_QUERY` | 8 | tags per `listFiles({tags})` query |
| `LIST_LIMIT_MAX` | 1000 | listFiles page size |
| `LIST_LIMIT_DEFAULT` | 50 | listFiles default page size |

### `writeFile` extended options (metadata, tags, version)

Every existing `writeFile` call works unchanged. The new opts are additive:

```ts
await vfs.writeFile("/photos/hike.jpg", bytes, {
  mode: 0o644,
  mimeType: "image/jpeg",
  // metadata: undefined (default) keeps existing.
  // metadata: null clears.
  // metadata: {...} REPLACES (deep-validate; ≤ 64 KB).
  metadata: { camera: "Pixel 8", iso: 200, gps: { lat: 47.6, lng: -122.3 } },
  // tags: undefined (default) keeps. tags: [] drops all. tags: [...] REPLACES.
  tags: ["nature", "2026", "hike"],
  // Per-version flags (ignored on non-versioning tenants).
  version: { label: "trail-summit", userVisible: true },
});
```

### `copyFile(src, dest)` — chunk-refcount-aware, no bytes re-uploaded

Same-tenant copy. Three internal tiers; the SDK picks one based on `src`:

- **Inline tier**: copies `inline_data` bytes in-DO. Zero ShardDO work.
- **Chunked (versioning OFF)**: per-shard `chunksAlive` preflight, then `putChunk(empty)` under the new `file_id` for every chunk. Each unique chunk hash gets `+1 ref` on the dest shard slot — bytes are never re-transmitted.
- **Versioned**: same fan-out, plus a new `file_versions` row pointing at the same chunk graph.
- **Yjs-mode src**: materialises via `readYjsAsBytes`, then `writeFile`s to `dest` as a plain (non-yjs) file.

```ts
// Inherit src's metadata + tags. Default overwrite=true.
await vfs.copyFile("/photos/hike.jpg", "/album/2026/hike.jpg");

// Override metadata/tags on the dest. Refuse if dest exists.
await vfs.copyFile("/photos/hike.jpg", "/album/2026/hike.jpg", {
  metadata: { album: "2026", source: "/photos/hike.jpg" }, // REPLACES
  tags: ["album", "2026"],                                  // REPLACES
  version: { label: "filed", userVisible: true },
  overwrite: false, // throws EEXIST if dest exists
});
```

A formal Lean theorem (`Mossaic.Vfs.Refcount.copyFile_chunks_length_invariant`) proves a `copyFile` never grows the chunk row set — it only bumps `ref_count`. See `lean/Mossaic/Vfs/Refcount.lean`.

### `patchMetadata(path, patch, opts)` — partial-update (deep-merge with null-tombstone)

Surgical metadata edits without rewriting the file. Tags can be added/removed in the same call:

```ts
// Deep-merge patch. Arrays are REPLACED (not merged).
await vfs.patchMetadata("/photos/hike.jpg", { iso: 400, gps: { lat: 47.7 } });
// → metadata is { camera: "Pixel 8", iso: 400, gps: { lat: 47.7, lng: -122.3 } }

// null at any leaf REMOVES that key.
await vfs.patchMetadata("/photos/hike.jpg", { iso: null });

// Pass null at the root to CLEAR all metadata.
await vfs.patchMetadata("/photos/hike.jpg", null);

// Tag adds/removes are atomic with the metadata patch.
await vfs.patchMetadata("/photos/hike.jpg", { reviewed: true }, {
  addTags:    ["approved"],
  removeTags: ["pending"],
});
```

### `listFiles(opts)` — indexed query + HMAC-signed cursor pagination

Replaces the natural-but-expensive "scan everything in a directory" pattern with an index-driven enumeration over four axes — prefix, tags (AND), metadata exact-match, ordering.

```ts
// Default: 50 newest files in the tenant, by mtime desc.
const page1 = await vfs.listFiles();

// Filter to a folder, then tags (AND). Up to 8 tags per query.
const tagged = await vfs.listFiles({
  prefix: "/photos/2026/",
  tags: ["approved", "nature"],
  limit: 100,
  orderBy: "mtime",   // 'mtime' (default) | 'name' | 'size'
  direction: "desc",  // 'desc' default for mtime/size, 'asc' for name
});

// Pagination via opaque cursor. Each page has a stable boundary
// `(orderbyValue, file_id)` with strict tie-break on file_id.
let cursor: string | undefined;
do {
  const page = await vfs.listFiles({ tags: ["approved"], cursor, limit: 50 });
  for (const item of page.items) {
    item.path;       // absolute path
    item.pathId;     // stable file_id
    item.stat;       // VFSStat (omit via includeStat: false)
    item.tags;       // sorted tag array — always present
    item.metadata;   // only when includeMetadata: true
  }
  cursor = page.cursor; // undefined when end-of-list
} while (cursor);

// Exact-match metadata filter (post-filtered, not indexed — pair with
// prefix or tags for index-driven performance).
await vfs.listFiles({
  prefix: "/photos/",
  metadata: { camera: "Pixel 8" },
  includeMetadata: true,
});
```

**Cursor codec.** Each cursor is a base64-url payload `{ v: 1, ob, d, ov, pid, sig }`. `sig` is HMAC-SHA256 (truncated to 128 bits) keyed off `env.JWT_SECRET`. Tampered cursors, cursor-with-different-orderBy, and cursor-with-different-direction all throw `EINVAL` — the next-page query MUST agree with the cursor's encoded shape. There is **no** dev fallback for the secret in source: a deploy without `JWT_SECRET` returns 503 (`VFSConfigError`) on listFiles, exactly like VFS-token verification.

**Index plan.** `listFiles` does NOT let SQLite's planner choose. The driver is selected explicitly:

| Query                         | Index used                    |
|-------------------------------|-------------------------------|
| prefix only, mtime ordering   | `idx_files_parent_mtime`       |
| prefix only, name ordering    | `idx_files_parent_name`        |
| prefix only, size ordering    | `idx_files_parent_size`        |
| single tag                    | `idx_file_tags_tag_mtime`      |
| multiple tags (AND)           | rarest-tag drive + intersect   |
| metadata only                 | post-filter (no JSON index)    |
| prefix + tags                 | rarest dimension drives        |

### `markVersion(path, versionId, opts)` — label + user-visible flag

splits versions into **opportunistic** (e.g. Yjs compaction snapshots, `userVisible=0`) and **user-visible** (`writeFile`, `copyFile`, `restoreVersion`, `flush()`, `markVersion`). The flag is **monotonic** — once visible, always visible. `userVisible: false` on `markVersion` is rejected `EINVAL`.

```ts
const versions = await vfs.listVersions("/notes/today.md");
// Promote the third-newest (an opportunistic compaction) to user-visible
// and attach a label.
await vfs.markVersion("/notes/today.md", versions[2].id, {
  label: "before-edit",
  userVisible: true,
});

// Then surface only user-visible versions to a UI.
const visible = await vfs.listVersions("/notes/today.md", {
  userVisibleOnly: true,
  includeMetadata: true,
});
```

### `YDocHandle.flush({ label })` — explicit Yjs checkpoint → user-visible Mossaic version

Yjs compactions normally fire opportunistically (every 50 ops or 60 s) and produce `userVisible=0` checkpoint rows so a heavy collaborative editor doesn't pollute a user's "save history" view. `flush()` is the explicit save:

```ts
import { openYDoc } from "@mossaic/sdk/yjs";

const handle = await openYDoc(vfs, "/notes/today.md");
await handle.synced;

handle.doc.getText("content").insert(0, "DRAFT — ");

// Force a compaction NOW; mark its checkpoint user-visible; attach a label.
const { versionId, checkpointSeq } = await handle.flush({ label: "draft" });
// versionId === null on tenants without versioning enabled — the compaction
// still happens; just no Mossaic version row.

await handle.close();
```

The compaction snapshot captures whatever the live `Y.Doc` holds at the moment of the flush. Local edits made on this handle are streamed to the server via the open WebSocket synchronously inside `doc.transact`, so the compaction always observes them.

### Wire summary

| Method | Binding RPC | HTTP route |
|---|---|---|
| `writeFile` (extended opts) | `vfsWriteFile` | `POST /api/vfs/writeFile` |
| `copyFile` | `vfsCopyFile` | `POST /api/vfs/copy` (alias `copyFile`) |
| `patchMetadata` | `vfsPatchMetadata` | `PATCH /api/vfs/metadata` (alias `POST /patchMetadata`) |
| `listFiles` | `vfsListFiles` | `POST /api/vfs/list` (alias `listFiles`) |
| `markVersion` | `vfsMarkVersion` | `PUT /api/vfs/version-mark` (alias `POST /markVersion`) |
| `listVersions` (extended opts: `userVisibleOnly`, `includeMetadata`) | `vfsListVersions` | `POST /api/vfs/listVersions` |
| `YDocHandle.flush({ label })` | `vfsFlushYjs` | binding-only (Yjs WS upgrade is binding-only too) |

## Subrequest model

> **Each VFS method = exactly 1 outbound DO RPC subrequest in the consumer's Worker invocation, regardless of internal chunk fan-out.**

Cloudflare Workers cap subrequests per invocation at 50 (free) / 10,000 (paid). A `readFile` of a 100-chunk file would otherwise burn 100 subrequests in the consumer's budget. With Mossaic's typed-DO-RPC architecture:

| Consumer call | Consumer subrequests | UserDO internal subrequests |
|---|---|---|
| `vfs.stat(path)` | 1 | 0 |
| `vfs.readFile(small inline)` | 1 | 0 |
| `vfs.readFile(N-chunk file)` | 1 | N (to ShardDOs) |
| `vfs.writeFile(N-chunk payload)` | 1 | N (to ShardDOs) |
| `vfs.readManyStat(10k paths)` | 1 | 0 |
| `vfs.unlink(file)` | 1 | U (one per touched shard) |

The internal fan-out is billed against Mossaic's per-DO-invocation budget (10,000 paid). Consumer Workers stay well under their own cap. This is the load-bearing efficiency property of the as-Library architecture, pinned by `tests/integration/consumer-fixture.test.ts`.

---

## Errors

All errors are `VFSFsError` subclasses with Node-fs-shaped fields:

```ts
import { ENOENT, EEXIST, EFBIG, VFSFsError } from "@mossaic/sdk";

try {
  await vfs.stat("/missing");
} catch (e) {
  if (e instanceof ENOENT) { /* handle missing */ }
  if (e instanceof VFSFsError) {
    e.code;     // "ENOENT"
    e.errno;    // -2 (Linux libuv convention)
    e.syscall;  // "stat"
    e.path;     // "/missing"
  }
}
```

Codes covered: `ENOENT`, `EEXIST`, `EISDIR`, `ENOTDIR`, `EFBIG`, `ELOOP`, `EBUSY`, `EINVAL`, `EACCES`, `EROFS`, `ENOTEMPTY`, `EAGAIN` (rate-limit, see below), plus `MossaicUnavailableError` (code `EMOSSAIC_UNAVAILABLE`).

isomorphic-git's index-lock retry path checks `e.code === "EEXIST"` / `EBUSY` / `EACCES` directly. The SDK's errors satisfy that contract.

### Graceful degradation

Transport-level failures (DO hibernation timeout, network drop, fetch reject, ECONNREFUSED) map to `MossaicUnavailableError` automatically. Consumers can soft-fail rather than seeing a raw `TypeError` or untyped error:

```ts
import { MossaicUnavailableError } from "@mossaic/sdk";

try {
  return await vfs.readFile("/important.json");
} catch (e) {
  if (e instanceof MossaicUnavailableError) {
    // Mossaic is down. Soft-fail: return cached, schedule retry,
    // serve a degraded experience.
    return cached.fallback;
  }
  throw e; // application errors propagate
}
```

The pattern detector matches: `fetch failed`, `Network connection lost`, `Durable Object hibernation timed out`, `ECONNREFUSED`, `ECONNRESET`, plus the standard fetch `TypeError`. Application-level errors (ENOENT, EEXIST, etc.) are NOT remapped — only true transport failures.

## HTTP fallback (non-Worker consumers)

Inside a Cloudflare Worker, prefer `createVFS(env, opts)` — it dispatches DO RPC over the binding with no network hop. From a non-Worker consumer (browser, Node server, third-party cloud), use the HTTP fallback:

```ts
// Non-Worker side (browser / Node / etc.)
import { createMossaicHttpClient } from "@mossaic/sdk";

const vfs = createMossaicHttpClient({
  url: "https://mossaic.example.com",
  // Token minted by the operator's Worker via issueVFSToken().
  apiKey: process.env.MOSSAIC_VFS_TOKEN!,
});

await vfs.writeFile("/foo.txt", "hello");
const back = await vfs.readFile("/foo.txt", { encoding: "utf8" });
```

Operator-side token issuance:

```ts
// Inside the Mossaic Worker, holding env.JWT_SECRET:
import { issueVFSToken } from "@mossaic/sdk";

const token = await issueVFSToken(env, {
  ns: "default",
  tenant: "acme-corp",
  sub: "alice",
});
// Hand `token` to the downstream consumer. They send it as the
// HTTP client's apiKey.
```

The HTTP client speaks the same `VFSClient` interface — same methods, same typed errors, same multi-tenant scope semantics — so consumer code is portable between binding and HTTP transports. Streaming methods (`createReadStream`, `createWriteStream`) throw `EINVAL` on the HTTP client in v1; use `openManifest` + `readChunk` for caller-orchestrated multi-invocation reads instead.

## Per-tenant rate limits

Each tenant DO instance runs a token-bucket limiter on the VFS RPC surface. Defaults: **100 ops/sec refill, 200 burst**. The legacy user-facing app (the `/api/upload`, `/api/download` routes) is exempt — back-compat with existing traffic.

When a tenant exceeds its bucket, the next op throws `EAGAIN`:

```ts
import { EAGAIN } from "@mossaic/sdk";

try {
  await vfs.readFile("/x.txt");
} catch (e) {
  if (e instanceof EAGAIN) {
    // Bucket exhausted; back off + retry.
    await delay(100);
    return retry();
  }
  throw e;
}
```

Operators tighten or loosen per-tenant via direct SQL on the `quota` row (the columns `rate_limit_per_sec` and `rate_limit_burst` accept null to inherit defaults). Different tenants are in different DO instances entirely, so tenant A's burst doesn't affect tenant B.

---

## Mossaic vs Cloudflare Artifacts

[Cloudflare Artifacts](https://developers.cloudflare.com/artifacts/) is **versioned storage that speaks Git**. Inside a Worker the binding (`env.ARTIFACTS`) creates / discovers repos and returns Git-protocol remote URLs. Standard Git clients then push/pull/clone over HTTPS. Use Artifacts when you want:

- Real Git semantics: branches, tags, atomic ref updates, parallel forks-and-diff workflows.
- A repo your Git tooling, CI, agents, or sandboxes can clone via the standard protocol.
- ArtifactFS lazy-mount in sandboxes / VMs when full clones are too slow.

Mossaic ships **horizontally-parallel chunked `fs/promises` with content-addressed dedup**, accessed inside the consumer Worker as a typed Durable-Object-RPC client. Use Mossaic when you want:

- A general-purpose filesystem inside the Worker itself: photos, video, ML datasets, archives, attachments.
- 1 MB / 1.5 MB / 2 MB adaptive chunking for memory-bounded streaming of multi-GB files.
- Per-chunk SHA-256 dedup *within a tenant* (identical bytes stored once).
- A Node `fs/promises` surface that works with any tool that already speaks it (tar, zip, image processing, isomorphic-git).
- Per-tenant isolation guarantees (separate DO instance per tenant; cross-tenant dedup impossible by construction).
- One outbound DO RPC per VFS call regardless of internal chunk fan-out — the consumer's per-invocation subrequest budget is preserved.

### How they compose

The two products target **different shapes of state** and live in different parts of the request graph:

| | Cloudflare Artifacts | @mossaic/sdk |
|---|---|---|
| Shape of state | Versioned Git repo (commits, trees, refs) | Flat namespaced filesystem (paths → bytes) |
| In-Worker API | `env.ARTIFACTS.create(name)` → `{remote, token}` | `createVFS(env, opts)` → `vfs.readFile(...)` |
| Worker-to-storage path | Worker mints repo + token → Git client over HTTPS | Worker → typed DO RPC (in-process) |
| Dedup | Git-native content-addressing per-blob | Per-chunk SHA-256 within tenant |
| Streaming | Git protocol packfiles / ArtifactFS lazy hydrate | Web Streams over DO RPC, per-chunk |
| Best for | Code, configs, agent workspaces, branching | Photos, datasets, attachments, container layers |

They compose at the Worker level: a single Worker can hold both bindings, hand out an Artifacts repo + token to a downstream Git client for source-shaped state, AND store blob-shaped state in Mossaic VFS:

```ts
// src/index.ts (consumer Worker)
import { UserDO, ShardDO, createVFS } from "@mossaic/sdk";
export { UserDO, ShardDO };

export interface Env {
  ARTIFACTS: Artifacts;                       // CF Artifacts binding
  MOSSAIC_USER:  DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}

export default {
  async fetch(req: Request, env: Env) {
    const url = new URL(req.url);

    // Source-shaped state → Artifacts. Worker creates the repo + token,
    // hands the Git remote URL to the caller. The caller's git client
    // pushes / clones via the Artifacts Git protocol endpoint.
    if (req.method === "POST" && url.pathname === "/repos") {
      const { name } = await req.json<{ name: string }>();
      const created = await env.ARTIFACTS.create(name);
      return Response.json({
        remote: created.remote,
        token: created.token,
      });
    }

    // Asset-shaped state → Mossaic. The Worker stores the blob inline
    // alongside the repo metadata; large files chunk + stream. Per-tenant
    // dedup means two users uploading the same 100 MB tarball pay for
    // one copy each (within their own tenant).
    if (req.method === "PUT" && url.pathname.startsWith("/assets/")) {
      const tenant = req.headers.get("x-tenant") ?? "default";
      const vfs = createVFS(env, { tenant });
      const path = url.pathname.replace(/^\/assets/, "");
      const out = await vfs.createWriteStream(path);
      await req.body!.pipeTo(out);
      return new Response("stored", { status: 201 });
    }

    return new Response("ok");
  },
};
```

Use Artifacts when the consumer wants Git semantics. Use Mossaic when the consumer wants `fs/promises`. They are complementary, not competitive.

---

## Configuration

Most behaviour is sensible-default. Knobs you can turn:

| Knob | Default | Where |
|---|---|---|
| `READFILE_MAX` | 100 MB | `shared/inline.ts` constant; deployment-time. Lowered from 500 MB per audit H7 — Worker soft memory is ~128 MB and `readFile` allocates the full buffer before chunk fetches. |
| `WRITEFILE_MAX` | 100 MB | `shared/inline.ts` constant; deployment-time. Same rationale as `READFILE_MAX`. |
| `INLINE_LIMIT`  | 16 KB  | `shared/inline.ts` constant; raise to ≤2 MB if you store many slightly-larger small files (note SQLite BLOB row cap) |
| `JWT_SECRET`    | **required, no fallback** | `wrangler secret put JWT_SECRET` in production. There is **no** dev fallback string in the source. Any `/api/auth/*` or `/api/vfs/*` request on a deploy without this secret returns 503 (`VFSConfigError`); the legacy `/api/upload`/`/api/download` and SPA assets remain available. Set the secret BEFORE routing real traffic. |

The SDK does not currently expose these as runtime options on `createVFS`; they're per-deployment compile-time constants. Override by patching `shared/inline.ts` and rebuilding.

---

## Testing locally

The SDK ships with:

- `tests/integration/igit-smoke.test.ts` — full isomorphic-git round-trip
- `tests/integration/consumer-fixture.test.ts` — pins the 1-subrequest-per-VFS-call architectural promise
- `tests/integration/efbig.test.ts` — size-cap enforcement
- `tests/integration/dedupe-paths.test.ts` — admin tooling
- `tests/integration/worker-smoke.test.ts` — production regression gate (boots the actual Hono app)
- `tests/integration/streaming.test.ts` — read/write streams, byte-range, handle-based primitives
- `tests/integration/tenant-isolation.test.ts` — cross-tenant impossibility
- `tests/integration/readmany-stat.test.ts` — git-status-style batched lstat
- ... plus path-walk, ino, refcount, migration, legacy-smoke, vfs-read, vfs-write

Run `pnpm test` at the repo root.

---

## License

MIT.
