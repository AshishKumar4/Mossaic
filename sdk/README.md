# @mossaic/sdk

Cloudflare-Worker-native VFS over Mossaic. fs/promises-shaped, isomorphic-git compatible, multi-tenant, content-addressed, deduplicating, streaming-aware.

```
┌────────────────┐  1 DO RPC   ┌─────────────────┐  N internal   ┌─────────────────┐
│ Consumer       │ ──────────► │ MOSSAIC_USER DO │ ────────────► │ MOSSAIC_SHARD   │
│ Worker         │ ◄────────── │ (per-tenant)    │ ◄──────────── │ DO instances    │
└────────────────┘             └─────────────────┘   chunks      └─────────────────┘
   1 outbound /                   metadata + manifest             content-addressed
   VFS call                       per (ns, tenant, sub?)          chunks, refcounted
```

---

## Install

```bash
pnpm add @mossaic/sdk
```

The package re-exports the Mossaic Durable Object classes; consumer Workers re-export them in their own entry module so wrangler can discover them at deploy time.

---

## Setup

### 1. Re-export the DO classes from your Worker entry

```ts
// src/index.ts
import { UserDO, ShardDO, SearchDO, createVFS } from "@mossaic/sdk";

// wrangler discovers DO classes from the Worker's main-module exports.
export { UserDO, ShardDO, SearchDO };

export interface Env {
  MOSSAIC_USER:   DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD:  DurableObjectNamespace<ShardDO>;
  MOSSAIC_SEARCH: DurableObjectNamespace<SearchDO>;
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
      { "name": "MOSSAIC_USER",   "class_name": "UserDO" },
      { "name": "MOSSAIC_SHARD",  "class_name": "ShardDO" },
      { "name": "MOSSAIC_SEARCH", "class_name": "SearchDO" }
    ]
  },
  "migrations": [
    { "tag": "mossaic-v1", "new_sqlite_classes": ["UserDO", "ShardDO", "SearchDO"] }
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

The default `readFile` / `writeFile` cap at **500 MB** (configurable). Above that, use the streaming API:

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
import { UserDO, ShardDO, SearchDO, createVFS } from "@mossaic/sdk";
export { UserDO, ShardDO, SearchDO };

export interface Env {
  ARTIFACTS: Artifacts;                       // CF Artifacts binding
  MOSSAIC_USER:   DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD:  DurableObjectNamespace<ShardDO>;
  MOSSAIC_SEARCH: DurableObjectNamespace<SearchDO>;
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
| `READFILE_MAX` | 500 MB | `shared/inline.ts` constant; deployment-time |
| `WRITEFILE_MAX` | 500 MB | `shared/inline.ts` constant; deployment-time |
| `INLINE_LIMIT`  | 16 KB  | `shared/inline.ts` constant; raise to ≤2 MB if you store many slightly-larger small files (note SQLite BLOB row cap) |
| `JWT_SECRET`    | required | `wrangler secret put JWT_SECRET` in production |

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
