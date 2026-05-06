# @mossaic/sdk

Cloudflare-Worker-native VFS over Mossaic. fs/promises-shaped, isomorphic-git compatible.

## Install

```bash
pnpm add @mossaic/sdk
```

## Quick start

```ts
// src/index.ts
import { UserDO, ShardDO, createVFS } from "@mossaic/sdk";

// Re-export the DO classes so wrangler can discover them.
export { UserDO, ShardDO };

export interface Env {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}

export default {
  async fetch(req: Request, env: Env) {
    const vfs = createVFS(env, { tenant: "acme" });
    await vfs.writeFile("/foo.txt", "hello");
    const back = await vfs.readFile("/foo.txt", { encoding: "utf8" });
    return new Response(back);
  },
};
```

Copy [`templates/wrangler.jsonc`](./templates/wrangler.jsonc) into your project's wrangler config.

## What you get

- `vfs.readFile / writeFile / readdir / stat / lstat / unlink / mkdir / rmdir`
- `vfs.rename / chmod / symlink / readlink / exists`
- `vfs.removeRecursive` (paginated, handles arbitrarily-large subtrees)
- `vfs.createReadStream / createWriteStream` for memory-bounded streaming
- `vfs.openManifest / readChunk` low-level escape hatch for caller-orchestrated multi-invocation reads
- `vfs.readManyStat(paths[])` batched stat for git-style workloads (10k paths in one RPC)
- `vfs.promises === vfs` — works as an isomorphic-git `fs` plugin directly:
  ```ts
  import git from "isomorphic-git";
  import { createVFS, createIgitFs } from "@mossaic/sdk/fs";
  const vfs = createVFS(env, { tenant: "acme" });
  await git.clone({ fs: createIgitFs(vfs), dir: "/repo", url: "..." });
  ```

## Subrequest model

Every VFS method is **one DO RPC** in the consumer's Worker invocation,
regardless of how many internal subrequests Mossaic dispatches to ShardDOs.
A `readFile` of a 100-chunk file costs the consumer 1 subrequest.
Internal chunk fan-out is billed against Mossaic's per-invocation budget
(1,000 free / 10,000 paid).

## Multi-tenancy

The DO instance for a given `(namespace, tenant, sub?)` triple is named
`vfs:${ns}:${tenant}[:${sub}]`. Different triples → different DO
instances → different SQLite databases. Cross-tenant chunk dedup is
impossible by construction.

## Errors

All errors are `VFSFsError` subclasses with `code` (string), `errno`
(int, libc convention), `syscall`, `path`. Match Node's `fs` shape so
isomorphic-git's index-lock retry path and any other Node-style
consumer recognises them.

```ts
import { ENOENT, EEXIST } from "@mossaic/sdk";
try { await vfs.stat("/missing"); }
catch (e) {
  if (e instanceof ENOENT) { /* ... */ }
}
```

## License

MIT.
