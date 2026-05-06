# @mossaic/cli — Mossaic VFS command-line tool

`mossaic` (alias `mscli`) is a Node 20+ CLI that speaks to a deployed
Mossaic Service worker over HTTP/WSS. It mints VFS tokens locally
(matching `worker/core/lib/auth.ts:signVFSToken`) using the operator's
`JWT_SECRET`, then drives the SDK's `HttpVFS` HTTP fallback for every
`fs/promises`-shaped operation, and a Node-side Yjs adapter for live
CRDT editing over the `/api/vfs/yjs/ws` WebSocket route.

## Install

```bash
pnpm add -g @mossaic/cli
# or, in this monorepo:
pnpm -F @mossaic/cli build
node cli/dist/bin.js --help
```

The package ships a single bundled binary (`dist/bin.js`) registered
under both `mossaic` and `mscli`. Node 20+ is required.

## Auth & config

Configuration lives at `~/.mossaic/config.json` (mode `0600`) under
the dir `~/.mossaic/` (mode `0700`). Layout:

```json
{
  "active": "default",
  "profiles": {
    "default": {
      "endpoint": "https://mossaic-core.ashishkmr472.workers.dev",
      "jwtSecret": "<copy of the wrangler secret>",
      "scope": { "ns": "default", "tenant": "team-acme", "sub": null }
    }
  }
}
```

Set up the active profile interactively:

```bash
mossaic auth setup \
  --name default \
  --endpoint https://mossaic-core.ashishkmr472.workers.dev \
  --secret "$JWT_SECRET" \
  --tenant team-acme

mossaic auth whoami         # round-trips /api/health + stat /
mossaic auth use default    # switch active profile
```

Env-var overrides (highest priority):

| Variable | Effect |
|---|---|
| `MOSSAIC_ENDPOINT` | overrides endpoint URL |
| `MOSSAIC_JWT_SECRET` | overrides the JWT secret |
| `MOSSAIC_NS` | overrides namespace |
| `MOSSAIC_TENANT` | overrides tenant |
| `MOSSAIC_SUB` | overrides sub-tenant |
| `MOSSAIC_CONFIG_HOME` | overrides config dir (default `~/.mossaic`) |

### Security model

The CLI is an **operator** tool. The same secret already lives on the
operator's machine in `wrangler secret put` history; storing it under
`0600` in `~/.mossaic/config.json` is not a stronger threat than
`~/.config/.wrangler/`. v1 trades remote token issuance for a
single-process, zero-network startup; minted tokens are never
persisted to disk and default to a 1-hour TTL.

## Commands

Global flags (every command):

| Flag | Effect |
|---|---|
| `--profile <name>` | pick a non-active profile |
| `--json` | JSON output for list-style commands |

### Auth

| Command | Description |
|---|---|
| `mossaic auth setup` | write/update a profile |
| `mossaic auth use <profile>` | set active profile |
| `mossaic auth whoami` | verify endpoint + scope round-trip |

### File ops (HTTP, buffered)

| Command | Description |
|---|---|
| `mossaic ls <path>` | list directory entries (`vfs.readdir`) |
| `mossaic cat <path> [--encoding utf8] [--version <id>]` | read to stdout |
| `mossaic write <path> [--text \| --from <local>] [--mode] [--mime] [--metadata <json>] [--tag <t>...] [--version-label]` | write a file |
| `mossaic put <local> <remote>` | upload local file |
| `mossaic get <remote> [-o <local>]` | download |
| `mossaic stream-put <remote> [--max-size <bytes>]` | buffered stdin upload (≤ 32 MiB DO RPC cap) |
| `mossaic stream-get <remote> [-o <local>]` | `openManifest` + `readChunk` loop |
| `mossaic rm <path> [-r]` | unlink (or removeRecursive with `-r`) |
| `mossaic mv <src> <dst>` | rename |
| `mossaic cp <src> <dest> [--no-overwrite] [--metadata] [--tag]` | `copyFile` |
| `mossaic mkdir <path> [-p] [--mode <octal>]` | mkdir |
| `mossaic rmdir <path>` | empty dir only |
| `mossaic rm-rf <path>` | paginated `removeRecursive` |
| `mossaic stat <path...> [--lstat] [--many]` | stat / lstat / readManyStat |
| `mossaic ln <target> <path>` | symlink |
| `mossaic readlink <path>` | read symlink target |
| `mossaic chmod <mode\|true\|false> <path> [--yjs]` | chmod (numeric) or `setYjsMode` |
| `mossaic exists <path>` | exits 0 if present, 1 otherwise |

### Phase 12 (metadata + indexed query)

| Command | Description |
|---|---|
| `mossaic meta patch <path> [--patch <json>] [--from <file>] [--null] [--add-tag <t>...] [--remove-tag <t>...]` | atomic deep-merge metadata + tag delta |
| `mossaic find [--prefix] [--tag <t>...] [--metadata <json>] [--limit] [--cursor] [--order-by mtime\|name\|size] [--direction asc\|desc] [--include-metadata] [--all]` | indexed `listFiles` |

### Versioning (S3-style; opt-in per tenant)

| Command | Description |
|---|---|
| `mossaic versions ls <path> [--user-visible-only] [--include-metadata] [--limit]` | list versions |
| `mossaic versions restore <path> <versionId>` | create a new version from history |
| `mossaic versions drop <path> [--keep-last] [--older-than] [--except <id>...]` | retention sweep (head always preserved) |
| `mossaic versions mark <path> <versionId> [--label] [--user-visible]` | set label + monotonic visible flag |

### Yjs (live CRDT editing)

| Command | Description |
|---|---|
| `mossaic yjs init <path>` | flip yjs-mode bit |
| `mossaic yjs edit <path> [--flush] [--label]` | append stdin to `Y.Text("content")` over WS |
| `mossaic yjs awareness <path> [--name] [--watch <s>]` | watch awareness updates |
| `mossaic yjs flush <path> [--label]` | trigger compaction → user-visible version |

### Token utility

| Command | Description |
|---|---|
| `mossaic token mint [--ttl <ms>]` | print a fresh JWT to stdout |

## Exit codes

| Class | Code |
|---|---|
| `MossaicUnavailableError` (transport / endpoint unreachable) | `2` |
| `VFSFsError` (`ENOENT`, `EACCES`, `EAGAIN`, etc.) | `1` |
| programmer error / config missing / arg parse failure | `1` |

The richer POSIX-style code (e.g. `ENOENT`) is printed on stderr as
`mossaic: ENOENT, stat '/foo': no such file or directory` for
debuggability, but the process exit is binary (`1` / `2`).

## Testing

```bash
pnpm -F @mossaic/cli test         # unit tests (jwt, config, exit-codes, format)
MOSSAIC_E2E_JWT_SECRET=<value> \
  pnpm -F @mossaic/cli test:e2e   # live E2E against mossaic-core
```

The E2E suite runs against `MOSSAIC_E2E_ENDPOINT` (default
`https://mossaic-core.ashishkmr472.workers.dev`). Each test creates a
fresh ULID-suffixed tenant and tears down via `removeRecursive`
on every top-level entry. When `MOSSAIC_E2E_JWT_SECRET` is unset,
the E2E suite skips with a clear console warning rather than
producing cryptic 401 failures.

E2E categories (≥58 SDK + ≥10 functional):

| Category | File | Cases |
|---|---|---|
| A — Auth & scope isolation | `tests/e2e/a-auth.test.ts` | 5 |
| B — Basic file ops | `tests/e2e/b-fileops.test.ts` | 10 |
| C — Streaming + Phase 13 metadata | `tests/e2e/c-streaming.test.ts` | 5 |
| D — Phase 12 metadata/tags/copy/listFiles | `tests/e2e/d-phase12.test.ts` | 12 |
| E — Versioning | `tests/e2e/e-versioning.test.ts` | 8 |
| F — Yjs (over wss://) | `tests/e2e/f-yjs.test.ts` | 6 |
| G — Tenant isolation | `tests/e2e/g-tenant-iso.test.ts` | 4 |
| H — Performance smoke (advisory) | `tests/e2e/h-perf.test.ts` | 3 |
| I — Error surface | `tests/e2e/i-errors.test.ts` | 5 |
| J — Functional via execa | `tests/e2e/j-functional.test.ts` | 13 |
| **Total** | | **71** |

## License

MIT.
