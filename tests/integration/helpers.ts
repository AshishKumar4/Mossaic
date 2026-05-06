/**
 * Phase 29 — shared test helpers.
 *
 * Centralizes the two patterns that produce ~50% of the test-pool
 * tsc baseline noise:
 *
 *  1. The MOSSAIC_USER + MOSSAIC_SHARD env shape that every
 *     `MossaicEnv` consumer needs. Each test file used to repeat
 *     `interface E { MOSSAIC_USER: ...; MOSSAIC_SHARD: ...; }` and
 *     then a partial `envFor()` helper that omitted MOSSAIC_SHARD,
 *     causing TS2741.
 *
 *  2. The DO-namespace cast for App-side methods. The SDK's
 *     `UserDO` type only exposes the Core surface (vfs* + admin*).
 *     Tests calling App-only methods like `appListUnindexedFiles`
 *     or `appMarkFileIndexed` saw TS2339 because those live on the
 *     App's `UserDO` class (`worker/app/objects/user/user-do.ts`),
 *     not on `UserDOCore`. The structural type below mirrors only
 *     the App-only surface that tests reach for; vfs* methods
 *     remain reached via the SDK's typed surface.
 *
 * Tests can opt-in by importing from this module:
 *
 *     import { mossaicEnvForTest, type AppOnlyMethods } from "./helpers";
 *     const E = env as TestEnv;
 *     const envFor = (): MossaicEnv => mossaicEnvForTest(E);
 *
 * Existing tests keep their inline `envFor` patterns until they
 * touch the file for other reasons (no churn-for-churn).
 */

import { env } from "cloudflare:test";
import type { MossaicEnv, UserDO } from "../../sdk/src/index";

/**
 * The exact shape `env` carries in this repo's vitest-pool-workers
 * config. Every test that wants both bindings can `as TestEnv`.
 */
export interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
  JWT_SECRET?: string;
}

/**
 * Build a `MossaicEnv` carrying BOTH required bindings. The SDK
 * never reads `env.MOSSAIC_SHARD` directly (the worker's own
 * context does), but the type requires it for wrangler-binding
 * symmetry with production. Centralizing the cast here means tests
 * never have to hand-roll the pair.
 */
export function mossaicEnvForTest(e: TestEnv = env as TestEnv): MossaicEnv {
  return {
    MOSSAIC_USER: e.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD: e.MOSSAIC_SHARD as unknown as MossaicEnv["MOSSAIC_SHARD"],
  };
}

/**
 * Structural type for the App-side methods that tests reach via
 * the typed DO stub. The SDK's `UserDO` (re-exported from
 * `@app/objects/user/user-do` for binding-mode consumers) has
 * these — but tests that import `UserDO` from the SDK (which is
 * the Core surface) lose them at the type boundary.
 *
 * Use as: `const stub = userStubAs<AppOnlyMethods>(tenant)` to
 * get a typed surface for App-only RPCs. NOT a runtime cast — the
 * binding is the same DO; this only paints types.
 */
export interface AppOnlyMethods {
  /** Phase 23 — search-index reconciler enumeration. */
  appListUnindexedFiles(
    userId: string,
    limit: number
  ): Promise<
    Array<{
      file_id: string;
      file_name: string;
      mime_type: string;
      file_size: number;
    }>
  >;
  /** Phase 23 — stamp `indexed_at` on a row after successful indexing. */
  appMarkFileIndexed(fileId: string): Promise<void>;
  /** Phase 23 — admin route for tenant deduplication. */
  adminDedupePaths(scope: { ns: string; tenant: string; sub?: string }): Promise<{
    rewritten: number;
    dropped: number;
  }>;
  /** Phase 21 — admin gate for tenant versioning. */
  adminSetVersioning(userId: string, enabled: boolean): Promise<void>;
}

/**
 * Project a typed stub from the test env's MOSSAIC_USER namespace.
 * The cast through `unknown` is unavoidable: workers-types models
 * `DurableObjectNamespace<T>.get()` as `DurableObjectStub<T>` but
 * `T extends Rpc.DurableObjectBranded`, which is incompatible with
 * the structural `UserDO` re-export. The runtime stub has every
 * method we declare; this helper paints the type without touching
 * the binding.
 */
export function userStubAs<T = UserDO>(
  e: TestEnv,
  name: string
): DurableObjectStub<UserDO> & T {
  const stub = e.MOSSAIC_USER.get(e.MOSSAIC_USER.idFromName(name));
  return stub as unknown as DurableObjectStub<UserDO> & T;
}

/**
 * Typed alias for `vfsListFiles` / `vfsListVersions` results. The
 * `Rpc.Result` filter in workers-types collapses to `never` when
 * any field in the response shape uses `Record<string, unknown>`
 * (its `Serializable<R>` predicate doesn't recognize plain object
 * records). At runtime the workers-pool stubs ignore that
 * constraint and forward the actual returned object — so consumers
 * cast through these aliases to recover the structural type
 * without reintroducing `as any`.
 */
export interface ListFilesPageResult {
  items: Array<{
    path: string;
    pathId: string;
    stat?: {
      type: "file" | "dir" | "symlink";
      mode: number;
      size: number;
      mtimeMs: number;
      uid: number;
      gid: number;
      ino: number;
      encryption?: { mode: "convergent" | "random"; keyId?: string };
    };
    metadata?: Record<string, unknown> | null;
    tags: string[];
  }>;
  cursor?: string;
}

export interface VersionRowResult {
  versionId: string;
  mtimeMs: number;
  size: number;
  mode: number;
  deleted: boolean;
  label?: string | null;
  userVisible?: boolean;
  metadata?: Record<string, unknown> | null;
  encryption?: { mode: "convergent" | "random"; keyId?: string };
}

/**
 * Wrapper for `stub.vfsListVersions(...)` that casts past the
 * `Rpc.Result === never` collapse (workers-types collapses returns
 * containing `Record<string, unknown>` to `never`; the runtime
 * stub still forwards the actual shape). Identical runtime; only
 * the TS surface differs.
 *
 * Parameter is `unknown` because the stub's method signature has
 * been narrowed to `never` by the type-system at this point;
 * accepting `unknown` lets the call site pass through.
 */
export async function listVersionsVia(
  stub: unknown,
  scope: { ns: string; tenant: string; sub?: string },
  path: string,
  opts?: {
    limit?: number;
    userVisibleOnly?: boolean;
    includeMetadata?: boolean;
  }
): Promise<VersionRowResult[]> {
  const s = stub as {
    vfsListVersions: (
      scope: { ns: string; tenant: string; sub?: string },
      path: string,
      opts?: {
        limit?: number;
        userVisibleOnly?: boolean;
        includeMetadata?: boolean;
      }
    ) => Promise<VersionRowResult[]>;
  };
  return s.vfsListVersions(scope, path, opts);
}

/**
 * Wrapper for `stub.vfsListFiles(...)` mirroring `listVersionsVia`.
 */
export interface ListFilesViaOpts {
  prefix?: string;
  tags?: readonly string[];
  metadata?: Record<string, unknown>;
  limit?: number;
  cursor?: string;
  orderBy?: "mtime" | "name" | "size";
  direction?: "asc" | "desc";
  includeStat?: boolean;
  includeMetadata?: boolean;
  includeTombstones?: boolean;
  includeArchived?: boolean;
}

export async function listFilesVia(
  stub: unknown,
  scope: { ns: string; tenant: string; sub?: string },
  opts?: ListFilesViaOpts
): Promise<ListFilesPageResult> {
  const s = stub as {
    vfsListFiles: (
      scope: { ns: string; tenant: string; sub?: string },
      opts?: ListFilesViaOpts
    ) => Promise<ListFilesPageResult>;
  };
  return s.vfsListFiles(scope, opts);
}
