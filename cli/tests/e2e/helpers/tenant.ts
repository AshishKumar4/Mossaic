/**
 * Per-test tenant harness.
 *
 * Each test gets its own ulid-suffixed tenant string, a freshly-minted
 * VFS token, an `HttpVFS` client, and a teardown that walks the tenant
 * tree and removes every subtree.
 *
 * Why per-test tenant: the per-tenant rate limiter (100 ops/s, 200
 * burst) is keyed by DO instance, which is keyed by `vfs:${ns}:${tenant}`.
 * Running every test on its own tenant means cross-test interference
 * is structurally impossible, AND we don't have to worry about
 * leftover state from prior runs colliding with assertions.
 */

import { ulid } from "ulid";
import { createMossaicHttpClient, type HttpVFS } from "@mossaic/sdk/http";
import { mintToken } from "../../../src/jwt.js";
import { ENDPOINT, SECRET } from "./env.js";

export interface TenantCtx {
  tenant: string;
  endpoint: string;
  token: string;
  vfs: HttpVFS;
  scope: { ns: string; tenant: string; sub?: string };
  /** Re-mint a token with overridden scope (useful for negative tests). */
  mintToken: (overrides?: {
    ns?: string;
    tenant?: string;
    sub?: string;
    ttlMs?: number;
  }) => Promise<string>;
  /** Build an HttpVFS using a custom token (useful for cross-tenant probes). */
  altClient: (token: string) => HttpVFS;
  teardown: () => Promise<void>;
}

export async function freshTenant(opts?: {
  ns?: string;
  sub?: string;
}): Promise<TenantCtx> {
  const ns = opts?.ns ?? "default";
  const tenant = "e2e-" + ulid().toLowerCase();
  const sub = opts?.sub;
  const token = await mintToken({
    secret: SECRET,
    ns,
    tenant,
    sub,
  });
  const vfs = createMossaicHttpClient({ url: ENDPOINT, apiKey: token });

  const ctx: TenantCtx = {
    tenant,
    endpoint: ENDPOINT,
    token,
    vfs,
    scope: { ns, tenant, sub },
    mintToken: (overrides = {}) =>
      mintToken({
        secret: SECRET,
        ns: overrides.ns ?? ns,
        tenant: overrides.tenant ?? tenant,
        sub: overrides.sub ?? sub,
        ttlMs: overrides.ttlMs,
      }),
    altClient: (tok: string) =>
      createMossaicHttpClient({ url: ENDPOINT, apiKey: tok }),
    teardown: async () => {
      // Walk top-level entries and removeRecursive each. removeRecursive
      // on "/" itself is rejected (EBUSY) by design.
      // ENOTDIR/ENOENT mid-sweep are expected: a previous removeRecursive
      // can collapse a parent before we reach a child path. We swallow
      // those silently and only warn on unexpected codes.
      try {
        const entries = await vfs.readdir("/");
        for (const entry of entries) {
          const p = "/" + entry;
          try {
            await vfs.removeRecursive(p);
          } catch (err) {
            const code = (err as { code?: string }).code;
            if (code !== "ENOENT" && code !== "ENOTDIR" && code !== "EBUSY") {
              // eslint-disable-next-line no-console
              console.warn(`teardown(${tenant}): failed to remove ${p}: ${code ?? err}`);
            }
          }
        }
      } catch (err) {
        const code = (err as { code?: string }).code;
        if (code !== "ENOENT" && code !== "ENOTDIR") {
          // eslint-disable-next-line no-console
          console.warn(`teardown(${tenant}): readdir / failed:`, err);
        }
      }
    },
  };
  return ctx;
}
