/**
 * App-side gate helpers — per-tenant rate limiting + write gating
 * for all `app*` RPC methods on `UserDO`.
 *
 * Background. The Core+App split moved Core's per-tenant rate
 * limiter (`gateVfs` in user-do-core.ts) onto every Core RPC method
 * but none of the App-side `app*` RPCs gained the equivalent gate.
 * Production result: legacy SPA hot paths ran unrate-limited and
 * misbehaving / abusive integrations could hammer the tenant DO
 * without ever tripping the per-tenant token bucket that VFS
 * tenants get for free.
 *
 * Core's `gateVfs` is `private`; the App subclass cannot call it.
 * This module reproduces the three things it does — `ensureInit`,
 * `enforceRateLimit`, and scope persistence to `vfs_meta` — using
 * the protected/exported pieces that ARE accessible from outside
 * Core. The implementation is a pure free-function set so the
 * class file stays under the 800-LoC hygiene ceiling.
 *
 * Three gate flavours, mirroring Core:
 *
 *   - `appGate` — read-side: ensureInit + rate-limit + scope-persist.
 *   - `appGateWrite` — read-side gate plus EBUSY refusal when the
 *     H6 partial-UNIQUE-INDEX marker is present (`files_unique_index`).
 *   - `appGateFromPersistedScope` — for app* methods whose RPC
 *     signature lacks userId (caller routes via per-userId stub
 *     so the DO instance is already tenant-scoped, but the method
 *     itself can't construct a fresh scope from arguments). Reads
 *     scope from `vfs_meta.scope` (set by any prior gated call) and
 *     applies `enforceRateLimit` against it. No-op when scope is
 *     absent or malformed — `ensureInit()` remains the load-bearing
 *     call in that case.
 *
 * Per-tenant scope: the App always uses `{ ns: "default", tenant: userId }`
 * — `auth-bridge` mints VFS tokens with that exact shape and DO
 * routing keys on `vfs:default:<userId>`. `appScopeFor()` /
 * `appAuthScopeFor()` are the two derivation points.
 *
 * Brute-force defense for `appHandleSignup` / `appHandleLogin`:
 * these methods land on the `auth:<email>` DO (route layer pins
 * that). Each email gets its own DO ⇒ its own bucket. Per-IP
 * limiting at the DO level is architecturally impossible (the DO
 * does not see the request IP); the per-email bucket is what's
 * implementable here AND is the load-bearing brute-force defense
 * (an attacker hammering one account hits the bucket on attempt
 * ~200 regardless of source IP).
 */

import { UserDOCore } from "@core/objects/user/user-do-core";
import { enforceRateLimit } from "@core/objects/user/rate-limit";
import { VFSError, type VFSScope } from "@shared/vfs-types";

/**
 * Sibling-friendly view of `UserDOCore` whose `ensureInit` is
 * exposed to this module without changing Core's `protected`
 * declaration. The class itself can call `this.ensureInit()` from
 * a method on its own subclass — but we want plain-function
 * helpers that take the DO as an argument. Mapping `protected →
 * public` requires either a subclass-only access pattern or this
 * structural cast at the boundary. We do the cast inside each
 * helper rather than widening the public surface of Core.
 */
type CoreView = UserDOCore & { ensureInit: () => void };

/** Derive the canonical VFSScope for an App tenant. */
export function appScopeFor(userId: string): VFSScope {
  return { ns: "default", tenant: userId };
}

/** Same idea but for the email-keyed auth DO. */
export function appAuthScopeFor(email: string): VFSScope {
  return { ns: "default", tenant: `auth:${email}` };
}

/**
 * Read-side gate: ensureInit + per-tenant rate-limit + scope
 * persistence. Throws `VFSError("EAGAIN", ...)` when the bucket is
 * exhausted (the route handlers already map this onto the wire as
 * 429).
 *
 * Scope is persisted so Core's `loadScope` (used by alarm + index
 * reconciler) finds it on App-only tenants whose only prior touches
 * have been app* methods (no canonical `vfs*` calls).
 */
export function appGate(do_: UserDOCore, scope: VFSScope): void {
  (do_ as CoreView).ensureInit();
  enforceRateLimit(do_, scope);
  const value = JSON.stringify(
    scope.sub !== undefined
      ? { ns: scope.ns, tenant: scope.tenant, sub: scope.sub }
      : { ns: scope.ns, tenant: scope.tenant }
  );
  do_.sql.exec(
    "INSERT OR REPLACE INTO vfs_meta (key, value) VALUES ('scope', ?)",
    value
  );
}

/**
 * Write-side gate: `appGate` plus an EBUSY refusal when the partial
 * UNIQUE index on `files` is missing (H6 marker present). Reads
 * tolerate dupes; writes cannot — two concurrent appCreateFolder /
 * appDeleteFile calls on the same `(parent, name)` would otherwise
 * both succeed.
 */
export function appGateWrite(do_: UserDOCore, scope: VFSScope): void {
  appGate(do_, scope);
  const degraded = do_.sql
    .exec(
      "SELECT key FROM vfs_meta WHERE key IN ('files_unique_index', 'folders_unique_index')"
    )
    .toArray() as { key: string }[];
  if (degraded.some((r) => r.key === "files_unique_index")) {
    throw new VFSError(
      "EBUSY",
      "App writes refused: legacy duplicate rows block uniq_files_parent_name. " +
        "Run admin dedupe (`POST /admin/dedupe-paths`) and reload the DO."
    );
  }
}

/**
 * Rate-limit using the scope persisted in `vfs_meta.scope` by a
 * prior gated call. Used by app* methods that don't carry a
 * userId argument (`appGetFile`, `appGetFilePath`, `appGetFolderPath`,
 * `appMarkFileIndexed`) — their route caller routes through
 * `userStub(c.env, userId)` so the DO instance is already
 * tenant-scoped, but the RPC signature lacks the userId to
 * construct a fresh scope from.
 *
 * No-op when `vfs_meta.scope` is absent (first-call-on-fresh-DO
 * or operator-wiped state) or malformed — the caller is expected
 * to have already called `ensureInit()`.
 *
 * Reachability of the no-op branch in production: every App route
 * path that lands a fresh DO requires going through one of the
 * userId-bearing gated calls FIRST (`appGetQuota`, `appListFiles`,
 * `appGetGalleryPhotos`, etc.) — `auth-bridge` mints a VFS token
 * that the SPA uses against analytics + listings before any
 * fileId-only call could fire. The shared-token routes also touch
 * `appGetFile` only after the operator has previously written +
 * shared content (which itself ran through gated paths). So the
 * "no scope persisted" branch is reachable only on a freshly-wiped
 * tenant that the operator immediately probes via fileId-only
 * paths — pathological enough to accept the bypass.
 *
 * IMPORTANT: any throw from `enforceRateLimit` (EAGAIN) MUST
 * propagate. We catch only `JSON.parse` failures; the rate-limit
 * call lives outside the catch.
 */
export function appGateFromPersistedScope(do_: UserDOCore): void {
  const scopeRow = do_.sql
    .exec("SELECT value FROM vfs_meta WHERE key = 'scope'")
    .toArray()[0] as { value: string } | undefined;
  if (scopeRow === undefined) return;
  let parsed: { ns: string; tenant: string; sub?: string } | null = null;
  try {
    parsed = JSON.parse(scopeRow.value) as {
      ns: string;
      tenant: string;
      sub?: string;
    };
  } catch {
    // Malformed scope row — bypass rate limit (keeps ensureInit).
    return;
  }
  if (typeof parsed.ns !== "string" || typeof parsed.tenant !== "string") {
    return;
  }
  enforceRateLimit(do_, {
    ns: parsed.ns,
    tenant: parsed.tenant,
    sub: parsed.sub,
  });
}
