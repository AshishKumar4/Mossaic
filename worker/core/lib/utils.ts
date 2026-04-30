/**
 * Generate a ULID-like sortable unique ID.
 * Uses timestamp prefix + random suffix.
 */
export function generateId(): string {
  const timestamp = Date.now().toString(36);
  const random = crypto.getRandomValues(new Uint8Array(8));
  const randomStr = Array.from(random)
    .map((b) => b.toString(36).padStart(2, "0"))
    .join("")
    .slice(0, 12);
  return `${timestamp}${randomStr}`;
}

/**
 * Create a JSON error response.
 */
export function errorResponse(
  message: string,
  status: number = 400
): Response {
  return Response.json({ error: message }, { status });
}

/**
 * Create a JSON success response.
 */
export function jsonResponse<T>(data: T, status: number = 200): Response {
  return Response.json(data, { status });
}

// ── VFS DO naming ──────────────────────────────────────────────────────
//
// The VFS uses a *new* DO name pattern that the legacy app does NOT
// share: `vfs:${ns}:${tenant}[:${sub}]` for UserDO and
// `vfs:${ns}:${tenant}[:${sub}]:s${shardIndex}` for ShardDO. Different
// names ⇒ different DO instances ⇒ different SQLite databases. The
// legacy `user:${userId}` and `shard:${userId}:${idx}` instances are
// untouched. Cross-tenant chunk dedup is impossible by construction:
// a tenant's chunks live in a DO whose name embeds (ns, tenant), so two
// tenants with identical content hit different ShardDO instances.
//
// Inputs are validated to keep them in a tight character class so a
// malicious tenant cannot inject `:` and steal another tenant's DO. We
// also bound length to keep DO names within Cloudflare's name limits.

const VFS_NAME_TOKEN = /^[A-Za-z0-9._-]{1,128}$/;

/** Validate a single namespace/tenant/sub component. Throws on bad input. */
function validateVfsToken(label: string, value: string): void {
  if (typeof value !== "string" || !VFS_NAME_TOKEN.test(value)) {
    throw new Error(
      `invalid vfs ${label}: ${JSON.stringify(value)}; allowed: [A-Za-z0-9._-], 1-128 chars`
    );
  }
}

/**
 * Build the UserDO name for a VFS scope.
 *
 *   vfs:${ns}:${tenant}              when sub is undefined
 *   vfs:${ns}:${tenant}:${sub}       when sub is set
 *
 * Each component must match `[A-Za-z0-9._-]{1,128}`. Cross-tenant
 * collisions are impossible by construction because no character in the
 * allowed class is `:`.
 *
 * @lean-invariant Mossaic.Generated.UserDO.cross_tenant_user_isolation
 *   Lean proves that distinct tenants under valid scope produce distinct
 *   UserDO names. See `lean/Mossaic/Vfs/Tenant.lean :: userName_inj` and
 *   the corollary `cross_tenant_user_isolation`.
 */
export function vfsUserDOName(
  ns: string,
  tenant: string,
  sub?: string
): string {
  validateVfsToken("namespace", ns);
  validateVfsToken("tenant", tenant);
  if (sub !== undefined) {
    validateVfsToken("sub", sub);
    return `vfs:${ns}:${tenant}:${sub}`;
  }
  return `vfs:${ns}:${tenant}`;
}

/**
 * Build the ShardDO name for a VFS scope + shard index.
 *
 *   vfs:${ns}:${tenant}:s${shardIndex}                  when sub is undefined
 *   vfs:${ns}:${tenant}:${sub}:s${shardIndex}           when sub is set
 *
 * shardIndex must be a non-negative finite integer. The leading "s"
 * disambiguates the shard suffix from a sub-tenant whose name happens
 * to be a number.
 *
 * @lean-invariant Mossaic.Generated.UserDO.cross_tenant_shard_isolation
 *   Lean proves that, at any fixed shard index, distinct tenants under
 *   valid scope produce distinct ShardDO names. See
 *   `lean/Mossaic/Vfs/Tenant.lean :: shardName_inj_fixed_idx` and the
 *   corollary `cross_tenant_isolation`.
 */
export function vfsShardDOName(
  ns: string,
  tenant: string,
  sub: string | undefined,
  shardIndex: number
): string {
  if (
    !Number.isFinite(shardIndex) ||
    !Number.isInteger(shardIndex) ||
    shardIndex < 0
  ) {
    throw new Error(`invalid vfs shardIndex: ${shardIndex}`);
  }
  const base = vfsUserDOName(ns, tenant, sub);
  return `${base}:s${shardIndex}`;
}
