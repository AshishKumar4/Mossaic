/**
 * Local VFS-token minter for the @mossaic/cli.
 *
 * The Mossaic Service worker (and any consumer Worker holding the
 * SDK's binding) verifies VFS tokens via `verifyVFSToken` (worker/
 * core/lib/auth.ts). The wire shape is HS256 with claims
 * `{ scope: "vfs", ns, tn, sub? }` plus standard `iat` / `exp`. To
 * stay byte-equivalent with the worker-side `signVFSToken`, this
 * module reproduces the same shape using `jose.SignJWT`.
 *
 * Justification for client-side minting in v1:
 *   - The CLI is an OPERATOR tool. The same secret already lives on
 *     the operator's machine in `wrangler secret put` history; storing
 *     it in `~/.mossaic/config.json` (mode 0600) is no weaker than
 *     `~/.config/.wrangler/`.
 *   - The CLI runs as a single short-lived process; minted tokens
 *     never live on disk. The `--ttl` flag defaults to 1h.
 *   - A future v2 may add an `--issuer-url` mode that POSTs to an
 *     operator Worker behind an OAuth wall.
 */

import { SignJWT } from "jose";

export interface MintTokenInput {
  /** The shared secret used to sign tokens. Must match the worker's `JWT_SECRET`. */
  secret: string;
  /** Logical namespace, e.g. "default" / "prod" / "staging". */
  ns: string;
  /** Tenant identifier. */
  tenant: string;
  /** Optional sub-tenant. Omit for tenant-wide tokens. */
  sub?: string;
  /** Time-to-live in milliseconds. Default 1 hour. */
  ttlMs?: number;
}

/**
 * Sign a VFS-scoped JWT. The output is wire-identical to
 * `worker/core/lib/auth.ts:signVFSToken(env, payload)`.
 *
 * Throws `Error` if `secret` is empty — empty-secret signing would
 * succeed locally but every server-side verify would fail with
 * `VFSConfigError`, masking the real misconfiguration.
 */
export async function mintToken(input: MintTokenInput): Promise<string> {
  if (typeof input.secret !== "string" || input.secret.length === 0) {
    throw new Error(
      "mintToken: missing JWT secret. Run `mossaic auth setup` first or set MOSSAIC_JWT_SECRET.",
    );
  }
  if (typeof input.ns !== "string" || input.ns.length === 0) {
    throw new Error("mintToken: ns must be a non-empty string");
  }
  if (typeof input.tenant !== "string" || input.tenant.length === 0) {
    throw new Error("mintToken: tenant must be a non-empty string");
  }
  const key = new TextEncoder().encode(input.secret);
  const claims: Record<string, unknown> = {
    scope: "vfs",
    ns: input.ns,
    tn: input.tenant,
  };
  if (input.sub !== undefined && input.sub !== null) {
    claims.sub = input.sub;
  }
  return await new SignJWT(claims)
    .setProtectedHeader({ alg: "HS256" })
    .setIssuedAt()
    .setExpirationTime(Date.now() + (input.ttlMs ?? 60 * 60 * 1000))
    .sign(key);
}
