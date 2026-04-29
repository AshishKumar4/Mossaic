/**
 * Shared E2E test environment.
 *
 * - MOSSAIC_E2E_ENDPOINT: defaults to the live Service worker URL.
 * - MOSSAIC_E2E_JWT_SECRET: REQUIRED. The same secret deployed via
 *   `wrangler secret put JWT_SECRET --config deployments/service/wrangler.jsonc`.
 *   When unset, every E2E test SKIPS (with a console.warn) so a
 *   developer running the suite without the secret gets a clear
 *   message instead of cryptic 401 failures.
 *
 * Use `requireSecret(t)` at the top of any describe-block to gate
 * the entire suite on the secret being present.
 */

export const ENDPOINT =
  process.env.MOSSAIC_E2E_ENDPOINT ??
  "https://mossaic-core.ashishkmr472.workers.dev";

export const SECRET = process.env.MOSSAIC_E2E_JWT_SECRET ?? "";

export function hasSecret(): boolean {
  return SECRET.length > 0;
}

export function requireSecret(): void {
  if (!hasSecret()) {
    // eslint-disable-next-line no-console
    console.warn(
      "[mossaic-cli e2e] MOSSAIC_E2E_JWT_SECRET is not set; skipping live E2E tests.\n" +
        "  Set it to the same value as the worker's `wrangler secret put JWT_SECRET` value.\n" +
        "  Endpoint: " +
        ENDPOINT,
    );
  }
}
