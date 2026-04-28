import type { UserDO } from "./user-do";
import { VFSError, type VFSScope } from "@shared/vfs-types";

/**
 * Token-bucket rate limiter, per-tenant.
 *
 * The bucket lives in the `quota` row's rate_limit_* columns:
 *   - rate_limit_per_sec (INTEGER NULL): refill rate; null → default
 *   - rate_limit_burst   (INTEGER NULL): max bucket capacity; null → default
 *   - rl_tokens          (REAL NULL):    current token balance
 *   - rl_updated_at      (INTEGER NULL): last refill timestamp (ms)
 *
 * On each VFS RPC call, the limiter:
 *   1. Reads (or initialises) the bucket state.
 *   2. Refills tokens proportional to elapsed time × rate.
 *   3. If tokens >= 1, decrements 1 and persists. Allow.
 *   4. Otherwise, throws `VFSError("EAGAIN", ...)` — consumer's
 *      `mapServerError` re-types this to the typed `EAGAIN` subclass.
 *
 * Defaults are deliberately generous (100 ops/sec, 200 burst) — most
 * legitimate workloads stay well below. Pathological abuse (DoS,
 * runaway clients, misconfigured retries) trips the limit before
 * burning DO storage / capacity. Operators tighten via:
 *
 *     UPDATE quota
 *        SET rate_limit_per_sec = 10, rate_limit_burst = 20
 *      WHERE user_id = '<tenant>'
 *
 * Per-tenant scope: each (ns, tenant, sub) hits a different DO
 * instance, so bucket state is naturally per-tenant. Tenant A's
 * burst doesn't affect tenant B because they're in different
 * SQLite databases entirely.
 */

const DEFAULT_RATE_PER_SEC = 100;
const DEFAULT_BURST = 200;

interface BucketRow {
  rate_limit_per_sec: number | null;
  rate_limit_burst: number | null;
  rl_tokens: number | null;
  rl_updated_at: number | null;
}

/**
 * Check + decrement the rate-limit bucket for the given scope's
 * underlying user_id. Throws `VFSError("EAGAIN", ...)` when over
 * limit.
 *
 * `userIdFor(scope)` from vfs-ops is the same canonical mapping —
 * we reproduce it here to avoid a circular import. Each
 * (tenant, sub?) gets a distinct bucket because the user_id key
 * differs.
 */
export function enforceRateLimit(
  durableObject: UserDO,
  scope: VFSScope
): void {
  const userId = scope.sub
    ? `${scope.tenant}::${scope.sub}`
    : scope.tenant;

  // Ensure a quota row exists. Default everything if missing — the
  // caller's first VFS op against a brand-new tenant should not
  // fail because of a missing-row race.
  durableObject.sql.exec(
    `INSERT OR IGNORE INTO quota
       (user_id, storage_used, storage_limit, file_count, pool_size)
     VALUES (?, 0, 107374182400, 0, 32)`,
    userId
  );

  const row = durableObject.sql
    .exec(
      `SELECT rate_limit_per_sec, rate_limit_burst, rl_tokens, rl_updated_at
         FROM quota WHERE user_id = ?`,
      userId
    )
    .toArray()[0] as unknown as BucketRow | undefined;

  const ratePerSec = row?.rate_limit_per_sec ?? DEFAULT_RATE_PER_SEC;
  const burst = row?.rate_limit_burst ?? DEFAULT_BURST;
  const now = Date.now();
  const lastUpdate = row?.rl_updated_at ?? now;
  // Initialise to full bucket on the first observation.
  const tokensBefore = row?.rl_tokens ?? burst;

  // Refill: dt seconds × rate, capped at burst.
  const dtSec = Math.max(0, (now - lastUpdate) / 1000);
  const refilled = Math.min(burst, tokensBefore + dtSec * ratePerSec);

  if (refilled < 1) {
    // Persist the refill state even on rejection so subsequent
    // calls don't lose accumulated time.
    durableObject.sql.exec(
      `UPDATE quota SET rl_tokens = ?, rl_updated_at = ? WHERE user_id = ?`,
      refilled,
      now,
      userId
    );
    throw new VFSError(
      "EAGAIN",
      `rate limit exceeded for tenant; ${ratePerSec} ops/sec, ${burst} burst — retry shortly`
    );
  }

  // Allow: decrement and persist.
  const remaining = refilled - 1;
  durableObject.sql.exec(
    `UPDATE quota SET rl_tokens = ?, rl_updated_at = ? WHERE user_id = ?`,
    remaining,
    now,
    userId
  );
}

/**
 * Operator helper: configure a tenant's rate limit. Pass null to
 * either field to revert to defaults. Resets the bucket to full
 * capacity so the new limit takes effect immediately.
 */
export function setRateLimit(
  durableObject: UserDO,
  scope: VFSScope,
  limits: { perSec: number | null; burst: number | null }
): void {
  const userId = scope.sub
    ? `${scope.tenant}::${scope.sub}`
    : scope.tenant;
  durableObject.sql.exec(
    `INSERT OR IGNORE INTO quota
       (user_id, storage_used, storage_limit, file_count, pool_size)
     VALUES (?, 0, 107374182400, 0, 32)`,
    userId
  );
  const burst = limits.burst ?? DEFAULT_BURST;
  durableObject.sql.exec(
    `UPDATE quota
        SET rate_limit_per_sec = ?,
            rate_limit_burst   = ?,
            rl_tokens          = ?,
            rl_updated_at      = ?
      WHERE user_id = ?`,
    limits.perSec,
    limits.burst,
    burst,
    Date.now(),
    userId
  );
}
