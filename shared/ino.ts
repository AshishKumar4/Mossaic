/**
 * Stable 53-bit inode synthesis from the ULID-like row identifiers used
 * by Mossaic (worker/lib/utils.ts:5-13).
 *
 * isomorphic-git, like POSIX, uses `ino` for rename detection and dedup
 * inside its index. The inode must be:
 *   - stable across reads (same id → same ino, every time, forever)
 *   - within Number.MAX_SAFE_INTEGER (53 bits) so it round-trips through
 *     JSON without precision loss
 *   - low collision rate at expected scale (~10^6 files per tenant)
 *
 * Two MurmurHash3 passes with different seeds compose 21+32 bits = 53 bits.
 * Birthday collision at p=0.5 ≈ √(2 · 2^53) ≈ 134 million ids — safely
 * past the 10 GB DO storage cap on metadata alone.
 *
 * `uidFromTenant` / `gidFromTenant` synthesise a stable 32-bit POSIX
 * uid/gid per tenant. POSIX permission checks aren't enforced by the VFS
 * (and won't be — see plan §11 "punt"), but isomorphic-git's stat object
 * needs *some* number, and stability per tenant means git's index
 * comparisons don't churn.
 */

import { murmurhash3 } from "./hash";

const SEED_A = 0x12345678;
const SEED_B = 0x9abcdef0;
const SEED_UID = 0x55534552; // "USER"
const SEED_GID = 0x47525550; // "GRUP"

/** Map a ULID-like id (or any string) to a stable 53-bit safe integer. */
export function inoFromId(id: string): number {
  const a = murmurhash3(id, SEED_A) >>> 0;
  const b = murmurhash3(id, SEED_B) >>> 0;
  // 21 high bits from a, 32 low bits from b. Total 53 bits → safe.
  // a is masked to 21 bits, then shifted up by 32; b fills the low 32.
  // Math: (a & 0x1FFFFF) * 2^32 + b
  return (a & 0x1fffff) * 0x100000000 + b;
}

/** Stable 32-bit unsigned uid per tenant. */
export function uidFromTenant(tenant: string): number {
  return murmurhash3(tenant, SEED_UID) >>> 0;
}

/** Stable 32-bit unsigned gid per tenant (different seed → different value). */
export function gidFromTenant(tenant: string): number {
  return murmurhash3(tenant, SEED_GID) >>> 0;
}
