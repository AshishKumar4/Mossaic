/-
Mossaic.Generated.Placement — thin wrapper for shared/placement.ts.

Modeled file: shared/placement.ts (270 LoC). Now exposes the
`Placement` interface + two concrete implementations:
`canonicalPlacement` (used by `createVFS()` consumers) and
`legacyAppPlacement` (App-internal). Both share the same
rendezvous score (`shard:${userId}:${idx}` template) — only the
DO instance name differs.

NOTE: We do NOT model rendezvous hashing in Lean. The placement function
is pure and deterministic, but its mathematical content (best-shard
selection via murmurhash3 score) is opaque from a verification standpoint
— murmurhash3 is not a Lean-modeled hash function.

What we DO state, as a structural property: the shard-name builder used
in conjunction with placement is `vfsShardDOName`, whose injectivity is
proved in `Mossaic.Vfs.Tenant.shardName_inj_fixed_idx`. The
`canonicalPlacement.shardDOName` impl in `shared/placement.ts:235`
delegates to `buildCanonicalShardDOName` which mirrors `vfsShardDOName`
byte-exact — so the proof carries through. So the TS sequence:
    sc → idx ← Placement.placeChunk(sc, fileKey, chunkIndex, poolSize)
       → Placement.shardDOName(sc, idx) → DO name

inherits tenant isolation from the shardName injectivity, because no
cross-tenant collisions are possible at the name layer regardless of how
`placeChunk` chose `idx`.

Audit reference:
  H4 (placement on pool growth) is about a different invariant
  (cross-version dedup), not tenant isolation. We do NOT prove H4 here.
-/

namespace Mossaic.Generated.Placement

end Mossaic.Generated.Placement
