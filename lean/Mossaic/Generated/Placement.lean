/-
Mossaic.Generated.Placement — thin wrapper for shared/placement.ts.

Modeled file: shared/placement.ts (73 LoC).

NOTE: We do NOT model rendezvous hashing in Lean. The placement function
is pure and deterministic, but its mathematical content (best-shard
selection via murmurhash3 score) is opaque from a verification standpoint
— murmurhash3 is not a Lean-modeled hash function.

What we DO state, as a structural property: the shard-name builder used
in conjunction with placement is `vfsShardDOName`, whose injectivity is
proved in `Mossaic.Vfs.Tenant.shardName_inj_fixed_idx`. So the TS
sequence:
    sc → idx ← placeChunk(sc.tenant, hash, chunkIndex, poolSize)
       → vfsShardDOName(sc.ns, sc.tenant, sc.sub, idx) → DO name

inherits tenant isolation from the shardName injectivity, because no
cross-tenant collisions are possible at the name layer regardless of how
`placeChunk` chose `idx`.

Audit reference:
  H4 (placement on pool growth) is about a different invariant
  (cross-version dedup), not tenant isolation. We do NOT prove H4 here.
-/

namespace Mossaic.Generated.Placement

end Mossaic.Generated.Placement
