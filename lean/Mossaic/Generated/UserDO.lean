/-
Mossaic.Generated.UserDO — thin wrapper module for the UserDO TS files.

Modeled files (HEAD post-Phase-19 vfs-ops split):
  worker/core/objects/user/user-do-core.ts (2007 LoC; canonical surface)
  worker/core/objects/user/vfs-ops.ts (71 LoC barrel) re-exporting:
    worker/core/objects/user/vfs/helpers.ts      (393 LoC)
    worker/core/objects/user/vfs/reads.ts        (704 LoC)
    worker/core/objects/user/vfs/write-commit.ts (959 LoC)
    worker/core/objects/user/vfs/mutations.ts    (540 LoC)
    worker/core/objects/user/vfs/metadata.ts     (204 LoC)
    worker/core/objects/user/vfs/streams.ts      (734 LoC)
  worker/core/objects/user/vfs-versions.ts (887 LoC)
  worker/app/objects/user/user-do.ts (App-side typed-RPC subclass; not
    a Lean target — its body is wire-shape adaptation, no new
    invariants)

Re-exports tenant-isolation theorems used by callers of `vfsUserDOName`
and `vfsShardDOName` (worker/core/lib/utils.ts; canonical placement
also wraps these via `shared/placement.ts:canonicalPlacement`).
-/

import Mossaic.Vfs.Tenant
import Mossaic.Vfs.Versioning

namespace Mossaic.Generated.UserDO

/-- Re-export: distinct tenants ⇒ distinct UserDO instance names.
Mirrors worker/core/lib/utils.ts:vfsUserDOName. -/
theorem cross_tenant_user_isolation :
    ∀ (sc₁ sc₂ : Mossaic.Vfs.Tenant.VFSScope),
      Mossaic.Vfs.Tenant.validScope sc₁ →
      Mossaic.Vfs.Tenant.validScope sc₂ →
      sc₁.tenant ≠ sc₂.tenant →
      Mossaic.Vfs.Tenant.userName sc₁ ≠ Mossaic.Vfs.Tenant.userName sc₂ :=
  Mossaic.Vfs.Tenant.cross_tenant_user_isolation

/-- Re-export: distinct tenants ⇒ distinct ShardDO instance names at any
fixed shard index. Mirrors worker/core/lib/utils.ts:vfsShardDOName. -/
theorem cross_tenant_shard_isolation :
    ∀ (sc₁ sc₂ : Mossaic.Vfs.Tenant.VFSScope) (idx : Nat),
      Mossaic.Vfs.Tenant.validScope sc₁ →
      Mossaic.Vfs.Tenant.validScope sc₂ →
      sc₁.tenant ≠ sc₂.tenant →
      Mossaic.Vfs.Tenant.shardName sc₁ idx ≠ Mossaic.Vfs.Tenant.shardName sc₂ idx :=
  Mossaic.Vfs.Tenant.cross_tenant_isolation

/-- Re-export (stretch): insertVersion advances mtime monotonicity.
Mirrors worker/core/objects/user/vfs-versions.ts. -/
theorem insertVersion_advances :
    ∀ (s : Mossaic.Vfs.Versioning.VersionState)
      (pid : Mossaic.Vfs.Common.PathId) (vid : String)
      (mtime : Mossaic.Vfs.Common.TimeMs) (del : Bool),
      ∃ m, (Mossaic.Vfs.Versioning.step s
              (.insertVersion pid vid mtime del)).maxMtime pid = some m
            ∧ m ≥ mtime :=
  Mossaic.Vfs.Versioning.insertVersion_max_ge

end Mossaic.Generated.UserDO
