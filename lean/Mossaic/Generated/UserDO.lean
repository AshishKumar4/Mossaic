/-
Mossaic.Generated.UserDO — thin wrapper module for the UserDO TS file.

Modeled file: worker/objects/user/user-do.ts (1342 LoC) and
worker/objects/user/vfs-ops.ts (2771 LoC) and
worker/objects/user/vfs-versions.ts (720 LoC).

Re-exports tenant-isolation theorems used by callers of `vfsUserDOName`
and `vfsShardDOName` (worker/lib/utils.ts:82-121).
-/

import Mossaic.Vfs.Tenant
import Mossaic.Vfs.Versioning

namespace Mossaic.Generated.UserDO

/-- Re-export: distinct tenants ⇒ distinct UserDO instance names.
Mirrors worker/lib/utils.ts:82-94. -/
theorem cross_tenant_user_isolation :
    ∀ (sc₁ sc₂ : Mossaic.Vfs.Tenant.VFSScope),
      Mossaic.Vfs.Tenant.validScope sc₁ →
      Mossaic.Vfs.Tenant.validScope sc₂ →
      sc₁.tenant ≠ sc₂.tenant →
      Mossaic.Vfs.Tenant.userName sc₁ ≠ Mossaic.Vfs.Tenant.userName sc₂ :=
  Mossaic.Vfs.Tenant.cross_tenant_user_isolation

/-- Re-export: distinct tenants ⇒ distinct ShardDO instance names at any
fixed shard index. Mirrors worker/lib/utils.ts:106-121. -/
theorem cross_tenant_shard_isolation :
    ∀ (sc₁ sc₂ : Mossaic.Vfs.Tenant.VFSScope) (idx : Nat),
      Mossaic.Vfs.Tenant.validScope sc₁ →
      Mossaic.Vfs.Tenant.validScope sc₂ →
      sc₁.tenant ≠ sc₂.tenant →
      Mossaic.Vfs.Tenant.shardName sc₁ idx ≠ Mossaic.Vfs.Tenant.shardName sc₂ idx :=
  Mossaic.Vfs.Tenant.cross_tenant_isolation

/-- Re-export (stretch): insertVersion advances mtime monotonicity.
Mirrors vfs-versions.ts:166-200. -/
theorem insertVersion_advances :
    ∀ (s : Mossaic.Vfs.Versioning.VersionState)
      (pid : Mossaic.Vfs.Common.PathId) (vid : String)
      (mtime : Mossaic.Vfs.Common.TimeMs) (del : Bool),
      ∃ m, (Mossaic.Vfs.Versioning.step s
              (.insertVersion pid vid mtime del)).maxMtime pid = some m
            ∧ m ≥ mtime :=
  Mossaic.Vfs.Versioning.insertVersion_max_ge

end Mossaic.Generated.UserDO
