/-
Mossaic.Generated.ShardDO — thin wrapper module for the ShardDO TS file.

This file does not contain any new theorems. Its purpose is to provide a
single Lean module whose name shadows the TS file `worker/core/objects/
shard/shard-do.ts`, with `theorem` names that match the `@lean-invariant`
annotations in the TS source. This makes drift detection mechanical:

    rg "@lean-invariant Mossaic\.Generated\.ShardDO\.\w+" worker/

should yield only theorem names that exist in this file (verified by
`lean/scripts/check-xrefs.sh`).

Modeled file: worker/core/objects/shard/shard-do.ts (777 LoC).
-/

import Mossaic.Vfs.Refcount
import Mossaic.Vfs.Gc

namespace Mossaic.Generated.ShardDO

/-- Re-export: the chunk-uniqueness + ref-uniqueness + ref→chunk-existence
invariant proved at `Mossaic.Vfs.Refcount.step_preserves_validState`.
The TS functions `writeChunkInternal` and `removeFileRefs` (in
`worker/core/objects/shard/shard-do.ts`) collectively realize the `step`
function whose invariance we proved. -/
theorem chunk_invariant_preserved :
    ∀ (s : Mossaic.Vfs.Refcount.ShardState) (op : Mossaic.Vfs.Refcount.Op),
      Mossaic.Vfs.Refcount.validState s →
      Mossaic.Vfs.Refcount.validState (Mossaic.Vfs.Refcount.step s op) :=
  Mossaic.Vfs.Refcount.step_preserves_validState

/-- Re-export: GC safety. Mirrors the alarm sweeper in
`worker/core/objects/shard/shard-do.ts`. -/
theorem alarm_safe :
    ∀ (s : Mossaic.Vfs.Refcount.ShardState) (now : Mossaic.Vfs.Common.TimeMs),
      Mossaic.Vfs.Refcount.validState s →
      Mossaic.Vfs.Refcount.validState (Mossaic.Vfs.Gc.alarm s now) :=
  Mossaic.Vfs.Gc.alarm_preserves_validState

/-- Re-export: alarm only deletes refCount=0 chunks. -/
theorem alarm_only_deletes_zero :
    ∀ (s : Mossaic.Vfs.Refcount.ShardState) (now : Mossaic.Vfs.Common.TimeMs)
      (c : Mossaic.Vfs.Refcount.Chunk),
      c ∈ s.chunks →
      (∀ c' ∈ (Mossaic.Vfs.Gc.alarm s now).chunks, c'.hash ≠ c.hash) →
      c.refCount = 0 :=
  Mossaic.Vfs.Gc.alarm_only_deletes_zero_refCount

end Mossaic.Generated.ShardDO
