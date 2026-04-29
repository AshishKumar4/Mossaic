/-
Mossaic.Vfs.Common — shared types and helpers used by every invariant module.

Models: shared types referenced across worker/core/objects/{user,shard}/*.ts
and shared/vfs-types.ts. This module is intentionally minimal — each
invariant file owns its own state-machine types; only types used by more
than one invariant live here.

Design choice: SQL tables are modeled as `List Row`. Numerical invariants
are stated via `Mathlib.Data.List.Count.countP` so that the relationship
between cardinality and a stored counter is provable as a List equation,
not axiomatized.
-/

import Mathlib.Data.List.Pairwise
import Mathlib.Data.List.Count

namespace Mossaic.Vfs.Common

/-- Hash strings are opaque. We never appeal to collision-resistance of
SHA-256 in any theorem — that is a cryptographic axiom of the runtime,
not a property of the TypeScript code. -/
abbrev Hash := String

/-- Synthetic file_id used by versioning. Mirrors `shardRefId` in
worker/core/objects/user/vfs-versions.ts. -/
abbrev FileId := String

/-- Path identifier for VFS-level paths. -/
abbrev PathId := String

/-- Time in milliseconds since epoch, as used by `Date.now()` in TS. -/
abbrev TimeMs := Nat

/-- Unique-by-key list helper: every key appears at most once. Used as
the table-uniqueness invariant for chunks (by hash) and chunk_refs (by
composite key). -/
def UniqueBy {α β : Type} [DecidableEq β] (key : α → β) (l : List α) : Prop :=
  l.Pairwise (fun x y => key x ≠ key y)

theorem UniqueBy.nil {α β : Type} [DecidableEq β] (key : α → β) :
    UniqueBy key ([] : List α) := List.Pairwise.nil

end Mossaic.Vfs.Common
