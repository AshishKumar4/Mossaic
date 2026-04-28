/-
Mossaic.Vfs.Common — shared types and helpers used by every invariant module.

Models: shared types referenced across worker/objects/{user,shard}/*.ts and
shared/vfs-types.ts. This module is intentionally minimal — each invariant
file owns its own state-machine types; only types used by more than one
invariant live here.

Design choice: SQL tables are modeled as `List Row` (not `AssocMap`) because
our invariants are about cardinality / membership / row-counting, not lookup
performance. Lean stdlib's `List.filter` / `List.length` / `List.find?` are
sufficient and keep proofs in `simp`/`omega` reach without Mathlib.
-/

namespace Mossaic.Vfs.Common

/-- Hash strings are opaque. We never appeal to collision-resistance of
SHA-256 in any theorem — that is a cryptographic axiom, not a code property. -/
abbrev Hash := String

/-- Synthetic file_id used by Phase 9 versioning. Mirrors `shardRefId` in
worker/objects/user/vfs-versions.ts:166-200. We do not model the
construction (string concat); we only require that it be a String. -/
abbrev FileId := String

/-- Path identifier for VFS-level paths. Phase 9 versioning rows are keyed
by `path_id`; Phase 8 plain rows by `file_id`. We do not distinguish here. -/
abbrev PathId := String

/-- Time in milliseconds since epoch, as used by `Date.now()` in TS. -/
abbrev TimeMs := Nat

/-- Unique-by-key list helper: every key appears at most once. Used as the
table-uniqueness invariant for chunks (by hash) and chunk_refs (by composite
key). The list-based formulation makes `simp` lemmas trivial. -/
def UniqueBy {α β : Type} [DecidableEq β] (key : α → β) (l : List α) : Prop :=
  l.Pairwise (fun x y => key x ≠ key y)

theorem UniqueBy.nil {α β : Type} [DecidableEq β] (key : α → β) :
    UniqueBy key ([] : List α) := List.Pairwise.nil

end Mossaic.Vfs.Common
