/-
Mossaic.Vfs.Versioning — I4: Versioning monotonicity & sortedness.
Mathlib-backed, NO AXIOM, NO SORRY.

Models:
  worker/objects/user/vfs-versions.ts:166-200 (insertVersion: new mtime)
  worker/objects/user/vfs-versions.ts:288-330 (listVersions ORDER BY DESC)
  worker/objects/user/vfs-versions.ts:533-560 (restoreVersion)
  worker/objects/user/user-do.ts:298-313     (file_versions schema)

Audit reference:
  /workspace/local/audit-report.md §I4.

Invariants:
  (V1) `listVersions` is sorted DESC by `mtimeMs`.
  (V3) Under monotonic clock, `insertVersion mtime` advances `maxMtime`
       for the path: post-state's max ≥ mtime.

What we prove:
  - (V0) Step semantics (`insertVersion` appends; `dropVersion` filters).
  - (V1) `listVersions_sorted` via `List.sorted_mergeSort` from Mathlib.
  - (V3) `insertVersion_max_ge` — fully proved.
  - Non-vacuity witnesses for both.
-/

import Mathlib.Data.List.Sort
import Mossaic.Vfs.Common

namespace Mossaic.Vfs.Versioning

open Mossaic.Vfs.Common

/-- A version row. Mirrors `file_versions` schema. -/
structure Version where
  versionId : String
  pathId    : PathId
  mtimeMs   : TimeMs
  deleted   : Bool
  deriving DecidableEq, Repr

/-- Versioning state. -/
structure VersionState where
  versions : List Version
  deriving Repr

def VersionState.empty : VersionState := ⟨[]⟩

/-- All versions for a path, in insertion order. -/
def VersionState.forPath (s : VersionState) (pid : PathId) : List Version :=
  s.versions.filter (·.pathId = pid)

/-- Comparator for sorting versions DESC by mtimeMs. -/
def mtimeGe (a b : Version) : Bool :=
  decide (a.mtimeMs ≥ b.mtimeMs)

/-- Maximum mtime for a path (none if no versions exist). -/
def VersionState.maxMtime (s : VersionState) (pid : PathId) : Option TimeMs :=
  (s.forPath pid).foldl (fun acc v =>
    match acc with
    | none => some v.mtimeMs
    | some m => some (Nat.max m v.mtimeMs)) none

/-- listVersions, modeled as: filter by pathId, sort DESC by mtimeMs, take N.
Mirrors `SELECT ... FROM file_versions WHERE path_id=? ORDER BY mtime_ms
DESC LIMIT N`. -/
def VersionState.listVersions (s : VersionState) (pid : PathId) (n : Nat) :
    List Version :=
  ((s.forPath pid).mergeSort mtimeGe).take n

-- ─── Operations ─────────────────────────────────────────────────────────

inductive Op where
  | insertVersion (pid : PathId) (vid : String) (mtime : TimeMs) (deleted : Bool)
  | dropVersion   (pid : PathId) (vid : String)
  deriving Repr

def step (s : VersionState) : Op → VersionState
  | .insertVersion pid vid mtime del =>
    { s with versions := s.versions ++ [⟨vid, pid, mtime, del⟩] }
  | .dropVersion pid vid =>
    { s with versions :=
        s.versions.filter (fun v => v.pathId ≠ pid ∨ v.versionId ≠ vid) }

-- ─── (V0) basic step semantics ──────────────────────────────────────────

theorem insertVersion_appends (s : VersionState) (pid : PathId) (vid : String)
    (mtime : TimeMs) (del : Bool) :
    (step s (.insertVersion pid vid mtime del)).versions =
      s.versions ++ [⟨vid, pid, mtime, del⟩] := rfl

theorem dropVersion_filters (s : VersionState) (pid : PathId) (vid : String) :
    (step s (.dropVersion pid vid)).versions =
      s.versions.filter (fun v => v.pathId ≠ pid ∨ v.versionId ≠ vid) := rfl

-- ─── (V1) listVersions_sorted ───────────────────────────────────────────

/-- mtimeGe is transitive. -/
theorem mtimeGe_trans : ∀ (a b c : Version),
    mtimeGe a b = true → mtimeGe b c = true → mtimeGe a c = true := by
  intro a b c hab hbc
  unfold mtimeGe at hab hbc ⊢
  have hab' : a.mtimeMs ≥ b.mtimeMs := by simpa using hab
  have hbc' : b.mtimeMs ≥ c.mtimeMs := by simpa using hbc
  have : a.mtimeMs ≥ c.mtimeMs := Nat.le_trans hbc' hab'
  simpa using this

/-- mtimeGe is total. -/
theorem mtimeGe_total : ∀ (a b : Version), mtimeGe a b || mtimeGe b a := by
  intro a b
  unfold mtimeGe
  by_cases h : a.mtimeMs ≥ b.mtimeMs
  · simp [h]
  · -- ¬ (a.mtimeMs ≥ b.mtimeMs) ⇒ a.mtimeMs < b.mtimeMs ⇒ b.mtimeMs ≥ a.mtimeMs
    have h2 : b.mtimeMs ≥ a.mtimeMs := Nat.le_of_lt (Nat.lt_of_not_le h)
    simp [h, h2]

/-- listVersions output is sorted in non-increasing order of mtime. -/
theorem listVersions_sorted (s : VersionState) (pid : PathId) (n : Nat) :
    (s.listVersions pid n).Pairwise (fun a b => a.mtimeMs ≥ b.mtimeMs) := by
  unfold VersionState.listVersions
  -- The mergeSort with mtimeGe gives Pairwise (fun a b => mtimeGe a b = true).
  -- We then apply List.Pairwise.imp to convert mtimeGe to ≥, and
  -- List.Pairwise.sublist for the .take.
  have hsorted : (((s.forPath pid)).mergeSort mtimeGe).Pairwise
                  (fun a b => mtimeGe a b = true) :=
    List.pairwise_mergeSort mtimeGe_trans mtimeGe_total _
  have htake : ((s.forPath pid).mergeSort mtimeGe |>.take n).Sublist
                ((s.forPath pid).mergeSort mtimeGe) :=
    List.take_sublist _ _
  have h_pair_take : (((s.forPath pid).mergeSort mtimeGe).take n).Pairwise
                      (fun a b => mtimeGe a b = true) :=
    List.Pairwise.sublist htake hsorted
  apply List.Pairwise.imp _ h_pair_take
  intro a b hab
  unfold mtimeGe at hab
  simpa using hab

-- ─── (V3) After insertVersion, maxMtime ≥ mtime ─────────────────────────

/-- For any list `l`, the foldl-based `maxMtime` over `l ++ [newV]` (starting
from any `acc`) is `some m` for some `m ≥ newV.mtimeMs`. -/
private theorem maxFold_bound_general :
    ∀ (l : List Version) (acc : Option TimeMs) (newV : Version),
      ∃ m, (List.foldl (fun acc v =>
            match acc with
            | none => some v.mtimeMs
            | some m => some (Nat.max m v.mtimeMs)) acc (l ++ [newV])) = some m
            ∧ m ≥ newV.mtimeMs := by
  intro l
  induction l with
  | nil =>
    intro acc newV
    cases acc with
    | none =>
      refine ⟨newV.mtimeMs, ?_, Nat.le_refl _⟩
      simp [List.foldl]
    | some m =>
      refine ⟨Nat.max m newV.mtimeMs, ?_, Nat.le_max_right _ _⟩
      simp [List.foldl]
  | cons hd tl ih =>
    intro acc newV
    have hcons : (hd :: tl) ++ [newV] = hd :: (tl ++ [newV]) := rfl
    rw [hcons]
    cases acc with
    | none => exact ih (some hd.mtimeMs) newV
    | some m => exact ih (some (Nat.max m hd.mtimeMs)) newV

/-- Main monotonicity: `maxMtime` of the post-insertVersion state is `some m
≥ mtime`. -/
theorem insertVersion_max_ge (s : VersionState) (pid : PathId) (vid : String)
    (mtime : TimeMs) (del : Bool) :
    ∃ m, (step s (.insertVersion pid vid mtime del)).maxMtime pid = some m
       ∧ m ≥ mtime := by
  unfold VersionState.maxMtime VersionState.forPath
  rw [insertVersion_appends]
  rw [List.filter_append]
  have hnew : ([(⟨vid, pid, mtime, del⟩ : Version)].filter (·.pathId = pid)) =
              [⟨vid, pid, mtime, del⟩] := by simp
  rw [hnew]
  exact maxFold_bound_general (s.versions.filter (·.pathId = pid)) none
        ⟨vid, pid, mtime, del⟩

-- ─── Non-vacuity sanity checks ──────────────────────────────────────────

/-- Concrete witness: maxMtime increases after an insert with a larger time.
Proved by direct computation (no native_decide). -/
theorem witness_insert_increases_max :
    let s₀ : VersionState := ⟨[⟨"v1", "p1", 100, false⟩]⟩
    let s₁ := step s₀ (.insertVersion "p1" "v2" 200 false)
    s₁.maxMtime "p1" = some 200 := by
  decide

/-- Liveness: insertVersion changes the state. -/
theorem insertVersion_changes_state :
    step (VersionState.empty) (.insertVersion "p1" "v1" 100 false) ≠ VersionState.empty := by
  intro hcontra
  have heq : (step VersionState.empty (.insertVersion "p1" "v1" 100 false)).versions
           = VersionState.empty.versions := congrArg VersionState.versions hcontra
  rw [insertVersion_appends] at heq
  simp [VersionState.empty] at heq

/-- Sanity: the max-after-insert theorem is non-vacuous, witnessed by the
empty starting state with `mtime = 100 > 0`. -/
theorem insertVersion_max_ge_nonvacuous :
    ∃ s pid vid mtime del,
      (step s (.insertVersion pid vid mtime del)).maxMtime pid = some mtime
        ∧ mtime > 0 := by
  refine ⟨VersionState.empty, "p", "v", 100, false, ?_, ?_⟩
  · decide
  · decide

/-- Sanity: the sortedness theorem is non-vacuous — there exist states whose
listVersions output has length > 1 (so Pairwise has actual content). We
exhibit one and lean on `listVersions_sorted` for the sortedness witness. -/
theorem listVersions_sorted_nonvacuous :
    ∃ (s : VersionState) (pid : PathId) (n : Nat),
      1 < (s.listVersions pid n).length ∧
      (s.listVersions pid n).Pairwise (fun a b => a.mtimeMs ≥ b.mtimeMs) := by
  let s_witness : VersionState :=
    { versions := [⟨"v1", "p1", 100, false⟩, ⟨"v2", "p1", 300, false⟩] }
  refine ⟨s_witness, "p1", 10, ?_, ?_⟩
  · -- length > 1: mergeSort is a permutation, so length = forPath.length = 2.
    show 1 < (s_witness.listVersions "p1" 10).length
    unfold VersionState.listVersions VersionState.forPath
    -- Show: 1 < (mergeSort filtered).take 10 |>.length
    rw [List.length_take]
    have hperm : List.Perm ((s_witness.versions.filter (·.pathId = "p1")).mergeSort mtimeGe)
                  (s_witness.versions.filter (·.pathId = "p1")) :=
      List.mergeSort_perm _ mtimeGe
    have hlen_filter : (s_witness.versions.filter (·.pathId = "p1")).length = 2 := by
      show ([(⟨"v1", "p1", 100, false⟩ : Version), ⟨"v2", "p1", 300, false⟩].filter
              (·.pathId = "p1")).length = 2
      decide
    have hlen_sort : ((s_witness.versions.filter (·.pathId = "p1")).mergeSort mtimeGe).length = 2 := by
      rw [List.Perm.length_eq hperm]
      exact hlen_filter
    rw [hlen_sort]
    decide
  · exact listVersions_sorted _ _ _

end Mossaic.Vfs.Versioning
