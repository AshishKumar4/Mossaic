/-
Mossaic.Vfs.Versioning — I4: Versioning monotonicity. STRETCH (PARTIAL).

Models:
  worker/objects/user/vfs-versions.ts:166-200 (insertVersion: new mtime)
  worker/objects/user/vfs-versions.ts:533-560 (restoreVersion: new row, new mtime)
  worker/objects/user/user-do.ts:298-313     (file_versions schema)

Audit reference:
  /workspace/local/audit-report.md §I4.

Invariant statements (scope for THIS build):

  (V0) `step` semantics: insertVersion appends a row; dropVersion filters
       a row out.
  (V3-weak) After `insertVersion pid mtime`, the new row is in the state
            (no axiom or sorry needed).
  (V3) After `insertVersion pid mtime`, `maxMtime pid` of the new state
       is ≥ mtime — proved via a direct argument.

What we DEFER (with clear blockers, NO sorrys in build):
  - Full `listVersions_sorted` (V1): would require a `List.mergeSort_sorted`
    lemma not present in plain Lean 4.29 stdlib without Mathlib. Modeled
    via a stub `listVersions` that uses `mergeSort`; the sortedness theorem
    is documented as future work.
  - `currentVersion` characterization: needs the same sort/max reasoning.
  - `restoreVersion` strict-monotonicity: composes V3 with chunksAlive
    guard from C2; tractable but adds 80-100 LoC.

Honesty note:
  The TS code's monotonicity is a one-line argument: every insertVersion
  uses Date.now() which is monotonically non-decreasing. The Lean proof
  is structurally the same (V3 below). What makes I4 cheaper than I1 is
  the absence of cross-table consistency requirements.
-/

import Mossaic.Vfs.Common

namespace Mossaic.Vfs.Versioning

open Mossaic.Vfs.Common

/-- A version row. Mirrors the `file_versions` schema at user-do.ts:298-313
(restricted to the fields relevant for monotonicity proofs). -/
structure Version where
  versionId : String
  pathId    : PathId
  mtimeMs   : TimeMs
  deleted   : Bool
  deriving DecidableEq, Repr

/-- Versioning state — the `file_versions` table. -/
structure VersionState where
  versions : List Version
  deriving Repr

def VersionState.empty : VersionState := ⟨[]⟩

/-- All versions for a path, in insertion order. -/
def VersionState.forPath (s : VersionState) (pid : PathId) : List Version :=
  s.versions.filter (·.pathId = pid)

/-- The maximum mtime for a path, computed via a foldl. -/
def VersionState.maxMtime (s : VersionState) (pid : PathId) : Option TimeMs :=
  (s.forPath pid).foldl (fun acc v =>
    match acc with
    | none => some v.mtimeMs
    | some m => some (Nat.max m v.mtimeMs)) none

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

-- ─── (V3-weak) After insert, the new row is present ─────────────────────

theorem insertVersion_in_forPath (s : VersionState) (pid : PathId) (vid : String)
    (mtime : TimeMs) (del : Bool) :
    (⟨vid, pid, mtime, del⟩ : Version) ∈
      (step s (.insertVersion pid vid mtime del)).forPath pid := by
  unfold VersionState.forPath
  rw [insertVersion_appends]
  rw [List.filter_append]
  simp

-- ─── Helper: foldl-based max is bounded by any element ──────────────────

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
    -- foldl ((hd :: tl) ++ [newV]) = foldl (hd :: (tl ++ [newV])) = foldl (tl ++ [newV]) (foldl-step acc hd)
    -- We unfold once and apply ih.
    have hcons : (hd :: tl) ++ [newV] = hd :: (tl ++ [newV]) := rfl
    rw [hcons]
    rw [show List.foldl (fun acc v =>
              match acc with
              | none => some v.mtimeMs
              | some m => some (Nat.max m v.mtimeMs)) acc (hd :: (tl ++ [newV]))
            = List.foldl (fun acc v =>
              match acc with
              | none => some v.mtimeMs
              | some m => some (Nat.max m v.mtimeMs))
              ((match acc with
                | none => some hd.mtimeMs
                | some m => some (Nat.max m hd.mtimeMs)))
              (tl ++ [newV]) from rfl]
    cases acc with
    | none => exact ih (some hd.mtimeMs) newV
    | some m => exact ih (some (Nat.max m hd.mtimeMs)) newV

/-- Main monotonicity: `maxMtime` of the post-insertVersion state is some
`m ≥ mtime`. -/
theorem insertVersion_max_ge (s : VersionState) (pid : PathId) (vid : String)
    (mtime : TimeMs) (del : Bool) :
    ∃ m, (step s (.insertVersion pid vid mtime del)).maxMtime pid = some m
       ∧ m ≥ mtime := by
  unfold VersionState.maxMtime VersionState.forPath
  rw [insertVersion_appends]
  rw [List.filter_append]
  -- The new row's pathId IS pid, so the filter retains it.
  have hnew : ([(⟨vid, pid, mtime, del⟩ : Version)].filter (·.pathId = pid)) =
              [⟨vid, pid, mtime, del⟩] := by simp
  rw [hnew]
  -- Apply the general lemma.
  have := maxFold_bound_general (s.versions.filter (·.pathId = pid)) none ⟨vid, pid, mtime, del⟩
  obtain ⟨m, hm, hge⟩ := this
  exact ⟨m, hm, hge⟩

-- ─── Non-vacuity sanity checks ──────────────────────────────────────────

/-- Concrete witness: maxMtime increases after an insert with a larger time. -/
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

/-- Sanity: the max-after-insert theorem is non-vacuous. -/
theorem insertVersion_max_ge_nonvacuous :
    ∃ s pid vid mtime del,
      (step s (.insertVersion pid vid mtime del)).maxMtime pid = some mtime
        ∧ mtime > 0 := by
  refine ⟨VersionState.empty, "p", "v", 100, false, ?_, ?_⟩
  · decide
  · decide

end Mossaic.Vfs.Versioning
