/-
Mossaic.Vfs.Tombstone — Phase 25 tombstone-consistency invariant.

Models:
  worker/core/objects/user/list-files.ts (765 LoC):
    :93   (vfsListFiles — head-tombstone filter at hydrateItem)
    :203  (vfsFileInfo — strict-stat ENOENT on tombstoned head)
    :467  (NOT EXISTS .. fv.deleted = 1 — listFiles tombstone exclusion)
    :619  (hydrateItem — read head version row + return null on tombstone)
    :679  (head.deleted === 1 && !includeTombstones → null)
  worker/core/objects/user/vfs/reads.ts:186 (vfsReadManyStat — null per tombstone)
  worker/core/objects/user/vfs/reads.ts:300-330 (readFileVersioned ENOENT on tombstone head)
  worker/core/objects/user/vfs/reads.ts:455-460 (yjs-mode head-tombstone gate)
  worker/core/objects/user/vfs/reads.ts:670-680 (vfsOpenManifest tombstone gate)
  worker/core/objects/user/vfs/reads.ts:790-800 (vfsReadChunk tombstone gate)
  worker/core/objects/user/vfs/preview.ts:118 (vfsReadPreview ENOENT on tombstone)
  worker/core/objects/user/vfs/streams.ts:196-200 (vfsOpenReadStream ENOENT on tombstone)

Test pins:
  tests/integration/versioning.test.ts (rmrf + tombstone visibility)
  tests/integration/version-mark.test.ts
  tests/integration/list-files.test.ts (fileInfo tombstone-ENOENT)

What we prove:

  (T1) listFiles_excludes_tombstoned_head — A file whose `head_version_id`
       points at a `file_versions` row with `deleted=1` is NOT in the
       result of `vfsListFiles` when `includeTombstones=false`. With
       `includeTombstones=true` the row IS surfaced.
  (T2) fileInfo_returns_enoent_on_tombstone — `vfsFileInfo` on a
       tombstoned-head path raises ENOENT (default opts), matching the
       strict-stat surface contract.
  (T3) readManyStat_returns_null_per_tombstone — A batch `readManyStat`
       call returns `null` (NOT throws) for each tombstoned-head path,
       isolating the per-path failure mode.
  (T4) readPreview_returns_enoent_on_tombstone — `vfsReadPreview` raises
       ENOENT on a tombstoned-head path before attempting to render.
  (T5) consistency_invariant — every path surfaced by `vfsListFiles`
       (default opts) is statable: its head row exists and is not
       tombstoned.

Each theorem is a structural property of the predicate composition
between the underlying state machine (versions list + tombstone flag)
and the head-tombstone filter. No state-machine ops are introduced —
the existing `Versioning.Op` (insertVersion / dropVersion) is enough,
and tombstones are encoded as `Version.deleted = true`.

NO `axiom`. NO `sorry`. Mathlib v4.29.0.
-/

import Mossaic.Vfs.Common
import Mossaic.Vfs.Versioning

namespace Mossaic.Vfs.Tombstone

open Mossaic.Vfs.Common
open Mossaic.Vfs.Versioning

-- ─── Types ──────────────────────────────────────────────────────────────

/-- A `files` row, restricted to the fields relevant to tombstone consistency.
The `headVersionId` matches the TS `files.head_version_id` column. -/
structure FileRow where
  fileId        : FileId
  parentId      : Option String
  fileName      : String
  /-- `none` = legacy non-versioned tenant; `some vid` = versioning ON. -/
  headVersionId : Option String
  deriving DecidableEq, Repr

/-- The path-side state used by listFiles / fileInfo / readPreview. The
`files` list mirrors the `files` table, the `versions` list mirrors
`file_versions` (re-using `Versioning.Version`). -/
structure PathState where
  files    : List FileRow
  versions : List Version
  deriving Repr

def PathState.empty : PathState := ⟨[], []⟩

/-- Lookup the head version row for a file, if any. -/
def PathState.headVersion (s : PathState) (f : FileRow) : Option Version :=
  match f.headVersionId with
  | none      => none
  | some vid  =>
    s.versions.find? (fun v => v.pathId = f.fileId ∧ v.versionId = vid)

/-- The TS `head.deleted === 1` predicate. A file is "tombstoned at the
head" if it has a `head_version_id` AND that row's `deleted` flag is set. -/
def isTombstonedHead (s : PathState) (f : FileRow) : Bool :=
  match s.headVersion f with
  | some v => v.deleted
  | none   => false

-- ─── Read-side filters mirroring the TS surface ─────────────────────────

/-- `vfsListFiles` semantics (Phase 25 head-tombstone filter at
hydrateItem, list-files.ts:679). When `includeTombstones=false` (the
default), tombstoned-head rows are dropped. When `=true`, they survive. -/
def listFiles (s : PathState) (includeTombstones : Bool) : List FileRow :=
  s.files.filter (fun f =>
    if includeTombstones then true else ! isTombstonedHead s f)

/-- A successful `vfsFileInfo` returns `some f` (the row); a tombstoned
head with default opts returns `none` which the TS surface raises as
ENOENT (list-files.ts:236). -/
def fileInfo (s : PathState) (path : String) (includeTombstones : Bool) :
    Option FileRow :=
  match s.files.find? (·.fileName = path) with
  | none => none
  | some f =>
    if includeTombstones then some f
    else if isTombstonedHead s f then none
    else some f

/-- `vfsReadManyStat` semantics (reads.ts:186). Returns one entry per
input path: the `FileRow` if found and statable; `none` otherwise.
Phase 25 contract: ANY ENOENT (resolution failure OR tombstoned head)
becomes `none` for that single entry. -/
def readManyStat (s : PathState) (paths : List String) :
    List (Option FileRow) :=
  paths.map (fun p =>
    match s.files.find? (·.fileName = p) with
    | none   => none
    | some f => if isTombstonedHead s f then none else some f)

/-- `vfsReadPreview` semantics (preview.ts:118). Returns `some f` if the
file is renderable; `none` (TS raises ENOENT) on tombstoned head. -/
def readPreview (s : PathState) (path : String) : Option FileRow :=
  match s.files.find? (·.fileName = path) with
  | none   => none
  | some f => if isTombstonedHead s f then none else some f

-- ─── Theorems ───────────────────────────────────────────────────────────

/-- (T1) `vfsListFiles` (default opts) excludes any file whose head is
tombstoned. -/
theorem listFiles_excludes_tombstoned_head (s : PathState) (f : FileRow) :
    isTombstonedHead s f = true →
    f ∉ listFiles s false := by
  intro htomb hmem
  unfold listFiles at hmem
  rw [List.mem_filter] at hmem
  have hfilt := hmem.2
  -- hfilt is the boolean `(if false then true else ! isTombstonedHead s f) = true`.
  -- The literal `false` reduces the if; the rest follows from htomb.
  simp [htomb] at hfilt

/-- (T1b) `vfsListFiles` with `includeTombstones=true` surfaces the row
even if its head is tombstoned. Closes the recovery surface. -/
theorem listFiles_includes_tombstoned_when_opted_in (s : PathState) (f : FileRow) :
    f ∈ s.files →
    f ∈ listFiles s true := by
  intro hmem
  unfold listFiles
  rw [List.mem_filter]
  refine ⟨hmem, ?_⟩
  simp

/-- (T2) `vfsFileInfo` returns `none` (TS: throws ENOENT) on a tombstoned
head with default opts. -/
theorem fileInfo_returns_enoent_on_tombstone
    (s : PathState) (path : String) (f : FileRow)
    (h_lookup : s.files.find? (·.fileName = path) = some f)
    (h_tomb : isTombstonedHead s f = true) :
    fileInfo s path false = none := by
  unfold fileInfo
  rw [h_lookup]
  simp [h_tomb]

/-- (T2b) `vfsFileInfo` returns `some f` for a non-tombstoned head with
default opts. Pairs with T2 to characterise the contract. -/
theorem fileInfo_returns_row_on_non_tombstone
    (s : PathState) (path : String) (f : FileRow)
    (h_lookup : s.files.find? (·.fileName = path) = some f)
    (h_not_tomb : isTombstonedHead s f = false) :
    fileInfo s path false = some f := by
  unfold fileInfo
  rw [h_lookup]
  simp [h_not_tomb]

/-- Auxiliary: `(l.map f).get? i` corresponds to `(l.get? i).map f`.
This is `List.get?_map` in newer Mathlib; we prove it by induction
to avoid name-stability concerns across Lean / Mathlib versions. -/
private theorem get?_map_eq {α β : Type} (f : α → β) (l : List α) (i : Nat) :
    (l.map f).get? i = (l.get? i).map f := by
  induction l generalizing i with
  | nil => simp
  | cons hd tl ih =>
    cases i with
    | zero => simp
    | succ k => simp [List.get?, ih]

/-- (T3) `vfsReadManyStat` returns `none` at every position whose path
resolves to a tombstoned-head file. The batch isolates each per-path
ENOENT to a single position; surrounding positions are untouched. -/
theorem readManyStat_returns_null_per_tombstone
    (s : PathState) (paths : List String) (i : Nat) (path : String) (f : FileRow)
    (h_idx : paths.get? i = some path)
    (h_lookup : s.files.find? (·.fileName = path) = some f)
    (h_tomb : isTombstonedHead s f = true) :
    (readManyStat s paths).get? i = some none := by
  unfold readManyStat
  rw [get?_map_eq, h_idx]
  simp
  rw [h_lookup]
  simp [h_tomb]

/-- (T4) `vfsReadPreview` returns `none` (TS: throws ENOENT) on a
tombstoned head. -/
theorem readPreview_returns_enoent_on_tombstone
    (s : PathState) (path : String) (f : FileRow)
    (h_lookup : s.files.find? (·.fileName = path) = some f)
    (h_tomb : isTombstonedHead s f = true) :
    readPreview s path = none := by
  unfold readPreview
  rw [h_lookup]
  simp [h_tomb]

/-- (T5) Consistency invariant: every file in `listFiles s false` is
non-tombstoned at the head, hence statable via `fileInfo` with default
opts. -/
theorem consistency_invariant (s : PathState) (f : FileRow) :
    f ∈ listFiles s false →
    isTombstonedHead s f = false := by
  intro hmem
  unfold listFiles at hmem
  rw [List.mem_filter] at hmem
  have hfilt := hmem.2
  -- hfilt : (if false then true else ! isTombstonedHead s f) = true.
  -- This forces ! isTombstonedHead s f = true, hence isTombstonedHead = false.
  cases htomb : isTombstonedHead s f
  · rfl
  · simp [htomb] at hfilt

/-- Helper: when the predicate uniquely picks `f` from the list, find?
returns `some f`. We prove this by direct induction. -/
private theorem find?_eq_some_of_mem_unique
    (l : List FileRow) (f : FileRow)
    (h_in : f ∈ l)
    (h_unique : ∀ f' ∈ l, f'.fileName = f.fileName → f' = f) :
    l.find? (·.fileName = f.fileName) = some f := by
  induction l with
  | nil => exact absurd h_in List.not_mem_nil
  | cons hd tl ih =>
    by_cases hhd : hd.fileName = f.fileName
    · -- hd matches the predicate. By unique, hd = f.
      have hhd_eq : hd = f := h_unique hd List.mem_cons_self hhd
      -- find? on hd::tl with predicate true at hd returns some hd = some f.
      simp [List.find?, hhd, hhd_eq]
    · -- hd doesn't match. Recurse on tl with f ∈ tl.
      have h_in_tl : f ∈ tl := by
        rcases List.mem_cons.mp h_in with rfl | h
        · exact absurd rfl hhd
        · exact h
      have h_unique_tl : ∀ f' ∈ tl, f'.fileName = f.fileName → f' = f :=
        fun f' hf' => h_unique f' (List.mem_cons_of_mem _ hf')
      simp [List.find?, hhd]
      exact ih h_in_tl h_unique_tl

/-- (T5b) Stronger consistency: every file in `listFiles s false` whose
name is `path` is also surfaced by `fileInfo s path false`. This is the
contract the SPA gallery relies on: "if it's listed, you can stat it". -/
theorem listFiles_implies_fileInfo_succeeds
    (s : PathState) (f : FileRow)
    (h_unique : ∀ f' ∈ s.files, f'.fileName = f.fileName → f' = f) :
    f ∈ listFiles s false →
    fileInfo s f.fileName false = some f := by
  intro hmem
  have h_in_files : f ∈ s.files := by
    unfold listFiles at hmem
    rw [List.mem_filter] at hmem
    exact hmem.1
  have h_not_tomb : isTombstonedHead s f = false := consistency_invariant s f hmem
  have h_lookup : s.files.find? (·.fileName = f.fileName) = some f :=
    find?_eq_some_of_mem_unique s.files f h_in_files h_unique
  exact fileInfo_returns_row_on_non_tombstone s f.fileName f h_lookup h_not_tomb

-- ─── Non-vacuity sanity checks ──────────────────────────────────────────

/-- Concrete witness: a state with one tombstoned + one live file.
listFiles drops the tombstoned, keeps the live. -/
theorem witness_listFiles_drops_tombstone :
    let v_tomb : Version := ⟨"v1", "f_tomb", 100, true⟩
    let v_live : Version := ⟨"v2", "f_live", 200, false⟩
    let f_tomb : FileRow := ⟨"f_tomb", none, "/tomb", some "v1"⟩
    let f_live : FileRow := ⟨"f_live", none, "/live", some "v2"⟩
    let s : PathState := ⟨[f_tomb, f_live], [v_tomb, v_live]⟩
    listFiles s false = [f_live] := by
  decide

/-- Concrete witness: with `includeTombstones=true`, both rows survive. -/
theorem witness_listFiles_includes_tombstone_when_opted_in :
    let v_tomb : Version := ⟨"v1", "f_tomb", 100, true⟩
    let f_tomb : FileRow := ⟨"f_tomb", none, "/tomb", some "v1"⟩
    let s : PathState := ⟨[f_tomb], [v_tomb]⟩
    listFiles s true = [f_tomb] := by
  decide

/-- Liveness: T1 is non-vacuous — there exists a state where a file IS
tombstoned, and the conclusion `f ∉ listFiles s false` is meaningful. -/
theorem listFiles_excludes_tombstoned_head_nonvacuous :
    ∃ (s : PathState) (f : FileRow),
      isTombstonedHead s f = true ∧ f ∉ listFiles s false := by
  refine ⟨
    { files := [⟨"f1", none, "/x", some "v1"⟩],
      versions := [⟨"v1", "f1", 100, true⟩] },
    ⟨"f1", none, "/x", some "v1"⟩, ?_, ?_⟩
  · decide
  · decide

end Mossaic.Vfs.Tombstone
