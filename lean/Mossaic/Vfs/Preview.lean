/-
Mossaic.Vfs.Preview — Phase 24: preview pipeline + file_variants table.

Models:
  worker/core/objects/user/user-do-core.ts:691-714 (file_variants schema:
                                                    PK (file_id, variant_kind,
                                                    renderer_kind); ON DELETE
                                                    CASCADE FK to files)
  worker/core/objects/user/preview-variants.ts:209,217 (renderAndStoreVariant:
                                                    putChunk(variantBytes) +
                                                    INSERT OR IGNORE INTO
                                                    file_variants)
  worker/core/objects/user/vfs/preview.ts:172      (DELETE FROM file_variants
                                                    on dangling-row recovery)
  worker/core/lib/preview-pipeline/registry.ts     (renderer registration order)
  worker/core/lib/preview-pipeline/index.ts        (concurrency cap of 6)

What we prove:

  variant_uniqueness — at most one row per (file_id, variant_kind,
                       renderer_kind). Enforced by PK; the model
                       captures it as a `UniqueBy` invariant on the
                       variants list.
  variant_cascade_delete — deleting a `files` row drops all `file_variants`
                       rows with that file_id. Models the FK ON DELETE
                       CASCADE semantics.
  variant_chunk_refcount_transitivity — variant chunks register through
                       the same `step` (putChunk) machinery as primary
                       chunks; the existing `Refcount.step_preserves_validState`
                       covers them transitively. We package the connection
                       as a structural theorem: a variant's chunk
                       registration corresponds to an `Op.putChunk` and
                       therefore inherits validState preservation.

What we DO NOT prove (intentionally out of scope):
  - Renderer registration order (registry.ts walks renderers in
    registration order; correctness is purely a TS-level invariant
    about a constant-fold list).
  - Concurrency cap of 6 (semaphore-based; Lean does not model
    concurrency).

NO `axiom`. NO `sorry`. Mathlib v4.29.0.
-/

import Mossaic.Vfs.Common
import Mossaic.Vfs.Refcount
import Mathlib.Data.List.Pairwise

namespace Mossaic.Vfs.Preview

open Mossaic.Vfs.Common
open Mossaic.Vfs.Refcount

-- ─── Types ──────────────────────────────────────────────────────────────

/-- A variant kind tag (e.g., "thumb", "medium", "lightbox"). -/
abbrev VariantKind := String

/-- A renderer kind tag (e.g., "image", "code", "waveform"). -/
abbrev RendererKind := String

/-- A `file_variants` row. Mirrors the schema at user-do-core.ts:691-714. -/
structure FileVariant where
  fileId       : FileId
  variantKind  : VariantKind
  rendererKind : RendererKind
  chunkHash    : Hash
  shardIndex   : Nat
  byteSize     : Nat
  deriving DecidableEq, Repr

/-- Composite primary key: `(file_id, variant_kind, renderer_kind)`. -/
def FileVariant.key (v : FileVariant) : FileId × VariantKind × RendererKind :=
  (v.fileId, v.variantKind, v.rendererKind)

/-- The variants table state. -/
structure VariantState where
  variants : List FileVariant
  deriving Repr

def VariantState.empty : VariantState := ⟨[]⟩

-- ─── Invariant ──────────────────────────────────────────────────────────

/-- The variants invariant: every row is unique by composite PK. -/
def validVariantState (s : VariantState) : Prop :=
  UniqueBy FileVariant.key s.variants

theorem validVariantState_empty : validVariantState VariantState.empty :=
  UniqueBy.nil _

-- ─── Operations ─────────────────────────────────────────────────────────

inductive VariantOp where
  /-- `INSERT OR IGNORE INTO file_variants` at preview-variants.ts:217.
  Insert iff PK absent; otherwise no-op. -/
  | insertVariant (v : FileVariant)
  /-- `DELETE FROM file_variants WHERE file_id = ? AND variant_kind = ?
  AND renderer_kind = ?` at vfs/preview.ts:172. Dangling-row recovery. -/
  | deleteVariant (fileId : FileId) (vk : VariantKind) (rk : RendererKind)
  /-- `ON DELETE CASCADE` from `files`: drop ALL variants for fileId. -/
  | cascadeFileDelete (fileId : FileId)
  deriving Repr

/-- The variant-state transition. -/
def stepVariant (s : VariantState) : VariantOp → VariantState
  | .insertVariant v =>
    let exists' := s.variants.any (fun x => x.key = v.key)
    if exists' then s
    else { s with variants := s.variants ++ [v] }
  | .deleteVariant fid vk rk =>
    { s with variants := s.variants.filter
        (fun v => ¬ (v.fileId = fid ∧ v.variantKind = vk ∧ v.rendererKind = rk)) }
  | .cascadeFileDelete fid =>
    { s with variants := s.variants.filter (fun v => v.fileId ≠ fid) }

-- ─── §1 variant_uniqueness ──────────────────────────────────────────────

/--
Filter preserves UniqueBy: dropping rows from a unique-by-key list
keeps it unique-by-key.
-/
private theorem filter_preserves_unique
    (l : List FileVariant) (p : FileVariant → Bool)
    (hu : UniqueBy FileVariant.key l) :
    UniqueBy FileVariant.key (l.filter p) :=
  List.Pairwise.sublist List.filter_sublist hu

/-- Auxiliary: appending a row whose composite key is fresh preserves
the unique-by-key invariant on the variants list. -/
private theorem appendFresh_preserves_unique
    (l : List FileVariant) (v : FileVariant)
    (hu : UniqueBy FileVariant.key l)
    (hfresh : ∀ x ∈ l, x.key ≠ v.key) :
    UniqueBy FileVariant.key (l ++ [v]) := by
  unfold UniqueBy at hu ⊢
  apply List.pairwise_append.mpr
  refine ⟨hu, List.pairwise_singleton _ _, ?_⟩
  intro a ha b hb
  simp at hb
  subst hb
  exact hfresh a ha

/-- The variants invariant is preserved under any operation. -/
theorem stepVariant_preserves_validState
    (s : VariantState) (op : VariantOp)
    (hv : validVariantState s) :
    validVariantState (stepVariant s op) := by
  match op with
  | .insertVariant v =>
    unfold stepVariant
    -- The body is `if s.variants.any (...) then s else (... ++ [v])`.
    by_cases hex : s.variants.any (fun x => x.key = v.key) = true
    · -- PK collision: state unchanged.
      simp [hex]; exact hv
    · -- Fresh key: append.
      simp [hex]
      apply appendFresh_preserves_unique _ _ hv
      intro x hx hxv
      apply hex
      rw [List.any_eq_true]
      exact ⟨x, hx, by simp [hxv]⟩
  | .deleteVariant _ _ _ =>
    unfold stepVariant
    exact filter_preserves_unique _ _ hv
  | .cascadeFileDelete _ =>
    unfold stepVariant
    exact filter_preserves_unique _ _ hv

-- ─── §2 variant_cascade_delete ──────────────────────────────────────────

/-- After a `cascadeFileDelete fid`, no variant for that fileId remains. -/
theorem cascade_delete_drops_all
    (s : VariantState) (fid : FileId) :
    ∀ v ∈ (stepVariant s (.cascadeFileDelete fid)).variants,
      v.fileId ≠ fid := by
  intro v hv
  unfold stepVariant at hv
  simp only [] at hv
  -- hv : v ∈ s.variants.filter (fun v => v.fileId ≠ fid)
  rw [List.mem_filter] at hv
  -- hv : v ∈ s.variants ∧ decide (v.fileId ≠ fid) = true
  have hsnd : decide (v.fileId ≠ fid) = true := hv.2
  -- Convert decide-equality back to the underlying proposition.
  exact of_decide_eq_true hsnd

/-- `cascadeFileDelete` does not affect variants of OTHER fileIds. -/
theorem cascade_delete_preserves_other_files
    (s : VariantState) (fid otherFid : FileId) (vk : VariantKind)
    (rk : RendererKind) (h_ne : otherFid ≠ fid)
    (v : FileVariant) (h_v : v.fileId = otherFid ∧ v.variantKind = vk ∧
                              v.rendererKind = rk) :
    v ∈ s.variants →
    v ∈ (stepVariant s (.cascadeFileDelete fid)).variants := by
  intro hmem
  unfold stepVariant
  simp only []
  rw [List.mem_filter]
  refine ⟨hmem, ?_⟩
  -- Goal: decide (v.fileId ≠ fid) = true
  -- From h_v : v.fileId = otherFid; h_ne : otherFid ≠ fid.
  have hne : v.fileId ≠ fid := by rw [h_v.1]; exact h_ne
  exact decide_eq_true hne

-- ─── §3 variant_chunk_refcount_transitivity ─────────────────────────────

/--
Variant chunks register through the same `Refcount.step` machinery as
primary file chunks: `preview-variants.ts:209` calls
`shardStub.putChunk(variantHash, variantBytes, refId, 0, userId)` which
hits the same `writeChunkInternal` path. Therefore the existing
refcount invariant `step_preserves_validState` covers variant
registration transitively.

We package this as: a variant insertion that is paired with a
`putChunk` of the variant's chunk-hash preserves the shard's
`validState`. This is the formal statement of the architectural
promise that "variants are first-class refcounted chunks".
-/
theorem variant_chunk_putChunk_preserves_shard_validState
    (s : ShardState) (v : FileVariant) (refId : FileId)
    (hv : validState s) :
    -- Inserting the variant's chunk via the canonical `Op.putChunk` path
    -- preserves the shard's refcount invariant.
    validState (step s (.putChunk v.chunkHash v.byteSize refId 0)) :=
  step_preserves_validState s _ hv

-- ─── §4 deleteVariant + cascadeFileDelete preserve unique-by-key ────────

/-- Both deletion ops preserve the variants invariant. (Already covered
by `stepVariant_preserves_validState`; restated for clarity.) -/
theorem deleteVariant_preserves_invariant
    (s : VariantState) (fid : FileId) (vk : VariantKind) (rk : RendererKind)
    (hv : validVariantState s) :
    validVariantState (stepVariant s (.deleteVariant fid vk rk)) :=
  stepVariant_preserves_validState s (.deleteVariant fid vk rk) hv

theorem cascadeFileDelete_preserves_invariant
    (s : VariantState) (fid : FileId) (hv : validVariantState s) :
    validVariantState (stepVariant s (.cascadeFileDelete fid)) :=
  stepVariant_preserves_validState s (.cascadeFileDelete fid) hv

-- ─── §5 Reachability ───────────────────────────────────────────────────

/-- Generalised reachability: any sequence of variant ops from a valid
state remains valid. -/
theorem reachable_validVariantState_gen
    (s : VariantState) (ops : List VariantOp)
    (hv : validVariantState s) :
    validVariantState (ops.foldl stepVariant s) := by
  induction ops generalizing s with
  | nil => exact hv
  | cons op tl ih =>
    simp [List.foldl_cons]
    exact ih (stepVariant s op) (stepVariant_preserves_validState s op hv)

theorem reachable_validVariantState (ops : List VariantOp) :
    validVariantState (ops.foldl stepVariant VariantState.empty) :=
  reachable_validVariantState_gen VariantState.empty ops validVariantState_empty

-- ─── §6 Non-vacuity sanity checks ───────────────────────────────────────

/-- Liveness: inserting a variant into the empty state changes it. -/
theorem insertVariant_changes_state :
    let v : FileVariant := ⟨"f1", "thumb", "image", "h1", 0, 100⟩
    stepVariant VariantState.empty (.insertVariant v) ≠ VariantState.empty := by
  intro v hcontra
  have h := congrArg VariantState.variants hcontra
  unfold stepVariant at h
  simp [VariantState.empty] at h

/-- Witness: cascadeFileDelete on a state with multiple files preserves
non-target rows. -/
theorem cascade_preserves_non_target :
    let v1 : FileVariant := ⟨"f1", "thumb", "image", "h1", 0, 100⟩
    let v2 : FileVariant := ⟨"f2", "thumb", "image", "h2", 1, 200⟩
    let s : VariantState := ⟨[v1, v2]⟩
    v2 ∈ (stepVariant s (.cascadeFileDelete "f1")).variants := by
  decide

/-- Witness: insert-twice with the same key is a no-op (PK uniqueness). -/
theorem insert_twice_same_key_is_idempotent :
    let v : FileVariant := ⟨"f1", "thumb", "image", "h1", 0, 100⟩
    let s1 := stepVariant VariantState.empty (.insertVariant v)
    let s2 := stepVariant s1 (.insertVariant v)
    s1 = s2 := by
  decide

/-- Sanity: `stepVariant_preserves_validState` is non-vacuous. -/
theorem stepVariant_preserves_validState_nonvacuous :
    ∃ (s : VariantState) (op : VariantOp),
      stepVariant s op ≠ s ∧ validVariantState (stepVariant s op) := by
  refine ⟨VariantState.empty, .insertVariant ⟨"f1", "thumb", "image", "h1", 0, 100⟩,
          ?_, ?_⟩
  · -- The step changes the empty state.
    intro h
    have hh := congrArg VariantState.variants h
    unfold stepVariant at hh
    simp [VariantState.empty] at hh
  · exact stepVariant_preserves_validState _ _ validVariantState_empty

end Mossaic.Vfs.Preview
