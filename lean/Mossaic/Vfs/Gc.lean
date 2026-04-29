/-
Mossaic.Vfs.Gc — I5: Garbage collection safety. Mathlib-backed, NO AXIOM.

Models:
  worker/core/objects/shard/shard-do.ts (777 LoC)
    — alarm sweeper: hard-delete after 30s grace
    — scheduleSweep: alarm-set on first soft-mark
    — soft-mark: ref_count=0 stamps deleted_at; alarm reaps later

Audit reference:
  /workspace/local/audit-report.md §I5.

Invariants:
  (G1) `alarm` only hard-deletes chunks whose `refCount = 0`.
  (G2) `alarm` respects the 30-second grace window.
  (G3) `alarm` preserves `validState` (UNCONDITIONAL — no axiom). The
       previous version used a documented `axiom` for the numerical
       refcount=liveRefs equality. With the Mathlib-backed I1 invariant
       carrying that equality as part of `validState`, we can now derive
       it as a theorem (`refCount_zero_implies_no_refs` in Refcount.lean).
  (G4) Resurrection (refCount > 0 with deletedAt set) survives sweep.

NO `axiom`, NO `sorry`. Mathlib v4.29.0.
-/

import Mossaic.Vfs.Refcount

namespace Mossaic.Vfs.Gc

open Mossaic.Vfs.Common
open Mossaic.Vfs.Refcount

/-- Grace window in milliseconds. Mirrors shard-do.ts:392 `30_000`. -/
def graceWindowMs : Nat := 30000

/-- A chunk is eligible for hard-delete by `alarm now` if:
  - it has `refCount = 0`,
  - it has `deletedAt = some t` with `t + grace ≤ now`. -/
def eligibleForSweep (c : Chunk) (now : TimeMs) : Bool :=
  match c.deletedAt with
  | some t => decide (c.refCount = 0) && decide (t + graceWindowMs ≤ now)
  | none   => false

/-- A chunk is "resurrected": soft-marked but refCount went back up. -/
def isResurrected (c : Chunk) : Bool :=
  c.deletedAt.isSome && decide (c.refCount > 0)

/-- The `alarm` operation. Mirrors shard-do.ts:415-455. -/
def alarm (s : ShardState) (now : TimeMs) : ShardState :=
  { s with chunks :=
      (s.chunks.filter (fun c => ¬ eligibleForSweep c now)).map (fun c =>
        if isResurrected c then { c with deletedAt := none } else c) }

-- ─── Helper: filter+map preserves UniqueBy ──────────────────────────────

private theorem uniqueBy_filter_map_preserve {β : Type} [DecidableEq β]
    (l : List Chunk) (p : Chunk → Bool) (f : Chunk → Chunk) (key : Chunk → β)
    (hf : ∀ c, key (f c) = key c)
    (hu : UniqueBy key l) :
    UniqueBy key ((l.filter p).map f) := by
  unfold UniqueBy at hu ⊢
  rw [List.pairwise_map]
  have hfilt : (l.filter p).Pairwise (fun x y => key x ≠ key y) :=
    List.Pairwise.sublist List.filter_sublist hu
  apply List.Pairwise.imp _ hfilt
  intro a b hab
  rw [hf a, hf b]
  exact hab

-- ─── liveRefs is unaffected by alarm (alarm doesn't touch refs) ─────────

theorem alarm_refs (s : ShardState) (now : TimeMs) :
    (alarm s now).refs = s.refs := rfl

theorem alarm_liveRefs (s : ShardState) (now : TimeMs) (h : Hash) :
    liveRefs (alarm s now) h = liveRefs s h := rfl

-- ─── (G3) alarm preserves validState — UNCONDITIONAL ────────────────────

/-- `alarm` preserves the `validState` invariant. No axiom: relies on
the I1 numerical equality `refCount_zero_implies_no_refs`. -/
theorem alarm_preserves_validState (s : ShardState) (now : TimeMs)
    (hv : validState s) : validState (alarm s now) := by
  obtain ⟨huC, huR, hrc, hrcEq⟩ := hv
  refine ⟨?_, ?_, ?_, ?_⟩
  · -- chunk uniqueness
    unfold alarm
    apply uniqueBy_filter_map_preserve _ _ _ _ _ huC
    intro c
    by_cases hres : isResurrected c <;> simp [hres]
  · -- ref uniqueness
    unfold alarm
    exact huR
  · -- ref→chunk existence
    intro r hr
    have hr' : r ∈ s.refs := hr
    obtain ⟨c, hc, hch⟩ := hrc r hr'
    -- Show c is NOT eligible for sweep, hence its image survives.
    by_cases hsweep : eligibleForSweep c now
    · -- c eligible ⇒ refCount = 0 ⇒ no ref points at c.hash (by Refcount).
      -- This contradicts r ∈ refs with chunkHash = c.hash.
      exfalso
      have hzero : c.refCount = 0 := by
        unfold eligibleForSweep at hsweep
        cases hd : c.deletedAt with
        | none => rw [hd] at hsweep; simp at hsweep
        | some _ =>
          rw [hd] at hsweep
          simp at hsweep
          exact hsweep.1
      -- Use the numerical corollary (no axiom).
      have hnoref := refCount_zero_implies_no_refs s c
        ⟨huC, huR, hrc, hrcEq⟩ hc hzero
      exact hnoref r hr' hch.symm
    · -- c survives the filter; its image (post-map) has same hash.
      refine ⟨if isResurrected c then { c with deletedAt := none } else c, ?_, ?_⟩
      · unfold alarm
        simp [List.mem_map, List.mem_filter]
        refine ⟨c, ⟨hc, ?_⟩, rfl⟩
        simp [hsweep]
      · by_cases hres : isResurrected c <;> simp [hres] <;> exact hch
  · -- refCount = liveRefs preservation
    intro c hc
    -- c is in (alarm s now).chunks, which is the post-filter, post-map state.
    -- So c is the image of some c0 ∈ s.chunks where ¬ eligibleForSweep c0 now,
    -- and either (resurrected ⇒ deletedAt cleared) or (not ⇒ unchanged).
    -- Either way, c.hash = c0.hash and c.refCount = c0.refCount.
    unfold alarm at hc
    simp [List.mem_map, List.mem_filter] at hc
    obtain ⟨c0, ⟨hc0, hsweep⟩, heq⟩ := hc
    -- c.refCount = c0.refCount, c.hash = c0.hash regardless of branch.
    have h_rc : c.refCount = c0.refCount := by
      by_cases hres : isResurrected c0 <;> simp [hres] at heq <;> rw [← heq]
    have h_hash : c.hash = c0.hash := by
      by_cases hres : isResurrected c0 <;> simp [hres] at heq <;> rw [← heq]
    rw [h_rc, h_hash, alarm_liveRefs]
    exact hrcEq c0 hc0

-- ─── (G1) alarm only deletes refCount=0 chunks ──────────────────────────

theorem alarm_only_deletes_zero_refCount
    (s : ShardState) (now : TimeMs) (c : Chunk)
    (hin : c ∈ s.chunks)
    (hout : ∀ c' ∈ (alarm s now).chunks, c'.hash ≠ c.hash) :
    c.refCount = 0 := by
  by_cases hsweep : eligibleForSweep c now
  · unfold eligibleForSweep at hsweep
    cases hd : c.deletedAt with
    | none => rw [hd] at hsweep; simp at hsweep
    | some _ =>
      rw [hd] at hsweep
      simp at hsweep
      exact hsweep.1
  · exfalso
    have himg_in : (if isResurrected c then { c with deletedAt := none } else c) ∈
                   (alarm s now).chunks := by
      unfold alarm
      simp [List.mem_map, List.mem_filter]
      refine ⟨c, ⟨hin, ?_⟩, rfl⟩
      simp [hsweep]
    have himg_hash : (if isResurrected c then { c with deletedAt := none } else c).hash = c.hash := by
      by_cases hres : isResurrected c <;> simp [hres]
    exact hout _ himg_in himg_hash

-- ─── (G4) Resurrection: survive sweep ───────────────────────────────────

theorem resurrected_not_eligible (c : Chunk) (now : TimeMs)
    (hres : c.refCount > 0) :
    eligibleForSweep c now = false := by
  unfold eligibleForSweep
  cases c.deletedAt with
  | none => simp
  | some _ => simp; intro h0; omega

theorem alarm_unmarks_resurrected
    (s : ShardState) (now : TimeMs) (c : Chunk)
    (hin : c ∈ s.chunks) (hres : isResurrected c) :
    { c with deletedAt := none } ∈ (alarm s now).chunks := by
  unfold alarm
  simp [List.mem_map, List.mem_filter]
  refine ⟨c, ⟨hin, ?_⟩, ?_⟩
  · have hrc : c.refCount > 0 := by
      unfold isResurrected at hres
      simp at hres
      exact hres.2
    rw [resurrected_not_eligible c now hrc]
  · simp [hres]

-- ─── Non-vacuity sanity checks ──────────────────────────────────────────

theorem witness_alarm_sweeps :
    (alarm ({ chunks := [⟨"abc", 100, 0, some 0⟩], refs := [] } : ShardState) 100000).chunks
      = [] := by
  simp [alarm, eligibleForSweep, isResurrected, graceWindowMs]

theorem witness_alarm_unmarks_resurrected :
    (alarm ({ chunks := [⟨"abc", 100, 1, some 0⟩], refs := [⟨"abc", "f", 0⟩] } : ShardState)
            100000).chunks
      = [⟨"abc", 100, 1, none⟩] := by
  simp [alarm, eligibleForSweep, isResurrected, graceWindowMs]

theorem alarm_changes_state :
    alarm ({ chunks := [⟨"abc", 100, 0, some 0⟩], refs := [] } : ShardState) 100000
      ≠ ({ chunks := [⟨"abc", 100, 0, some 0⟩], refs := [] } : ShardState) := by
  intro hcontra
  have heq : (alarm
    ({ chunks := [⟨"abc", 100, 0, some 0⟩], refs := [] } : ShardState)
    100000).chunks =
      ({ chunks := [⟨"abc", 100, 0, some 0⟩], refs := [] } : ShardState).chunks :=
    congrArg ShardState.chunks hcontra
  rw [witness_alarm_sweeps] at heq
  simp at heq

end Mossaic.Vfs.Gc
