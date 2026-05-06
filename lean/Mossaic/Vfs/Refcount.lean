/-
Mossaic.Vfs.Refcount — I1: Per-shard refcount well-formedness.

Models:
  worker/objects/shard/shard-do.ts:17-63     (chunks + chunk_refs schema)
  worker/objects/shard/shard-do.ts:160-265   (putChunk / writeChunkInternal)
  worker/objects/shard/shard-do.ts:282-378   (deleteChunks / removeFileRefs)

Audit reference:
  /workspace/local/audit-report.md §I1 (refcount invariant: holds at HEAD,
  with structural-fix C2 landed at b20e1e6).

Invariant statement (SCOPE FOR THIS BUILD):

  Structural well-formedness — for every shard state reachable from `empty`
  via any sequence of `putChunk` / `deleteChunks` operations:

    (S1) `chunks` are unique by hash.
    (S2) `chunk_refs` are unique by composite primary key
         (chunkHash, fileId, chunkIndex).
    (S3) Every `chunk_refs` row's hash has a corresponding `chunks` row
         (no dangling refs).

  The audit report's I1 stated a stronger invariant:

    (N)  `chunks[h].refCount = |{r ∈ chunk_refs : r.chunkHash = h}|`.

  We prove (S1)-(S3) as `validState` here. The numerical equality (N) is
  a follow-up: its proof requires multiset/cardinality reasoning that is
  available in Mathlib but not in plain Lean 4 stdlib. Documenting that
  gap explicitly per the no-fake-proofs rule.

What we prove:
  - `validState` definition (S1, S2, S3).
  - `putChunk` preserves `validState` in both dedup and cold-path branches.
  - `deleteChunks` preserves `validState`.
  - Non-vacuity: each operation is non-trivial — there exist states + ops
    where `step s op ≠ s` and the invariant still holds.
  - `empty` satisfies `validState`.

What we do NOT prove (intentional gaps, marked):
  - The numerical refCount = liveRefs equality (gap noted; would need
    Multiset / Finset.sum from Mathlib).
  - Cross-shard placement stability (audit H4 — different invariant).
  - Hash collision-resistance (axiomatized at the cryptography boundary).
-/

import Mossaic.Vfs.Common

namespace Mossaic.Vfs.Refcount

open Mossaic.Vfs.Common

/-- A chunk row. Mirrors `chunks` table at shard-do.ts:17-25 (sans the
opaque `data` BLOB and `created_at`, which are irrelevant to refcount). -/
structure Chunk where
  hash      : Hash
  size      : Nat
  refCount  : Nat
  /-- Soft-delete marker; `none` = live, `some t` = marked at time t.
  Used by I5 (GC). For I1's purposes it's just data. -/
  deletedAt : Option TimeMs := none
  deriving DecidableEq, Repr

/-- A chunk_refs row. Mirrors shard-do.ts:27-35. -/
structure ChunkRef where
  chunkHash  : Hash
  fileId     : FileId
  chunkIndex : Nat
  deriving DecidableEq, Repr

/-- Composite primary key for `chunk_refs`: (chunkHash, fileId, chunkIndex). -/
def ChunkRef.key (r : ChunkRef) : Hash × FileId × Nat :=
  (r.chunkHash, r.fileId, r.chunkIndex)

/-- ShardDO state at the model level. The two SQL tables flatten to lists. -/
structure ShardState where
  chunks : List Chunk
  refs   : List ChunkRef
  deriving Repr

/-- Empty state. -/
def ShardState.empty : ShardState := ⟨[], []⟩

/-- Find a chunk by hash. -/
def ShardState.findChunk (s : ShardState) (h : Hash) : Option Chunk :=
  s.chunks.find? (·.hash = h)

/-- Hashes appearing as chunks. -/
def ShardState.hashes (s : ShardState) : List Hash :=
  s.chunks.map Chunk.hash

-- ─── Invariant ──────────────────────────────────────────────────────────

/-- Structural well-formedness: chunk uniqueness by hash, ref uniqueness
by composite key, ref→chunk existence. -/
def validState (s : ShardState) : Prop :=
  UniqueBy Chunk.hash s.chunks ∧
  UniqueBy ChunkRef.key s.refs ∧
  (∀ r ∈ s.refs, ∃ c ∈ s.chunks, c.hash = r.chunkHash)

/-- The empty state is valid. -/
theorem validState_empty : validState ShardState.empty := by
  refine ⟨?_, ?_, ?_⟩
  · exact UniqueBy.nil _
  · exact UniqueBy.nil _
  · intro r hr; exact absurd hr List.not_mem_nil

-- ─── Operations ─────────────────────────────────────────────────────────

/-- The shard operations we model. Mirrors shard-do.ts:153-365. -/
inductive Op where
  /-- `putChunk` (shard-do.ts:153-258). -/
  | putChunk (h : Hash) (size : Nat) (fid : FileId) (idx : Nat)
  /-- `deleteChunks` (shard-do.ts:273-365). -/
  | deleteChunks (fid : FileId) (now : TimeMs)
  deriving Repr

/-- Increment refCount on the chunk with the given hash, if present.
Resurrection: clears deletedAt, mirroring shard-do.ts:213-217. -/
def ShardState.incrRef (s : ShardState) (h : Hash) : ShardState :=
  { s with chunks := s.chunks.map (fun c =>
      if c.hash = h then { c with refCount := c.refCount + 1, deletedAt := none } else c) }

/-- Decrement refCount on the chunk with the given hash (saturating at 0).
If the post-decrement count is 0 and `deletedAt` is currently `none`, set
`deletedAt := some now` (soft-mark for sweeper, shard-do.ts:347-358). -/
def ShardState.decrRef (s : ShardState) (h : Hash) (now : TimeMs) : ShardState :=
  { s with chunks := s.chunks.map (fun c =>
      if c.hash = h then
        let newCount := c.refCount - 1
        let newDel := if newCount = 0 ∧ c.deletedAt.isNone then some now else c.deletedAt
        { c with refCount := newCount, deletedAt := newDel }
      else c) }

/-- Append a chunk with refCount=1 (cold-path). -/
def ShardState.coldInsert (s : ShardState) (h : Hash) (size : Nat) : ShardState :=
  { s with chunks := s.chunks ++ [⟨h, size, 1, none⟩] }

/-- Append a chunk_ref. -/
def ShardState.appendRef (s : ShardState) (r : ChunkRef) : ShardState :=
  { s with refs := s.refs ++ [r] }

/-- Drop every ref with the given file_id. Returns the updated state and
the list of (hash) for each dropped ref (one entry per dropped ref, with
duplicates if the same hash had multiple chunks for the file). -/
def ShardState.dropRefsForFile (s : ShardState) (fid : FileId) :
    ShardState × List Hash :=
  let dropped := (s.refs.filter (·.fileId = fid)).map ChunkRef.chunkHash
  let kept := s.refs.filter (fun r => r.fileId ≠ fid)
  ({ s with refs := kept }, dropped)

/-- The state transition. Faithful to shard-do.ts:180-258 (writeChunkInternal)
and shard-do.ts:324-365 (removeFileRefs). -/
def step (s : ShardState) : Op → ShardState
  | .putChunk h size fid idx =>
    let r : ChunkRef := ⟨h, fid, idx⟩
    let refExists := s.refs.any (fun x => x.key = r.key)
    let s' := if refExists then s else s.appendRef r
    match s'.findChunk h with
    | some _ =>
      if refExists then s'
      else s'.incrRef h
    | none =>
      if refExists then s'
      else s'.coldInsert h size
  | .deleteChunks fid now =>
    let (s', droppedHashes) := s.dropRefsForFile fid
    droppedHashes.foldl (fun acc h => acc.decrRef h now) s'

-- ─── Helpers ────────────────────────────────────────────────────────────

/-- `incrRef` doesn't change refs. -/
theorem incrRef_refs (s : ShardState) (h : Hash) :
    (s.incrRef h).refs = s.refs := rfl

/-- `decrRef` doesn't change refs. -/
theorem decrRef_refs (s : ShardState) (h : Hash) (now : TimeMs) :
    (s.decrRef h now).refs = s.refs := rfl

/-- General lemma: a hash-preserving map of chunks preserves UniqueBy. -/
private theorem uniqueBy_hash_map_preserve (l : List Chunk) (f : Chunk → Chunk)
    (hf : ∀ c, (f c).hash = c.hash) (hu : UniqueBy Chunk.hash l) :
    UniqueBy Chunk.hash (l.map f) := by
  unfold UniqueBy at hu ⊢
  rw [List.pairwise_map]
  apply List.Pairwise.imp _ hu
  intro a b hab
  rw [hf a, hf b]
  exact hab

/-- `incrRef` preserves chunk uniqueness. -/
theorem incrRef_preserves_uniqueChunks (s : ShardState) (h : Hash)
    (huC : UniqueBy Chunk.hash s.chunks) :
    UniqueBy Chunk.hash (s.incrRef h).chunks := by
  unfold ShardState.incrRef
  simp
  apply uniqueBy_hash_map_preserve _ _ _ huC
  intro c
  by_cases hc : c.hash = h <;> simp [hc]

/-- `decrRef` preserves chunk uniqueness. -/
theorem decrRef_preserves_uniqueChunks (s : ShardState) (h : Hash) (now : TimeMs)
    (huC : UniqueBy Chunk.hash s.chunks) :
    UniqueBy Chunk.hash (s.decrRef h now).chunks := by
  unfold ShardState.decrRef
  simp
  apply uniqueBy_hash_map_preserve _ _ _ huC
  intro c
  by_cases hc : c.hash = h <;> simp [hc]

/-- `incrRef` preserves the ref→chunk existence property. -/
theorem incrRef_preserves_refChunk (s : ShardState) (h : Hash)
    (hrc : ∀ r ∈ s.refs, ∃ c ∈ s.chunks, c.hash = r.chunkHash) :
    ∀ r ∈ (s.incrRef h).refs, ∃ c ∈ (s.incrRef h).chunks, c.hash = r.chunkHash := by
  intro r hr
  rw [incrRef_refs] at hr
  obtain ⟨c, hc, hch⟩ := hrc r hr
  by_cases hc_eq : c.hash = h
  · -- Witness: the bumped chunk
    refine ⟨{ c with refCount := c.refCount + 1, deletedAt := none }, ?_, ?_⟩
    · unfold ShardState.incrRef
      simp [List.mem_map]
      refine ⟨c, hc, ?_⟩
      simp [hc_eq]
    · simp; exact hch
  · -- Witness: c unchanged
    refine ⟨c, ?_, hch⟩
    unfold ShardState.incrRef
    simp [List.mem_map]
    refine ⟨c, hc, ?_⟩
    simp [hc_eq]

/-- `decrRef` preserves the ref→chunk existence property. -/
theorem decrRef_preserves_refChunk (s : ShardState) (h : Hash) (now : TimeMs)
    (hrc : ∀ r ∈ s.refs, ∃ c ∈ s.chunks, c.hash = r.chunkHash) :
    ∀ r ∈ (s.decrRef h now).refs, ∃ c ∈ (s.decrRef h now).chunks, c.hash = r.chunkHash := by
  intro r hr
  rw [decrRef_refs] at hr
  obtain ⟨c, hc, hch⟩ := hrc r hr
  -- Witness: the mapped image of c. We split into the two cases of the
  -- decrRef map's `if`, and provide the matching witness explicitly.
  by_cases hc_eq : c.hash = h
  · refine ⟨{ c with
                refCount := c.refCount - 1,
                deletedAt := if c.refCount - 1 = 0 ∧ c.deletedAt.isNone then some now
                             else c.deletedAt }, ?_, ?_⟩
    · unfold ShardState.decrRef
      simp [List.mem_map]
      refine ⟨c, hc, ?_⟩
      simp [hc_eq]
    · simp; exact hch
  · refine ⟨c, ?_, hch⟩
    unfold ShardState.decrRef
    simp [List.mem_map]
    refine ⟨c, hc, ?_⟩
    simp [hc_eq]

/-- `decrRef` preserves `validState`. -/
theorem decrRef_preserves_validState (s : ShardState) (h : Hash) (now : TimeMs)
    (hv : validState s) : validState (s.decrRef h now) := by
  obtain ⟨huC, huR, hrc⟩ := hv
  refine ⟨decrRef_preserves_uniqueChunks s h now huC, ?_, decrRef_preserves_refChunk s h now hrc⟩
  rw [decrRef_refs]
  exact huR

/-- `appendRef r` to s preserves `validState` provided:
    (a) `r.key` is fresh in `s.refs` (uniqueness);
    (b) `r.chunkHash` has a chunk in `s` (existence). -/
theorem appendRef_preserves_validState (s : ShardState) (r : ChunkRef)
    (hv : validState s)
    (hkey_fresh : ∀ r' ∈ s.refs, r'.key ≠ r.key)
    (hch_exists : ∃ c ∈ s.chunks, c.hash = r.chunkHash) :
    validState (s.appendRef r) := by
  obtain ⟨huC, huR, hrc⟩ := hv
  refine ⟨huC, ?_, ?_⟩
  · unfold UniqueBy at huR ⊢
    unfold ShardState.appendRef
    simp
    apply List.pairwise_append.mpr
    refine ⟨huR, List.pairwise_singleton _ _, ?_⟩
    intro a ha b hb
    simp at hb
    subst hb
    exact hkey_fresh a ha
  · intro r' hr'
    unfold ShardState.appendRef at hr'
    simp at hr'
    rcases hr' with hold | hnew
    · exact hrc r' hold
    · subst hnew; exact hch_exists

/-- `coldInsert h size` to s preserves `validState` provided h is fresh
in chunks (uniqueness). -/
theorem coldInsert_preserves_validState (s : ShardState) (h : Hash) (size : Nat)
    (hv : validState s)
    (hh_fresh : ∀ c ∈ s.chunks, c.hash ≠ h) :
    validState (s.coldInsert h size) := by
  obtain ⟨huC, huR, hrc⟩ := hv
  refine ⟨?_, huR, ?_⟩
  · unfold UniqueBy at huC ⊢
    unfold ShardState.coldInsert
    simp
    apply List.pairwise_append.mpr
    refine ⟨huC, List.pairwise_singleton _ _, ?_⟩
    intro c hc c' hc'
    simp at hc'
    subst hc'
    exact hh_fresh c hc
  · intro r hr
    unfold ShardState.coldInsert at hr ⊢
    simp at hr
    obtain ⟨c, hc, hch⟩ := hrc r hr
    refine ⟨c, ?_, hch⟩
    simp [List.mem_append]
    exact Or.inl hc

-- ─── putChunk preservation ──────────────────────────────────────────────

/-- `putChunk` preserves `validState`. -/
theorem putChunk_preserves_invariant
    (s : ShardState) (h : Hash) (size : Nat) (fid : FileId) (idx : Nat)
    (hv : validState s) : validState (step s (.putChunk h size fid idx)) := by
  obtain ⟨huChunks, huRefs, hrefChunk⟩ := hv
  unfold step
  by_cases hex : s.refs.any (fun x => x.key = (⟨h, fid, idx⟩ : ChunkRef).key)
  · -- refExists branch: state is exactly s
    simp [hex]
    cases hfind : s.findChunk h with
    | some _ => simp; exact ⟨huChunks, huRefs, hrefChunk⟩
    | none => simp; exact ⟨huChunks, huRefs, hrefChunk⟩
  · -- New ref to insert.
    simp [hex]
    -- Show the new ref's key is fresh.
    have hkey_fresh : ∀ r' ∈ s.refs, r'.key ≠ (⟨h, fid, idx⟩ : ChunkRef).key := by
      intro r' hr' habs
      apply hex
      rw [List.any_eq_true]
      refine ⟨r', hr', ?_⟩
      simp [habs]
    -- After appendRef, findChunk on the new state matches findChunk on s
    -- because appendRef does not touch chunks.
    have hfind_eq : ∀ h', (s.appendRef ⟨h, fid, idx⟩).findChunk h' = s.findChunk h' := by
      intro h'; unfold ShardState.findChunk ShardState.appendRef; rfl
    rw [hfind_eq h]
    cases hfind : s.findChunk h with
    | some c =>
      simp
      -- Pre-conditions for both appendRef and incrRef.
      have hc_mem : c ∈ s.chunks := by
        have := List.mem_of_find?_eq_some hfind
        simpa [ShardState.findChunk] using this
      have hch : c.hash = h := by
        have := List.find?_some hfind
        simpa using this
      have hch_exists : ∃ c' ∈ s.chunks, c'.hash = h := ⟨c, hc_mem, hch⟩
      -- After appendRef: refs has one more, chunks unchanged. Then incrRef on h.
      have hv_app : validState (s.appendRef ⟨h, fid, idx⟩) :=
        appendRef_preserves_validState s ⟨h, fid, idx⟩ ⟨huChunks, huRefs, hrefChunk⟩
          hkey_fresh hch_exists
      -- incrRef preserves validState.
      obtain ⟨huC', huR', hrc'⟩ := hv_app
      refine ⟨incrRef_preserves_uniqueChunks _ _ huC',
              by rw [incrRef_refs]; exact huR',
              incrRef_preserves_refChunk _ _ hrc'⟩
    | none =>
      simp
      -- Cold path: appendRef + coldInsert.
      -- Pre-condition for appendRef: chunkHash existence — but we're inserting
      -- the chunk WITH the ref. Order: appendRef first (which needs chunk
      -- existence) — but the chunk doesn't exist yet!
      --
      -- Actually re-examining shard-do.ts:240-256: TS code does
      -- INSERT INTO chunks first, THEN INSERT INTO chunk_refs. So our
      -- step function should be: coldInsert first, then appendRef.
      -- (But our step does appendRef first then coldInsert in the `else`
      -- branch — we need to fix the model OR prove via a re-association.)
      --
      -- Let's just do coldInsert first then appendRef in the proof:
      -- The two operations COMMUTE (one touches chunks, the other touches
      -- refs), so ordering doesn't matter for the final state.
      have hh_fresh : ∀ c ∈ s.chunks, c.hash ≠ h := by
        intro c hc heq
        unfold ShardState.findChunk at hfind
        rw [List.find?_eq_none] at hfind
        have hh := hfind c hc
        simp at hh
        exact hh heq
      -- The combined state appendRef + coldInsert (in either order) is:
      --   { chunks := s.chunks ++ [new_chunk], refs := s.refs ++ [new_ref] }
      have hcomm : ((s.appendRef ⟨h, fid, idx⟩).coldInsert h size) =
                   ({ chunks := s.chunks ++ [⟨h, size, 1, none⟩],
                      refs := s.refs ++ [⟨h, fid, idx⟩] } : ShardState) := by
        unfold ShardState.appendRef ShardState.coldInsert; rfl
      rw [hcomm]
      -- Now prove validity directly.
      refine ⟨?_, ?_, ?_⟩
      · -- chunk uniqueness
        unfold UniqueBy at huChunks ⊢
        apply List.pairwise_append.mpr
        refine ⟨huChunks, List.pairwise_singleton _ _, ?_⟩
        intro a ha b hb
        simp at hb
        subst hb
        exact hh_fresh a ha
      · -- ref uniqueness
        unfold UniqueBy at huRefs ⊢
        apply List.pairwise_append.mpr
        refine ⟨huRefs, List.pairwise_singleton _ _, ?_⟩
        intro a ha b hb
        simp at hb
        subst hb
        exact hkey_fresh a ha
      · -- ref→chunk existence
        intro r hr
        simp at hr
        rcases hr with hold | hnew
        · obtain ⟨c, hc, hch⟩ := hrefChunk r hold
          refine ⟨c, ?_, hch⟩
          simp [List.mem_append]
          exact Or.inl hc
        · subst hnew
          refine ⟨⟨h, size, 1, none⟩, ?_, rfl⟩
          simp [List.mem_append]

-- ─── deleteChunks preservation ──────────────────────────────────────────

/-- A foldl of `decrRef` operations preserves `validState`. -/
theorem foldl_decrRef_preserves_validState
    (s : ShardState) (hashes : List Hash) (now : TimeMs)
    (hv : validState s) :
    validState (hashes.foldl (fun acc h => acc.decrRef h now) s) := by
  induction hashes generalizing s with
  | nil => exact hv
  | cons h tl ih =>
    simp [List.foldl_cons]
    exact ih (s.decrRef h now) (decrRef_preserves_validState s h now hv)

/-- Filtering refs preserves `validState` (drops only — never adds). -/
theorem filterRefs_preserves_validState (s : ShardState) (p : ChunkRef → Bool)
    (hv : validState s) :
    validState { s with refs := s.refs.filter p } := by
  obtain ⟨huC, huR, hrc⟩ := hv
  refine ⟨huC, ?_, ?_⟩
  · unfold UniqueBy at huR ⊢
    exact List.Pairwise.sublist List.filter_sublist huR
  · intro r hr
    have : r ∈ s.refs := by
      rw [List.mem_filter] at hr; exact hr.1
    exact hrc r this

/-- `deleteChunks` preserves `validState`. -/
theorem deleteChunks_preserves_invariant
    (s : ShardState) (fid : FileId) (now : TimeMs)
    (hv : validState s) : validState (step s (.deleteChunks fid now)) := by
  unfold step
  simp only [ShardState.dropRefsForFile]
  -- The result is: foldl decrRef ... { s with refs := filtered }
  apply foldl_decrRef_preserves_validState
  exact filterRefs_preserves_validState s (fun r => r.fileId ≠ fid) hv

-- ─── Master theorem: `step` preserves `validState` ──────────────────────

/-- I1's main theorem: every operation preserves `validState`. This is the
state-machine inductive invariant — combined with `validState_empty`, it
establishes that every reachable state is well-formed. -/
theorem step_preserves_validState (s : ShardState) (op : Op)
    (hv : validState s) : validState (step s op) := by
  match op with
  | .putChunk h size fid idx => exact putChunk_preserves_invariant s h size fid idx hv
  | .deleteChunks fid now => exact deleteChunks_preserves_invariant s fid now hv

/-- Generalized reachability: any state produced by a sequence of ops from
a valid starting state is valid. -/
theorem reachable_validState_gen (s : ShardState) (ops : List Op)
    (hv : validState s) :
    validState (ops.foldl step s) := by
  induction ops generalizing s with
  | nil => exact hv
  | cons op tl ih =>
    simp [List.foldl_cons]
    exact ih (step s op) (step_preserves_validState s op hv)

/-- Reachability from empty: every reachable state is valid. -/
theorem reachable_validState (ops : List Op) :
    validState (ops.foldl step ShardState.empty) :=
  reachable_validState_gen ShardState.empty ops validState_empty

-- ─── Non-vacuity sanity checks ──────────────────────────────────────────

/-- Liveness: putChunk on a fresh state actually inserts a chunk + ref. -/
theorem putChunk_changes_state (h : Hash) (size : Nat) (fid : FileId) (idx : Nat) :
    step ShardState.empty (.putChunk h size fid idx) ≠ ShardState.empty := by
  intro hcontra
  unfold step at hcontra
  simp [ShardState.empty, ShardState.appendRef, ShardState.coldInsert,
        ShardState.findChunk] at hcontra

/-- Liveness: deleteChunks on a state with matching refs actually drops them. -/
theorem deleteChunks_changes_state :
    let h : Hash := "abc"
    let fid : FileId := "f1"
    let s₀ : ShardState := ⟨[⟨h, 1, 1, none⟩], [⟨h, fid, 0⟩]⟩
    step s₀ (.deleteChunks fid 100) ≠ s₀ := by
  simp only
  intro hcontra
  unfold step at hcontra
  simp [ShardState.dropRefsForFile, ShardState.decrRef] at hcontra

/-- Concrete witness: a non-trivial reachable state is valid. -/
theorem witness_putChunk_valid :
    validState (step ShardState.empty (.putChunk "abc" 100 "f1" 0)) :=
  step_preserves_validState _ _ validState_empty

/-- Concrete witness: putChunk then deleteChunks is valid. -/
theorem witness_putChunk_deleteChunks_valid :
    validState (step
      (step ShardState.empty (.putChunk "abc" 100 "f1" 0))
      (.deleteChunks "f1" 100)) := by
  apply step_preserves_validState
  exact step_preserves_validState _ _ validState_empty

end Mossaic.Vfs.Refcount
