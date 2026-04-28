/-
Mossaic.Vfs.Refcount — I1: Per-shard refcount = liveRefs equality.

Models:
  worker/objects/shard/shard-do.ts:17-63     (chunks + chunk_refs schema)
  worker/objects/shard/shard-do.ts:160-265   (putChunk / writeChunkInternal)
  worker/objects/shard/shard-do.ts:282-378   (deleteChunks / removeFileRefs)

Audit reference:
  /workspace/local/audit-report.md §I1.

Invariant `validState` (now includes the numerical equality):

  (S1) `chunks` are unique by hash.
  (S2) `chunk_refs` are unique by composite key (chunkHash, fileId, chunkIndex).
  (S3) Every `chunk_refs` row's hash has a corresponding `chunks` row.
  (S4) For every chunk row, `refCount = countP (·.chunkHash = c.hash) refs`.

This file proves `step` preserves all four properties. The proof is
mechanical but spans both `putChunk` (dedup + cold-path) and `deleteChunks`
(foldl over dropped refs). With Mathlib's `List.countP` and `List.Pairwise`
machinery, every step closes via `simp`/`omega`/`decide` or one of the
named Mathlib lemmas.

NO `axiom`, NO `sorry`. Mathlib v4.29.0.
-/

import Mossaic.Vfs.Common

namespace Mossaic.Vfs.Refcount

open Mossaic.Vfs.Common

/-- A chunk row. Mirrors `chunks` table. -/
structure Chunk where
  hash      : Hash
  size      : Nat
  refCount  : Nat
  /-- Soft-delete marker; `none` = live, `some t` = marked at time t. -/
  deletedAt : Option TimeMs := none
  deriving DecidableEq, Repr

/-- A chunk_refs row. Mirrors shard-do.ts:27-35. -/
structure ChunkRef where
  chunkHash  : Hash
  fileId     : FileId
  chunkIndex : Nat
  deriving DecidableEq, Repr

/-- Composite primary key for `chunk_refs`. -/
def ChunkRef.key (r : ChunkRef) : Hash × FileId × Nat :=
  (r.chunkHash, r.fileId, r.chunkIndex)

/-- ShardDO state at the model level. -/
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

/-- Number of `chunk_refs` rows referencing `h` — provided as a `countP`
so it can be reasoned about with Mathlib's `List.countP_*` lemmas. -/
def liveRefs (s : ShardState) (h : Hash) : Nat :=
  s.refs.countP (·.chunkHash = h)

-- ─── Invariant ──────────────────────────────────────────────────────────

/-- The combined refcount invariant, including the numerical equality. -/
structure validState (s : ShardState) : Prop where
  uChunks  : UniqueBy Chunk.hash s.chunks
  uRefs    : UniqueBy ChunkRef.key s.refs
  refChunk : ∀ r ∈ s.refs, ∃ c ∈ s.chunks, c.hash = r.chunkHash
  /-- Numerical equality: refCount = liveRefs cardinality. -/
  refCountEq : ∀ c ∈ s.chunks, c.refCount = liveRefs s c.hash

/-- The empty state is valid. -/
theorem validState_empty : validState ShardState.empty := by
  refine ⟨?_, ?_, ?_, ?_⟩
  · exact UniqueBy.nil _
  · exact UniqueBy.nil _
  · intro r hr; exact absurd hr List.not_mem_nil
  · intro c hc; exact absurd hc List.not_mem_nil

-- ─── Operations ─────────────────────────────────────────────────────────

inductive Op where
  | putChunk (h : Hash) (size : Nat) (fid : FileId) (idx : Nat)
  | deleteChunks (fid : FileId) (now : TimeMs)
  deriving Repr

/-- Append a chunk_ref. -/
def ShardState.appendRef (s : ShardState) (r : ChunkRef) : ShardState :=
  { s with refs := s.refs ++ [r] }

/-- Append a chunk with refCount=1 (cold-path). -/
def ShardState.coldInsert (s : ShardState) (h : Hash) (size : Nat) : ShardState :=
  { s with chunks := s.chunks ++ [⟨h, size, 1, none⟩] }

/-- Increment refCount on the chunk with hash `h`, clearing `deletedAt`. -/
def ShardState.incrRef (s : ShardState) (h : Hash) : ShardState :=
  { s with chunks := s.chunks.map (fun c =>
      if c.hash = h then { c with refCount := c.refCount + 1, deletedAt := none } else c) }

/-- Decrement refCount on the chunk with hash `h`. Saturates at 0; sets
`deletedAt = some now` on first-to-zero (mirrors shard-do.ts:347-358). -/
def ShardState.decrRef (s : ShardState) (h : Hash) (now : TimeMs) : ShardState :=
  { s with chunks := s.chunks.map (fun c =>
      if c.hash = h then
        let newCount := c.refCount - 1
        let newDel := if newCount = 0 ∧ c.deletedAt.isNone then some now else c.deletedAt
        { c with refCount := newCount, deletedAt := newDel }
      else c) }

/-- Drop every ref with the given file_id. Returns the updated state and
the multiset of dropped hashes (one entry per ref dropped). -/
def ShardState.dropRefsForFile (s : ShardState) (fid : FileId) :
    ShardState × List Hash :=
  let dropped := (s.refs.filter (·.fileId = fid)).map ChunkRef.chunkHash
  let kept := s.refs.filter (fun r => r.fileId ≠ fid)
  ({ s with refs := kept }, dropped)

/-- The state transition. -/
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

-- ─── Helpers about the row mutators ─────────────────────────────────────

theorem incrRef_refs (s : ShardState) (h : Hash) :
    (s.incrRef h).refs = s.refs := rfl

theorem decrRef_refs (s : ShardState) (h : Hash) (now : TimeMs) :
    (s.decrRef h now).refs = s.refs := rfl

theorem appendRef_chunks (s : ShardState) (r : ChunkRef) :
    (s.appendRef r).chunks = s.chunks := rfl

theorem coldInsert_refs (s : ShardState) (h : Hash) (size : Nat) :
    (s.coldInsert h size).refs = s.refs := rfl

/-- liveRefs after appending a ref: increments by 1 if the appended ref's
hash matches, unchanged otherwise. -/
theorem liveRefs_appendRef (s : ShardState) (r : ChunkRef) (h : Hash) :
    liveRefs (s.appendRef r) h = liveRefs s h + (if r.chunkHash = h then 1 else 0) := by
  unfold liveRefs ShardState.appendRef
  rw [List.countP_append]
  simp [List.countP_cons, List.countP_nil]

/-- liveRefs is unaffected by `incrRef` (which only touches chunks). -/
theorem liveRefs_incrRef (s : ShardState) (h h' : Hash) :
    liveRefs (s.incrRef h') h = liveRefs s h := rfl

/-- liveRefs is unaffected by `decrRef`. -/
theorem liveRefs_decrRef (s : ShardState) (h h' : Hash) (now : TimeMs) :
    liveRefs (s.decrRef h' now) h = liveRefs s h := rfl

/-- liveRefs after `coldInsert`: unchanged (coldInsert only touches chunks). -/
theorem liveRefs_coldInsert (s : ShardState) (hh : Hash) (size : Nat) (h : Hash) :
    liveRefs (s.coldInsert hh size) h = liveRefs s h := rfl

-- ─── Hash-preserving map preserves UniqueBy ─────────────────────────────

private theorem uniqueBy_hash_map_preserve (l : List Chunk) (f : Chunk → Chunk)
    (hf : ∀ c, (f c).hash = c.hash) (hu : UniqueBy Chunk.hash l) :
    UniqueBy Chunk.hash (l.map f) := by
  unfold UniqueBy at hu ⊢
  rw [List.pairwise_map]
  apply List.Pairwise.imp _ hu
  intro a b hab
  rw [hf a, hf b]
  exact hab

theorem incrRef_preserves_uniqueChunks (s : ShardState) (h : Hash)
    (huC : UniqueBy Chunk.hash s.chunks) :
    UniqueBy Chunk.hash (s.incrRef h).chunks := by
  unfold ShardState.incrRef
  apply uniqueBy_hash_map_preserve _ _ _ huC
  intro c
  by_cases hc : c.hash = h <;> simp [hc]

theorem decrRef_preserves_uniqueChunks (s : ShardState) (h : Hash) (now : TimeMs)
    (huC : UniqueBy Chunk.hash s.chunks) :
    UniqueBy Chunk.hash (s.decrRef h now).chunks := by
  unfold ShardState.decrRef
  apply uniqueBy_hash_map_preserve _ _ _ huC
  intro c
  by_cases hc : c.hash = h <;> simp [hc]

-- ─── Hash equality after find?_eq_some ──────────────────────────────────

theorem findChunk_some_hash (s : ShardState) (h : Hash) (c : Chunk)
    (hfind : s.findChunk h = some c) : c.hash = h := by
  have := List.find?_some hfind
  simpa using this

theorem findChunk_some_mem (s : ShardState) (h : Hash) (c : Chunk)
    (hfind : s.findChunk h = some c) : c ∈ s.chunks := by
  have := List.mem_of_find?_eq_some hfind
  simpa [ShardState.findChunk] using this

theorem findChunk_none_no_hash (s : ShardState) (h : Hash)
    (hfind : s.findChunk h = none) :
    ∀ c ∈ s.chunks, c.hash ≠ h := by
  intro c hc heq
  unfold ShardState.findChunk at hfind
  rw [List.find?_eq_none] at hfind
  have := hfind c hc
  simp at this
  exact this heq

-- ─── Combined operations (the actual `step` cases) ──────────────────────

/-- `putChunk` cold-path: `appendRef r` + `coldInsert h size`, when the
chunk row was absent and the ref was fresh. The combined state has refs
extended by `[r]` and chunks extended by `[⟨h, size, 1, none⟩]`. -/
private theorem putChunk_cold_preserves
    (s : ShardState) (h : Hash) (size : Nat) (fid : FileId) (idx : Nat)
    (hv : validState s)
    (hkey_fresh : ∀ r' ∈ s.refs, r'.key ≠ (⟨h, fid, idx⟩ : ChunkRef).key)
    (hh_fresh : ∀ c ∈ s.chunks, c.hash ≠ h) :
    validState
      ({ chunks := s.chunks ++ [⟨h, size, 1, none⟩],
         refs := s.refs ++ [⟨h, fid, idx⟩] } : ShardState) := by
  obtain ⟨huC, huR, hrc, hrcEq⟩ := hv
  refine ⟨?_, ?_, ?_, ?_⟩
  · -- chunk uniqueness
    unfold UniqueBy
    apply List.pairwise_append.mpr
    refine ⟨huC, List.pairwise_singleton _ _, ?_⟩
    intro a ha b hb
    simp at hb
    subst hb
    exact hh_fresh a ha
  · -- ref uniqueness
    unfold UniqueBy
    apply List.pairwise_append.mpr
    refine ⟨huR, List.pairwise_singleton _ _, ?_⟩
    intro a ha b hb
    simp at hb
    subst hb
    exact hkey_fresh a ha
  · -- ref→chunk existence
    intro r hr
    simp [List.mem_append] at hr
    rcases hr with hold | hnew
    · obtain ⟨c, hc, hch⟩ := hrc r hold
      refine ⟨c, ?_, hch⟩
      simp [List.mem_append]
      exact Or.inl hc
    · subst hnew
      refine ⟨⟨h, size, 1, none⟩, ?_, rfl⟩
      simp [List.mem_append]
  · -- refCount = liveRefs
    intro c hc
    -- Note: `liveRefs` is computed against the new state.
    -- For old chunks: c.hash ≠ h (by hh_fresh); liveRefs counts +0 from
    -- the new ref (since new ref's chunkHash = h ≠ c.hash).
    -- For the new chunk: refCount = 1 = liveRefs (since the new ref
    -- contributes 1, and the old refs contribute 0 — by the original
    -- invariant, all old refs have hashes that are present in old chunks,
    -- and h is fresh among old chunks, so no old ref points to h).
    simp [List.mem_append] at hc
    rcases hc with hc_old | hc_new
    · -- old chunk
      have hne : h ≠ c.hash := fun heq => hh_fresh c hc_old heq.symm
      have hgoal :
          liveRefs ({ chunks := s.chunks ++ [⟨h, size, 1, none⟩],
                      refs := s.refs ++ [⟨h, fid, idx⟩] } : ShardState) c.hash
            = liveRefs s c.hash := by
        unfold liveRefs
        show (s.refs ++ [(⟨h, fid, idx⟩ : ChunkRef)]).countP (·.chunkHash = c.hash) = _
        rw [List.countP_append]
        simp [List.countP_nil, hne]
      rw [hgoal]
      exact hrcEq c hc_old
    · -- new chunk: refCount = 1
      subst hc_new
      have h0 : s.refs.countP (·.chunkHash = h) = 0 := by
        rw [List.countP_eq_zero]
        intro r hr habs
        simp at habs
        obtain ⟨c', hc', hch'⟩ := hrc r hr
        rw [habs] at hch'
        exact hh_fresh c' hc' hch'
      have hgoal :
          liveRefs ({ chunks := s.chunks ++ [⟨h, size, 1, none⟩],
                      refs := s.refs ++ [⟨h, fid, idx⟩] } : ShardState) h
            = 1 := by
        unfold liveRefs
        show (s.refs ++ [(⟨h, fid, idx⟩ : ChunkRef)]).countP (·.chunkHash = h) = 1
        rw [List.countP_append, h0]
        simp [List.countP_nil]
      rw [hgoal]

/-- `putChunk` dedup-path with new ref: `appendRef r` + `incrRef h`, when
the chunk row exists and the ref was fresh. -/
private theorem putChunk_dedup_new_ref_preserves
    (s : ShardState) (h : Hash) (fid : FileId) (idx : Nat) (c : Chunk)
    (hv : validState s)
    (hkey_fresh : ∀ r' ∈ s.refs, r'.key ≠ (⟨h, fid, idx⟩ : ChunkRef).key)
    (hc_mem : c ∈ s.chunks) (hc_hash : c.hash = h) :
    validState ((s.appendRef ⟨h, fid, idx⟩).incrRef h) := by
  obtain ⟨huC, huR, hrc, hrcEq⟩ := hv
  -- The combined state: chunks updated via map (incrRef), refs ++ [new].
  -- Compute the post state explicitly.
  have hpost_chunks :
      ((s.appendRef ⟨h, fid, idx⟩).incrRef h).chunks =
        s.chunks.map (fun c' =>
          if c'.hash = h then { c' with refCount := c'.refCount + 1, deletedAt := none } else c') := by
    unfold ShardState.appendRef ShardState.incrRef; rfl
  have hpost_refs :
      ((s.appendRef ⟨h, fid, idx⟩).incrRef h).refs = s.refs ++ [⟨h, fid, idx⟩] := by
    unfold ShardState.appendRef ShardState.incrRef; rfl
  refine ⟨?_, ?_, ?_, ?_⟩
  · -- chunk uniqueness: hash-preserving map
    rw [hpost_chunks]
    apply uniqueBy_hash_map_preserve _ _ _ huC
    intro c'
    by_cases hc' : c'.hash = h <;> simp [hc']
  · -- ref uniqueness
    rw [hpost_refs]
    unfold UniqueBy
    apply List.pairwise_append.mpr
    refine ⟨huR, List.pairwise_singleton _ _, ?_⟩
    intro a ha b hb
    simp at hb
    subst hb
    exact hkey_fresh a ha
  · -- ref→chunk existence
    intro r hr
    rw [hpost_refs, List.mem_append] at hr
    rcases hr with hold | hnew
    · -- old ref: still has chunk (the mapped one with same hash)
      obtain ⟨c', hc', hch'⟩ := hrc r hold
      by_cases hh' : c'.hash = h
      · -- bumped witness
        refine ⟨{ c' with refCount := c'.refCount + 1, deletedAt := none }, ?_, ?_⟩
        · rw [hpost_chunks]
          simp [List.mem_map]
          refine ⟨c', hc', ?_⟩
          simp [hh']
        · simpa using hch'
      · -- unchanged witness
        refine ⟨c', ?_, hch'⟩
        rw [hpost_chunks]
        simp [List.mem_map]
        refine ⟨c', hc', ?_⟩
        simp [hh']
    · -- new ref: ⟨h, fid, idx⟩
      simp at hnew
      subst hnew
      -- Witness: the bumped c
      refine ⟨{ c with refCount := c.refCount + 1, deletedAt := none }, ?_, ?_⟩
      · rw [hpost_chunks]
        simp [List.mem_map]
        refine ⟨c, hc_mem, ?_⟩
        simp [hc_hash]
      · simpa using hc_hash
  · -- refCount = liveRefs
    intro c' hc'
    rw [hpost_chunks] at hc'
    simp [List.mem_map] at hc'
    obtain ⟨c'', hc'', heq⟩ := hc'
    -- liveRefs (post) c'.hash = countP (·.chunkHash = c'.hash) (s.refs ++ [⟨h, fid, idx⟩])
    --                       = liveRefs s c'.hash + (if h = c'.hash then 1 else 0)
    by_cases hh' : c''.hash = h
    · -- c' = bumped c'' with refCount = c''.refCount + 1
      simp [hh'] at heq
      subst heq
      -- c'.hash = h (since c''.hash = h)
      -- new refCount = c''.refCount + 1 = liveRefs s h + 1 = liveRefs (post) h
      show c''.refCount + 1 = liveRefs ((s.appendRef ⟨h, fid, idx⟩).incrRef h) h
      have h1 : liveRefs ((s.appendRef ⟨h, fid, idx⟩).incrRef h) h
              = liveRefs (s.appendRef ⟨h, fid, idx⟩) h := liveRefs_incrRef _ _ _
      have h2 : liveRefs (s.appendRef ⟨h, fid, idx⟩) h = liveRefs s h + 1 := by
        rw [liveRefs_appendRef]; simp
      rw [h1, h2, hrcEq c'' hc'', hh']
    · -- c' = c'' (the if-else branch), unchanged
      simp [hh'] at heq
      subst heq
      -- c'.hash = c''.hash ≠ h
      show c''.refCount = liveRefs ((s.appendRef ⟨h, fid, idx⟩).incrRef h) c''.hash
      have h1 : liveRefs ((s.appendRef ⟨h, fid, idx⟩).incrRef h) c''.hash
              = liveRefs (s.appendRef ⟨h, fid, idx⟩) c''.hash := liveRefs_incrRef _ _ _
      have h2 : liveRefs (s.appendRef ⟨h, fid, idx⟩) c''.hash = liveRefs s c''.hash := by
        rw [liveRefs_appendRef]
        have : ¬ ((⟨h, fid, idx⟩ : ChunkRef).chunkHash = c''.hash) := by
          intro habs; exact hh' (by simpa using habs.symm)
        simp [this]
      rw [h1, h2, hrcEq c'' hc'']

/-- `putChunk` dedup-path with EXISTING ref: state unchanged. -/
private theorem putChunk_dedup_existing_ref_preserves
    (s : ShardState) (hv : validState s) : validState s := hv

/-- `putChunk` cold-path with EXISTING ref but absent chunk: this is the
"defensive no-op" branch in shard-do.ts (TS-unreachable in practice). The
state doesn't change. Note: validState is preserved trivially. -/
private theorem putChunk_cold_existing_ref_preserves
    (s : ShardState) (hv : validState s) : validState s := hv

/-- Master: `putChunk` preserves `validState`. -/
theorem putChunk_preserves_invariant
    (s : ShardState) (h : Hash) (size : Nat) (fid : FileId) (idx : Nat)
    (hv : validState s) : validState (step s (.putChunk h size fid idx)) := by
  unfold step
  by_cases hex : s.refs.any (fun x => x.key = (⟨h, fid, idx⟩ : ChunkRef).key)
  · -- refExists: state unchanged regardless of findChunk
    simp [hex]
    cases hfind : s.findChunk h with
    | some _ => simp; exact hv
    | none => simp; exact hv
  · -- new ref
    simp [hex]
    have hkey_fresh : ∀ r' ∈ s.refs, r'.key ≠ (⟨h, fid, idx⟩ : ChunkRef).key := by
      intro r' hr' habs
      apply hex
      rw [List.any_eq_true]
      refine ⟨r', hr', ?_⟩
      simp [habs]
    -- After appendRef, findChunk on new state = findChunk on s (chunks unchanged)
    have hfind_eq : ∀ h', (s.appendRef ⟨h, fid, idx⟩).findChunk h' = s.findChunk h' := by
      intro h'; unfold ShardState.findChunk ShardState.appendRef; rfl
    rw [hfind_eq h]
    cases hfind : s.findChunk h with
    | some c =>
      simp
      have hc_mem := findChunk_some_mem s h c hfind
      have hc_hash := findChunk_some_hash s h c hfind
      exact putChunk_dedup_new_ref_preserves s h fid idx c hv hkey_fresh hc_mem hc_hash
    | none =>
      simp
      have hh_fresh := findChunk_none_no_hash s h hfind
      -- The combined state s.appendRef.coldInsert is: chunks ++ [new], refs ++ [new ref]
      have hcomm : ((s.appendRef ⟨h, fid, idx⟩).coldInsert h size) =
                   ({ chunks := s.chunks ++ [⟨h, size, 1, none⟩],
                      refs := s.refs ++ [⟨h, fid, idx⟩] } : ShardState) := by
        unfold ShardState.appendRef ShardState.coldInsert; rfl
      rw [hcomm]
      exact putChunk_cold_preserves s h size fid idx hv hkey_fresh hh_fresh

-- ─── deleteChunks ───────────────────────────────────────────────────────

/-- liveRefs after filtering refs by predicate `p`: cardinality drops by
the count of filtered-out matching refs. -/
theorem liveRefs_filter (s : ShardState) (p : ChunkRef → Bool) (h : Hash) :
    liveRefs ({ s with refs := s.refs.filter p } : ShardState) h =
      s.refs.countP (fun r => p r ∧ r.chunkHash = h) := by
  unfold liveRefs
  show (s.refs.filter p).countP (·.chunkHash = h) = _
  rw [List.countP_filter]
  congr 1
  funext r
  simp [Bool.and_comm]

/-- For the specific filter-out predicate `r.fileId ≠ fid`, liveRefs splits
as: liveRefs(s, h) = (count of fileId=fid AND chunkHash=h) + (count of fileId≠fid AND chunkHash=h).
Proved by partition: every ref either has fileId = fid or not, and within
each part we count those with chunkHash = h. -/
theorem liveRefs_split_by_fileId (s : ShardState) (fid : FileId) (h : Hash) :
    liveRefs s h =
      s.refs.countP (fun r => decide (r.fileId = fid) && decide (r.chunkHash = h)) +
      s.refs.countP (fun r => !decide (r.fileId = fid) && decide (r.chunkHash = h)) := by
  unfold liveRefs
  induction s.refs with
  | nil => simp [List.countP_nil]
  | cons r tl ih =>
    simp only [List.countP_cons]
    by_cases hfid : r.fileId = fid <;> by_cases hh : r.chunkHash = h <;>
      simp [hfid, hh] <;> omega

/-- Foldl of decrRef preserves the refs field. -/
theorem foldl_decrRef_refs (s : ShardState) (hashes : List Hash) (now : TimeMs) :
    (hashes.foldl (fun acc h => acc.decrRef h now) s).refs = s.refs := by
  induction hashes generalizing s with
  | nil => rfl
  | cons hd tl ih =>
    show (tl.foldl _ (s.decrRef hd now)).refs = s.refs
    rw [ih]
    rfl

/-- Foldl of decrRef preserves liveRefs. -/
theorem foldl_decrRef_liveRefs (s : ShardState) (hashes : List Hash) (now : TimeMs) (h : Hash) :
    liveRefs (hashes.foldl (fun acc h' => acc.decrRef h' now) s) h = liveRefs s h := by
  unfold liveRefs
  rw [foldl_decrRef_refs]

/-- Foldl of decrRef preserves chunk hashes (per-chunk). -/
theorem decrRef_chunk_hash_unchanged (c : Chunk) (h : Hash) (now : TimeMs) :
    (if c.hash = h then
        let newCount := c.refCount - 1
        let newDel := if newCount = 0 ∧ c.deletedAt.isNone then some now else c.deletedAt
        ({ c with refCount := newCount, deletedAt := newDel } : Chunk)
      else c).hash = c.hash := by
  by_cases hh : c.hash = h <;> simp [hh]

/-- Foldl of decrRef preserves UniqueBy chunks.hash. -/
theorem foldl_decrRef_preserves_uniqueChunks (s : ShardState) (hashes : List Hash) (now : TimeMs)
    (huC : UniqueBy Chunk.hash s.chunks) :
    UniqueBy Chunk.hash (hashes.foldl (fun acc h => acc.decrRef h now) s).chunks := by
  induction hashes generalizing s with
  | nil => exact huC
  | cons hd tl ih =>
    simp [List.foldl_cons]
    apply ih
    exact decrRef_preserves_uniqueChunks s hd now huC

/-- Foldl of decrRef preserves ref→chunk existence (each step preserves
hashes, so existence chain follows). -/
theorem foldl_decrRef_preserves_refChunk (s : ShardState) (hashes : List Hash) (now : TimeMs)
    (hrc : ∀ r ∈ s.refs, ∃ c ∈ s.chunks, c.hash = r.chunkHash) :
    ∀ r ∈ (hashes.foldl (fun acc h => acc.decrRef h now) s).refs,
      ∃ c ∈ (hashes.foldl (fun acc h => acc.decrRef h now) s).chunks, c.hash = r.chunkHash := by
  induction hashes generalizing s with
  | nil => exact hrc
  | cons hd tl ih =>
    simp [List.foldl_cons]
    apply ih
    intro r hr
    rw [decrRef_refs] at hr
    obtain ⟨c, hc, hch⟩ := hrc r hr
    by_cases hc_eq : c.hash = hd
    · refine ⟨{ c with
                refCount := c.refCount - 1,
                deletedAt := if c.refCount - 1 = 0 ∧ c.deletedAt.isNone then some now
                             else c.deletedAt }, ?_, ?_⟩
      · unfold ShardState.decrRef
        simp [List.mem_map]
        exact ⟨c, hc, by simp [hc_eq]⟩
      · simp; exact hch
    · refine ⟨c, ?_, hch⟩
      unfold ShardState.decrRef
      simp [List.mem_map]
      exact ⟨c, hc, by simp [hc_eq]⟩

/-- Filtering refs preserves UniqueBy refs.key. -/
theorem filter_preserves_uniqueRefs (refs : List ChunkRef) (p : ChunkRef → Bool)
    (hu : UniqueBy ChunkRef.key refs) : UniqueBy ChunkRef.key (refs.filter p) :=
  List.Pairwise.sublist List.filter_sublist hu

/-- Auxiliary induction: starting from `s` with the "refCount = liveRefs +
count of c.hash in remaining" invariant, `remaining.foldl decrRef` produces
a state satisfying the standard `validState`. -/
theorem foldl_decrRef_preserves_aux
    (s : ShardState) (remaining : List Hash) (now : TimeMs)
    (huC : UniqueBy Chunk.hash s.chunks)
    (huR : UniqueBy ChunkRef.key s.refs)
    (hrc : ∀ r ∈ s.refs, ∃ c ∈ s.chunks, c.hash = r.chunkHash)
    (hcount : ∀ c ∈ s.chunks, c.refCount = liveRefs s c.hash + remaining.count c.hash) :
    validState (remaining.foldl (fun acc h => acc.decrRef h now) s) := by
  induction remaining generalizing s with
  | nil =>
    -- remaining = []; count = 0 ⇒ refCount = liveRefs.
    refine ⟨huC, huR, hrc, ?_⟩
    intro c hc
    have := hcount c hc
    simp [List.count_nil] at this
    exact this
  | cons hd tl ih =>
    show validState (tl.foldl _ (s.decrRef hd now))
    apply ih (s.decrRef hd now)
    · exact decrRef_preserves_uniqueChunks s hd now huC
    · rw [decrRef_refs]; exact huR
    · intro r hr
      rw [decrRef_refs] at hr
      obtain ⟨c, hc, hch⟩ := hrc r hr
      by_cases hc_eq : c.hash = hd
      · refine ⟨{ c with
                refCount := c.refCount - 1,
                deletedAt := if c.refCount - 1 = 0 ∧ c.deletedAt.isNone then some now
                             else c.deletedAt }, ?_, ?_⟩
        · unfold ShardState.decrRef
          simp [List.mem_map]
          exact ⟨c, hc, by simp [hc_eq]⟩
        · simp; exact hch
      · refine ⟨c, ?_, hch⟩
        unfold ShardState.decrRef
        simp [List.mem_map]
        exact ⟨c, hc, by simp [hc_eq]⟩
    · -- Maintain the count invariant: each chunk in (decrRef hd) state has
      -- refCount equal to liveRefs (unchanged) + count of c.hash in tl.
      intro c hc
      unfold ShardState.decrRef at hc
      simp [List.mem_map] at hc
      obtain ⟨c0, hc0, heq⟩ := hc
      have hlive : liveRefs (s.decrRef hd now) c.hash = liveRefs s c.hash := by
        rw [liveRefs_decrRef]
      rw [hlive]
      by_cases hc0_eq : c0.hash = hd
      · -- c is the bumped version of c0; c.hash = c0.hash = hd
        simp [hc0_eq] at heq
        subst heq
        have h0 := hcount c0 hc0
        rw [List.count_cons] at h0
        simp [hc0_eq] at h0
        -- After simp, h0 : c0.refCount = liveRefs s hd + (tl.count hd + 1)
        -- Goal: (record).refCount = liveRefs s (record).hash + tl.count (record).hash
        -- where the record has hash = hd. Reduce (record).hash := hd:
        show c0.refCount - 1 = liveRefs s hd + tl.count hd
        omega
      · -- c = c0 (unchanged)
        simp [hc0_eq] at heq
        subst heq
        have h0 := hcount c0 hc0
        rw [List.count_cons] at h0
        -- h0 : c0.refCount = liveRefs s c0.hash + (tl.count c0.hash + (if hd = c0.hash then 1 else 0))
        -- We need: ¬ (hd = c0.hash). hc0_eq says ¬ (c0.hash = hd), so by symmetry.
        have hne : hd ≠ c0.hash := fun heq => hc0_eq heq.symm
        simp [hne] at h0
        exact h0

/-- The crucial counting invariant for deleteChunks: after dropping all
refs with fileId = fid and decrementing each touched chunk's refCount, the
new refCount equals the new liveRefs cardinality. -/
theorem deleteChunks_preserves_invariant
    (s : ShardState) (fid : FileId) (now : TimeMs)
    (hv : validState s) : validState (step s (.deleteChunks fid now)) := by
  obtain ⟨huC, huR, hrc, hrcEq⟩ := hv
  unfold step
  simp only [ShardState.dropRefsForFile]
  -- Let `s'` be the state after filtering refs.
  set sFiltered : ShardState :=
    { s with refs := s.refs.filter (fun r => r.fileId ≠ fid) } with hsf_def
  -- Let `dropped` be the list of dropped hashes.
  set dropped : List Hash := (s.refs.filter (·.fileId = fid)).map ChunkRef.chunkHash with hdrop_def
  -- The result of step is `dropped.foldl (fun acc h => acc.decrRef h now) sFiltered`.
  -- We use a strong-enough auxiliary that captures the exact relationship.
  -- The induction is on `dropped`, but we must generalize over the
  -- "remaining-to-drop multiset" so the count equation tracks correctly.
  -- We define:
  --   inv s remaining := validState s except refCountEq is replaced by
  --     ∀ c ∈ s.chunks, c.refCount = liveRefs s c.hash + (count of c.hash in remaining)
  -- Initially (sFiltered, dropped): remaining = dropped; refCount equals
  --   liveRefs s c.hash = liveRefs sFiltered c.hash + countP (... ) (the dropped portion)
  -- Finally (after all dropped): remaining = []; refCount = liveRefs s' c.hash.
  -- Each decrRef step: pops one h from remaining, decrements one chunk's
  -- refCount, leaving the equation balanced.
  --
  -- This is the structural heart of the proof. We package the auxiliary
  -- as a separate inductive predicate.
  apply foldl_decrRef_preserves_aux sFiltered dropped now
  · -- chunk uniqueness preserved by filter (refs only changed)
    exact huC
  · -- ref uniqueness preserved by filter
    exact filter_preserves_uniqueRefs s.refs (fun r => r.fileId ≠ fid) huR
  · -- ref→chunk existence preserved by filter
    intro r hr
    have hr_orig : r ∈ s.refs := by
      rw [List.mem_filter] at hr; exact hr.1
    exact hrc r hr_orig
  · -- crucial: c.refCount = liveRefs sFiltered c.hash + (count of c.hash in dropped)
    intro c hc
    -- c ∈ sFiltered.chunks = s.chunks (chunks unchanged by filter on refs)
    have hc_orig : c ∈ s.chunks := hc
    -- Old equation: c.refCount = liveRefs s c.hash
    rw [hrcEq c hc_orig]
    -- Goal: liveRefs s c.hash = liveRefs sFiltered c.hash + dropped.count(c.hash)
    show liveRefs s c.hash = liveRefs sFiltered c.hash + dropped.count c.hash
    -- liveRefs s c.hash = countP (chunkHash=c.hash) s.refs
    -- liveRefs sFiltered c.hash = countP (chunkHash=c.hash) (s.refs.filter fileId≠fid)
    --                          = countP (chunkHash=c.hash ∧ fileId≠fid) s.refs
    -- dropped.count c.hash = number of refs with fileId=fid AND chunkHash=c.hash
    --                      = countP (chunkHash=c.hash ∧ fileId=fid) s.refs
    -- (proved via the map+filter relationship)
    have h_filtered : liveRefs sFiltered c.hash =
        s.refs.countP (fun r => !decide (r.fileId = fid) && decide (r.chunkHash = c.hash)) := by
      rw [hsf_def]
      rw [liveRefs_filter]
      apply List.countP_congr
      intro r _
      by_cases h1 : r.fileId = fid <;> by_cases h2 : r.chunkHash = c.hash <;> simp [h1, h2]
    have h_dropped : dropped.count c.hash =
        s.refs.countP (fun r => decide (r.fileId = fid) && decide (r.chunkHash = c.hash)) := by
      rw [hdrop_def]
      rw [List.count_eq_countP, List.countP_map]
      rw [List.countP_filter]
      apply List.countP_congr
      intro r _
      simp [Function.comp]
      by_cases h1 : r.fileId = fid <;> by_cases h2 : r.chunkHash = c.hash <;> simp [h1, h2]
    rw [h_filtered, h_dropped]
    rw [liveRefs_split_by_fileId s fid c.hash]
    omega

-- ─── Master theorem ─────────────────────────────────────────────────────

/-- I1's main theorem: every operation preserves `validState`. -/
theorem step_preserves_validState (s : ShardState) (op : Op)
    (hv : validState s) : validState (step s op) := by
  match op with
  | .putChunk h size fid idx => exact putChunk_preserves_invariant s h size fid idx hv
  | .deleteChunks fid now => exact deleteChunks_preserves_invariant s fid now hv

/-- Generalized reachability. -/
theorem reachable_validState_gen (s : ShardState) (ops : List Op)
    (hv : validState s) :
    validState (ops.foldl step s) := by
  induction ops generalizing s with
  | nil => exact hv
  | cons op tl ih =>
    simp [List.foldl_cons]
    exact ih (step s op) (step_preserves_validState s op hv)

theorem reachable_validState (ops : List Op) :
    validState (ops.foldl step ShardState.empty) :=
  reachable_validState_gen ShardState.empty ops validState_empty

-- ─── Numerical corollary used by I5 (GC) ────────────────────────────────

/-- Critical corollary used by GC: a chunk with refCount = 0 has zero
live refs to its hash. This is what makes GC sweep safe. -/
theorem refCount_zero_implies_no_refs (s : ShardState) (c : Chunk)
    (hv : validState s) (hc : c ∈ s.chunks) (hzero : c.refCount = 0) :
    ∀ r ∈ s.refs, r.chunkHash ≠ c.hash := by
  intro r hr habs
  have heq := hv.refCountEq c hc
  rw [hzero] at heq
  -- 0 = liveRefs s c.hash, but r ∈ refs with chunkHash = c.hash ⇒ liveRefs > 0.
  have hpos : liveRefs s c.hash > 0 := by
    unfold liveRefs
    rw [List.countP_eq_length_filter]
    have hp : decide (r.chunkHash = c.hash) = true := by simp [habs]
    have hmem : r ∈ s.refs.filter (·.chunkHash = c.hash) := by
      rw [List.mem_filter]
      exact ⟨hr, hp⟩
    exact List.length_pos_of_mem hmem
  omega

-- ─── Non-vacuity sanity checks ──────────────────────────────────────────

theorem putChunk_changes_state (h : Hash) (size : Nat) (fid : FileId) (idx : Nat) :
    step ShardState.empty (.putChunk h size fid idx) ≠ ShardState.empty := by
  intro hcontra
  unfold step at hcontra
  simp [ShardState.empty, ShardState.appendRef, ShardState.coldInsert,
        ShardState.findChunk] at hcontra

theorem deleteChunks_changes_state :
    step (⟨[⟨"abc", 1, 1, none⟩], [⟨"abc", "f1", 0⟩]⟩ : ShardState) (.deleteChunks "f1" 100)
      ≠ (⟨[⟨"abc", 1, 1, none⟩], [⟨"abc", "f1", 0⟩]⟩ : ShardState) := by
  intro hcontra
  unfold step at hcontra
  simp [ShardState.dropRefsForFile, ShardState.decrRef] at hcontra

theorem witness_putChunk_valid :
    validState (step ShardState.empty (.putChunk "abc" 100 "f1" 0)) :=
  step_preserves_validState _ _ validState_empty

theorem witness_putChunk_deleteChunks_valid :
    validState (step
      (step ShardState.empty (.putChunk "abc" 100 "f1" 0))
      (.deleteChunks "f1" 100)) := by
  apply step_preserves_validState
  exact step_preserves_validState _ _ validState_empty

end Mossaic.Vfs.Refcount
