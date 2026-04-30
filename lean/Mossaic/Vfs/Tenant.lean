/-
Mossaic.Vfs.Tenant — I3: Tenant isolation.

Models:
  worker/core/lib/utils.ts (118 LoC)
    — validateVfsToken (character class enforcement, lines 47-56)
    — vfsUserDOName (canonical UserDO name builder, lines 73-85)
    — vfsShardDOName (canonical ShardDO name builder, lines 103-117)
  shared/vfs-types.ts            (VFSScope)
  tests/integration/tenant-isolation.test.ts (`:` rejection test)

Audit reference: /workspace/local/audit-report.md §I3 (verdict: Pass).

What we prove:
  - The token charset `[A-Za-z0-9._-]` excludes `:` (`isTokenChar_not_colon`).
  - `vfsUserDOName` is injective on `validScope` inputs (`userName_inj`).
  - `vfsShardDOName` at a *fixed* shard index is injective on `validScope`
    inputs (`shardName_inj_fixed_idx`).
  - Distinct tenants at any same shard index produce distinct ShardDO
    names (`cross_tenant_isolation`).
  - Distinct tenants produce distinct UserDO names
    (`cross_tenant_user_isolation`).
  - Non-vacuity: there exist distinct valid scopes that DO produce
    distinct names (`cross_tenant_isolation_nonvacuous`,
    `shardName_nonconst`).

What we do NOT prove (intentionally out of scope):
  - Full `(scope, idx) ↔ name` bijection. The shard-index suffix involves
    `Nat.toString`, whose injectivity has no stdlib lemma in Lean 4.29 (no
    Mathlib by design). Tenant isolation does not require it: we hold idx
    fixed across the comparison, and the `:s${idx}` suffix string-cancels
    on both sides.
  - JWT signing/verifying — that's a cryptographic property at the runtime
    layer, axiomatized.
  - Cloudflare's DO routing actually honoring the name string. That is a
    Workers-runtime guarantee.
-/

namespace Mossaic.Vfs.Tenant

/-- VFS scope — mirrors `VFSScope` in shared/vfs-types.ts:10-17. -/
structure VFSScope where
  ns     : String
  tenant : String
  sub    : Option String
  deriving DecidableEq, Repr

/-- Per worker/core/lib/utils.ts:47-56, a scope token must be a non-empty
ASCII string of length ≤ 128 drawn from `[A-Za-z0-9._-]`. The crucial
exclusion for cross-tenant isolation is the colon `:`; everything else
is just hygiene. -/
def isTokenChar (c : Char) : Bool :=
  c.isAlphanum || c == '.' || c == '_' || c == '-'

theorem isTokenChar_not_colon (c : Char) (h : isTokenChar c = true) : c ≠ ':' := by
  intro hc
  rw [hc] at h
  simp [isTokenChar] at h

/-- Decidable predicate: every char of `s` passes `isTokenChar`. -/
def allTokenChars (s : String) : Bool :=
  s.toList.all isTokenChar

/-- A valid scope token: 1-128 chars, every char in the token charset. -/
def validToken (s : String) : Prop :=
  1 ≤ s.length ∧ s.length ≤ 128 ∧ allTokenChars s = true

/-- A valid scope: every component is a valid token (sub is optional). -/
def validScope (sc : VFSScope) : Prop :=
  validToken sc.ns ∧ validToken sc.tenant ∧
  (match sc.sub with
   | none => True
   | some s => validToken s)

/-- Helper: a valid token never contains `:`. -/
theorem validToken_no_colon (s : String) (h : validToken s) :
    ∀ c ∈ s.toList, c ≠ ':' := by
  intro c hc
  have hall : allTokenChars s = true := h.2.2
  unfold allTokenChars at hall
  rw [List.all_eq_true] at hall
  exact isTokenChar_not_colon c (hall c hc)

/-- ":" toList is [':']. -/
theorem colon_toList : (":" : String).toList = [':'] := rfl

-- ─── String append cancellation ─────────────────────────────────────────

/-- Left-cancellation: `a ++ s₁ = a ++ s₂ → s₁ = s₂`. -/
theorem string_append_left_inj (a s₁ s₂ : String) (h : a ++ s₁ = a ++ s₂) : s₁ = s₂ := by
  have hl : (a ++ s₁).toList = (a ++ s₂).toList := by rw [h]
  simp only [String.toList_append] at hl
  exact String.toList_inj.mp (List.append_cancel_left hl)

/-- Right-cancellation: `s₁ ++ a = s₂ ++ a → s₁ = s₂`. -/
theorem string_append_right_inj (a s₁ s₂ : String) (h : s₁ ++ a = s₂ ++ a) : s₁ = s₂ := by
  have hl : (s₁ ++ a).toList = (s₂ ++ a).toList := by rw [h]
  simp only [String.toList_append] at hl
  exact String.toList_inj.mp (List.append_cancel_right hl)

-- ─── List-level injectivity ─────────────────────────────────────────────

/-- If two char-lists with no internal `:` are equal after appending `:`+suffix,
the prefixes and suffixes are equal pairwise. Proved by structural induction
on the first list. This is the load-bearing lemma for DO-name injectivity. -/
theorem list_split_at_colon :
    ∀ (la lb lx ly : List Char),
      (∀ c ∈ la, c ≠ ':') → (∀ c ∈ lb, c ≠ ':') →
      la ++ ':' :: lx = lb ++ ':' :: ly →
      la = lb ∧ lx = ly := by
  intro la
  induction la with
  | nil =>
    intro lb lx ly _ hb hh
    cases lb with
    | nil =>
      simp at hh
      exact ⟨rfl, hh⟩
    | cons hd tl =>
      simp at hh
      have : hd = ':' := hh.1.symm
      exact absurd this (hb hd List.mem_cons_self)
  | cons ha_hd ha_tl ih =>
    intro lb lx ly ha hb hh
    cases lb with
    | nil =>
      simp at hh
      have : ha_hd = ':' := hh.1
      exact absurd this (ha ha_hd List.mem_cons_self)
    | cons hb_hd hb_tl =>
      simp at hh
      obtain ⟨hhd, htl⟩ := hh
      have ha_tl_no : ∀ c ∈ ha_tl, c ≠ ':' :=
        fun c hc => ha c (List.mem_cons_of_mem _ hc)
      have hb_tl_no : ∀ c ∈ hb_tl, c ≠ ':' :=
        fun c hc => hb c (List.mem_cons_of_mem _ hc)
      have ⟨heq_tl, heq_x⟩ := ih hb_tl lx ly ha_tl_no hb_tl_no htl
      exact ⟨by rw [hhd, heq_tl], heq_x⟩

/-- Lift `list_split_at_colon` to strings. -/
theorem string_split_at_colon (a b x y : String)
    (ha : ∀ c ∈ a.toList, c ≠ ':') (hb : ∀ c ∈ b.toList, c ≠ ':')
    (h : a ++ ":" ++ x = b ++ ":" ++ y) : a = b ∧ x = y := by
  have hl : (a ++ ":" ++ x).toList = (b ++ ":" ++ y).toList := by rw [h]
  simp only [String.toList_append, colon_toList] at hl
  -- a.toList ++ [':'] ++ x.toList = b.toList ++ [':'] ++ y.toList
  -- Reassociate to a.toList ++ ':' :: x.toList = b.toList ++ ':' :: y.toList.
  have hl' : a.toList ++ ':' :: x.toList = b.toList ++ ':' :: y.toList := by
    have := hl; simp [List.append_assoc, List.cons_append] at this; exact this
  obtain ⟨h1, h2⟩ := list_split_at_colon a.toList b.toList x.toList y.toList ha hb hl'
  exact ⟨String.toList_inj.mp h1, String.toList_inj.mp h2⟩

-- ─── DO name builders ───────────────────────────────────────────────────

/-- UserDO instance name. Mirrors `vfsUserDOName` in
worker/core/lib/utils.ts:73-85: `vfs:{ns}:{tenant}` or `vfs:{ns}:{tenant}:{sub}`. -/
def userName (sc : VFSScope) : String :=
  match sc.sub with
  | none   => "vfs:" ++ sc.ns ++ ":" ++ sc.tenant
  | some s => "vfs:" ++ sc.ns ++ ":" ++ sc.tenant ++ ":" ++ s

/-- ShardDO instance name. Mirrors `vfsShardDOName` in
worker/core/lib/utils.ts:103-117: `userName(sc) + ":s" + idx`. -/
def shardName (sc : VFSScope) (idx : Nat) : String :=
  userName sc ++ ":s" ++ toString idx

-- ─── Helpers about literal prefixes ─────────────────────────────────────

private theorem vfs_colon_toList : ("vfs:" : String).toList = ['v','f','s',':'] := by decide

-- ─── userName injectivity ───────────────────────────────────────────────

/-- UserDO names are injective on valid scopes. -/
theorem userName_inj (sc₁ sc₂ : VFSScope)
    (hv₁ : validScope sc₁) (hv₂ : validScope sc₂)
    (h : userName sc₁ = userName sc₂) : sc₁ = sc₂ := by
  unfold userName at h
  have hns₁ := validToken_no_colon _ hv₁.1
  have hns₂ := validToken_no_colon _ hv₂.1
  have ht₁  := validToken_no_colon _ hv₁.2.1
  have ht₂  := validToken_no_colon _ hv₂.2.1
  match h_sub₁ : sc₁.sub, h_sub₂ : sc₂.sub with
  | none, none =>
    rw [h_sub₁, h_sub₂] at h
    simp only at h
    -- h : "vfs:" ++ ns₁ ++ ":" ++ tenant₁ = "vfs:" ++ ns₂ ++ ":" ++ tenant₂
    have hl : ("vfs:" ++ sc₁.ns ++ ":" ++ sc₁.tenant).toList =
              ("vfs:" ++ sc₂.ns ++ ":" ++ sc₂.tenant).toList := by rw [h]
    simp only [String.toList_append, vfs_colon_toList, colon_toList] at hl
    -- Strip "vfs:" prefix.
    have hl' : sc₁.ns.toList ++ ':' :: sc₁.tenant.toList =
               sc₂.ns.toList ++ ':' :: sc₂.tenant.toList := by
      have := hl; simp [List.append_assoc, List.cons_append] at this; exact this
    obtain ⟨hns_eq, ht_eq⟩ :=
      list_split_at_colon sc₁.ns.toList sc₂.ns.toList _ _ hns₁ hns₂ hl'
    have heq_ns : sc₁.ns = sc₂.ns := String.toList_inj.mp hns_eq
    have heq_tn : sc₁.tenant = sc₂.tenant := String.toList_inj.mp ht_eq
    obtain ⟨ns₁, tn₁, sub₁⟩ := sc₁
    obtain ⟨ns₂, tn₂, sub₂⟩ := sc₂
    simp_all
  | none, some s₂ =>
    rw [h_sub₁, h_sub₂] at h
    simp only at h
    exfalso
    -- h : "vfs:" ++ ns₁ ++ ":" ++ tenant₁ = "vfs:" ++ ns₂ ++ ":" ++ tenant₂ ++ ":" ++ s₂
    have hl : ("vfs:" ++ sc₁.ns ++ ":" ++ sc₁.tenant).toList =
              ("vfs:" ++ sc₂.ns ++ ":" ++ sc₂.tenant ++ ":" ++ s₂).toList := by rw [h]
    simp only [String.toList_append, vfs_colon_toList, colon_toList] at hl
    have hl' : sc₁.ns.toList ++ ':' :: sc₁.tenant.toList =
               sc₂.ns.toList ++ ':' :: (sc₂.tenant.toList ++ ':' :: s₂.toList) := by
      have := hl; simp [List.append_assoc, List.cons_append] at this; exact this
    obtain ⟨_, ht_post⟩ :=
      list_split_at_colon sc₁.ns.toList sc₂.ns.toList _ _ hns₁ hns₂ hl'
    -- ht_post : sc₁.tenant.toList = sc₂.tenant.toList ++ ':' :: s₂.toList
    have : ':' ∈ sc₁.tenant.toList := by rw [ht_post]; simp
    exact ht₁ ':' this rfl
  | some s₁, none =>
    rw [h_sub₁, h_sub₂] at h
    simp only at h
    exfalso
    have hl : ("vfs:" ++ sc₁.ns ++ ":" ++ sc₁.tenant ++ ":" ++ s₁).toList =
              ("vfs:" ++ sc₂.ns ++ ":" ++ sc₂.tenant).toList := by rw [h]
    simp only [String.toList_append, vfs_colon_toList, colon_toList] at hl
    have hl' : sc₁.ns.toList ++ ':' :: (sc₁.tenant.toList ++ ':' :: s₁.toList) =
               sc₂.ns.toList ++ ':' :: sc₂.tenant.toList := by
      have := hl; simp [List.append_assoc, List.cons_append] at this; exact this
    obtain ⟨_, ht_post⟩ :=
      list_split_at_colon sc₁.ns.toList sc₂.ns.toList _ _ hns₁ hns₂ hl'
    -- ht_post : sc₁.tenant.toList ++ ':' :: s₁.toList = sc₂.tenant.toList
    have : ':' ∈ sc₂.tenant.toList := by rw [← ht_post]; simp
    exact ht₂ ':' this rfl
  | some s₁, some s₂ =>
    rw [h_sub₁, h_sub₂] at h
    simp only at h
    have hl : ("vfs:" ++ sc₁.ns ++ ":" ++ sc₁.tenant ++ ":" ++ s₁).toList =
              ("vfs:" ++ sc₂.ns ++ ":" ++ sc₂.tenant ++ ":" ++ s₂).toList := by rw [h]
    simp only [String.toList_append, vfs_colon_toList, colon_toList] at hl
    have hl' :
        sc₁.ns.toList ++ ':' :: (sc₁.tenant.toList ++ ':' :: s₁.toList) =
        sc₂.ns.toList ++ ':' :: (sc₂.tenant.toList ++ ':' :: s₂.toList) := by
      have := hl; simp [List.append_assoc, List.cons_append] at this; exact this
    obtain ⟨hns_eq, hrest⟩ :=
      list_split_at_colon sc₁.ns.toList sc₂.ns.toList _ _ hns₁ hns₂ hl'
    obtain ⟨ht_eq, hs_eq⟩ :=
      list_split_at_colon sc₁.tenant.toList sc₂.tenant.toList _ _ ht₁ ht₂ hrest
    have heq_ns : sc₁.ns = sc₂.ns := String.toList_inj.mp hns_eq
    have heq_tn : sc₁.tenant = sc₂.tenant := String.toList_inj.mp ht_eq
    have heq_s  : s₁ = s₂ := String.toList_inj.mp hs_eq
    obtain ⟨ns₁, tn₁, sub₁⟩ := sc₁
    obtain ⟨ns₂, tn₂, sub₂⟩ := sc₂
    simp_all

-- ─── shardName injectivity (fixed idx) ──────────────────────────────────

/-- ShardDO names are injective on valid scopes when the shard index is held
fixed. The proof reduces to `userName_inj` via right-cancellation of the
common `":s${idx}"` suffix. -/
theorem shardName_inj_fixed_idx (sc₁ sc₂ : VFSScope) (idx : Nat)
    (hv₁ : validScope sc₁) (hv₂ : validScope sc₂)
    (h : shardName sc₁ idx = shardName sc₂ idx) : sc₁ = sc₂ := by
  unfold shardName at h
  -- h : userName sc₁ ++ ":s" ++ toString idx = userName sc₂ ++ ":s" ++ toString idx
  -- Cancel the common suffix step by step.
  have h1 : userName sc₁ ++ ":s" = userName sc₂ ++ ":s" :=
    string_append_right_inj _ _ _ h
  have h2 : userName sc₁ = userName sc₂ :=
    string_append_right_inj _ _ _ h1
  exact userName_inj sc₁ sc₂ hv₁ hv₂ h2

-- ─── Cross-tenant isolation ─────────────────────────────────────────────

/-- Cross-tenant isolation: distinct tenants ⇒ distinct ShardDO names at
any same shard index. This is the main consumer corollary. -/
theorem cross_tenant_isolation (sc₁ sc₂ : VFSScope) (idx : Nat)
    (hv₁ : validScope sc₁) (hv₂ : validScope sc₂)
    (htenant : sc₁.tenant ≠ sc₂.tenant) :
    shardName sc₁ idx ≠ shardName sc₂ idx := by
  intro h
  have hsc := shardName_inj_fixed_idx sc₁ sc₂ idx hv₁ hv₂ h
  exact htenant (by rw [hsc])

/-- Same for UserDO names. -/
theorem cross_tenant_user_isolation (sc₁ sc₂ : VFSScope)
    (hv₁ : validScope sc₁) (hv₂ : validScope sc₂)
    (htenant : sc₁.tenant ≠ sc₂.tenant) :
    userName sc₁ ≠ userName sc₂ := by
  intro h
  have hsc := userName_inj sc₁ sc₂ hv₁ hv₂ h
  exact htenant (by rw [hsc])

-- ─── Non-vacuity sanity checks ──────────────────────────────────────────

/-- Concrete witness: two distinct tenants produce distinct shard names. -/
theorem cross_tenant_isolation_nonvacuous :
    ∃ sc₁ sc₂ : VFSScope, ∃ idx : Nat,
      validScope sc₁ ∧ validScope sc₂ ∧
      sc₁.tenant ≠ sc₂.tenant ∧
      shardName sc₁ idx ≠ shardName sc₂ idx := by
  refine ⟨⟨"default", "alice", none⟩, ⟨"default", "bob", none⟩, 0, ?_, ?_, ?_, ?_⟩
  · refine ⟨?_, ?_, ?_⟩ <;> first | (simp [validToken, allTokenChars, isTokenChar]; decide) | trivial | decide
  · refine ⟨?_, ?_, ?_⟩ <;> first | (simp [validToken, allTokenChars, isTokenChar]; decide) | trivial | decide
  · decide
  · simp [shardName, userName]

/-- Sanity: shardName depends on its scope argument — it is not a constant
function. This rules out trivial proofs of `shardName_inj_fixed_idx`. -/
theorem shardName_nonconst :
    ∃ sc₁ sc₂ : VFSScope, ∃ idx : Nat, shardName sc₁ idx ≠ shardName sc₂ idx := by
  refine ⟨⟨"default", "alice", none⟩, ⟨"default", "bob", none⟩, 0, ?_⟩
  simp [shardName, userName]

/-- Sanity: userName depends on its argument. -/
theorem userName_nonconst :
    ∃ sc₁ sc₂ : VFSScope, userName sc₁ ≠ userName sc₂ := by
  refine ⟨⟨"default", "alice", none⟩, ⟨"default", "bob", none⟩, ?_⟩
  simp [userName]

end Mossaic.Vfs.Tenant
