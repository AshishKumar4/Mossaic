/-
Mossaic.Vfs.Folder — folder-surface cache invariants (Phase 2).

Phase 2 extends `caches.default`-backed responses to folder-shape
operations (`readdir` / `listChildren` / `listFiles` / `stat` /
`fileInfo` / `readManyStat`). Where the file surface buses on a
5-tuple `(fileId, headVersionId, updatedAt, encryptionMode,
encryptionKeyId)` proven in `Mossaic.Vfs.Cache`, the folder surface
busts on a single monotonic counter: `folders.revision` per parent
folder (or `root_folder_revision.revision` for the synthetic root).

Models:
  worker/core/objects/user/vfs/helpers.ts:536-589
    (bumpFolderRevision / readFolderRevision)
  worker/core/objects/user/vfs/metadata.ts
    (gap closures FR8..FR14 — chmod / patchMetadata / addTags /
     removeTags / setYjsMode / encryption-stamp / markVersion)

What we prove:

  (L1) folder_revision_monotonic — every `bump` strictly increases
       the counter; no rollback path exists in `BumpOp`.

  (L2) covered_mutation_bumps_revision — the model maps every listed
       mutation constructor to a parent revision bump and proves that
       the mapped transition increments the revision.

  (L3) covered_mutation_changes_cache_identity — every modeled mutation
       changes the structured `(tenant, parentId, revision)` identity.

  (L4) tenant_isolation — distinct tenants produce distinct cache
       keys at the same `(parentId, revision)` (witness via decide).

  (L5) cross_folder_rename_bumps_both — a rename across folder
       boundaries bumps BOTH src and dst parent counters.

  (L6) attribute_mutation_bumps_parent — chmod / patchMetadata /
       setYjsMode / addTags / removeTags / markVersion /
       encryptionStamp all bump the file's parent revision.

NO `axiom`. NO `sorry`. Mathlib v4.29.0.
-/

import Mossaic.Vfs.Common

namespace Mossaic.Vfs.Folder

open Mossaic.Vfs.Common

-- ─── Types ──────────────────────────────────────────────────────────────

/-- Per-tenant per-folder revision counter. The pair `(tenant,
parentId)` is the key; `revision : Nat` is the value. The root folder
uses a synthetic `parentId := none`. -/
structure FolderRevState where
  tenant   : String
  parentId : Option FileId
  revision : Nat
  deriving DecidableEq, Repr

/-- The subset of mutation classes that affect a folder's listing
response bytes. Mirrors the audit at `local/mossaic-cache-phase2-plan.md`.

Each constructor names a real call site in the Mossaic worker. The
`bumpsParent` predicate below witnesses that each is closed by
`bumpFolderRevision`. -/
inductive FolderMutation where
  /-- chmod on a file or folder. `metadata.ts:34-66` (FR8 / FR14). -/
  | chmod
  /-- patchMetadata. `metadata.ts:90-129` (FR9). -/
  | patchMetadata
  /-- patchMetadata.addTags. `metadata-tags.ts:103-120` (FR10). -/
  | addTags
  /-- patchMetadata.removeTags. `metadata-tags.ts:123-145` (FR11). -/
  | removeTags
  /-- markVersion(label / userVisible). `vfs-versions.ts:669-711` (FR12). -/
  | markVersion
  /-- setYjsMode promotion. `metadata.ts:145-183` (FR13). -/
  | setYjsMode
  /-- stampFileEncryption. `encryption-stamp.ts:180-194` (G7). -/
  | encryptionStamp
  /-- File / folder create or unlink. Already pinned by !661 baseline. -/
  | createOrUnlink
  /-- Cross-folder rename. Bumps BOTH parents (L5). -/
  | renameCrossFolder
  deriving DecidableEq, Repr

/-- `affectsListing m` is `true` for every mutation kind that changes
a parent folder's listing-shape response — i.e. needs a bust.

For Phase 2 every constructor of `FolderMutation` was selected
precisely because it affects listing; the predicate is trivially
true. The structural placeholder keeps L2 readable as
"if a mutation affects listing, it busts the parent." -/
def FolderMutation.affectsListing : FolderMutation → Bool
  | _ => true

/-- Apply a bump to a folder's revision counter. Mirrors
`bumpFolderRevision` at `helpers.ts:536-558`. -/
def FolderRevState.bump (s : FolderRevState) : FolderRevState :=
  { s with revision := s.revision + 1 }

/-- Explicit mutation-to-transition mapping for the covered mutation
classes. This is an abstract audit model, not an extraction from TypeScript. -/
def FolderMutation.apply (m : FolderMutation) (s : FolderRevState) : FolderRevState :=
  match m with
  | .chmod => s.bump
  | .patchMetadata => s.bump
  | .addTags => s.bump
  | .removeTags => s.bump
  | .markVersion => s.bump
  | .setYjsMode => s.bump
  | .encryptionStamp => s.bump
  | .createOrUnlink => s.bump
  | .renameCrossFolder => s.bump

-- ─── (L1) folder_revision_monotonic ────────────────────────────────────

/--
**(L1) folder_revision_monotonic.**
Every bump strictly increases the revision counter. The SQL UPDATE
at `helpers.ts:552-557` is `revision = revision + 1`; no rollback
path exists in the bump primitive.

Operationally: a cached folder-surface response keyed on revision N
will never collide with a fresh response keyed on revision M where
M > N, and any mutation moves the counter strictly forward.
-/
theorem folder_revision_monotonic (s : FolderRevState) :
    s.bump.revision > s.revision := by
  unfold FolderRevState.bump
  exact Nat.lt_succ_self s.revision

-- ─── (L2) covered_mutation_bumps_revision ──────────────────────────────

/--
**(L2) covered_mutation_bumps_revision.**
Every constructor in the model is explicitly mapped to a transition that
increments the parent revision and changes the state. This proves the
mapping inside this model; it does not prove that every TypeScript call
site invokes the corresponding transition.
-/
theorem covered_mutation_bumps_revision
    (s : FolderRevState) (m : FolderMutation) :
    (m.apply s).revision = s.revision + 1 ∧ m.apply s ≠ s := by
  cases m <;> refine ⟨rfl, ?_⟩ <;> intro h <;>
    have h_revision := congrArg FolderRevState.revision h <;>
    simp [FolderMutation.apply, FolderRevState.bump] at h_revision

-- ─── (L3) structured cache identity + concrete rendering witnesses ─────

/-- Structured identity consumed by the abstract cache model. Keeping the
components separate avoids claiming that arbitrary strings are injectively
encoded by the illustrative URL renderer below. -/
structure FolderCacheIdentity where
  tenant   : String
  parentId : Option FileId
  revision : Nat
  deriving DecidableEq, Repr

def FolderRevState.cacheIdentity (s : FolderRevState) : FolderCacheIdentity :=
  ⟨s.tenant, s.parentId, s.revision⟩

/-- Every covered mutation changes the structured cache identity because it
changes the revision component. -/
theorem covered_mutation_changes_cache_identity
    (s : FolderRevState) (m : FolderMutation) :
    (m.apply s).cacheIdentity ≠ s.cacheIdentity := by
  cases m <;> simp [FolderMutation.apply, FolderRevState.bump,
    FolderRevState.cacheIdentity]

/-- Illustrative rendering of the structured identity into a URL-shaped
string. The Lean corpus does not model or refine the external consumer's
actual key encoder. -/
def folderCacheKey (tenant : String) (parentId : Option FileId)
    (revision : Nat) : String :=
  let pid := match parentId with
    | some id => id
    | none    => "__root__"
  "https://seal-folder.mossaic.local/" ++ tenant ++ "/"
    ++ pid ++ "/" ++ toString revision

/--
Concrete renderer witness for one canonical tenant/parent shape. It is not
a general injectivity theorem for arbitrary unescaped string components.
-/
theorem bump_yields_different_cache_key_witness :
    folderCacheKey "alice" (some "f-1") 0 ≠
    folderCacheKey "alice" (some "f-1") 1 := by
  decide

-- ─── (L4) tenant_isolation (via concrete witnesses, see below) ────────
--
-- (L4) is proven by the witness theorems `witness_tenant_isolation`
-- and `witness_distinct_tenant_revision` below. A fully-general
-- string-level theorem would require Mathlib's `String.append_left_cancel`
-- which is not part of the Common.lean prelude; the concrete witness
-- pattern matches `Cache.lean`'s `witness_namespace_isolation`.

-- ─── (L5) cross_folder_rename_bumps_both ───────────────────────────────

/-- Apply a cross-folder rename: both src and dst parent revisions bump. -/
def renameCrossFolder (src dst : FolderRevState) :
    FolderRevState × FolderRevState :=
  (src.bump, dst.bump)

/--
**(L5) cross_folder_rename_bumps_both.**
A cross-folder rename bumps BOTH the source's parent revision and
the destination's parent revision. Mirrors `mutations.ts:700-702`.

Without this both folders' listings could go stale: src has a
disappearing entry, dst a new one — each needs invalidation.
-/
theorem cross_folder_rename_bumps_both (src dst : FolderRevState) :
    (renameCrossFolder src dst).1.revision > src.revision ∧
    (renameCrossFolder src dst).2.revision > dst.revision := by
  refine ⟨?_, ?_⟩
  · unfold renameCrossFolder
    exact folder_revision_monotonic src
  · unfold renameCrossFolder
    exact folder_revision_monotonic dst

-- ─── (L6) attribute_mutation_bumps_parent (gap closure) ────────────────

/-- Witness that every attribute mutation in `FolderMutation` bumps
the file's parent revision. The predicate is the closure of FR8..FR14
plus G7. -/
def bumpsParent : FolderMutation → Bool
  | .chmod              => true
  | .patchMetadata      => true
  | .addTags            => true
  | .removeTags         => true
  | .markVersion        => true
  | .setYjsMode         => true
  | .encryptionStamp    => true
  | .createOrUnlink     => true
  | .renameCrossFolder  => true

/--
**(L6) attribute_mutation_bumps_parent.**
Every kind in `FolderMutation` is closed by a `bumpFolderRevision`
call at its named file:line. The predicate `bumpsParent` is `true`
on every constructor — the proof is `decide` over the finite
enumeration.

The TypeScript proof is the per-constructor file:line; the Lean
witness is that we declared all of them.
-/
theorem attribute_mutation_bumps_parent (m : FolderMutation) :
    bumpsParent m = true := by
  cases m <;> rfl

-- ─── Concrete witnesses (decide) ───────────────────────────────────────

/--
**(L4-witness) tenant_isolation.**
The concrete tenant strings `alice` and `bob` produce distinct rendered keys
for this one root/revision shape. This is not a general renderer-injectivity or
implementation-isolation theorem.
-/
theorem witness_tenant_isolation :
    folderCacheKey "alice" none 1 ≠ folderCacheKey "bob" none 1 := by
  decide

/--
**(witness) Distinct revisions → distinct keys.**
After a bump, the cache key for the parent folder is different.
This is the load-bearing claim behind the bust-completeness chain.
-/
theorem witness_distinct_revision :
    folderCacheKey "alice" (some "f-parent") 7 ≠
    folderCacheKey "alice" (some "f-parent") 8 := by
  decide

/--
**(witness) Root vs one nested folder.**
The rendered root key differs from the concrete parent `f-1`. The renderer is
not generally injective over arbitrary strings (for example, reserved
components require implementation-side validation or escaping).
-/
theorem witness_root_isolation :
    folderCacheKey "alice" none 1 ≠
    folderCacheKey "alice" (some "f-1") 1 := by
  decide

/--
**(witness) Two concrete parent ids produce distinct rendered keys.**
-/
theorem witness_distinct_parent :
    folderCacheKey "alice" (some "f-1") 5 ≠
    folderCacheKey "alice" (some "f-2") 5 := by
  decide

/--
**(witness) One concrete tenant/revision pair differs from another.**
-/
theorem witness_distinct_tenant_revision :
    folderCacheKey "alice" (some "f-1") 5 ≠
    folderCacheKey "bob" (some "f-1") 6 := by
  decide

/--
**(witness) Bump+1 vs bump+2 produce distinct keys.**
Two successive bumps yield two distinct cache keys. Pins the
monotonic-cache-key contract for chained mutations.
-/
theorem witness_successive_bumps :
    folderCacheKey "alice" none 10 ≠
    folderCacheKey "alice" none 11 := by
  decide

-- ─── Non-vacuity sanity checks ─────────────────────────────────────────

/-- A `bumpFolderRevision` invocation strictly changes state. -/
theorem witness_bump_changes_state :
    ({ tenant := "u1", parentId := none, revision := 0 } : FolderRevState).bump
      ≠ ({ tenant := "u1", parentId := none, revision := 0 } : FolderRevState) := by
  intro h
  have h2 := congrArg FolderRevState.revision h
  simp [FolderRevState.bump] at h2

/-- Liveness for L2: at least one (state, mutation) pair strictly
changes the bust state. -/
theorem covered_mutation_bumps_revision_nonvacuous :
    ∃ (s : FolderRevState) (m : FolderMutation),
      m.affectsListing = true ∧ m.apply s ≠ s := by
  refine ⟨{ tenant := "u1", parentId := none, revision := 0 },
          .chmod, ?_, ?_⟩
  · rfl
  · intro h
    have h2 := congrArg FolderRevState.revision h
    simp [FolderMutation.apply, FolderRevState.bump] at h2

end Mossaic.Vfs.Folder
