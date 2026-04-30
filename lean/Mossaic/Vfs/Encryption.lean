/-
Mossaic.Vfs.Encryption — Phase 15: opt-in end-to-end encryption.
Phase 24 cleanup: removed 4 vacuous-True theorems and the AES_GCM_IND_CPA
"axiom" whose conclusion was also `True` (logically equivalent to
declaring nothing). The cryptographic claims they pretended to formalise
are now in `docs/encryption-security-claims.md` (an honest documentation
file with literature citations, not a Lean theorem).

Models:
  shared/encryption-types.ts                       (envelope shape, mode discriminator)
  shared/encryption.ts (71 LoC barrel)             (re-exports from shared/encryption/*.ts)
  shared/encryption/{chunk,envelope,internal,iv,keys}.ts (~800 LoC across 5 modules; the
                                                    763-LoC monolith was split at commit 52e77b2)
  worker/core/objects/user/encryption-stamp.ts (262 LoC) (column stamping, mode-monotonic)
  worker/core/objects/user/yjs.ts (1160 LoC)       (encrypted-yjs opaque relay)

What we actually prove (one theorem only):

  refcount_invariant_under_encryption — the chunk-refcount state machine
  is encryption-blind: applying any `step` to a `validState` produces a
  `validState`, regardless of whether the chunk hashes are derived from
  plaintext or from encryption envelope headers.

What we DO NOT prove here (and never did, despite earlier docstrings):
  - AES-GCM IND-CPA security. This is a cryptographic-literature result
    (NIST SP 800-38D, McGrew & Viega 2004, Bellare-Namprempre 2000). It
    has no formal Lean treatment in this corpus; the previous "axiom" of
    the same name had `True` as its conclusion and was therefore
    logically vacuous.
  - Convergent-mode leakage bounds (Bellare-Keelveedhi-Ristenpart 2013).
  - Cross-tenant non-leakage from distinct salts.
  - Tenant isolation under encryption (this follows from the existing
    `Tenant.cross_tenant_isolation` theorem, which is independent of
    encryption — distinct tenants get distinct DO names; encryption is
    layered on top and cannot weaken that).

Each of those four claims belongs in `docs/encryption-security-claims.md`,
where the citations live without pretending to be machine-checked theorems.

NO `axiom`. NO `sorry`. Mathlib v4.29.0.
-/

import Mossaic.Vfs.Common
import Mossaic.Vfs.Tenant
import Mossaic.Vfs.Refcount

namespace Mossaic.Vfs.Encryption

open Mossaic.Vfs.Common
open Mossaic.Vfs.Refcount

-- ─── Types (used by the one theorem below) ──────────────────────────────

/-- Encryption mode discriminator. Mirrors `shared/encryption-types.ts`. -/
inductive Mode where
  | convergent
  | random
  deriving DecidableEq, Repr

/-- AAD discriminator. Mirrors `shared/encryption-types.ts`. -/
inductive AadTag where
  | ck  -- file-content chunk
  | yj  -- Yjs sync_step_2 / update payload
  | aw  -- Yjs awareness payload
  deriving DecidableEq, Repr

-- ─── Theorem: refcount invariant is encryption-blind ────────────────────

/--
The chunk-refcount theorem from `Mossaic.Vfs.Refcount` is value-preserving
under encryption: chunk_hash is `SHA-256(envelopeHeader)` instead of
`SHA-256(plaintext)`, but the `(chunks, chunk_refs)` schema and the
count-preserving step function are byte-identical. The proof is a
one-step reduction to `step_preserves_validState`.

This is the ONLY non-trivial encryption-related Lean theorem in the
corpus. The other security claims (IND-CPA, convergent leakage bounds,
cross-tenant non-leakage) are cryptographic-literature results not
formalised in Lean.
-/
theorem refcount_invariant_under_encryption
    (s : ShardState) (op : Op) :
    validState s →
    validState (step s op) := by
  intro hv
  exact step_preserves_validState s op hv

end Mossaic.Vfs.Encryption
