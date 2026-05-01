/-
Mossaic.Vfs.ShareToken — Phase 32.5 share-token verify invariants.

Phase 32.5 introduced HMAC-signed share tokens for public album links.
The token carries `(scope = "vfs-share", userId, fileIds, albumName,
jti, iat, exp)` and is verified via `verifyAgainstSecrets` (multi-
secret aware for rotation windows).

Models:
  worker/core/lib/auth.ts (621+ LoC):
    :506   (VFS_SHARE_SCOPE = "vfs-share")
    :509   (SHARE_TOKEN_DEFAULT_TTL_MS = 90 days)
    :511-526 (ShareTokenPayload — scope + userId + fileIds + albumName + jti + iat + exp)
    :537-572 (signShareToken — HS256 + 96-bit jti)
    :584-621 (verifyShareToken — scope check + claim shape validation)
    verifyAgainstSecrets — primary + JWT_SECRET_PREVIOUS rotation window

  worker/app/routes/shared.ts:
    :13    ("token IS the auth")
    :33,92 (verifyShareToken at the route boundary)

What we prove (structural / non-cryptographic):

  (S1) forged_token_rejected — a token whose signature does NOT
       verify against any active secret returns `none`. This is the
       structural shape of `verifyAgainstSecrets`'s return contract.
  (S2) wrong_scope_rejected — even a validly-HMAC-signed token with
       `scope ≠ "vfs-share"` is rejected. Closes the cross-purpose
       forgery vector (RFC 8725 §2.8) — a `vfs` / `vfs-mp` / `vfs-dl`
       token cannot be replayed as a share.
  (S3) multi_secret_rotation_window — during an operator rotation,
       both JWT_SECRET and JWT_SECRET_PREVIOUS verify; tokens minted
       under the old secret remain valid through the rotation window.
  (S4) payload_pins_tenancy — a verified payload's `userId` and
       `fileIds` array are non-empty strings, ensuring the route
       layer (`appGetFile` consumer) cannot dereference a malformed
       payload smuggled past JWT verification.

What we explicitly DO NOT prove (literature-axiomatised):
  - HMAC-SHA-256 PRF unforgeability (Bellare-Canetti-Krawczyk 1996).
    See `docs/multipart-security-claims.md` §2 for the literature
    reference. The Lean theorems below treat HMAC verification as
    an opaque predicate `validHmac` and reason about the wrapper
    structure.

NO `axiom`. NO `sorry`. Mathlib v4.29.0.
-/

import Mossaic.Vfs.Common

namespace Mossaic.Vfs.ShareToken

open Mossaic.Vfs.Common

-- ─── Types ──────────────────────────────────────────────────────────────

/-- Parsed share-token payload. Mirrors `ShareTokenPayload` in
auth.ts:511-526. -/
structure Payload where
  scope     : String           -- must equal "vfs-share"
  userId    : String
  fileIds   : List FileId
  albumName : String
  jti       : String
  iat       : Nat              -- seconds since epoch
  exp       : Nat              -- seconds since epoch
  deriving DecidableEq, Repr

/-- The required scope sentinel. Mirrors `VFS_SHARE_SCOPE` at auth.ts:506. -/
def VFS_SHARE_SCOPE : String := "vfs-share"

/-- Active HMAC secrets — primary plus optional rotation-window previous.
Mirrors the env-derived secret pair used by `verifyAgainstSecrets`. -/
structure SecretSet where
  primary  : String           -- JWT_SECRET
  previous : Option String    -- JWT_SECRET_PREVIOUS (optional)
  deriving DecidableEq, Repr

/-- Abstract HMAC validity predicate. We treat HMAC-SHA-256 as opaque;
the predicate `validHmac secret token = true` iff the token's signature
verifies against `secret`. The TS implementation calls into `jose`
which handles the cryptographic step.

Invariants we assume of `validHmac` (these are HMAC-SHA-256 properties
documented in BCK 1996, NOT Lean theorems):
  - Verification is deterministic for fixed (secret, token).
  - Distinct secrets produce distinct verification outcomes for the
    same token (PRF property; not formalised here). -/
opaque validHmac : String → String → Bool

/-- A wire-format share token is the (HMAC-signed) JWT serialization.
We model it as the pair `(token : String, payload : Payload)` where
the token's bytes embed the payload via JWT base64url encoding. The
predicate `tokenEmbeds token payload = true` captures the JWT claim
extraction step. -/
opaque tokenEmbeds : String → Payload → Bool

/-- The full verify pipeline. Mirrors `verifyShareToken` in auth.ts:584-621:

  1. Try primary secret; on fail, try previous (multi-secret).
  2. If neither verifies, return `none`.
  3. If verification passes, extract claims and require:
     - `scope = "vfs-share"`
     - `userId` non-empty string
     - `fileIds` non-empty array of non-empty strings
     - `albumName` is a string
     - `jti` is non-empty string
     - return the parsed payload.
-/
def verify (secrets : SecretSet) (token : String) (payload : Payload) :
    Option Payload :=
  -- Step 1+2: HMAC check across primary, then previous.
  let hmacOk : Bool :=
    validHmac secrets.primary token ||
    (match secrets.previous with
     | none => false
     | some prev => validHmac prev token)
  -- Step 3: claim shape + scope binding.
  let embedded : Bool := tokenEmbeds token payload
  let scopeOk : Bool := payload.scope = VFS_SHARE_SCOPE
  let userOk  : Bool := payload.userId.length > 0
  let fidsOk  : Bool :=
    payload.fileIds.length > 0 &&
    payload.fileIds.all (fun f => f.length > 0)
  let jtiOk   : Bool := payload.jti.length > 0
  if hmacOk ∧ embedded ∧ scopeOk ∧ userOk ∧ fidsOk ∧ jtiOk then
    some payload
  else
    none

-- ─── (S1) forged_token_rejected ────────────────────────────────────────

/--
**(S1) forged_token_rejected.**
A token whose HMAC verification fails on every active secret is
rejected, regardless of how well-formed its payload claims are.
This is the structural floor that HMAC unforgeability (BCK 1996) sits
on top of: even WITH unforgeability, the wrapper must reject failed
HMACs, which it does.
-/
theorem forged_token_rejected
    (secrets : SecretSet) (token : String) (payload : Payload)
    (h_no_primary : validHmac secrets.primary token = false)
    (h_no_previous : ∀ prev, secrets.previous = some prev →
                              validHmac prev token = false) :
    verify secrets token payload = none := by
  unfold verify
  -- The hmacOk OR-chain reduces to false on both branches.
  have hprev : (match secrets.previous with
                | none => false
                | some prev => validHmac prev token) = false := by
    cases hp : secrets.previous with
    | none => rfl
    | some prev => exact h_no_previous prev hp
  simp [h_no_primary, hprev]

-- ─── (S2) wrong_scope_rejected ─────────────────────────────────────────

/--
**(S2) wrong_scope_rejected.**
A token whose payload's `scope ≠ "vfs-share"` is rejected even with
valid HMAC — the scope-binding check at auth.ts:592 is the gate.
This closes the RFC 8725 §2.8 cross-purpose-forgery vector: a
`vfs` / `vfs-mp` / `vfs-dl` token replayed at the share endpoint
fails the scope check.
-/
theorem wrong_scope_rejected
    (secrets : SecretSet) (token : String) (payload : Payload)
    (h_wrong_scope : payload.scope ≠ VFS_SHARE_SCOPE) :
    verify secrets token payload = none := by
  unfold verify
  -- scopeOk is false; the conjunction collapses regardless of HMAC.
  have hscope : (payload.scope = VFS_SHARE_SCOPE) = False := by
    apply propext
    constructor
    · exact fun h => h_wrong_scope h
    · exact fun h => h.elim
  simp [hscope]

-- ─── (S3) multi_secret_rotation_window ─────────────────────────────────

/--
**(S3) multi_secret_rotation_window.**
A token signed under the previous secret (stored in
`JWT_SECRET_PREVIOUS`) verifies during the rotation window. The
multi-secret OR-chain ensures rotation does NOT invalidate
in-flight shares — operators set both env vars during rotation,
then unset `JWT_SECRET_PREVIOUS` once outstanding shares have
expired (default TTL: 90 days).
-/
theorem multi_secret_rotation_window
    (secrets : SecretSet) (token : String) (payload : Payload) (prev : String)
    (h_prev : secrets.previous = some prev)
    (h_prev_valid : validHmac prev token = true)
    (h_embedded : tokenEmbeds token payload = true)
    (h_scope : payload.scope = VFS_SHARE_SCOPE)
    (h_user : payload.userId.length > 0)
    (h_fids : payload.fileIds.length > 0 ∧
              payload.fileIds.all (fun f => f.length > 0) = true)
    (h_jti : payload.jti.length > 0) :
    verify secrets token payload = some payload := by
  unfold verify
  -- The multi-secret OR resolves to true via `prev`.
  have hhmac : (validHmac secrets.primary token ||
                (match secrets.previous with
                 | none => false
                 | some p => validHmac p token)) = true := by
    rw [h_prev]
    simp [h_prev_valid]
  -- All claim-shape checks pass.
  have hfids_bool : (payload.fileIds.length > 0 &&
                     payload.fileIds.all (fun f => f.length > 0)) = true := by
    have hl : decide (payload.fileIds.length > 0) = true := decide_eq_true h_fids.1
    rw [Bool.and_eq_true]
    refine ⟨hl, h_fids.2⟩
  simp [hhmac, h_embedded, h_scope, h_user, hfids_bool, h_jti]

-- ─── (S4) payload_pins_tenancy ─────────────────────────────────────────

/--
**(S4) payload_pins_tenancy.**
A successful `verify` returns a payload whose `userId` and `fileIds`
fields are STRUCTURALLY validated: `userId` is a non-empty string
and `fileIds` is a non-empty list of non-empty strings.

This is the load-bearing post-condition the route layer
(`appGetFile` in shared.ts) relies on — it dereferences
`payload.userId` and `payload.fileIds[i]` immediately and would
otherwise face undefined behaviour on a malformed claim shape that
slipped past JWT.
-/
theorem payload_pins_tenancy
    (secrets : SecretSet) (token : String) (payload result : Payload)
    (h_verify : verify secrets token payload = some result) :
    result.userId.length > 0 ∧
    result.fileIds.length > 0 ∧
    result.fileIds.all (fun f => f.length > 0) = true ∧
    result.scope = VFS_SHARE_SCOPE := by
  unfold verify at h_verify
  -- The if-condition is the conjunction of all checks; if it returns
  -- `some payload` then the conjunction held and result = payload.
  by_cases h : (validHmac secrets.primary token ||
                  (match secrets.previous with
                   | none => false
                   | some prev => validHmac prev token)) = true ∧
                tokenEmbeds token payload = true ∧
                payload.scope = VFS_SHARE_SCOPE ∧
                payload.userId.length > 0 ∧
                (payload.fileIds.length > 0 &&
                 payload.fileIds.all (fun f => f.length > 0)) = true ∧
                payload.jti.length > 0
  · rw [if_pos h] at h_verify
    -- result = payload by Option.some.injection
    have hres : result = payload := Option.some.inj h_verify
    rw [hres]
    refine ⟨h.2.2.2.1, ?_, ?_, h.2.2.1⟩
    · have hbool : (payload.fileIds.length > 0 &&
                    payload.fileIds.all (fun f => f.length > 0)) = true :=
        h.2.2.2.2.1
      rw [Bool.and_eq_true] at hbool
      exact decide_eq_true_eq.mp hbool.1
    · have hbool : (payload.fileIds.length > 0 &&
                    payload.fileIds.all (fun f => f.length > 0)) = true :=
        h.2.2.2.2.1
      rw [Bool.and_eq_true] at hbool
      exact hbool.2
  · rw [if_neg h] at h_verify
    exact absurd h_verify (by simp)

-- ─── Non-vacuity sanity checks ──────────────────────────────────────────

/-- Liveness: a payload whose scope is "wrong" is rejected non-vacuously
(there exists a payload + token where verify yields `none`). -/
theorem wrong_scope_rejected_nonvacuous :
    ∃ (s : SecretSet) (t : String) (p : Payload),
      verify s t p = none := by
  refine ⟨{ primary := "k1", previous := none }, "tok",
          { scope := "vfs-mp",  -- WRONG scope
            userId := "u1", fileIds := ["f1"], albumName := "a", jti := "j1",
            iat := 0, exp := 100 }, ?_⟩
  apply wrong_scope_rejected
  unfold VFS_SHARE_SCOPE
  decide

/-- Liveness: the verify function is non-trivial (not constantly `none`):
on a well-formed payload with a HMAC-validating token (in the abstract
opaque sense), it returns `some payload`. The witness uses a hypothesis
about the opaque `validHmac` to assert the positive case exists. -/
theorem verify_can_succeed_nonvacuous
    (secrets : SecretSet) (token : String) (payload : Payload)
    (h_hmac : validHmac secrets.primary token = true)
    (h_embed : tokenEmbeds token payload = true)
    (h_scope : payload.scope = VFS_SHARE_SCOPE)
    (h_user : payload.userId.length > 0)
    (h_fids_pos : payload.fileIds.length > 0)
    (h_fids_all : payload.fileIds.all (fun f => f.length > 0) = true)
    (h_jti : payload.jti.length > 0) :
    verify secrets token payload = some payload := by
  unfold verify
  have hhmac : (validHmac secrets.primary token ||
                (match secrets.previous with
                 | none => false
                 | some p => validHmac p token)) = true := by
    simp [h_hmac]
  have hfids_bool : (payload.fileIds.length > 0 &&
                     payload.fileIds.all (fun f => f.length > 0)) = true := by
    rw [Bool.and_eq_true]
    refine ⟨decide_eq_true h_fids_pos, h_fids_all⟩
  simp [hhmac, h_embed, h_scope, h_user, hfids_bool, h_jti]

end Mossaic.Vfs.ShareToken
