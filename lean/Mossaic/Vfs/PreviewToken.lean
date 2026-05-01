/-
Mossaic.Vfs.PreviewToken — Phase 47 preview-variant signed-URL token.

Models:
  worker/core/lib/preview-token.ts (~330 LoC) — sign / verify HS256 JWT
    with scope `vfs-pv`. Multi-secret rotation aware (mirrors
    verifyVFSDownloadToken / verifyShareToken / verifyMultipartToken).
  shared/preview-token-types.ts                — wire-shape types (if any)

Theorems:

  (PT1) scope_binding —
        a token whose payload `scope ≠ "vfs-pv"` is rejected by
        `verifyPreviewToken`, even with valid HMAC. Closes the
        RFC 8725 §2.8 cross-purpose-forgery vector: a
        `vfs` / `vfs-mp` / `vfs-dl` / `vfs-share` token replayed
        at the preview-variant endpoint fails the scope check.

  (PT2) forged_token_rejected —
        a token that fails HMAC against BOTH the current AND the
        previous secret is rejected. Forged tokens cannot pass.

  (PT3) multi_secret_rotation_window —
        a token signed under the previous secret verifies during
        the rotation window. Pre-minted preview URLs survive a
        `JWT_SECRET` rotation.

NO `axiom`. NO `sorry`. Mathlib v4.29.0.

Design: same opaque-HMAC + structural-payload-check style as
ShareToken.lean. We don't formalise HMAC algebra; the `validHmac`
predicate is opaque.
-/

import Mossaic.Vfs.Common

namespace Mossaic.Vfs.PreviewToken

open Mossaic.Vfs.Common

/-- Scope sentinel. Mirrors `VFS_PREVIEW_SCOPE = "vfs-pv"` at
preview-token.ts:66. -/
def VFS_PREVIEW_SCOPE : String := "vfs-pv"

/-- Wire-shape of a parsed preview token payload. Mirrors
`PreviewTokenPayload` at preview-token.ts:93-128. -/
structure Payload where
  scope         : String
  tenantId      : String
  fileId        : String
  /-- May be `none` for legacy / versioning-OFF tenants. -/
  headVersionId : Option String
  variantKind   : String
  rendererKind  : String
  format        : String
  contentHash   : String
  iat           : Nat
  exp           : Nat
  deriving DecidableEq, Repr

/-- Set of HMAC secrets valid for verification. Mirrors the
`verifyAgainstSecrets` helper at preview-token.ts:148-167. -/
structure SecretSet where
  primary  : String
  /-- `JWT_SECRET_PREVIOUS`; `none` outside rotation windows. -/
  previous : Option String
  deriving DecidableEq, Repr

/-- Opaque: "the token's HMAC verifies under this secret". The TS
side delegates to `jose.jwtVerify`; we don't model HMAC algebra. -/
opaque validHmac : (secret token : String) → Bool

/-- Opaque: "the JWT-claims body of `token` matches `payload`".
Encodes the JOSE wire-shape↔structured-payload correspondence. -/
opaque tokenEmbeds : (token : String) → Payload → Bool

/-- The verify function. Mirrors `verifyPreviewToken` at
preview-token.ts:271-330. Returns `some payload` on success,
`none` on any failure. -/
def verify (secrets : SecretSet) (token : String) (payload : Payload) :
    Option Payload :=
  let hmacOk : Bool :=
    validHmac secrets.primary token ||
    (match secrets.previous with
     | none      => false
     | some prev => validHmac prev token)
  if hmacOk = true ∧
     tokenEmbeds token payload = true ∧
     payload.scope = VFS_PREVIEW_SCOPE ∧
     payload.tenantId.length > 0 ∧
     payload.fileId.length > 0 ∧
     payload.variantKind.length > 0 ∧
     payload.rendererKind.length > 0 ∧
     payload.format.length > 0 then some payload
  else none

-- ─── (PT1) scope_binding ───────────────────────────────────────────────

/--
**(PT1) scope_binding.**
A token whose payload's `scope ≠ "vfs-pv"` is rejected even with
valid HMAC and well-formed claims. The scope-binding check at
preview-token.ts:284 is the gate.

This closes RFC 8725 §2.8 cross-purpose-forgery: a `vfs` / `vfs-mp` /
`vfs-dl` / `vfs-share` token replayed at the preview-variant route
fails the scope check.
-/
theorem scope_binding
    (secrets : SecretSet) (token : String) (payload : Payload)
    (h_wrong_scope : payload.scope ≠ VFS_PREVIEW_SCOPE) :
    verify secrets token payload = none := by
  unfold verify
  have hscope : (payload.scope = VFS_PREVIEW_SCOPE) = False := by
    apply propext
    constructor
    · exact fun h => h_wrong_scope h
    · exact fun h => h.elim
  simp [hscope]

-- ─── (PT2) forged_token_rejected ───────────────────────────────────────

/--
**(PT2) forged_token_rejected.**
A token that fails HMAC under BOTH the primary and the previous
secret is rejected. Forged tokens cannot pass.
-/
theorem forged_token_rejected
    (secrets : SecretSet) (token : String) (payload : Payload)
    (h_no_primary : validHmac secrets.primary token = false)
    (h_no_previous : ∀ prev, secrets.previous = some prev →
                              validHmac prev token = false) :
    verify secrets token payload = none := by
  unfold verify
  have hprev : (match secrets.previous with
                | none      => false
                | some prev => validHmac prev token) = false := by
    cases hp : secrets.previous with
    | none      => rfl
    | some prev => exact h_no_previous prev hp
  simp [h_no_primary, hprev]

-- ─── (PT3) multi_secret_rotation_window ────────────────────────────────

/--
**(PT3) multi_secret_rotation_window.**
A token signed under the previous secret verifies during the
rotation window. The multi-secret OR-chain at
preview-token.ts:148-167 ensures pre-minted preview URLs survive
a `JWT_SECRET` rotation (default TTL: 24 h, max 30 d).
-/
theorem multi_secret_rotation_window
    (secrets : SecretSet) (token : String) (payload : Payload) (prev : String)
    (h_prev : secrets.previous = some prev)
    (h_prev_valid : validHmac prev token = true)
    (h_embed : tokenEmbeds token payload = true)
    (h_scope : payload.scope = VFS_PREVIEW_SCOPE)
    (h_tenant : payload.tenantId.length > 0)
    (h_fid : payload.fileId.length > 0)
    (h_vk : payload.variantKind.length > 0)
    (h_rk : payload.rendererKind.length > 0)
    (h_fmt : payload.format.length > 0) :
    verify secrets token payload = some payload := by
  unfold verify
  have hhmac : (validHmac secrets.primary token ||
                (match secrets.previous with
                 | none      => false
                 | some prev => validHmac prev token)) = true := by
    rw [h_prev]
    simp [h_prev_valid]
  simp [hhmac, h_embed, h_scope, h_tenant, h_fid, h_vk, h_rk, h_fmt]

-- ─── Non-vacuity sanity checks ─────────────────────────────────────────

/-- Liveness: the verify function is non-trivial (not constantly
`none`). On a well-formed payload with a primary-secret-validating
token, verify returns `some payload`. -/
theorem verify_can_succeed_nonvacuous
    (secrets : SecretSet) (token : String) (payload : Payload)
    (h_hmac : validHmac secrets.primary token = true)
    (h_embed : tokenEmbeds token payload = true)
    (h_scope : payload.scope = VFS_PREVIEW_SCOPE)
    (h_tenant : payload.tenantId.length > 0)
    (h_fid : payload.fileId.length > 0)
    (h_vk : payload.variantKind.length > 0)
    (h_rk : payload.rendererKind.length > 0)
    (h_fmt : payload.format.length > 0) :
    verify secrets token payload = some payload := by
  unfold verify
  have hhmac : (validHmac secrets.primary token ||
                (match secrets.previous with
                 | none      => false
                 | some p    => validHmac p token)) = true := by
    simp [h_hmac]
  simp [hhmac, h_embed, h_scope, h_tenant, h_fid, h_vk, h_rk, h_fmt]

/-- Liveness: a wrong-scope payload triggers rejection non-vacuously. -/
theorem scope_binding_nonvacuous :
    ∃ (s : SecretSet) (t : String) (p : Payload),
      verify s t p = none := by
  refine ⟨{ primary := "k1", previous := none }, "tok",
          { scope := "vfs-mp", tenantId := "u1", fileId := "f1",
            headVersionId := none, variantKind := "thumb",
            rendererKind := "image", format := "auto",
            contentHash := "0", iat := 0, exp := 100 }, ?_⟩
  apply scope_binding
  unfold VFS_PREVIEW_SCOPE
  decide

end Mossaic.Vfs.PreviewToken
