# Mossaic encryption — security claims (literature, not Lean)

This document lists the cryptographic-security claims the Mossaic
encryption layer relies on. **These are NOT formalised in our Lean
proofs.** They are standard literature results, cited here for
auditability. Phase 24 removed earlier `theorem foo : True := by
trivial` placeholders that pretended to formalise these claims; the
text below is the honest accounting.

## Claim 1 — AES-GCM IND-CPA

Under random IV, AES-GCM is IND-CPA secure.

**Sources:**
- McGrew, D. & Viega, J. (2004). *The Galois/Counter Mode of Operation*. NIST.
- NIST SP 800-38D §A.5 (security analysis), 2007.
- Bellare, M. & Namprempre, C. (2000). *Authenticated Encryption: Relations Among Notions and Analysis of the Generic Composition Paradigm*. ASIACRYPT.

**Implementation:** `shared/encryption/envelope.ts`. The IV is 96 bits
(12 bytes) drawn from `crypto.getRandomValues` for random-mode envelopes.

**Why not formalised:** AES-GCM IND-CPA reduces to AES PRP-PRF security
plus the Galois-field MAC unforgeability — neither has a formal Lean
treatment in this corpus. Standard practice in formally-verified
cryptosystems (F* + miTLS, EverCrypt) treats AES-GCM as an
axiomatised AEAD primitive; we follow the same convention but at
documentation level rather than via a vacuous Lean axiom.

## Claim 2 — Convergent-mode leakage bound

Within a tenant (fixed salt), convergent-mode envelopes leak ONLY the
equality relation: `pt(c₀) = pt(c₁) ⟺ headerHash(c₀) = headerHash(c₁)`.
No other plaintext information is recoverable.

**Source:** Bellare, M., Keelveedhi, S. & Ristenpart, T. (2013).
*Message-Locked Encryption and Secure Deduplication*. EUROCRYPT.

**Implementation:** `shared/encryption/keys.ts:deriveConvergentKey` uses
HKDF-SHA-256 of `(masterKey || tenantSalt || plaintextHash)` as the
chunk key seed.

**Why not formalised:** the proof would unfold HKDF determinism + AES-GCM
IV-deterministic-message equality; the underlying cryptographic
results are in the literature, not in a Lean library.

## Claim 3 — Cross-tenant non-leakage via salt

Distinct tenant salts produce distinct HKDF-derived chunk keys, hence
distinct ciphertexts even for identical plaintexts.

**Source:** Standard application of HKDF (RFC 5869) salt as a
domain-separator. Krawczyk, H. (2010), *Cryptographic Extraction and
Key Derivation: The HKDF Scheme*, CRYPTO.

**Implementation:** `shared/encryption/keys.ts` derives every chunk key
through HKDF with `tenantSalt` as the salt parameter.

**Why not formalised:** SHA-256 preimage resistance is not modelled in
Lean.

## Claim 4 — Tenant isolation under encryption

Cross-tenant tenant isolation holds independently of the encryption
layer.

**Argument:** Tenant isolation is enforced by the DO-namespace
partition (`vfs:{ns}:{tenant}[:{sub}]` for UserDO,
`vfs:{ns}:{tenant}[:{sub}]:s{idx}` for ShardDO). This partition is
established BEFORE any encryption operation and is a pure
type-level fact. Encryption adds defense-in-depth (cross-tenant leak
is negligible per Claim 3) but does not change the structural
property.

**Lean coverage:** `Mossaic.Vfs.Tenant.cross_tenant_isolation` and
`Mossaic.Vfs.Tenant.cross_tenant_user_isolation` already prove the
DO-namespace property. Encryption-layer isolation is an immediate
consequence; no separate Lean theorem is needed (the previous
`encryption_preserves_tenant_isolation : True := by trivial` was
removed in Phase 24 because its formal content was empty).

## Claim 5 — HMAC-SHA-256 PRF unforgeability (used by JWT signing)

Without knowledge of the HMAC key, no PPT adversary can produce a
valid HMAC tag for a fresh message with non-negligible probability.

**Source:** Bellare, M., Canetti, R. & Krawczyk, H. (1996).
*Keying Hash Functions for Message Authentication*. CRYPTO.

**Implementation:** `worker/core/lib/auth.ts` uses HS256 JWTs via the
`jose` library. The signing key is `JWT_SECRET` from Cloudflare
secrets.

**Why not formalised:** SHA-256 PRF security is a standard
cryptographic axiom. `jose`'s implementation correctness is also out
of scope.

## Refcount-blindness (the one claim we DO formalise)

The chunk-refcount state machine is encryption-blind: applying any
`step` to a `validState` produces a `validState`, regardless of
whether the chunk hashes are derived from plaintext or from
encryption envelope headers.

**Lean theorem:** `Mossaic.Vfs.Encryption.refcount_invariant_under_encryption`.

**Why this CAN be formalised:** the refcount machinery operates on
opaque `String` hashes; the actual byte content of envelopes vs.
plaintext is below the abstraction layer of `Mossaic.Vfs.Refcount`.
The proof is a one-line reduction to `step_preserves_validState`.
