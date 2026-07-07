# Mossaic multipart upload — security & atomicity claims (literature, not Lean)

This document lists the claims the Mossaic multipart upload pipeline
relies on but that are NOT formalised in our Lean proofs. Phase 24
removed earlier `theorem foo : True := by trivial` placeholders; the
text below is the honest accounting.

## Claim 1 — `vfsFinalizeMultipart` atomicity

**Statement:** Either the entire commit (file_chunks rows, file row
update, status flip, commit-rename) succeeds, or the pre-call state is
preserved.

**Implementation rationale, not a proof:** validation and shard collection
precede the UserDO-side manifest/file updates, and readers ignore uploading
temporary rows. The current Lean `AtomicWrite` model establishes selected
visibility properties for two atomic abstract transitions only. It does not
model the SQL sequence, cross-DO awaits, rollback, crashes, or the multipart
implementation, so it does not prove this claim.

**Why not formalised in Lean:** to establish this claim we would need to extend
`AtomicWrite.lean`'s state machine with a `multipartFinalize` op
that takes staging tables, SQL transaction boundaries, cross-DO results, and
failure points as state, then prove refinement from the implementation and
full trace linearizability. That work has not been done.

**Implementation:** `worker/core/objects/user/multipart-upload.ts:519`
(`vfsFinalizeMultipart`).

## Claim 2 — Session token unforgeability

**Statement:** Without `JWT_SECRET`, no PPT adversary can mint a valid
`vfs-mp` token.

**Reduction:** HMAC-SHA-256 PRF security
(Bellare-Canetti-Krawczyk 1996). Adversary forges valid HS256 JWT ⟹
adversary outputs `(m, σ)` such that `HMAC-Verify(K, m, σ) = 1` for
unknown K ⟺ adversary breaks HMAC-SHA-256 unforgeability ⟹ contradicts
PRF assumption on SHA-256.

The `scope: "vfs-mp"` sentinel + scope-binding cross-check at the
route layer ensures cross-purpose forgery is also impossible: even if
an adversary obtains a `vfs` or `vfs-dl` token, they cannot replay it
as a multipart session token because `verifyVFSMultipartToken`
rejects any token whose `scope` claim ≠ `"vfs-mp"`.

**Why not formalised in Lean:** SHA-256 PRF security is a standard
cryptographic axiom; JWT signing/verifying is axiomatised at the
runtime layer.

**Implementation:** `worker/core/lib/auth.ts` — `signVFSMultipartToken`
and `verifyVFSMultipartToken`. Constant-time verify via Web Crypto's
`subtle.verify` per spec, preventing timing side-channel leakage.

## Claim 3 — Multipart alarm idempotence

**Statement:** The UserDO orphan-session sweep is idempotent: running
it `n` times consecutively yields the same final state as running it
once.

**Argument:** Each iteration selects sessions where
`status = 'open' ∧ expires_at < now`. After processing,
`status = 'aborted'` for those rows. Subsequent iterations select an
empty set → no-op. Cloudflare alarms have at-least-once semantics with
exponential backoff retry on throw, so the sweep is safe to retry.

**Why not formalised in Lean:** would require extending `Gc.lean`'s
alarm model with a multipart-session table. Tracked as future work.

**Implementation:**
`worker/core/objects/user/multipart-upload.ts:840`
(`sweepExpiredMultipartSessions`).

## Claim 4 — Composition with encryption (Phase 15 byte-equivalence)

**Statement:** A multipart upload of an encrypted file yields the same
observable post-commit state as a single-shot encrypted `vfsWriteFile`
with the same plaintext under the same encryption opts.

**Argument:** Both paths converge on the same `commitRename` (or
`commitVersion + rename` for versioning) with identical
`files` / `file_chunks` row data: same envelope hashes, same chunk
sizes, same encryption metadata. The only observable difference is
timing (multipart faster) and partial visibility during upload (tmp
row in `uploading` state). After commit, byte-equivalence holds because
the envelope bytes on shards are identical (envelope hash determines
bytes uniquely under SHA-256 collision resistance).

**Why not formalised in Lean:** the byte-equivalence claim is about
envelope-header equality and chunk-storage immutability. The chunk-
refcount layer is proved encryption-blind by
`Mossaic.Vfs.Encryption.refcount_invariant_under_encryption`, but the
byte-equivalence claim itself is about SHA-256 collision-resistance,
which is not modeled in Lean.

**Reads via `vfsReadFile`, `vfsCreateReadStream`,
`vfsOpenManifest+readChunk`, and `parallelDownload` all return
identical bytes. Decryption yields identical plaintext.**

The encryption metadata is propagated through
`upload_sessions.encryption_mode` / `encryption_key_id` at begin and
re-stamped onto `files` / `file_versions` at finalize, mirroring the
`vfsWriteFile` path's stamping at commit time.

## What IS formalised in Lean

`Mossaic.Vfs.Multipart` includes these relevant abstract-model theorems:

  - `putChunkMultipart_idempotent` — same-ref retry yields exact abstract shard-state equality.
  - `putChunkMultipart_retry_preserves_validState` — same-ref retry preserves `validState`.
  - `commitManifest_success_is_complete` — modeled successful commit implies declared-count and collected index/hash completeness.
  - `putChunkMultipart_supersedes_safely` — coarse supersession preserves `validState`.
  - `putChunkMultipart_supersedes_safely_finegrained` — finer-grained version with prior-hash existence premise.
  - `multipart_refcount_valid` — induction over a list of multipart ops preserves `validState`.
  - `multipart_put_changes_state` — non-vacuity (the model is not `step = id`).

These cover parts of the abstract refcount, retry, and manifest-gate models.
They do not prove the SQL/TypeScript implementation, full finalize atomicity,
token unforgeability, alarm idempotence, or byte-equivalence with the
single-shot path.
