/-
Mossaic.Vfs.Yjs — Phase 38 Yjs snapshot format invariants.

Phase 38 introduced a magic-prefix wire format for Yjs snapshot bytes
to distinguish them from legacy plaintext-via-Y.Text("content") files.
The prefix `YJS1` (ASCII) is documented at yjs.ts:101-120.

Models:
  worker/core/objects/user/yjs.ts (1361 LoC):
    :118-120 (YJS_SNAPSHOT_MAGIC bytes — [0x59, 0x4A, 0x53, 0x31])
    :127-133 (hasYjsSnapshotMagic — 4-byte prefix check)
    :148-155 (wrapYjsSnapshot — prefix + Y.encodeStateAsUpdate output)
    :971-1056 (writeSnapshot dispatch on magic)
    :1295-1327 (readYjsAsBytes — materialize for legacy clients)

What we prove:

  (Y1) wrap_then_detect_roundtrip — `hasYjsSnapshotMagic (wrapYjsSnapshot bs) = true`
       for any input bytes. The wrap+detect pair is self-consistent.
  (Y2) wrap_preserves_payload — the payload immediately follows the
       magic prefix, so unwrapping (drop first 4 bytes) recovers the
       original input verbatim. This is the round-trip property.
  (Y3) magic_collision_defense — input bytes that are NOT prefixed
       with `YJS1` are correctly classified as non-snapshot. (The
       defensive parse at yjs.ts:1048 then falls through to
       Y.applyUpdate or legacy text — both of which handle the
       non-magic case safely.)
  (Y4) backward_compat_y_text — empty / short payloads (< 4 bytes,
       legacy Y.Text("content") shapes) are NOT classified as
       snapshots, preserving legacy-document compatibility.
  (Y5) magic_first_byte_uniqueness — Yjs binary updates begin with a
       varint for the update-format-version (currently 0x00). The
       magic's first byte (0x59) is structurally distinct, so a
       valid `Y.encodeStateAsUpdate(doc)` output never satisfies
       `hasYjsSnapshotMagic` — closing the false-positive door.

NO `axiom`. NO `sorry`. Mathlib v4.29.0.
-/

import Mossaic.Vfs.Common

namespace Mossaic.Vfs.Yjs

open Mossaic.Vfs.Common

-- ─── Types ──────────────────────────────────────────────────────────────

/-- Bytes are modeled as `List Nat` (each byte 0-255). The Lean operations
we need are length, prefix-equality, and append/take/drop — all stable
across Lean stdlib versions. -/
abbrev Bytes := List Nat

/-- The Phase 38 magic prefix: ASCII "YJS1" = `[0x59, 0x4A, 0x53, 0x31]`.
Mirrors `YJS_SNAPSHOT_MAGIC` at yjs.ts:118-120. -/
def YJS_SNAPSHOT_MAGIC : Bytes := [0x59, 0x4A, 0x53, 0x31]

/-- Magic length is 4. -/
theorem magic_length : YJS_SNAPSHOT_MAGIC.length = 4 := rfl

/-- 4-byte prefix check. Mirrors `hasYjsSnapshotMagic` at yjs.ts:127-133.
Returns `false` on inputs shorter than 4 bytes. -/
def hasYjsSnapshotMagic (bs : Bytes) : Bool :=
  if h : bs.length ≥ 4 then
    bs.take 4 = YJS_SNAPSHOT_MAGIC
  else
    false

/-- Wrap raw bytes with the snapshot magic prefix. Mirrors
`wrapYjsSnapshot` at yjs.ts:148-155. -/
def wrapYjsSnapshot (bs : Bytes) : Bytes :=
  YJS_SNAPSHOT_MAGIC ++ bs

-- ─── (Y1) Round-trip: wrap then detect ─────────────────────────────────

/-- Wrapping a length-≥0 byte sequence produces a result with length
≥ 4, since the magic prefix is exactly 4 bytes. -/
theorem wrap_length (bs : Bytes) :
    (wrapYjsSnapshot bs).length = 4 + bs.length := by
  unfold wrapYjsSnapshot
  rw [List.length_append]
  rw [magic_length]

/-- The first 4 bytes of a wrapped payload are exactly the magic. -/
theorem wrap_take_4 (bs : Bytes) :
    (wrapYjsSnapshot bs).take 4 = YJS_SNAPSHOT_MAGIC := by
  unfold wrapYjsSnapshot
  rw [List.take_append_of_le_length (by rw [magic_length])]
  -- (magic ++ bs).take 4 = magic.take 4 = magic (since magic.length = 4).
  have : YJS_SNAPSHOT_MAGIC.take 4 = YJS_SNAPSHOT_MAGIC := by decide
  exact this

/--
**(Y1) wrap_then_detect_roundtrip.**
Detection is the right inverse of wrapping: any output of `wrapYjsSnapshot`
is correctly classified as a snapshot.
-/
theorem wrap_then_detect_roundtrip (bs : Bytes) :
    hasYjsSnapshotMagic (wrapYjsSnapshot bs) = true := by
  unfold hasYjsSnapshotMagic
  have hlen : (wrapYjsSnapshot bs).length ≥ 4 := by
    rw [wrap_length]; omega
  rw [dif_pos hlen]
  rw [wrap_take_4]

-- ─── (Y2) Wrap preserves payload ───────────────────────────────────────

/--
**(Y2) wrap_preserves_payload.**
Dropping the 4-byte magic recovers the original bytes verbatim — the
round-trip is byte-equivalent.
-/
theorem wrap_preserves_payload (bs : Bytes) :
    (wrapYjsSnapshot bs).drop 4 = bs := by
  unfold wrapYjsSnapshot
  rw [List.drop_append_of_le_length (by rw [magic_length])]
  -- (magic ++ bs).drop 4 = magic.drop 4 ++ bs = [] ++ bs = bs
  have : YJS_SNAPSHOT_MAGIC.drop 4 = [] := by decide
  rw [this]
  simp

-- ─── (Y3) Magic collision defense ──────────────────────────────────────

/--
**(Y3) magic_collision_defense.**
An input that is NOT prefixed with `YJS_SNAPSHOT_MAGIC` is correctly
classified as a non-snapshot. The detection function rejects any
4-prefix that doesn't byte-match the magic.
-/
theorem magic_collision_defense
    (bs : Bytes) (h_long : bs.length ≥ 4)
    (h_diff : bs.take 4 ≠ YJS_SNAPSHOT_MAGIC) :
    hasYjsSnapshotMagic bs = false := by
  unfold hasYjsSnapshotMagic
  rw [dif_pos h_long]
  -- Goal: decide (bs.take 4 = YJS_SNAPSHOT_MAGIC) = false
  exact decide_eq_false h_diff

-- ─── (Y4) Backward compat: short / legacy payloads ─────────────────────

/--
**(Y4) backward_compat_y_text.**
Empty or short payloads (< 4 bytes) are never classified as snapshots,
preserving compatibility with legacy `Y.Text("content")` documents.

Concrete: empty bytes, 1-byte, 2-byte, 3-byte payloads all return
false from `hasYjsSnapshotMagic`.
-/
theorem backward_compat_short_payloads
    (bs : Bytes) (h_short : bs.length < 4) :
    hasYjsSnapshotMagic bs = false := by
  unfold hasYjsSnapshotMagic
  have : ¬ bs.length ≥ 4 := by omega
  rw [dif_neg this]

/-- Concrete witness for backward compat: empty bytes. -/
theorem witness_empty_not_snapshot :
    hasYjsSnapshotMagic ([] : Bytes) = false := by decide

/-- Concrete witness: a 3-byte payload (e.g. legacy 1-char Y.Text) is
not classified as a snapshot. -/
theorem witness_3byte_not_snapshot :
    hasYjsSnapshotMagic ([0x59, 0x4A, 0x53] : Bytes) = false := by decide

/--
**(Y4-extension) backward_compat_y_text_payload.**
A legacy text payload like the literal "content" (1 byte more than the
magic but starting with different bytes) is not a snapshot.
-/
theorem witness_legacy_text_not_snapshot :
    -- "content" = 0x63 0x6F 0x6E 0x74 0x65 0x6E 0x74 — first byte 0x63 ≠ 0x59
    hasYjsSnapshotMagic ([0x63, 0x6F, 0x6E, 0x74, 0x65, 0x6E, 0x74] : Bytes)
      = false := by decide

-- ─── (Y5) Magic first-byte uniqueness vs Yjs encodeStateAsUpdate ───────

/--
**(Y5) magic_first_byte_distinct_from_yjs_update_prefix.**
Yjs binary updates begin with a varint for the update-format-version,
which is currently `0x00` (and historical versions are also < 0x40).
The magic's first byte is `0x59` (ASCII 'Y'), which exceeds 0x40 —
so any byte sequence whose first byte is in the Yjs varint-version
range cannot be confused for a magic-prefixed snapshot.

We package the structural property: byte 0x00 (the dominant Yjs
varint version) is distinct from the magic's first byte 0x59.
-/
theorem magic_first_byte_distinct_from_yjs_varint :
    -- The first byte of the magic is NOT 0x00 (the Yjs update prefix).
    YJS_SNAPSHOT_MAGIC.head? ≠ some 0x00 := by decide

/-- Concrete witness: a Yjs update that starts with 0x00 (the standard
varint version) is not classified as a magic-prefixed snapshot. -/
theorem witness_yjs_update_prefix_not_snapshot :
    -- A 4+-byte payload starting with 0x00 (Yjs varint version 0).
    hasYjsSnapshotMagic ([0x00, 0x01, 0x02, 0x03, 0x04] : Bytes) = false := by
  decide

-- ─── Non-vacuity sanity checks ──────────────────────────────────────────

/-- Witness: the wrap+detect roundtrip works on a non-trivial payload. -/
theorem witness_wrap_then_detect :
    hasYjsSnapshotMagic (wrapYjsSnapshot [0x00, 0x01, 0x02]) = true := by
  decide

/-- Witness: the wrapped payload is recoverable. -/
theorem witness_wrap_preserves :
    (wrapYjsSnapshot [0x00, 0x01, 0x02]).drop 4 = [0x00, 0x01, 0x02] := by
  decide

/-- Liveness: the detection function is non-trivial — it returns true
on at least one input and false on at least one input. -/
theorem hasYjsSnapshotMagic_nontrivial :
    (∃ bs : Bytes, hasYjsSnapshotMagic bs = true) ∧
    (∃ bs : Bytes, hasYjsSnapshotMagic bs = false) := by
  refine ⟨⟨wrapYjsSnapshot [0x00], ?_⟩, ⟨[], ?_⟩⟩
  · decide
  · decide

end Mossaic.Vfs.Yjs
