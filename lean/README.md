# Mossaic Lean 4 Formal Proofs

Mathlib4-backed formal verification of Mossaic VFS invariants in Lean 4.

**Status (Phase 51 honest accounting):** **224 theorems**, zero `sorry`, zero project axioms. The only axioms used are Lean's three kernel axioms: `propext`, `Classical.choice`, `Quot.sound`, plus Mathlib's transitive use of them. Phase 51 audited every theorem for vacuous patterns and removed 7 vacuous statements (function congruences, `T → T := id`, excluded-middle); see `local/lean-vacuousness-audit.md`. Added `PreviewToken.lean` (5 theorems) to back the previously-orphaned `@lean-invariant` tag at `worker/core/lib/preview-token.ts:48`.

```bash
cd lean && lake build
# Build completed successfully.
```

```bash
grep -rnE '^axiom|^[[:space:]]+axiom' lean/Mossaic/  # → empty
grep -rn '\bsorry\b' lean/Mossaic/ | grep -v '^[^:]*:[0-9]*:[[:space:]]*--' | grep -v '`sorry`'  # → empty
```

## What is proved (Phase 24 inventory)

### I1 — Refcount validity (must-have, **proved**)

Invariant `validState` (four-clause, including the numerical equality):

  - **(S1)** `chunks` are unique by hash.
  - **(S2)** `chunk_refs` are unique by composite key.
  - **(S3)** Every `chunk_refs` row's hash has a corresponding `chunks` row.
  - **(S4)** For every chunk row, `refCount = countP (·.chunkHash = c.hash) refs` (numerical equality, proved using `Mathlib.Data.List.Count`).

Theorem: [`Mossaic.Vfs.Refcount.step_preserves_validState`](Mossaic/Vfs/Refcount.lean) — fully unconditional. Plus 40 helpers + Phase 12 copyFile theorems.

### I3 — Tenant isolation (must-have, **proved**)

Distinct tenants under valid scope produce distinct DO instance names — both `vfsUserDOName` and `vfsShardDOName`. 15 theorems in [`Mossaic/Vfs/Tenant.lean`](Mossaic/Vfs/Tenant.lean). Reduces to char-list-level colon-separator splitting.

### I5 — GC safety (must-have, **proved**, NO axiom)

  - **(G1)** `alarm` only hard-deletes chunks with `refCount = 0` — unconditional.
  - **(G3)** `alarm` preserves `validState` — unconditional (uses the I1 numerical equality).
  - **(G4)** Resurrection is preserved.

10 theorems in [`Mossaic/Vfs/Gc.lean`](Mossaic/Vfs/Gc.lean).

### I2 — Atomic-write linearizability (**proved**)

The temp-id-then-rename pipeline. `readFile` chunks are always sourced from a single, well-defined file_id (no torn reads). 11 theorems in [`Mossaic/Vfs/AtomicWrite.lean`](Mossaic/Vfs/AtomicWrite.lean).

### I4 — Versioning sortedness & monotonicity (**proved**)

`listVersions_sorted` (V1) via `List.pairwise_mergeSort`. `insertVersion_max_ge` (V3). Phase 12 user_visible monotonicity (T7.5). 13 theorems in [`Mossaic/Vfs/Versioning.lean`](Mossaic/Vfs/Versioning.lean).

### I7 — Pool growth (Phase 24 NEW)

  - `pool_size_monotone` — `recordWriteUsage` never shrinks `pool_size`.
  - `pool_growth_threshold` — post-update equals `max(prior, BASE_POOL + ⌊storage_used / 5GB⌋)`.
  - `pool_growth_at_5GB_boundary` — crossing a 5 GB boundary grows the pool by exactly 1.
  - `placement_immutability_under_resize` — chunks recorded at write time stay findable post-resize.
  - `stored_shard_within_resized_pool` — recorded shard indices remain in-bounds after pool growth.
  - `storage_used_monotone`, `file_count_monotone` — quota counters monotone under writes.

10 theorems in [`Mossaic/Vfs/Quota.lean`](Mossaic/Vfs/Quota.lean).

### I8 — Preview pipeline / file_variants (Phase 24 NEW)

  - `stepVariant_preserves_validState` — every variant op preserves PK uniqueness.
  - `cascade_delete_drops_all` — deleting a file drops all its variants.
  - `cascade_delete_preserves_other_files` — file deletion does NOT touch other files' variants.
  - `variant_chunk_putChunk_preserves_shard_validState` — variant chunks register through the same refcount machinery as primary chunks; transitively preserved.
  - `reachable_validVariantState` — any sequence of variant ops from empty preserves the invariant.

13 theorems in [`Mossaic/Vfs/Preview.lean`](Mossaic/Vfs/Preview.lean).

### Encryption — refcount-blindness only (Phase 24 cleanup)

Phase 24 removed 4 vacuous-`True` theorems and the `AES_GCM_IND_CPA` "axiom" whose conclusion was also `True`. The cryptographic claims they pretended to formalise (IND-CPA, convergent leakage, cross-tenant non-leakage, encryption-preserves-tenant-isolation) belong in `docs/encryption-security-claims.md` with literature citations, NOT in Lean.

The one remaining theorem in [`Mossaic/Vfs/Encryption.lean`](Mossaic/Vfs/Encryption.lean):

  - `refcount_invariant_under_encryption` — the chunk-refcount state machine is encryption-blind. Direct reduction to `step_preserves_validState`.

### Multipart — supersession + idempotence (Phase 24 cleanup)

Phase 24 removed 5 vacuous-`True` theorems (`finalize_atomic_commit`, `session_token_unforgeability`, `multipart_alarm_idempotent`, `composition_with_phase15`, `phase16_axiom_budget`). Their docstrings claimed serious results that were not formalised; the formal content was `True`. The remaining theorems in [`Mossaic/Vfs/Multipart.lean`](Mossaic/Vfs/Multipart.lean):

  - `putChunkMultipart_idempotent` — same-hash retry preserves `validState`.
  - `putChunkMultipart_supersedes_safely` — coarse supersession (drop-all-refs-for-fileId + put new) preserves `validState`.
  - `putChunkMultipart_supersedes_safely_finegrained` — finer-grained supersession with explicit prior-hash existence premise.
  - `multipart_refcount_valid` — induction over a list of multipart ops preserves `validState`.
  - `multipart_put_changes_state` — non-vacuity witness.

### I6 — UNIQUE serialization (deferred — out of scope, unchanged)

Would require modeling SQLite's UNIQUE INDEX semantics including SAVEPOINT rollback. Documented limitation.

## Phase 24 changes vs. previous version

  - **Removed 1 project axiom** (`AES_GCM_IND_CPA`, vacuous).
  - **Removed 9 `True := by trivial` theorems** (4 in Encryption.lean, 5 in Multipart.lean).
  - **Added 1 finer-grained supersession theorem** (`putChunkMultipart_supersedes_safely_finegrained`).
  - **Added 2 new modules** (`Quota.lean` for pool growth, `Preview.lean` for file_variants).
  - **Removed silent whitelist** in `check-no-sorry.sh` — the whitelist array is now explicitly empty; any future axiom addition fails the gate.
  - **Refreshed citations** for 3 stale-line-range theorems and 5 stale module-LoC headers.

## Phase 30 additions

Phase 30 adds Lean theorem coverage for the work that landed in
Phase 25 (tombstone consistency), Phase 27 (multipart × versioning),
the Phase 27 follow-ups (rmrf / stream-commit / copy-file history
preservation), and Phase 27.5 (read-stream version routing). Each
target is mirrored from the integration test that pinned it.

NEW modules (3):

  - **`Tombstone.lean`** (13 theorems) — Phase 25 tombstone consistency.
    Proves listFiles excludes tombstoned heads, fileInfo / readPreview /
    readManyStat all return ENOENT (or null) on a tombstoned head, and
    "every listed file is statable" (the SPA gallery contract).
  - **`HistoryPreservation.lean`** (14 theorems) — Phase 27 follow-ups
    (Fix 5/6/7). Proves rmrf / stream-commit / copyFile under versioning
    ON each preserve prior versions while appending a new (or tombstone)
    version row. Composes with `insertVersion_max_ge` to give maxMtime
    monotonicity for all three paths.
  - **`StreamRouting.lean`** (12 theorems) — Phase 27.5 read-stream
    version routing. Proves createReadStream / openManifest / readChunk
    all route to `version_chunks` (not legacy `file_chunks`) when the
    head version is non-inline, yjs mode materializes from the live
    Y.Doc, empty files don't divide-by-zero, and all four read surfaces
    agree on byte-source.

EXTENDED modules (1):

  - **`Multipart.lean`** (+5 theorems, 5 → 10) — Phase 27 multipart ×
    versioning split. Proves `vfsFinalizeMultipart` under versioning ON
    creates a new `file_versions` row via `commitVersion`; under OFF
    the legacy `commitRename` byte-equivalence holds; prior versions
    are preserved across overwrite.

## Phase 43 additions

Phase 43 adds Lean theorem coverage for Phase 32 / 32.5 / 36 / 36b /
37 / 38 / 39b — work that landed without theorems and was tracked by
@lean-invariant tags pointing at non-existent Lean modules. The two
xref failures (`Cache.bust_token_completeness`,
`Refcount.restoreChunkRef_atomic`) are now resolved.

NEW modules (4):

  - **`Cache.lean`** (11 theorems) — Phase 36/36b edge-cache + version
    cache key. Proves `bust_token_completeness` (every metadata
    mutation bumps the bust token), `cache_key_extensional`,
    `commit_version_bumps_state`, plus distinctness witnesses.
  - **`Yjs.lean`** (15 theorems) — Phase 38 Yjs magic-prefix wire
    format. Proves `wrap_then_detect_roundtrip`,
    `magic_collision_defense`, `backward_compat_short_payloads`, and
    distinctness from any Yjs varint first byte.
  - **`ShareToken.lean`** (6 theorems) — Phase 32.5 HMAC verify.
    Proves `forged_token_rejected`, `wrong_scope_rejected`,
    `multi_secret_rotation_window`, `payload_pins_tenancy`. Uses
    `opaque` model declarations to avoid axioms.
  - **`RPC.lean`** (12 theorems) — Phase 39b batch RPCs. Proves
    `batch_preserves_order`, `batch_index_correspondence`,
    `single_rpc_per_shard_def`, `missing_chunk_returns_null`,
    `batch_rpc_atomicity`.

EXTENDED modules (3):

  - **`Versioning.lean`** (+6 theorems) — Phase 36 commitVersion
    accounting. `commitVersion_idempotent`, `accounting_balanced`,
    `versioning_on_pool_growth_works`, `pool_growth_monotonic`.
  - **`Quota.lean`** (+12 theorems) — Phase 32 skip-full placement
    (`skip_full_shard_returns_non_full`, `skip_full_shard_in_bounds`,
    `pool_full_at_zero_pool`), Phase 25 inline-tier migration
    threshold (`inline_migration_threshold`,
    `inline_overflow_rejected`), and Phase 7 server-authoritative
    pool fact.
  - **`Refcount.lean`** (+2 theorems, +1 RestoreStatus type)
    — Phase 32 Fix C2 restoreChunkRef. Proves
    `restoreChunkRef_atomic` (refs/chunks update together, no race
    window) and `restoreChunkRef_liveRefs_bump` (numerical
    consequence: liveRefs grows by 1 in lockstep with refCount).

## Phase 51 vacuousness audit

Phase 51 audited all 226 pre-existing theorems against 9 vacuous-pattern
detectors (function congruence `f x = f x`, tautology `T → T := id`,
excluded middle `P ∨ ¬P`, hypothesis-equals-conclusion, etc.). Seven
theorems were caught and disposed:

  - `Cache.cache_key_extensional` — function congruence (Phase 43 def-fold). Deleted; witnesses cover the real claim.
  - `Cache.versioned_variant_chunk_hash_determines_bytes` — `T → T := id`. Deleted; SHA-256 collision-resistance is a cryptographic assumption, not a Lean theorem.
  - `Quota.witness_no_client_hint_in_placement` — function congruence (Phase 43 def-fold). Deleted; type-level argument captures the claim.
  - `Quota.placement_immutability_under_resize` — `cp.x = cp.x`; dead args. Deleted; `stored_shard_within_resized_pool` is the substantive corollary.
  - `RPC.single_rpc_per_shard_def` — definitional unfolding (Phase 43). Deleted; `witness_rpc_count_*` theorems cover batching.
  - `StreamRouting.read_surfaces_agree_on_byte_source` — `f(x) = f(x)` under three let-bound aliases. Deleted; per-surface routing theorems are the real claims.
  - `AtomicWrite.readFile_changes_only_at_state_change` — `P ∨ ¬P` (excluded middle). Deleted; `readFile_unchanged_under_beginWrite` plus witnesses are the real claims.

One theorem was strengthened rather than deleted:

  - `Refcount.metadata_mutation_preserves_chunk_invariant` — first conjunct was `validState s` from hypothesis (id-shaped), second was rfl from def. Strengthened by adding an `op : Op` parameter so the first conjunct now invokes `step_preserves_validState` — a real claim.

The previously-orphaned `@lean-invariant Mossaic.Vfs.PreviewToken.scope_binding` xref at `worker/core/lib/preview-token.ts:48` (Phase 47) is fixed by adding `lean/Mossaic/Vfs/PreviewToken.lean` (5 theorems, opaque-HMAC style matching `ShareToken.lean`).

Full report: `local/lean-vacuousness-audit.md`.

## Theorem totals (Phase 51 post-audit)

| Module | Theorems | Notes |
|---|--:|---|
| `Common.lean` | 1 | UniqueBy.nil. |
| `Tenant.lean` | 14 | I3 tenant isolation. |
| `Refcount.lean` | 39 | I1 + Phase 12 copyFile + Phase 32 restoreChunkRef atomicity. |
| `Gc.lean` | 9 | I5 GC safety. |
| `AtomicWrite.lean` | 10 | I2 linearizability (Phase 51: −1 vacuous). |
| `Versioning.lean` | 19 | I4 sortedness + Phase 12 user_visible + Phase 36 commitVersion. |
| `Encryption.lean` | 1 | refcount-blindness only. |
| `Multipart.lean` | 10 | Phase 24 base + Phase 30 versioning split (§4). |
| `Quota.lean` | 20 | Pool-growth + Phase 32 skip-full + Phase 25 inline-tier (Phase 51: −2 vacuous). |
| `Preview.lean` | 13 | file_variants invariants. |
| `Tombstone.lean` | 11 | Phase 25 tombstone consistency. |
| `HistoryPreservation.lean` | 14 | Phase 27 Fix 5/6/7. |
| `StreamRouting.lean` | 11 | Phase 27.5 read-stream routing (Phase 51: −1 vacuous). |
| `Cache.lean` | 9 | Phase 36/36b cache-key + bust-token completeness (Phase 51: −2 vacuous). |
| `Yjs.lean` | 15 | Phase 38 Yjs magic-prefix wire format. |
| `ShareToken.lean` | 6 | Phase 32.5 HMAC share-token verify. |
| `RPC.lean` | 11 | Phase 39b batch RPC ordering + atomicity (Phase 51: −1 vacuous). |
| `PreviewToken.lean` | 5 | NEW (Phase 51) — Phase 47 preview-variant token scope binding. |
| `Generated/Placement.lean` | 0 | Documentation. |
| `Generated/ShardDO.lean` | 3 | Re-exports. |
| `Generated/UserDO.lean` | 3 | Re-exports. |
| **Total** | **224** | (Phase 51: −7 vacuous, +5 PreviewToken; net −2) |

Plus **0 project axioms** (unchanged from Phase 24) and **0 sorrys**.

## Architecture: Mathlib4 + TSLean-inspired modeling

Architecturally inspired by [AshishKumar4/TSLean](https://github.com/AshishKumar4/TSLean) but does NOT depend on it as a runtime. Hand-written state-machine model + `Generated/` delegation pattern. `omega` / `decide` / `simp` proof style.

Mathlib4 v4.29.0 lets us prove the numerical refcount equality (`List.countP`-based) and the listVersions sortedness (`List.pairwise_mergeSort`) directly, without recourse to project axioms.

## Layout (post-Phase-43)

```
lean/
├── README.md                       (this file)
├── lakefile.lean                   (Mathlib v4.29.0 dependency)
├── lean-toolchain                  (leanprover/lean4:v4.29.0)
├── lake-manifest.json              (auto-generated; pinned)
├── Mossaic.lean                    (root, re-exports all 19 modules)
├── Mossaic/
│   ├── Vfs/
│   │   ├── Common.lean             (Hash/PathId/TimeMs aliases, UniqueBy)
│   │   ├── Tenant.lean             (I3)
│   │   ├── Refcount.lean           (I1, full numerical equality + Phase 32 restoreChunkRef)
│   │   ├── Gc.lean                 (I5, no axiom)
│   │   ├── AtomicWrite.lean        (I2, full linearizability)
│   │   ├── Versioning.lean         (I4 sortedness + Phase 36 commitVersion accounting)
│   │   ├── Encryption.lean         (refcount-blindness only — Phase 24 cleanup)
│   │   ├── Multipart.lean          (idempotence + supersession + Phase 27 versioning split)
│   │   ├── Quota.lean              (pool-growth + Phase 32 skip-full + Phase 25 inline-tier)
│   │   ├── Preview.lean            (file_variants invariants)
│   │   ├── Tombstone.lean          (Phase 25 tombstone consistency)
│   │   ├── HistoryPreservation.lean (Phase 27 Fix 5/6/7)
│   │   ├── StreamRouting.lean      (Phase 27.5 read-stream routing)
│   │   ├── Cache.lean              (Phase 36/36b cache-key + bust-token)
│   │   ├── Yjs.lean                (Phase 38 magic-prefix wire format)
│   │   ├── ShareToken.lean         (Phase 32.5 HMAC verify)
│   │   ├── RPC.lean                (Phase 39b batch RPC ordering)
│   │   └── PreviewToken.lean       (Phase 51 NEW: Phase 47 preview-variant token)
│   └── Generated/
│       ├── ShardDO.lean            (re-exports for shard-do.ts)
│       ├── UserDO.lean             (re-exports for user-do.ts)
│       └── Placement.lean          (architectural cross-ref)
└── scripts/
    ├── check-no-sorry.sh           (verifies sorry-free + zero project axioms)
    └── check-xrefs.sh              (verifies @lean-invariant TS comments resolve)
```

## TS ↔ Lean cross-references

Every TS function with a proven invariant carries a `@lean-invariant` JSDoc tag. CI script `lean/scripts/check-xrefs.sh` enforces these resolve to actual `theorem` declarations.

## Limitations

  - **Hash collision-resistance.** SHA-256 is opaque; no theorem relies on it.
  - **Cloudflare DO atomicity.** Each `Op` is modeled as atomic, mirroring runtime guarantees.
  - **Wall-clock alarm timeliness.** Alarm is modeled as an externally-triggered Op.
  - **SQLite engine.** UNIQUE INDEX, SAVEPOINT, transactional atomicity are SQLite properties — not Mossaic's TS — and are not modeled.
  - **Workers concurrent-subrequest cap.** Out of scope.
  - **Cryptographic primitives.** AES-GCM IND-CPA, HMAC-SHA-256 PRF, etc. — these are literature-axiomatised in security-properties docs, not in Lean.
  - **Renderer registration order, concurrency caps.** Runtime-level invariants; out of Lean scope.

## How to extend a proof

1. Identify the relevant invariant module under `lean/Mossaic/Vfs/`.
2. Extend the `Op` inductive type and `step` function.
3. Re-prove the relevant invariance theorem for the new case.
4. Add a non-vacuity sanity theorem.
5. Add an `@lean-invariant` comment in the TS source.
6. Run `pnpm verify:proofs`.

## Mathlib build performance

`lake build` first-ever build (no cache): ~20-40 min (Mathlib has ~5800 files). With `lake exe cache get` (Azure CDN cache): ~3-5 min. Warm cache (incremental): ~2-5 sec.

## CI

`.github/workflows/lean.yml` runs `lake build` + scripts on every push touching `lean/`, `worker/`, or `shared/`. Caches `~/.elan` and `lean/.lake/packages/mathlib/.lake/build`.

## License

Same as parent project.
