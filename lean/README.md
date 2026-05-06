# Mossaic Lean 4 Formal Proofs

Mathlib4-backed formal verification of Mossaic VFS invariants in Lean 4.

**Status (Phase 30 honest accounting):** **172 theorems**, zero `sorry`, zero project axioms. The only axioms used are Lean's three kernel axioms: `propext`, `Classical.choice`, `Quot.sound`, plus Mathlib's transitive use of them.

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

## Theorem totals (Phase 30 post-additions)

| Module | Theorems | Notes |
|---|--:|---|
| `Common.lean` | 1 | UniqueBy.nil. |
| `Tenant.lean` | 15 | I3 tenant isolation. |
| `Refcount.lean` | 41 | I1, including Phase 12 copyFile + metadata. |
| `Gc.lean` | 10 | I5 GC safety. |
| `AtomicWrite.lean` | 11 | I2 linearizability. |
| `Versioning.lean` | 13 | I4 sortedness + Phase 12 user_visible. |
| `Encryption.lean` | 1 | refcount-blindness only. |
| `Multipart.lean` | 10 | Phase 24 base + Phase 30 versioning split (§4). |
| `Quota.lean` | 10 | Pool-growth correctness. |
| `Preview.lean` | 15 | file_variants invariants. |
| `Tombstone.lean` | 13 | NEW (Phase 30) — Phase 25 tombstone consistency. |
| `HistoryPreservation.lean` | 14 | NEW (Phase 30) — Phase 27 Fix 5/6/7. |
| `StreamRouting.lean` | 12 | NEW (Phase 30) — Phase 27.5 read-stream routing. |
| `Generated/Placement.lean` | 0 | Documentation. |
| `Generated/ShardDO.lean` | 3 | Re-exports. |
| `Generated/UserDO.lean` | 3 | Re-exports. |
| **Total** | **172** | (was 128 post-Phase-24; +44 in Phase 30) |

Plus **0 project axioms** (unchanged from Phase 24) and **0 sorrys**.

## Architecture: Mathlib4 + TSLean-inspired modeling

Architecturally inspired by [AshishKumar4/TSLean](https://github.com/AshishKumar4/TSLean) but does NOT depend on it as a runtime. Hand-written state-machine model + `Generated/` delegation pattern. `omega` / `decide` / `simp` proof style.

Mathlib4 v4.29.0 lets us prove the numerical refcount equality (`List.countP`-based) and the listVersions sortedness (`List.pairwise_mergeSort`) directly, without recourse to project axioms.

## Layout (post-Phase-30)

```
lean/
├── README.md                       (this file)
├── lakefile.lean                   (Mathlib v4.29.0 dependency)
├── lean-toolchain                  (leanprover/lean4:v4.29.0)
├── lake-manifest.json              (auto-generated; pinned)
├── Mossaic.lean                    (root, re-exports all 14 modules)
├── Mossaic/
│   ├── Vfs/
│   │   ├── Common.lean             (Hash/PathId/TimeMs aliases, UniqueBy)
│   │   ├── Tenant.lean             (I3)
│   │   ├── Refcount.lean           (I1, full numerical equality)
│   │   ├── Gc.lean                 (I5, no axiom)
│   │   ├── AtomicWrite.lean        (I2, full linearizability)
│   │   ├── Versioning.lean         (I4, full sortedness via Mathlib)
│   │   ├── Encryption.lean         (refcount-blindness only — Phase 24 cleanup)
│   │   ├── Multipart.lean          (idempotence + supersession + Phase 27 versioning split)
│   │   ├── Quota.lean              (Phase 24: pool-growth correctness)
│   │   ├── Preview.lean            (Phase 24: file_variants invariants)
│   │   ├── Tombstone.lean          (Phase 30 NEW: Phase 25 tombstone consistency)
│   │   ├── HistoryPreservation.lean (Phase 30 NEW: Phase 27 Fix 5/6/7)
│   │   └── StreamRouting.lean      (Phase 30 NEW: Phase 27.5 read-stream routing)
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
