# Mossaic Lean 4 Formal Proofs

Mathlib4-backed formal verification of Mossaic VFS invariants in Lean 4.

**Status:** All six target invariants compile with **zero `sorry`** and **zero project `axiom`** declarations. The only axioms used are Lean's three kernel axioms: `propext`, `Classical.choice`, `Quot.sound` (plus Mathlib's transitive use of them).

```bash
cd lean && lake build
# Build completed successfully (575 jobs).
```

```bash
grep -rnE '^axiom|^[[:space:]]+axiom' lean/Mossaic/  # → empty
grep -rn 'sorry' lean/Mossaic/ | grep -v '\-\-'      # → empty
```

## What is proved

### I1 — Refcount validity (must-have, **proved**)

Invariant `validState` (now four-clause, including the numerical equality):

  - **(S1)** `chunks` are unique by hash.
  - **(S2)** `chunk_refs` are unique by composite key.
  - **(S3)** Every `chunk_refs` row's hash has a corresponding `chunks` row.
  - **(S4)** For every chunk row, `refCount = countP (·.chunkHash = c.hash) refs` (the **numerical** equality, proved using `Mathlib.Data.List.Count`).

Theorem: [`Mossaic.Vfs.Refcount.step_preserves_validState`](Mossaic/Vfs/Refcount.lean) — fully unconditional.

The previous build of this directory carried (S4) only as an axiom. The Mathlib4 dependency replaces that axiom with a proof using `List.countP`, `List.countP_append`, `List.countP_cons`, `List.countP_eq_zero`, `List.count_eq_countP`, `List.countP_filter`, `List.countP_congr`, and direct induction on the dropped-refs list inside `deleteChunks`.

Corollary: [`Mossaic.Vfs.Refcount.refCount_zero_implies_no_refs`](Mossaic/Vfs/Refcount.lean) — chunks with `refCount = 0` have zero live refs to their hash. This is the load-bearing fact for I5.

### I3 — Tenant isolation (must-have, **proved**)

Distinct tenants under valid scope produce distinct DO instance names — both `vfsUserDOName` and `vfsShardDOName`.

Theorems:
  - [`Mossaic.Vfs.Tenant.userName_inj`](Mossaic/Vfs/Tenant.lean)
  - [`Mossaic.Vfs.Tenant.shardName_inj_fixed_idx`](Mossaic/Vfs/Tenant.lean)
  - [`Mossaic.Vfs.Tenant.cross_tenant_isolation`](Mossaic/Vfs/Tenant.lean)
  - [`Mossaic.Vfs.Tenant.cross_tenant_user_isolation`](Mossaic/Vfs/Tenant.lean)

Reduces to char-list-level colon-separator splitting. The `[A-Za-z0-9._-]{1,128}` token charset (no `:`) carries the load.

### I5 — GC safety (must-have, **proved**, NO axiom)

  - **(G1)** `alarm` only hard-deletes chunks with `refCount = 0` — unconditional.
  - **(G3)** `alarm` preserves `validState` — **now unconditional** (was axiom-conditional in v1; the I1 numerical equality made the missing piece derivable).
  - **(G4)** Resurrection is preserved.

Theorems:
  - [`Mossaic.Vfs.Gc.alarm_only_deletes_zero_refCount`](Mossaic/Vfs/Gc.lean)
  - [`Mossaic.Vfs.Gc.alarm_preserves_validState`](Mossaic/Vfs/Gc.lean)
  - [`Mossaic.Vfs.Gc.alarm_unmarks_resurrected`](Mossaic/Vfs/Gc.lean)

### I2 — Atomic-write linearizability (now **proved**)

Mossaic's writeFile is a temp-id-then-rename sequence:
  - (W1) Insert `_vfs_tmp_<id>` row with `status = uploading`.
  - (W2) Insert chunk rows tagged by tmp file_id.
  - (W3) Atomic `commitRename`: flips status to `complete` and renames file_name.

`readFile path` filters on `status = complete ∧ file_name = path`, then fetches all chunks tagged by THAT file_id. We prove:

  - [`Mossaic.Vfs.AtomicWrite.readFile_unchanged_under_beginWrite`](Mossaic/Vfs/AtomicWrite.lean) — during (W1)/(W2), readFile returns the same as before.
  - [`Mossaic.Vfs.AtomicWrite.readFile_no_torn_state`](Mossaic/Vfs/AtomicWrite.lean) — readFile chunks are always sourced from a single, well-defined file_id (no mixing).
  - [`Mossaic.Vfs.AtomicWrite.readFile_post_commit_well_formed`](Mossaic/Vfs/AtomicWrite.lean) — after commitRename, readFile result is structurally well-formed.

Caveat: the proofs assume each `Op` in the model is atomic, which mirrors Cloudflare's documented "single-threaded fetch handler" guarantee. We do not prove the runtime semantics themselves.

### I4 — Versioning sortedness & monotonicity (now **proved**)

  - [`Mossaic.Vfs.Versioning.listVersions_sorted`](Mossaic/Vfs/Versioning.lean) — `listVersions` output is `Pairwise (a.mtimeMs ≥ b.mtimeMs)`. Proved via `Mathlib.Data.List.Sort.List.pairwise_mergeSort` and `Mathlib.Data.List.Pairwise.List.Pairwise.sublist`.
  - [`Mossaic.Vfs.Versioning.insertVersion_max_ge`](Mossaic/Vfs/Versioning.lean) — after `insertVersion mtime`, `maxMtime ≥ mtime`.

### I6 — UNIQUE serialization (deferred — out of scope, unchanged)

Would require modeling SQLite's UNIQUE INDEX semantics including SAVEPOINT rollback. Out of scope; documented limitation.

## Architecture: Mathlib4 + TSLean-inspired modeling

This work is **architecturally inspired by [AshishKumar4/TSLean](https://github.com/AshishKumar4/TSLean)** but does **not** depend on it as a runtime.

What we adopt from TSLean:
  - Hand-written state-machine model + `Generated/` delegation pattern.
  - `omega` / `decide` / `simp` proof style.

What we add over TSLean (it does not use Mathlib):
  - **Mathlib4 v4.29.0** as a dep. This is the key change vs. v1 of this directory: it lets us prove the numerical refcount equality (`List.countP`-based) and the listVersions sortedness (`List.pairwise_mergeSort`) directly, without recourse to project axioms.

Why we still **don't** vendor TSLean's transpiler:
  - Mossaic's DOs use raw SQL via `ctx.storage.sql` extensively; TSLean's transpiler can't model SQL semantics.

## Layout

```
lean/
├── README.md                       (this file)
├── lakefile.lean                   (Mathlib v4.29.0 dependency)
├── lean-toolchain                  (leanprover/lean4:v4.29.0)
├── lake-manifest.json              (auto-generated; pinned)
├── Mossaic.lean                    (root, re-exports)
├── Mossaic/
│   ├── Vfs/
│   │   ├── Common.lean             (Hash/PathId/TimeMs aliases, UniqueBy)
│   │   ├── Tenant.lean             (I3)
│   │   ├── Refcount.lean           (I1, full numerical equality)
│   │   ├── Gc.lean                 (I5, no axiom)
│   │   ├── AtomicWrite.lean        (I2, full linearizability)
│   │   └── Versioning.lean         (I4, full sortedness via Mathlib)
│   └── Generated/
│       ├── ShardDO.lean            (re-exports for shard-do.ts)
│       ├── UserDO.lean             (re-exports for user-do.ts)
│       └── Placement.lean          (architectural cross-ref)
└── scripts/
    ├── check-no-sorry.sh           (verifies must-have proofs sorry-free)
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

## How to extend a proof

1. Identify the relevant invariant module under `lean/Mossaic/Vfs/`.
2. Extend the `Op` inductive type and `step` function.
3. Re-prove the relevant invariance theorem for the new case.
4. Add a non-vacuity sanity theorem.
5. Add an `@lean-invariant` comment in the TS source.
6. Run `pnpm verify:proofs`.

## Mathlib build performance

`lake build` cold-cache without Mathlib is ~30s. With Mathlib v4.29.0:

  - **First-ever build (no cache):** ~20-40 minutes (Mathlib has ~5800 source files).
  - **First build with `lake exe cache get` (Azure CDN cache):** ~3-5 minutes.
  - **Warm cache (incremental):** ~2-5 seconds.

The CI workflow uses the official mathlib4 cache via `cd .lake/packages/mathlib && lake exe cache get`.

## CI

`.github/workflows/lean.yml` runs `lake build` + scripts on every push touching `lean/`, `worker/`, or `shared/`. Caches `~/.elan` and `lean/.lake/packages/mathlib/.lake/build`.

## License

Same as parent project.
