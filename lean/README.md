# Mossaic Lean 4 Formal Proofs

Formal verification of selected Mossaic VFS invariants in Lean 4.

**Status:** Must-have invariants (I1, I3, I5) compile with zero `sorry`. Stretch goals (I2, I4) partially complete — see [Status](#status) below.

**Audience:** anyone reading the [Mossaic VFS audit](../local/audit-report.md) who wants machine-checked confirmation that the structural invariants survive the operations they're advertised to survive.

## Quick start

```bash
# Install Lean toolchain (one-time, ~250MB)
curl -sSf https://raw.githubusercontent.com/leanprover/elan/master/elan-init.sh \
  | sh -s -- -y --default-toolchain none
export PATH=$HOME/.elan/bin:$PATH

# Build the proofs
cd lean && lake build

# Or from repo root:
pnpm lean:build
```

Cold-cache build time: **~30 seconds** (no Mathlib). Warm cache: **~5 seconds**.

## What is and is NOT proved

### I1 — Refcount well-formedness (must-have, **proved**)

For every shard state reachable from `empty` via any sequence of `putChunk` / `deleteChunks` operations:

  - **(S1)** `chunks` are unique by hash.
  - **(S2)** `chunk_refs` are unique by composite key `(chunkHash, fileId, chunkIndex)`.
  - **(S3)** Every `chunk_refs` row has a corresponding `chunks` row (no dangling refs).

Theorem: [`Mossaic.Vfs.Refcount.step_preserves_validState`](Mossaic/Vfs/Refcount.lean) — fully unconditional.

**What is NOT in I1:** the audit's full numerical statement (`refCount = |chunk_refs filtered by hash|`) is **NOT proved here**. Discharging it in Lean 4 stdlib (no Mathlib) would require multiset / `Finset.sum` reasoning that the plain stdlib does not offer. The structural part (S1-S3) IS proved unconditionally and rules out the most critical leak modes (dangling refs, duplicate chunks, duplicate ref rows). The numerical equality is captured as an explicit `axiom` (`numerical_refcount_dangling_axiom`) in `Gc.lean`, used only by the GC safety theorem.

### I3 — Tenant isolation (must-have, **proved**)

Distinct tenants under valid scope produce distinct DO instance names — both `vfsUserDOName` and `vfsShardDOName`.

Theorems:

  - [`Mossaic.Vfs.Tenant.userName_inj`](Mossaic/Vfs/Tenant.lean) — UserDO name injectivity on valid scopes.
  - [`Mossaic.Vfs.Tenant.shardName_inj_fixed_idx`](Mossaic/Vfs/Tenant.lean) — ShardDO name injectivity at fixed shard index.
  - [`Mossaic.Vfs.Tenant.cross_tenant_isolation`](Mossaic/Vfs/Tenant.lean) — corollary: distinct tenants ⇒ distinct shard names.
  - [`Mossaic.Vfs.Tenant.cross_tenant_user_isolation`](Mossaic/Vfs/Tenant.lean) — same for UserDO.

The proof reduces to char-list-level colon-separator splitting, leveraging the `[A-Za-z0-9._-]{1,128}` token charset (no `:`) to derive component-wise equality.

**What is NOT in I3:** full bidirectional `(scope, idx) ↔ name` bijection. The shard-index suffix involves `Nat.toString`, whose injectivity has no stdlib lemma in Lean 4.29 without Mathlib. Tenant isolation does not require it: we hold idx fixed across the comparison.

### I5 — GC safety (must-have, **proved**)

  - **(G1)** `alarm` only hard-deletes chunks whose `refCount = 0` — *unconditional*.
  - **(G3)** `alarm` preserves `validState` — *conditional* on `numerical_refcount_dangling_axiom`.
  - **(G4)** Resurrected chunks (refCount > 0 with deletedAt set) survive the sweep with `deletedAt` cleared — *unconditional*.

Theorems:

  - [`Mossaic.Vfs.Gc.alarm_only_deletes_zero_refCount`](Mossaic/Vfs/Gc.lean)
  - [`Mossaic.Vfs.Gc.alarm_preserves_validState`](Mossaic/Vfs/Gc.lean)
  - [`Mossaic.Vfs.Gc.alarm_unmarks_resurrected`](Mossaic/Vfs/Gc.lean)

### I2 — Atomic-write commit (stretch, **deferred**)

Not proved. Requires modeling 4-state `FileStatus` discriminated union + temp-id-then-rename two-phase commit + linearizability over interleaved reader Ops. Estimated 200-400 LoC beyond what was budgeted.

See [`Mossaic.Vfs.AtomicWrite`](Mossaic/Vfs/AtomicWrite.lean) for the type stub and a clear blocker comment.

### I4 — Versioning monotonicity (stretch, **partial**)

Proved:

  - **(V0)** Step semantics (`insertVersion` appends; `dropVersion` filters).
  - **(V3)** After `insertVersion mtime`, `maxMtime` of the path is `some m ≥ mtime`. Theorem: [`insertVersion_max_ge`](Mossaic/Vfs/Versioning.lean).

NOT proved (deferred to follow-up):

  - **(V1)** `listVersions` is sorted — would require `List.mergeSort_sorted` not present in stdlib.
  - Full `restoreVersion ⇒ current.mtime ≥ source.mtime` chain — composes V3 with chunksAlive guard from C2.

### I6 — UNIQUE serialization (deferred — out of scope)

Would require modeling SQLite's UNIQUE INDEX semantics including SAVEPOINT rollback. The load-bearing axiom is `SQLite enforces declared UNIQUE constraints atomically within a transaction`, which is a property of the SQLite engine, not of Mossaic's TypeScript. We do not attempt to model SQLite.

## Architecture: TSLean-inspired, transpiler-free

This work is **architecturally inspired by [AshishKumar4/TSLean](https://github.com/AshishKumar4/TSLean)** but does **not** depend on it as a runtime.

What we adopt from TSLean:

  - The shape of the state-machine model: `structure DOState`, `def step : State → Op → State`, `theorem invariant_preserved : ∀ s op, validState s → validState (step s op)`.
  - The plain Lean 4.29.0 + `omega` / `decide` / `simp` proof style — no Mathlib.
  - The separation between hand-written semantic models and `Generated/` delegation wrappers.

Why we do **not** vendor TSLean:

  - **The transpiler can't model SQL.** Mossaic's DOs use raw SQL via `ctx.storage.sql` extensively. TSLean's transpiler treats `ctx.storage.sql.exec("INSERT INTO ...")` as opaque — it would produce Lean code that "tracks TS shape" without modeling data movement, and proofs over it would be vacuous about VFS behavior. The TSLean DO models in `lean/TSLean/DurableObjects/RateLimiter.lean` are themselves hand-written, not auto-transpiled; their `Generated/RateLimiter.lean` thin wrapper merely delegates to a pre-built abstraction.
  - **Bus factor.** TSLean is single-author, 1 star. Acceptable to borrow ideas, not to pin a runtime dep.
  - **No transpiler runtime overhead.** Direct hand-written Lean is cheaper per LoC for our subset.

## Layout

```
lean/
├── README.md                       (this file)
├── lakefile.toml                   (no external deps, no Mathlib)
├── lean-toolchain                  (leanprover/lean4:v4.29.0)
├── Mossaic.lean                    (root, re-exports everything)
├── Mossaic/
│   ├── Vfs/
│   │   ├── Common.lean             (Hash/PathId/TimeMs aliases, UniqueBy)
│   │   ├── Tenant.lean             (I3)
│   │   ├── Refcount.lean           (I1, structural part)
│   │   ├── Gc.lean                 (I5)
│   │   ├── AtomicWrite.lean        (I2 stretch — deferred, type stubs only)
│   │   └── Versioning.lean         (I4 stretch — partial)
│   └── Generated/
│       ├── ShardDO.lean            (re-exports for shard-do.ts)
│       ├── UserDO.lean             (re-exports for user-do.ts / vfs-ops.ts)
│       └── Placement.lean          (architectural cross-ref for placement.ts)
└── scripts/
    ├── check-no-sorry.sh           (verifies must-have proofs sorry-free)
    └── check-xrefs.sh              (verifies @lean-invariant TS comments resolve)
```

## TS ↔ Lean cross-references

Every TS function whose correctness this build proves carries a `@lean-invariant` JSDoc tag:

```ts
/**
 * @lean-invariant Mossaic.Generated.ShardDO.alarm_safe
 *   The Lean model proves alarm preserves validState ...
 */
async alarm(): Promise<void> { ... }
```

The CI script `lean/scripts/check-xrefs.sh` enforces that every annotation resolves to an actual `theorem` in the named Lean module. If a Lean theorem is renamed/deleted but the TS annotation isn't updated, CI fails.

Annotations in scope at HEAD:

| TS file:line | Annotation |
|---|---|
| `worker/lib/utils.ts:82-105` | `@lean-invariant Mossaic.Generated.UserDO.cross_tenant_user_isolation` |
| `worker/lib/utils.ts:106-126` | `@lean-invariant Mossaic.Generated.UserDO.cross_tenant_shard_isolation` |
| `worker/objects/shard/shard-do.ts:180-258` | `@lean-invariant Mossaic.Generated.ShardDO.chunk_invariant_preserved` |
| `worker/objects/shard/shard-do.ts:324-365` | `@lean-invariant Mossaic.Generated.ShardDO.chunk_invariant_preserved` |
| `worker/objects/shard/shard-do.ts:402-430` | `@lean-invariant Mossaic.Generated.ShardDO.alarm_safe` |
| `worker/objects/shard/shard-do.ts:402-430` | `@lean-invariant Mossaic.Generated.ShardDO.alarm_only_deletes_zero` |
| `worker/objects/user/vfs-versions.ts:166-200` | `@lean-invariant Mossaic.Generated.UserDO.insertVersion_advances` |

## Limitations and out-of-scope

Things we explicitly **do not** prove, by design:

  - **Hash collision-resistance.** SHA-256 is treated as an opaque `String` type. No theorem in this build relies on collision resistance.
  - **Cloudflare DO runtime semantics.** We assume each `Op` is atomic at the model level, mirroring Cloudflare's documented "single-threaded fetch handler" guarantee. We do not prove the runtime semantics themselves.
  - **Wall-clock alarm timeliness.** The 30s grace window is modeled as a `now : TimeMs` parameter. We do not prove the alarm fires within any wall-clock bound.
  - **SQLite engine semantics.** UNIQUE constraints, SAVEPOINT rollback, and transactional atomicity are SQLite properties, not properties of Mossaic's TypeScript. We do not model SQLite.
  - **Concurrent subrequest cap.** Workers' 50/1000 subrequest limits are operational concerns outside the proof scope.
  - **Cross-shard placement stability** (audit H4). Per-shard refcount holds; cross-shard "same content lands on same shard under pool growth" is a different invariant that the audit explicitly flagged as broken. We do not contradict the audit.
  - **Numerical refcount = liveRefs equality.** Captured as the explicit axiom `numerical_refcount_dangling_axiom`, used only by I5. The structural well-formedness is proved unconditionally.

## How to extend a proof

If you add a TS function whose correctness should be machine-checked:

1. Identify the relevant invariant module under `lean/Mossaic/Vfs/`.
2. Extend the `Op` inductive type with a constructor for the new operation.
3. Extend the `step` function with the corresponding case, mirroring the SQL effects of the TS function one-for-one.
4. Re-prove `step_preserves_validState` (or the relevant theorem) for the new case.
5. Add a non-vacuity sanity theorem: prove that `step s newOp ≠ s` for at least one concrete `s`.
6. Add an `@lean-invariant Mossaic.Generated.<Module>.<theorem>` comment in the TS source.
7. Run `pnpm lean:build`. Both `lake build` and `check-xrefs.sh` must pass.

## Why no Mathlib

Mathlib4 is a fantastic library, but for state-machine invariant proofs at this scale:

  - Cold-cache build: 5-15 minutes per CI job (vs. ~30s without).
  - Hundreds of MB of dependency.
  - Most of the value (Multiset, Finset, group theory, etc.) is irrelevant to ours.

What we'd want from Mathlib (and currently lack) is mostly:

  - `List.countP` / `Multiset` for the numerical refcount equality.
  - A few minor lemmas like `List.mergeSort_sorted`.

Adding Mathlib later is a single-line `lake-manifest.json` change. We start without it to keep the build feedback loop tight.

## CI

GitHub Actions workflow at `.github/workflows/lean.yml` runs `lake build` + the two scripts on every push touching `lean/`, `worker/`, or `shared/`.

## License

Same as the parent project.
