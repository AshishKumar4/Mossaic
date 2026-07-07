# Mossaic Lean 4 Models And Proofs

This directory contains Mathlib-backed Lean 4 proofs about hand-written
abstract models of selected Mossaic VFS behavior.

These proofs do **not** establish that the TypeScript/SQL/Durable Object
implementation refines the Lean models. Read the
[`formal-verification-boundary.md`](../docs/formal-verification-boundary.md)
before relying on a theorem as an implementation guarantee.

## Current Status

- The exact public top-level theorem count and declaration list are generated in
  [`THEOREM_INVENTORY.md`](./THEOREM_INVENTORY.md).
- `Mossaic.lean` imports every proof module; the inventory gate fails if a module
  is omitted.
- The full imported corpus builds with the pinned Lean 4 and Mathlib versions.
- CI rejects executable `sorry`/`admit` and project `axiom` declarations.
- Kernel/Mathlib axioms and opaque model inputs remain part of the trusted
  computing base.

Run all proof checks from the repository root:

```bash
pnpm verify:proofs
```

Regenerate and check the inventory with:

```bash
bash lean/scripts/theorem-inventory.sh --write
bash lean/scripts/theorem-inventory.sh --check
```

## Modeled Properties

- `Refcount.lean`: uniqueness, ref-to-chunk existence, numerical refcount
  equality, modeled transition preservation, and exact same-ref put idempotence.
- `Gc.lean`: sweep properties over the abstract shard state.
- `Tenant.lean`: injectivity properties for the modeled scope/name encoding.
- `AtomicWrite.lean`: pre-commit invisibility, single-file-id chunk sourcing,
  and an explicit two-transition visibility boundary. It is not a full trace
  linearizability or implementation-refinement proof.
- `Versioning.lean` and `HistoryPreservation.lean`: selected ordering,
  monotonicity, and append/preservation properties in list-based models.
- `Multipart.lean`: exact same-ref state idempotence, modeled manifest
  index/hash completeness on successful commit, refcount preservation, and
  selected versioning effects.
- `Folder.lean`: an explicit model mapping covered mutation constructors to
  parent revision changes and structured cache-identity changes.
- `Cache.lean`: changed abstract bust state under an explicit changed-signal
  premise plus concrete key-rendering witnesses. It does not prove TypeScript
  write coverage or general string-key injectivity.
- Other modules cover selected quota, preview, tombstone, stream-routing,
  token-shape, RPC batching, Yjs-prefix, and encryption-blind refcount
  properties exactly as stated in their theorem signatures.

## Important Limits

- Models are manually authored, not extracted from production code.
- `Generated/` is a hand-written delegation/re-export layer, not generated
  implementation semantics.
- Placement hashing, SQLite semantics, cross-DO failures, JavaScript execution,
  cryptography, alarms, and implementation refinement are not modeled.
- Opaque HMAC/token predicates model verification outcomes; they do not prove
  HMAC or JWT security.
- A concrete `decide` witness establishes only that concrete case.
- Preconditions such as `validState`, freshness, changed signals, and row-shape
  invariants are material parts of the result.

## TypeScript Cross-References

`@lean-invariant` annotations are checked by
`lean/scripts/check-xrefs.sh`. The gate verifies that each name is an imported
Lean theorem declaration. It does **not** verify semantic correspondence between
the annotated TypeScript and the theorem.

## Extending The Corpus

1. State the implementation-independent safety property and its assumptions.
2. Extend the smallest relevant abstract state and transition.
3. Prove the property without `sorry`, `admit`, project axioms, tautologies, or
   hypothesis-equals-conclusion statements.
4. Add a non-trivial witness when it demonstrates reachability or liveness.
5. Add an xref only when its comment states the abstraction boundary precisely.
6. Regenerate the inventory and run `pnpm verify:proofs`.
