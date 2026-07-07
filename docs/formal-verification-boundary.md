# Formal Verification Boundary

Mossaic includes Lean 4 proofs about hand-written abstract models. The Lean
kernel machine-checks those theorem statements and proof terms. This is useful
assurance about the models, but it is not a mechanical proof that the
TypeScript, SQL, Durable Object, or deployed Worker implementation conforms to
them.

## Machine-Checked Scope

The imported corpus is rooted at `lean/Mossaic.lean`. Its generated inventory
is committed as [`lean/THEOREM_INVENTORY.md`](../lean/THEOREM_INVENTORY.md) and
is checked in CI. The models cover properties including:

- Refcount table well-formedness under the modeled shard transitions.
- Name-level tenant separation for the modeled scope encoding.
- GC behavior over the modeled chunk/refcount state.
- Version-list ordering and selected history-preservation transitions.
- Same-ref `putChunk` idempotence as exact abstract state equality.
- Multipart index/hash completeness when the modeled manifest gate succeeds.
- Folder revision changes for the mutation constructors explicitly included in
  the folder model.
- Visibility before and after the two atomic transitions in the atomic-write
  model.
- Selected routing, quota, tombstone, preview, token-shape, Yjs-prefix, and
  batching properties stated in the individual Lean modules.

Each guarantee is only as strong as its literal theorem statement. A theorem
about `validState` preservation is not state equality, a theorem with a
changed-signal premise does not prove the implementation changes that signal,
and a concrete witness is not a general injectivity theorem.

## Not Machine-Checked

The following are outside the current formal result:

- There is no generated or extracted Lean semantics for the TypeScript.
- There is no proved refinement relation from TypeScript, JavaScript execution,
  SQL statements, migrations, or Durable Object storage to the Lean states and
  transitions.
- `lean/Mossaic/Generated/` contains hand-written delegation/re-export modules;
  the name does not mean generated from production source.
- `@lean-invariant` annotations are checked for imported theorem-name
  resolution only. They do not establish semantic correspondence.
- SQLite uniqueness, transaction, rollback, savepoint, query, and migration
  semantics are not modeled.
- Cloudflare Durable Object scheduling, input/output gates, cross-object RPC,
  retries, partial failures, process restarts, and alarm delivery are not
  modeled.
- JavaScript behavior, promises, exceptions, typed-array behavior, serialization,
  and library correctness are not modeled.
- Rendezvous placement hashing and its distribution or collision behavior are
  not modeled. The quota model uses simpler abstract postconditions.
- SHA-256, HMAC, JWT, HKDF, AES-GCM, Web Crypto, and `jose` correctness and
  security are not proved. Opaque token predicates in Lean are model inputs,
  not cryptographic proofs.
- Full trace linearizability, multipart rollback/commit completeness, alarm
  idempotence, and cross-DO failure atomicity are not proved.
- Cache write-path coverage and arbitrary serialized cache-key injectivity are
  not proved.

## Trusted Computing Base

Current assurance trusts:

- The Lean kernel, pinned Lean toolchain, Mathlib, Lake, and the host used to run
  them.
- The hand-written correspondence chosen by model authors between implementation
  concepts and Lean definitions.
- Every abstraction premise in theorem statements, including freshness,
  validity, liveness, changed-signal, and row-shape assumptions.
- Opaque model functions such as token verification predicates.
- The inventory and source gates for coverage reporting. These gates catch
  omissions, stale names, `sorry`/`admit`, and project `axiom` declarations;
  they do not judge whether a theorem is the right specification.

## Gates

Run the full local proof gate with:

```bash
pnpm verify:proofs
```

It runs `lake build`, the executable `sorry`/`admit` and project-`axiom`
checks, imported xref resolution, and generated theorem-inventory validation.

## Path To Implementation-Level Verification

End-to-end verification requires a larger formal-methods program:

1. Define or adopt executable semantics for the supported TypeScript/JavaScript
   subset, Workers APIs, Durable Object scheduling, and the SQL fragment used by
   Mossaic.
2. Generate/extract implementation semantics or verify a small implementation
   core whose outputs are consumed by TypeScript through a narrow interface.
3. Specify storage schemas, transactions, migrations, cross-DO protocols,
   retries, crashes, and alarms, including safety and liveness properties.
4. Prove a refinement/simulation from each implementation transition and
   failure path to the abstract model.
5. Connect cryptographic operations to verified implementations and explicit
   security assumptions.
6. Make CI prove that the exact shipped source and generated proof artifact are
   linked, reproducible, and cannot drift independently.

Until those steps exist, the correct assurance statement is: Mossaic has a
machine-checked corpus of abstract-model properties, not a fully formally
verified implementation.
