/-
Mossaic.Vfs.AtomicWrite — I2: Atomic-write commit. STRETCH GOAL — DEFERRED.

Models (intended):
  worker/objects/user/vfs-ops.ts:1436-1506 (commitRename)
  worker/objects/user/vfs-ops.ts:959-960   (findLiveFile, status='complete')
  worker/objects/user/vfs-ops.ts:701-723   (readFile chunk loop)
  worker/objects/user/user-do.ts:200-216   (UNIQUE INDEX serialization)

Audit reference:
  /workspace/local/audit-report.md §I2 (verdict: holds in code at HEAD).

Intended invariant:
  For any sequence of operations, no `readFile(path)` ever observes a
  state where some chunks are pre-commit and others are post-commit.
  A read returns either the pre-commit content, the post-commit content,
  or ENOENT — never a "torn" mixture.

Status: DEFERRED for this build.

Blocker:
  Modeling I2 faithfully requires:
    (a) a 4-state `FileStatus` discriminated union (uploading | complete
        | superseded | deleted),
    (b) the temp-id-then-rename two-phase commit as a sequence of state
        transitions where the `commitRename` step is atomic,
    (c) a `readFile` operation that filters on `status='complete'`,
    (d) interleaving semantics: the proof needs to argue that no
        intermediate state is observable to a concurrent reader.

  Mossaic's atomicity claim relies on Cloudflare DO single-threaded
  execution (each fetch/RPC method runs to completion before the next
  begins). At the model level, we already implicitly assume each `Op` is
  atomic — but I2 is about a COMPOUND operation (writeFile = beginWrite
  + N×putChunk + commitRename) being atomic *as a group*.

  To prove this honestly, we would need:
    - A trace-based model where `writeFile` is a single Op composed of
      sub-ops, OR
    - A linearizability argument over interleaved reader Ops, OR
    - An invariant of the form "every observable state has a coherent
      readFile output".

  Each is 200-400 LoC and depends on the I1 numerical refcount equality
  (axiomatized in Gc.lean) for the cleanup paths.

  Effort estimate for a clean version: 3-5 person-days of Lean work,
  beyond what the must-have phase budgeted. We choose to ship the
  must-haves (I1 structural, I3 tenant, I5 GC) cleanly rather than
  ship I2 with sorrys.

Recommendation:
  Lift I2 to a follow-up issue. Expand the `Mossaic.Generated.UserDO`
  thin wrapper to expose `writeFile` and `readFile` operations once the
  model is built; for now, the wrapper is empty.

What this file provides:
  - Type stubs (FileStatus, FileRow) so downstream files can compile.
  - No theorems. No `sorry`. No `axiom` beyond what's already in scope.

When this file is filled in, the theorem to target is:

    theorem readFile_atomic
        (s : UserState) (op : UserOp) (p : Path) :
      let r₁ := readFile s p
      let s' := step s op
      let r₂ := readFile s' p
      r₂ = r₁ ∨ r₂ = postCommit_value s op p ∨ r₂ = ENOENT

  along with non-vacuity:

    theorem readFile_atomic_nonvacuous :
      ∃ s op p, readFile (step s op) p ≠ readFile s p
-/

import Mossaic.Vfs.Common

namespace Mossaic.Vfs.AtomicWrite

open Mossaic.Vfs.Common

/-- Mirrors the `status` column of `files` table. shared/types.ts and
worker/objects/user/files.ts. Provided as a type-level placeholder so
follow-up work can extend without redefining. -/
inductive FileStatus where
  | uploading
  | complete
  | superseded
  | deleted
  deriving DecidableEq, Repr

end Mossaic.Vfs.AtomicWrite
