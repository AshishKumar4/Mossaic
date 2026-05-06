/**
 * Phase 14 — Yjs ambient module shim.
 *
 * Workspace consumers that don't need live collaborative editing
 * may not install `yjs` or `y-protocols` (they're optional peer
 * deps). But TypeScript still typechecks the SDK-reachable closure,
 * which transitively reaches `worker/core/objects/user/yjs.ts` —
 * a file that statically imports from `yjs` and `y-protocols/awareness`.
 *
 * Without this shim, those consumers see TS2307 ("Cannot find
 * module 'yjs'") even though the runtime path is dead-code-eliminated
 * by the dynamic-import boundary in `user-do-core.ts`.
 *
 * The shim declares ONLY the surface area that
 * `worker/core/objects/user/yjs.ts` actually consumes. Consumers
 * with the real `yjs`/`y-protocols` packages installed get the real
 * types — npm module resolution prefers `node_modules/yjs/dist/yjs.d.ts`
 * over an ambient module declaration.
 *
 * Pure typecheck artifact. Zero runtime cost.
 */

declare module "yjs" {
  /** Top-level Y.Doc — the CRDT document. */
  export class Doc {
    constructor();
    getText(name: string): Text;
    transact(fn: () => void, origin?: unknown): void;
    on(event: "update", cb: (update: Uint8Array, origin: unknown) => void): void;
    off(event: "update", cb: (update: Uint8Array, origin: unknown) => void): void;
    clientID: number;
  }

  /** Y.Text — the only structured type we use server-side. */
  export class Text {
    insert(index: number, content: string): void;
    delete(index: number, length: number): void;
    toString(): string;
    length: number;
  }

  /** Encode the doc's current state for a peer with state vector `sv`. */
  export function encodeStateAsUpdate(doc: Doc, sv?: Uint8Array): Uint8Array;
  /** Encode the doc's state vector (for sync-step-1). */
  export function encodeStateVector(doc: Doc): Uint8Array;
  /** Apply a remote update bytes-blob to the local doc. */
  export function applyUpdate(doc: Doc, update: Uint8Array, origin?: unknown): void;
}

declare module "y-protocols/awareness" {
  import type { Doc } from "yjs";

  /** Awareness — Yjs's presence channel. */
  export class Awareness {
    constructor(doc: Doc);
    getStates(): Map<number, unknown>;
    setLocalState(state: unknown): void;
    on(
      event: "update",
      cb: (
        changed: { added: number[]; updated: number[]; removed: number[] },
        origin: unknown
      ) => void
    ): void;
    off(
      event: "update",
      cb: (
        changed: { added: number[]; updated: number[]; removed: number[] },
        origin: unknown
      ) => void
    ): void;
    /** Tear down listeners + cancel timers. */
    destroy(): void;
  }

  export function applyAwarenessUpdate(
    awareness: Awareness,
    update: Uint8Array,
    origin: unknown
  ): void;

  export function encodeAwarenessUpdate(
    awareness: Awareness,
    clients: number[]
  ): Uint8Array;

  export function removeAwarenessStates(
    awareness: Awareness,
    clients: number[],
    origin: unknown
  ): void;
}
