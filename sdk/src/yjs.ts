/**
 * `@mossaic/sdk/yjs` — opt-in Yjs adapter.
 *
 * Phase 10 adds per-file CRDT mode to Mossaic. Files toggled into
 * yjs-mode (via `vfs.setYjsMode(path, true)` or
 * `vfs.chmod(path, { yjs: true })`) live as a Yjs op log in the
 * UserDO; live editors connect via a Hibernation-API WebSocket and
 * exchange the standard Yjs sync protocol.
 *
 * This module is published as a separate entry point so the main
 * SDK bundle stays Yjs-free for consumers who don't need live
 * editing — `yjs` is a peerDependency, optional. Import like:
 *
 *     import { openYDoc, VFS_MODE_YJS_BIT } from "@mossaic/sdk/yjs";
 *
 * Design notes:
 *
 * - We do NOT introduce a JSON-RPC layer over the WebSocket. Yjs
 *   sync frames are binary, and adopting `@cloudflare/agents`-style
 *   text-frame envelopes would cost ~33% wire bloat and per-frame
 *   base64 CPU. Every WS frame is a Yjs message tagged with one
 *   byte (sync_step_1 / sync_step_2 / update). See
 *   `worker/objects/user/yjs.ts` for the protocol.
 *
 * - The handle's `Y.Doc` is owned by the caller — we install
 *   protocol listeners and a `close()` method, but we do NOT clone
 *   the doc. The caller can edit it through any Yjs API; updates
 *   are intercepted via `doc.on('update')` and shipped to the
 *   server. Inbound server frames are applied with the standard
 *   `Y.applyUpdate(doc, ...)`.
 *
 * - Reconnection is the consumer's responsibility for v1. We
 *   surface `onClose` and `onError` callbacks so a wrapper layer
 *   can implement back-off + resume. A future minor revision can
 *   add an opt-in auto-reconnect loop.
 */

import * as Y from "yjs";
import {
  Awareness,
  applyAwarenessUpdate,
  encodeAwarenessUpdate,
} from "y-protocols/awareness";
import type { VFS } from "./vfs";
import { VFS_MODE_YJS_BIT as _VFS_MODE_YJS_BIT } from "./yjs-mode-bit";

/**
 * Bit set on `stat.mode` for files in yjs-mode. Re-exported from
 * `./yjs-mode-bit` so consumers can pull it from EITHER the main
 * `@mossaic/sdk` import OR `@mossaic/sdk/yjs`. The two re-exports
 * share a single source-of-truth definition.
 */
export const VFS_MODE_YJS_BIT = _VFS_MODE_YJS_BIT;

// ── Wire protocol tags (mirror worker/objects/user/yjs.ts) ─────────────
const YJS_SYNC_STEP_1 = 0;
const YJS_SYNC_STEP_2 = 1;
const YJS_UPDATE = 2;
/**
 * Phase 13 — awareness relay. Payload is `encodeAwarenessUpdate(...)`
 * from `y-protocols/awareness`; relayed by the server but never
 * persisted (resets on DO eviction; clients re-broadcast on reconnect).
 */
const YJS_AWARENESS = 3;

function encodeUpdateMessage(update: Uint8Array): Uint8Array {
  const out = new Uint8Array(update.byteLength + 1);
  out[0] = YJS_UPDATE;
  out.set(update, 1);
  return out;
}

function encodeSyncStep1(stateVector: Uint8Array): Uint8Array {
  const out = new Uint8Array(stateVector.byteLength + 1);
  out[0] = YJS_SYNC_STEP_1;
  out.set(stateVector, 1);
  return out;
}

function encodeSyncStep2(diff: Uint8Array): Uint8Array {
  const out = new Uint8Array(diff.byteLength + 1);
  out[0] = YJS_SYNC_STEP_2;
  out.set(diff, 1);
  return out;
}

function encodeAwarenessMessage(update: Uint8Array): Uint8Array {
  const out = new Uint8Array(update.byteLength + 1);
  out[0] = YJS_AWARENESS;
  out.set(update, 1);
  return out;
}

/**
 * Handle returned by `openYDoc`. Owns the WebSocket; the `doc`
 * field is the caller-visible `Y.Doc`. Idiomatic usage:
 *
 *     const handle = await openYDoc(vfs, "/notes/today.md");
 *     await handle.synced; // first round of sync complete
 *     handle.doc.getText("content").insert(0, "hello");
 *     handle.awareness.setLocalState({ name: "alice", cursor: 0 });
 *     handle.awareness.on("change", () => render(handle.awareness.getStates()));
 *     // ... later ...
 *     await handle.close();
 */
export interface YDocHandle {
  /**
   * The Y.Doc for this file. Mutate via standard Yjs APIs; updates
   * are streamed to the server automatically.
   */
  readonly doc: Y.Doc;

  /**
   * Phase 13 — y-protocols/awareness instance for cursors,
   * selections, and presence. Local state is set via
   * `awareness.setLocalState({...})` and broadcast to the server,
   * which relays to other connected editors. The server NEVER
   * persists awareness — on DO eviction or restart the state
   * resets and clients re-broadcast on reconnect.
   *
   * Subscribe to remote changes with `awareness.on("change", cb)`
   * or `awareness.on("update", cb)`. Iterate states via
   * `awareness.getStates()` (`Map<clientID, state>`).
   *
   * Destroyed automatically when `handle.close()` is called.
   */
  readonly awareness: Awareness;

  /**
   * Resolves once the initial sync round-trip with the server has
   * completed (we've sent and received sync-step-2). Edits made
   * before this resolves are queued by Yjs and replayed once the
   * doc converges — they are never lost — but `await handle.synced`
   * is the natural moment to render initial UI.
   */
  readonly synced: Promise<void>;

  /**
   * Close the WebSocket and detach the protocol listeners from
   * `doc`. The Y.Doc itself is NOT destroyed — the caller may
   * keep using it offline, or pass it to a fresh `openYDoc` call.
   */
  close(): Promise<void>;

  /**
   * Phase 12: explicit flush — triggers a Yjs compaction on the
   * server whose checkpoint emits a USER-VISIBLE Mossaic version
   * row (when versioning is enabled for the tenant). Optionally
   * attach a human-readable `label` to the version.
   *
   * Opportunistic compactions (every 50 ops or 60s) keep producing
   * `userVisible=0` checkpoints behind the scenes — `flush` is the
   * way to mark a meaningful save point.
   *
   * Returns the new version_id (null if versioning is off for the
   * tenant — the checkpoint still happens, just without a Mossaic
   * version row) and the seq number of the checkpoint.
   */
  flush(opts?: { label?: string }): Promise<{
    versionId: string | null;
    checkpointSeq: number;
  }>;

  /**
   * Optional: register a callback for the underlying socket close.
   * Useful for reconnection logic. Called at most once per handle.
   */
  onClose(cb: (event: CloseEvent | { code: number; reason: string }) => void): void;

  /**
   * Optional: register a callback for socket errors. Called at most
   * once per handle.
   */
  onError(cb: (err: unknown) => void): void;
}

/**
 * Options for `openYDoc`.
 */
export interface OpenYDocOptions {
  /**
   * Bring-your-own Y.Doc — useful if you've already created one
   * (e.g. with awareness or other addons attached) and want to bind
   * it to a Mossaic file. If omitted, we create a fresh `new Y.Doc()`.
   */
  doc?: Y.Doc;
}

/**
 * Open a live Yjs editing session against a yjs-mode file in
 * Mossaic. Throws `EINVAL` if the path is not a regular file in
 * yjs-mode (toggle with `vfs.setYjsMode(path, true)` first).
 *
 * The returned handle owns the WebSocket; closing it via
 * `handle.close()` is the caller's responsibility. Multiple
 * concurrent `openYDoc` calls against the same path on the same
 * client share the server-side Y.Doc (the UserDO has one
 * `YjsRuntime` per tenant, one Y.Doc per pathId), but each gets
 * its own client-side Y.Doc + socket — coordinate locally if you
 * want to share editor state across calls.
 */
export async function openYDoc(
  vfs: VFS,
  path: string,
  opts: OpenYDocOptions = {}
): Promise<YDocHandle> {
  // The VFS class exposes a typed accessor that returns the
  // upgrade Response. We DO NOT use the public `fetch()`
  // surface — the consumer's worker dispatches RPC method calls
  // against the bound DO namespace directly.
  const response = await vfs._openYjsSocketResponse(path);
  const ws = response.webSocket;
  if (!ws) {
    throw new Error(
      "openYDoc: server did not return a WebSocket. " +
        "Confirm the path is a regular file in yjs-mode " +
        "(vfs.setYjsMode(path, true))."
    );
  }
  ws.accept();

  const doc = opts.doc ?? new Y.Doc();
  const awareness = new Awareness(doc);

  // Install the outbound update pump. Origin === ws means a frame
  // from this very socket — don't echo back.
  const onLocalUpdate = (update: Uint8Array, origin: unknown) => {
    if (origin === ws) return;
    try {
      ws.send(encodeUpdateMessage(update));
    } catch {
      /* socket may be closing; close handler will reject .synced */
    }
  };
  doc.on("update", onLocalUpdate);

  // Phase 13 — outbound awareness pump. y-protocols emits an
  // `update` event whenever the local Awareness state changes
  // (added/updated/removed clientIDs). We send the encoded update
  // to the server which relays to other peers. Origin === ws means
  // the update arrived FROM the server; don't echo back.
  const onLocalAwareness = (
    {
      added,
      updated,
      removed,
    }: { added: number[]; updated: number[]; removed: number[] },
    origin: unknown
  ) => {
    if (origin === ws) return;
    const ids = [...added, ...updated, ...removed];
    if (ids.length === 0) return;
    try {
      const payload = encodeAwarenessUpdate(awareness, ids);
      ws.send(encodeAwarenessMessage(payload));
    } catch {
      /* socket may be closing */
    }
  };
  awareness.on("update", onLocalAwareness);

  // Initial sync: send our state vector. Server will reply with
  // sync-step-2 (the diff we need) and its own sync-step-1.
  let resolveSynced: () => void;
  let rejectSynced: (err: unknown) => void;
  const synced = new Promise<void>((res, rej) => {
    resolveSynced = res;
    rejectSynced = rej;
  });
  let gotServerStep2 = false;

  const onMessage = (ev: MessageEvent) => {
    const data = ev.data;
    if (typeof data === "string") return; // we never send text frames
    const bytes =
      data instanceof ArrayBuffer
        ? new Uint8Array(data)
        : data instanceof Uint8Array
          ? data
          : null;
    if (!bytes || bytes.byteLength === 0) return;
    const tag = bytes[0];
    const body = bytes.subarray(1);
    switch (tag) {
      case YJS_SYNC_STEP_1: {
        // Server's state vector — reply with our diff.
        const diff = Y.encodeStateAsUpdate(doc, body);
        ws.send(encodeSyncStep2(diff));
        return;
      }
      case YJS_SYNC_STEP_2: {
        // Server's diff for us — apply, mark synced.
        Y.applyUpdate(doc, body, ws);
        if (!gotServerStep2) {
          gotServerStep2 = true;
          resolveSynced();
        }
        return;
      }
      case YJS_UPDATE: {
        Y.applyUpdate(doc, body, ws);
        return;
      }
      case YJS_AWARENESS: {
        // Phase 13 — apply remote awareness update. Origin === ws
        // tells our outbound pump (`onLocalAwareness`) not to echo
        // this back to the server.
        applyAwarenessUpdate(awareness, body, ws);
        return;
      }
      default:
        // Unknown — forward-compat ignore.
        return;
    }
  };
  ws.addEventListener("message", onMessage);

  // Kick off the handshake.
  ws.send(encodeSyncStep1(Y.encodeStateVector(doc)));

  let closeCb: ((event: CloseEvent | { code: number; reason: string }) => void) | null = null;
  let errorCb: ((err: unknown) => void) | null = null;
  let closed = false;

  ws.addEventListener("close", (ev: Event) => {
    if (closed) return;
    closed = true;
    doc.off("update", onLocalUpdate);
    awareness.off("update", onLocalAwareness);
    awareness.destroy();
    if (!gotServerStep2) {
      rejectSynced(new Error("openYDoc: socket closed before initial sync"));
    }
    const closeEv = ev as CloseEvent;
    if (closeCb) {
      closeCb({
        code: closeEv.code ?? 1006,
        reason: closeEv.reason ?? "",
      });
    }
  });
  ws.addEventListener("error", (ev: Event) => {
    if (errorCb) errorCb(ev);
    if (!gotServerStep2) rejectSynced(ev);
  });

  return {
    doc,
    awareness,
    synced,
    async close(): Promise<void> {
      if (closed) return;
      closed = true;
      doc.off("update", onLocalUpdate);
      awareness.off("update", onLocalAwareness);
      awareness.destroy();
      try {
        ws.close(1000, "client close");
      } catch {
        /* already closing */
      }
    },
    async flush(flushOpts?: { label?: string }): Promise<{
      versionId: string | null;
      checkpointSeq: number;
    }> {
      // Phase 12: call the typed RPC. The compaction snapshot
      // captures whatever the live Y.Doc currently holds — local
      // edits made on this handle are streamed to the server via
      // the open WebSocket BEFORE flush() runs (each Yjs update
      // is emitted synchronously by `doc.transact`), so the
      // compaction sees them.
      return vfs._flushYjs(path, flushOpts);
    },
    onClose(cb) {
      closeCb = cb;
    },
    onError(cb) {
      errorCb = cb;
    },
  };
}
