/**
 * `@mossaic/sdk/yjs` — opt-in Yjs adapter.
 *
 * Adds per-file CRDT mode to Mossaic. Files toggled into
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
import { VFS_MODE_YJS_BIT } from "../../shared/constants";
import { ENOENT } from "./errors";

/**
 * Bit set on `stat.mode` for files in yjs-mode. Re-exported here so
 * consumers can pull it from EITHER the main `@mossaic/sdk` import
 * OR `@mossaic/sdk/yjs`. Single source-of-truth: shared/constants.ts.
 */
export { VFS_MODE_YJS_BIT };

// ── Wire protocol tags (mirror worker/objects/user/yjs.ts) ─────────────
const YJS_SYNC_STEP_1 = 0;
const YJS_SYNC_STEP_2 = 1;
const YJS_UPDATE = 2;
/**
 * awareness relay. Payload is `encodeAwarenessUpdate(...)`
 * from `y-protocols/awareness`; relayed by the server but never
 * persisted (resets on DO eviction; clients re-broadcast on reconnect).
 */
const YJS_AWARENESS = 3;

/**
 * server → client advisory: "your op-log is approaching
 * the compaction threshold; please run vfs.compactYjs(path)". Payload
 * is a uint32 BE seq count. The SDK exposes this as the
 * `handle.onCompactNeeded?` callback.
 */
const YJS_COMPACT_PLEASE = 4;

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
   * y-protocols/awareness instance for cursors,
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
   * explicit flush — triggers a Yjs compaction on the
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

  /**
   * true iff the underlying file is encrypted. The SDK
   * encrypts outbound sync_step_2 / update / awareness frames and
   * decrypts inbound ones. Server-side compaction is disabled for
   * encrypted yjs files; the consumer is responsible for calling
   * `vfs.compactYjs(path)` when {@link onCompactNeeded} fires or on
   * a timer.
   */
  readonly encrypted: boolean;

  /**
   * register a callback for the server's compact-please
   * advisory (tag-4 frame). The argument is the current op-log seq
   * count. Called at most once per handle until manually re-armed.
   *
   * Plain (non-encrypted) yjs files compact server-side automatically
   * — this advisory is fired ONLY for encrypted files.
   */
  onCompactNeeded(cb: (seqCount: number) => void): void;
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
  // detect encryption status BEFORE the WS upgrade so that
  // a config-less consumer attempting to open an encrypted yjs file
  // gets EACCES synchronously (no WS connection wasted; no opaque
  // frames flowing through the consumer's reactive layer). For
  // plaintext yjs files behaviour is identical to the no-encryption path.
  let fileEnc: { mode: "convergent" | "random"; keyId?: string } | undefined;
  try {
    const stat = await vfs.stat(path);
    fileEnc = stat.encryption;
  } catch (err) {
    // Only swallow ENOENT (the documented "file doesn't exist yet"
    // case — openYDoc creates the file via the WS upgrade). Any
    // other stat error propagates so the consumer sees a typed
    // error rather than a silently-degraded plaintext session. A
    // bare `catch {}` would also eat transient errors (network
    // blip, EBUSY, EAGAIN, etc.); on an encrypted-tenant deployment
    // that would leave `fileEnc = undefined` and the subsequent
    // code would open the WebSocket in PLAINTEXT mode against an
    // ENCRYPTED file — a real correctness defect. Fail-safe-secure:
    // if we can't tell whether the file is encrypted, refuse to
    // proceed.
    if (!(err instanceof ENOENT)) {
      throw err;
    }
  }
  if (fileEnc !== undefined) {
    const config = (vfs as unknown as { opts: { encryption?: unknown } }).opts
      .encryption;
    if (!config) {
      const { makeEncryptionRequiredError } = await import("./encryption");
      throw makeEncryptionRequiredError("open", path);
    }
  }
  const encryptionConfig = fileEnc
    ? ((vfs as unknown as { opts: { encryption: unknown } }).opts
        .encryption as import("@shared/encryption-types").EncryptionConfig)
    : undefined;

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

  // encryption helpers (lazy-loaded once per handle).
  // Encrypt PAYLOAD bytes (NOT the tag prefix) for tags 1/2/3 when
  // the file is encrypted; tag 0 (sync_step_1) stays plaintext —
  // state vectors are clientID→seq maps and don't reveal content.
  let encryptForWire:
    | ((payload: Uint8Array, aadTag: "yj" | "aw") => Promise<Uint8Array>)
    | null = null;
  let decryptFromWire:
    | ((payload: Uint8Array, aadTag: "yj" | "aw") => Promise<Uint8Array>)
    | null = null;
  if (encryptionConfig) {
    const encMod = await import("./encryption");
    encryptForWire = (payload, aadTag) =>
      encMod.encryptPayload(
        payload,
        encryptionConfig,
        // Yjs ops always use random mode (per plan §10 Q10): they're
        // tiny + frequent + dedup gain is negligible + random IV
        // avoids the operational-data leak via op equality.
        "random",
        encryptionConfig.keyId,
        aadTag
      );
    decryptFromWire = (envelope, aadTag) =>
      encMod.decryptPayload(envelope, encryptionConfig, aadTag, {
        path,
        syscall: "yjs",
      });
  }

  // Install the outbound update pump. Origin === ws means a frame
  // from this very socket — don't echo back.
  const onLocalUpdate = (update: Uint8Array, origin: unknown) => {
    if (origin === ws) return;
    if (encryptForWire) {
      // Encrypt the update payload before sending. Errors during
      // encryption surface to onError but otherwise drop the frame
      // — we don't want to spam the server with malformed bytes.
      encryptForWire(update, "yj")
        .then((env) => {
          try {
            ws.send(encodeUpdateMessage(env));
          } catch {
            /* socket may be closing */
          }
        })
        .catch((err) => {
          if (errorCb) errorCb(err);
        });
      return;
    }
    try {
      ws.send(encodeUpdateMessage(update));
    } catch {
      /* socket may be closing; close handler will reject .synced */
    }
  };
  doc.on("update", onLocalUpdate);

  // outbound awareness pump. y-protocols emits an
  // `update` event whenever the local Awareness state changes
  // (added/updated/removed clientIDs). We send the encoded update
  // to the server which relays to other peers. Origin === ws means
  // the update arrived FROM the server; don't echo back.
  // when encrypted, the awareness payload is wrapped in
  // an envelope with AAD='aw' (distinct from 'yj' so a yjs envelope
  // can't be replayed as awareness or vice versa).
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
    const payload = encodeAwarenessUpdate(awareness, ids);
    if (encryptForWire) {
      encryptForWire(payload, "aw")
        .then((env) => {
          try {
            ws.send(encodeAwarenessMessage(env));
          } catch {
            /* socket may be closing */
          }
        })
        .catch((err) => {
          if (errorCb) errorCb(err);
        });
      return;
    }
    try {
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
    // when the file is encrypted, tags 1/2/3 carry envelope
    // bytes; we decrypt before dispatch. Tag 0 stays plaintext (state
    // vectors are content-free). Tag 4 is the compact-please advisory
    // and is plaintext (it's a uint32 BE seq count).
    //
    // Special case: an empty body on tag-1 (sync_step_2) is the
    // server's "no state to send" signal for encrypted files (the
    // server cannot materialise the doc to compute a diff). In that
    // case we mark synced and return without attempting decrypt.
    if (
      decryptFromWire &&
      tag === YJS_SYNC_STEP_2 &&
      body.byteLength === 0
    ) {
      if (!gotServerStep2) {
        gotServerStep2 = true;
        resolveSynced();
      }
      return;
    }
    if (
      decryptFromWire &&
      (tag === YJS_SYNC_STEP_2 ||
        tag === YJS_UPDATE ||
        tag === YJS_AWARENESS)
    ) {
      const aadTag = tag === YJS_AWARENESS ? "aw" : "yj";
      decryptFromWire(body, aadTag)
        .then((plaintext) => {
          if (tag === YJS_SYNC_STEP_2) {
            Y.applyUpdate(doc, plaintext, ws);
            if (!gotServerStep2) {
              gotServerStep2 = true;
              resolveSynced();
            }
          } else if (tag === YJS_UPDATE) {
            Y.applyUpdate(doc, plaintext, ws);
          } else {
            applyAwarenessUpdate(awareness, plaintext, ws);
          }
        })
        .catch((err) => {
          // Decrypt failure → close socket loudly + surface error.
          try {
            ws.close(1011, "decrypt failure");
          } catch {
            /* already closing */
          }
          if (errorCb) errorCb(err);
          if (!gotServerStep2) rejectSynced(err);
        });
      return;
    }
    switch (tag) {
      case YJS_SYNC_STEP_1: {
        // Server's state vector — reply with our diff. When encrypted,
        // we encrypt our diff before sending (handled inline below).
        const diff = Y.encodeStateAsUpdate(doc, body);
        if (encryptForWire) {
          encryptForWire(diff, "yj")
            .then((env) => ws.send(encodeSyncStep2(env)))
            .catch((err) => {
              if (errorCb) errorCb(err);
            });
        } else {
          ws.send(encodeSyncStep2(diff));
        }
        return;
      }
      case YJS_SYNC_STEP_2: {
        // Server's diff for us — apply, mark synced. (Plaintext path;
        // encrypted path is handled above.)
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
        // apply remote awareness update. Origin === ws
        // tells our outbound pump (`onLocalAwareness`) not to echo
        // this back to the server.
        applyAwarenessUpdate(awareness, body, ws);
        return;
      }
      case YJS_COMPACT_PLEASE: {
        // server advisory. body[0..3] = uint32 BE seq count.
        if (body.byteLength < 4) return;
        const seqCount =
          ((body[0] ?? 0) << 24) |
          ((body[1] ?? 0) << 16) |
          ((body[2] ?? 0) << 8) |
          (body[3] ?? 0);
        if (compactNeededCb) compactNeededCb(seqCount >>> 0);
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
  let compactNeededCb: ((seqCount: number) => void) | null = null;
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
    encrypted: encryptionConfig !== undefined,
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
      // The compaction snapshot captures whatever the live Y.Doc
      // currently holds — local edits made on this handle are
      // streamed to the server via the open WebSocket BEFORE
      // flush() runs (each Yjs update is emitted synchronously
      // by `doc.transact`), so the compaction sees them.
      //
      // Encrypted files: route to `compactYjs` (Phase 52 P3 #8)
      // because the server can't materialise an encrypted Y.Doc
      // — the client has to build the checkpoint envelope locally
      // and submit it via the encrypted-compact RPC. We pass
      // `userVisible: true` so versioning-on tenants get a
      // `file_versions` row mirroring the plain-yjs path.
      //
      // Plaintext files: server-driven `compact` does the same
      // thing on the server side.
      if (encryptionConfig) {
        const result = await vfs.compactYjs(path, {
          userVisible: true,
          label: flushOpts?.label,
        });
        if (result === null) {
          // Defensive: compactYjs returned null (which happens if
          // the SDK's encryption config is missing OR if stat
          // claims the file is plaintext). On the encrypted-handle
          // path neither should happen — but if it does, fall
          // back to the plain server-driven flush so the caller
          // still gets a checkpoint.
          return vfs._flushYjs(path, flushOpts);
        }
        return {
          versionId: result.versionId ?? null,
          checkpointSeq: result.checkpointSeq,
        };
      }
      return vfs._flushYjs(path, flushOpts);
    },
    onClose(cb) {
      closeCb = cb;
    },
    onError(cb) {
      errorCb = cb;
    },
    onCompactNeeded(cb) {
      compactNeededCb = cb;
    },
  };
}
