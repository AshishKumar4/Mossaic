/**
 * Node-side Yjs adapter — speaks the Mossaic Yjs WS protocol against
 * a deployed Mossaic Service worker via `wss://...` using the `ws`
 * package. Mirrors `sdk/src/yjs.ts` (which works only inside a
 * Cloudflare Worker via `stub.fetch`).
 *
 * Wire protocol (verified against `worker/core/objects/user/yjs.ts`
 * and `sdk/src/yjs.ts`):
 *
 *   tag=0  sync_step_1  payload = state vector
 *   tag=1  sync_step_2  payload = doc update bytes
 *   tag=2  update       payload = doc update bytes
 *   tag=3  awareness    payload = encodeAwarenessUpdate(...)
 *
 * Auth: Bearer token via the `Authorization` header (Node `ws`
 * supports it). Subprotocol auth (`bearer.<jwt>`) is also accepted
 * by the server for browser parity but not used here.
 */

import WebSocket from "ws";
import * as Y from "yjs";
import {
  Awareness,
  applyAwarenessUpdate,
  encodeAwarenessUpdate,
} from "y-protocols/awareness";

const TAG_SYNC_STEP_1 = 0;
const TAG_SYNC_STEP_2 = 1;
const TAG_UPDATE = 2;
const TAG_AWARENESS = 3;

function encode(tag: number, body: Uint8Array): Uint8Array {
  const out = new Uint8Array(body.byteLength + 1);
  out[0] = tag;
  out.set(body, 1);
  return out;
}

export interface OpenYDocOverWsOptions {
  endpoint: string;
  token: string;
  path: string;
  /** Bring-your-own Y.Doc (rare). */
  doc?: Y.Doc;
}

export interface NodeYDocHandle {
  doc: Y.Doc;
  awareness: Awareness;
  synced: Promise<void>;
  close(): Promise<void>;
  /**
   * Trigger a server-side flush via the HTTP /api/vfs/flushYjs route.
   * Returns the new version_id (null if versioning is off) and the
   * checkpoint seq number.
   */
  flush(opts?: { label?: string }): Promise<{
    versionId: string | null;
    checkpointSeq: number;
  }>;
}

export async function openYDocOverWs(
  opts: OpenYDocOverWsOptions,
): Promise<NodeYDocHandle> {
  const wsUrl =
    opts.endpoint.replace(/^http(s?):/, "ws$1:") +
    "/api/vfs/yjs/ws?path=" +
    encodeURIComponent(opts.path);
  const ws = new WebSocket(wsUrl, {
    headers: { Authorization: `Bearer ${opts.token}` },
  });
  // We need binary frames as Uint8Array.
  ws.binaryType = "nodebuffer";

  const doc = opts.doc ?? new Y.Doc();
  const awareness = new Awareness(doc);

  let resolveSynced!: () => void;
  let rejectSynced!: (err: unknown) => void;
  const synced = new Promise<void>((res, rej) => {
    resolveSynced = res;
    rejectSynced = rej;
  });
  let gotServerStep2 = false;
  let closed = false;

  // Outbound update pump — origin === ws means a frame from THIS socket;
  // don't echo back.
  const onLocalUpdate = (update: Uint8Array, origin: unknown): void => {
    if (origin === ws) return;
    if (ws.readyState !== WebSocket.OPEN) return;
    try {
      ws.send(encode(TAG_UPDATE, update));
    } catch {
      /* socket may be closing */
    }
  };
  doc.on("update", onLocalUpdate);

  const onLocalAwareness = (
    {
      added,
      updated,
      removed,
    }: { added: number[]; updated: number[]; removed: number[] },
    origin: unknown,
  ): void => {
    if (origin === ws) return;
    const ids = [...added, ...updated, ...removed];
    if (ids.length === 0) return;
    if (ws.readyState !== WebSocket.OPEN) return;
    try {
      ws.send(encode(TAG_AWARENESS, encodeAwarenessUpdate(awareness, ids)));
    } catch {
      /* socket may be closing */
    }
  };
  awareness.on("update", onLocalAwareness);

  ws.on("message", (raw: Buffer) => {
    if (raw.length === 0) return;
    const tag = raw[0];
    const body = new Uint8Array(raw.buffer, raw.byteOffset + 1, raw.length - 1);
    switch (tag) {
      case TAG_SYNC_STEP_1: {
        const diff = Y.encodeStateAsUpdate(doc, body);
        try {
          ws.send(encode(TAG_SYNC_STEP_2, diff));
        } catch {
          /* socket closing */
        }
        return;
      }
      case TAG_SYNC_STEP_2: {
        Y.applyUpdate(doc, body, ws);
        if (!gotServerStep2) {
          gotServerStep2 = true;
          resolveSynced();
        }
        return;
      }
      case TAG_UPDATE: {
        Y.applyUpdate(doc, body, ws);
        return;
      }
      case TAG_AWARENESS: {
        applyAwarenessUpdate(awareness, body, ws);
        return;
      }
      default:
        return;
    }
  });

  ws.on("close", () => {
    if (closed) return;
    closed = true;
    doc.off("update", onLocalUpdate);
    awareness.off("update", onLocalAwareness);
    awareness.destroy();
    if (!gotServerStep2) {
      rejectSynced(new Error("openYDocOverWs: socket closed before initial sync"));
    }
  });

  ws.on("error", (err) => {
    if (!gotServerStep2) rejectSynced(err);
  });

  await new Promise<void>((res, rej) => {
    const onOpen = (): void => {
      ws.off("error", onErr);
      // Kick off handshake.
      try {
        ws.send(encode(TAG_SYNC_STEP_1, Y.encodeStateVector(doc)));
        res();
      } catch (err) {
        rej(err);
      }
    };
    const onErr = (e: Error): void => {
      ws.off("open", onOpen);
      rej(new Error(`openYDocOverWs: ${e.message}`));
    };
    ws.once("open", onOpen);
    ws.once("error", onErr);
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
      // Wait for the close frame to round-trip so caller can rely on
      // teardown completion.
      await new Promise<void>((res) => {
        if (ws.readyState === WebSocket.CLOSED) {
          res();
        } else {
          ws.once("close", () => res());
          // Hard cap so a stuck close doesn't hang the test.
          setTimeout(res, 1500);
        }
      });
    },
    async flush(flushOpts?: { label?: string }) {
      const r = await fetch(opts.endpoint + "/api/vfs/flushYjs", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${opts.token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path: opts.path, label: flushOpts?.label }),
      });
      if (!r.ok) {
        const text = await r.text();
        throw new Error(`flushYjs failed: HTTP ${r.status} ${text}`);
      }
      return (await r.json()) as {
        versionId: string | null;
        checkpointSeq: number;
      };
    },
  };
}
