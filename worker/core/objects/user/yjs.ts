/**
 * Phase 10 — native Yjs per-file mode.
 *
 * Storage shape: a yjs-mode file at path `pathId` is materialised
 * by replaying a sequence of binary Y.Doc updates stored in
 * `yjs_oplog`. Each row in `yjs_oplog` has:
 *   - seq:        monotonic counter scoped to (path_id)
 *   - kind:       'op' | 'checkpoint'
 *   - chunk_hash: SHA-256 of the binary update bytes (content-
 *                 addressed → cross-write dedup of identical ops,
 *                 placeChunkForVersion places it on the shard
 *                 owning that hash)
 *   - chunk_size, shard_index: usual ShardDO chunk pointer
 *   - created_at: ms epoch
 *
 * Each op is ALSO a refcounted chunk_refs entry on its ShardDO.
 * The synthetic file_id for the ref is `${pathId}#yjs#${seq}`,
 * which means the existing Phase 1 chunk_refs PK
 * `(chunk_hash, file_id, chunk_index)` keeps every op as its own
 * refcountable slot. When compaction drops historical ops, we
 * dispatch `deleteChunks(${pathId}#yjs#${seq})` per touched shard
 * and the alarm sweeper reclaims the blob if no other ref points
 * at the hash.
 *
 * Materialisation cache: in-memory per UserDO instance. Maps
 * `pathId → { doc, seqApplied }`. On every persisted op we apply
 * to the cached doc (no replay). On cold reads we load the
 * latest checkpoint, then replay ops since that checkpoint's seq.
 *
 * Compaction: every COMPACT_OP_THRESHOLD ops or
 * COMPACT_INTERVAL_MS, write a fresh checkpoint chunk that
 * encodes the full Y.Doc state, then drop op rows whose seq is
 * strictly less than the new checkpoint's seq. The checkpoint
 * itself is a row in yjs_oplog with kind='checkpoint'.
 *
 * Versioning interop (Phase 9): when versioning is enabled for
 * the tenant, every successful compaction ALSO creates a Mossaic
 * version row whose inline_data references the checkpoint chunk
 * hash (or, for small docs, contains the encoded state inline).
 * Live ops between checkpoints are NOT versioned — Yjs op log
 * IS the live history, and Mossaic versioning marks
 * "compaction-stable" snapshots.
 *
 * Pure-function-ish: all SQL access is via durableObject.sql, but
 * the in-memory doc cache + WebSocket session list live on the
 * UserDO instance. Compaction logic is broken into testable
 * helpers (computeNextSeq, shouldCompact, encodeStateAsUpdate)
 * that don't touch the DO state.
 */

import * as Y from "yjs";
import type { UserDOCore as UserDO } from "./user-do-core";
import type { ShardDO } from "../shard/shard-do";
import { VFSError, type VFSScope } from "@shared/vfs-types";
import { hashChunk } from "@shared/crypto";
import { vfsShardDOName } from "../../lib/utils";
import { placeChunkForVersion } from "./vfs-versions";

/** POSIX-style mode bit reused as the SDK-facing yjs flag. */
export const VFS_MODE_YJS_BIT = 0o4000; // S_ISUID — repurposed since we don't enforce setuid

/**
 * Compaction thresholds. Tunable knobs.
 *   COMPACT_OP_THRESHOLD: trigger compaction after this many ops
 *                        accumulate since the last checkpoint.
 *   COMPACT_INTERVAL_MS:  also trigger after this much wall time,
 *                        even if op count is below threshold.
 *
 * 50 ops + 60 s is generous for typical collaborative editing —
 * one autosave every minute or so on a busy doc. Tests use
 * lower thresholds via the explicit `compactNow` API.
 */
export const COMPACT_OP_THRESHOLD = 50;
export const COMPACT_INTERVAL_MS = 60_000;

/** Synthetic shard ref key for a single op or checkpoint. */
export function yjsShardRefId(pathId: string, seq: number): string {
  return `${pathId}#yjs#${seq}`;
}

/**
 * Yjs sync protocol message types (subset we actually use).
 * Standard Yjs protocol from y-protocols/sync.
 */
export const YJS_SYNC_STEP_1 = 0;
export const YJS_SYNC_STEP_2 = 1;
export const YJS_UPDATE = 2;

/** Result of bumping the seq counter — pure function exposed for tests. */
export function computeNextSeq(currentNextSeq: number): {
  seq: number;
  nextSeq: number;
} {
  return { seq: currentNextSeq, nextSeq: currentNextSeq + 1 };
}

/** Compaction trigger — pure predicate. */
export function shouldCompact(
  opCountSinceCkpt: number,
  lastCompactAt: number,
  now: number,
  opThreshold: number = COMPACT_OP_THRESHOLD,
  intervalMs: number = COMPACT_INTERVAL_MS
): boolean {
  if (opCountSinceCkpt >= opThreshold) return true;
  if (opCountSinceCkpt > 0 && now - lastCompactAt >= intervalMs) return true;
  return false;
}

interface YjsMetaRow {
  next_seq: number;
  last_checkpoint_seq: number;
  op_count_since_ckpt: number;
  last_compact_at: number;
  materialized_at: number;
}

/**
 * Read-or-init the yjs_meta row for a file. Idempotent and safe
 * to call concurrently — the INSERT uses ON CONFLICT DO NOTHING
 * so a racing peer's row wins without surfacing a constraint
 * error. The follow-up SELECT returns whichever row landed.
 */
function getYjsMeta(durableObject: UserDO, pathId: string): YjsMetaRow {
  durableObject.sql.exec(
    `INSERT INTO yjs_meta (path_id, next_seq, last_checkpoint_seq,
                            op_count_since_ckpt, last_compact_at, materialized_at)
     VALUES (?, 0, -1, 0, 0, 0)
     ON CONFLICT(path_id) DO NOTHING`,
    pathId
  );
  const row = durableObject.sql
    .exec(
      `SELECT next_seq, last_checkpoint_seq, op_count_since_ckpt,
              last_compact_at, materialized_at
         FROM yjs_meta WHERE path_id = ?`,
      pathId
    )
    .toArray()[0] as unknown as YjsMetaRow;
  return row;
}

/**
 * Push one update (op or checkpoint) to its ShardDO and write the
 * yjs_oplog row. Returns the assigned seq.
 *
 * Single source of truth for both live ops AND compaction
 * checkpoints — keeps shard fan-out + refcount accounting in one
 * place.
 */
async function appendUpdate(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  pathId: string,
  poolSize: number,
  bytes: Uint8Array,
  kind: "op" | "checkpoint"
): Promise<number> {
  // Concurrency invariant: this function CAN run concurrently for
  // the same pathId — two webSocketMessage handlers on different
  // sockets, or a writeFile racing with a remote update. The
  // PRIMARY KEY (path_id, seq) on yjs_oplog catches accidental
  // collisions, but we want a clean monotonic seq instead of
  // surfacing a SQL constraint error to the caller.
  //
  // Strategy: the seq is reserved in a SINGLE atomic SQL statement
  // (the meta UPDATE) BEFORE any await crosses an event-loop
  // boundary. Because DO storage SQL runs in input gates relative
  // to in-memory state, two concurrent callers serialise on the
  // UPDATE — the second sees a higher next_seq.
  //
  // After reserving the seq we do the (slow) shard putChunk, then
  // insert the oplog row. If putChunk fails, we leave a "hole" in
  // the seq sequence — that's harmless because loadDoc reads ops
  // by ORDER BY seq and tolerates gaps.

  const hash = await hashChunk(bytes);
  // hashChunk awaits crypto.subtle — keep it before the seq
  // reservation so the work is parallelisable across concurrent
  // callers (each gets their own hash before queueing on the
  // meta UPDATE).

  // Reserve the seq atomically. We use a single UPSERT-style
  // sequence so the meta row is created on first call AND the
  // counter advances even when multiple callers race.
  durableObject.sql.exec(
    `INSERT INTO yjs_meta (path_id, next_seq, last_checkpoint_seq,
                            op_count_since_ckpt, last_compact_at, materialized_at)
     VALUES (?, 0, -1, 0, 0, 0)
     ON CONFLICT(path_id) DO NOTHING`,
    pathId
  );
  const reserved = durableObject.sql
    .exec(
      `UPDATE yjs_meta
          SET next_seq = next_seq + 1
        WHERE path_id = ?
        RETURNING next_seq - 1 AS seq`,
      pathId
    )
    .toArray()[0] as { seq: number };
  const seq = reserved.seq;

  const sIdx = placeChunkForVersion(durableObject, userId, hash, poolSize);
  const refId = yjsShardRefId(pathId, seq);

  const env = durableObject.envPublic;
  const shardNs = env.SHARD_DO as unknown as DurableObjectNamespace<ShardDO>;
  const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
  const stub = shardNs.get(shardNs.idFromName(shardName));
  await stub.putChunk(hash, bytes, refId, 0, userId);

  const now = Date.now();
  durableObject.sql.exec(
    `INSERT INTO yjs_oplog
       (path_id, seq, kind, chunk_hash, chunk_size, shard_index, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    pathId,
    seq,
    kind,
    hash,
    bytes.byteLength,
    sIdx,
    now
  );

  // Update meta counters that depend on `kind`. The seq counter is
  // already advanced; this updates checkpoint-tracking only.
  if (kind === "op") {
    durableObject.sql.exec(
      `UPDATE yjs_meta
          SET op_count_since_ckpt = op_count_since_ckpt + 1
        WHERE path_id = ?`,
      pathId
    );
  } else {
    durableObject.sql.exec(
      `UPDATE yjs_meta
          SET last_checkpoint_seq = ?,
              op_count_since_ckpt = 0,
              last_compact_at = ?
        WHERE path_id = ?`,
      seq,
      now,
      pathId
    );
  }

  return seq;
}

/**
 * Drop op rows whose seq < cutoff and dispatch deleteChunks to
 * each touched shard. Used by compaction to free old ops AND by
 * unlink/dropVersions when a yjs-mode file is being reaped.
 */
async function dropOpsBefore(
  durableObject: UserDO,
  scope: VFSScope,
  pathId: string,
  cutoffSeq: number
): Promise<{ dropped: number }> {
  const rows = durableObject.sql
    .exec(
      "SELECT seq, shard_index FROM yjs_oplog WHERE path_id = ? AND seq < ?",
      pathId,
      cutoffSeq
    )
    .toArray() as { seq: number; shard_index: number }[];
  if (rows.length === 0) return { dropped: 0 };

  durableObject.sql.exec(
    "DELETE FROM yjs_oplog WHERE path_id = ? AND seq < ?",
    pathId,
    cutoffSeq
  );

  const env = durableObject.envPublic;
  const shardNs = env.SHARD_DO as unknown as DurableObjectNamespace<ShardDO>;
  for (const { seq, shard_index } of rows) {
    const refId = yjsShardRefId(pathId, seq);
    const shardName = vfsShardDOName(
      scope.ns,
      scope.tenant,
      scope.sub,
      shard_index
    );
    const stub = shardNs.get(shardNs.idFromName(shardName));
    try {
      await stub.deleteChunks(refId);
    } catch {
      // Best-effort during compaction GC. The underlying chunk_refs
      // row is gone; the alarm sweeper handles refcount=0 reclaim.
    }
  }
  return { dropped: rows.length };
}

/**
 * Materialise the Y.Doc for a path. Loads the most recent
 * checkpoint (if any) + replays every op with seq > checkpoint.
 * Returns a fresh Y.Doc — caller is responsible for caching.
 *
 * This is the slow path; the hot path is the in-memory cache on
 * the UserDO instance (see YjsRuntime below).
 */
async function loadDoc(
  durableObject: UserDO,
  scope: VFSScope,
  pathId: string
): Promise<{ doc: Y.Doc; seqApplied: number }> {
  const meta = getYjsMeta(durableObject, pathId);
  const doc = new Y.Doc();
  let seqApplied = -1;

  // 1. Load latest checkpoint if one exists.
  if (meta.last_checkpoint_seq >= 0) {
    const ck = durableObject.sql
      .exec(
        `SELECT seq, chunk_hash, shard_index
           FROM yjs_oplog
          WHERE path_id = ? AND kind = 'checkpoint' AND seq = ?`,
        pathId,
        meta.last_checkpoint_seq
      )
      .toArray()[0] as
      | { seq: number; chunk_hash: string; shard_index: number }
      | undefined;
    if (ck) {
      const env = durableObject.envPublic;
      const shardName = vfsShardDOName(
        scope.ns,
        scope.tenant,
        scope.sub,
        ck.shard_index
      );
      const stub = env.SHARD_DO.get(env.SHARD_DO.idFromName(shardName));
      const res = await stub.fetch(
        new Request(`http://internal/chunk/${ck.chunk_hash}`)
      );
      if (res.ok) {
        const bytes = new Uint8Array(await res.arrayBuffer());
        Y.applyUpdate(doc, bytes, "checkpoint");
        seqApplied = ck.seq;
      }
    }
  }

  // 2. Replay all ops with seq > seqApplied. SQL handles the
  // ordering; we trust the (path_id, seq) PK monotonicity.
  const ops = durableObject.sql
    .exec(
      `SELECT seq, chunk_hash, shard_index
         FROM yjs_oplog
        WHERE path_id = ? AND kind = 'op' AND seq > ?
        ORDER BY seq ASC`,
      pathId,
      seqApplied
    )
    .toArray() as { seq: number; chunk_hash: string; shard_index: number }[];

  const env = durableObject.envPublic;
  for (const op of ops) {
    const shardName = vfsShardDOName(
      scope.ns,
      scope.tenant,
      scope.sub,
      op.shard_index
    );
    const stub = env.SHARD_DO.get(env.SHARD_DO.idFromName(shardName));
    const res = await stub.fetch(
      new Request(`http://internal/chunk/${op.chunk_hash}`)
    );
    if (!res.ok) {
      throw new VFSError(
        "ENOENT",
        `yjs: op chunk ${op.chunk_hash} missing on shard ${op.shard_index}`
      );
    }
    const bytes = new Uint8Array(await res.arrayBuffer());
    Y.applyUpdate(doc, bytes, "replay");
    seqApplied = op.seq;
  }

  durableObject.sql.exec(
    "UPDATE yjs_meta SET materialized_at = ? WHERE path_id = ?",
    Date.now(),
    pathId
  );

  return { doc, seqApplied };
}

/**
 * The runtime layer. One YjsRuntime per UserDO instance; lazily
 * created on first yjs-mode call. Holds the in-memory doc cache,
 * the WebSocket session lists, and the global update observer
 * that listens on every cached doc.
 *
 * On Cloudflare Worker hibernation the YjsRuntime evaporates but
 * the SQL state survives. On wake we reconstruct from SQL —
 * checkpoint + ops replay. The WebSocket Hibernation API
 * preserves the connections themselves so clients see no
 * disconnect.
 */
export class YjsRuntime {
  private readonly durableObject: UserDO;

  /** Materialised docs keyed by pathId. */
  private readonly docs = new Map<
    string,
    { doc: Y.Doc; seqApplied: number; observer: (u: Uint8Array, origin: unknown) => void }
  >();

  /**
   * Per-pathId set of attached WebSockets. We use the Hibernation
   * API; sockets in this set are accepted via
   * ctx.acceptWebSocket(ws, [tag]) where the tag is the pathId.
   * On wake we read tags via ctx.getWebSockets(tag) to rebuild
   * this map without per-socket state in memory.
   */
  private readonly sockets = new Map<string, Set<WebSocket>>();

  constructor(durableObject: UserDO) {
    this.durableObject = durableObject;
  }

  /**
   * Get the cached doc for a path, materialising it from SQL on a
   * miss. Idempotent — multiple opens of the same path share the
   * same Y.Doc (different connections, same doc).
   */
  async getDoc(scope: VFSScope, pathId: string): Promise<Y.Doc> {
    const cached = this.docs.get(pathId);
    if (cached) return cached.doc;
    const { doc, seqApplied } = await loadDoc(
      this.durableObject,
      scope,
      pathId
    );

    // Install an observer that captures every local update made
    // through this doc (origin !== 'replay' / 'checkpoint' / 'remote').
    // Live writeFile transactions OR WebSocket-applied remote
    // updates BOTH need to reach the persistence path; we
    // discriminate via origin so we don't re-persist what we just
    // pulled out of SQL.
    const observer = (update: Uint8Array, origin: unknown) => {
      if (origin === "replay" || origin === "checkpoint") return;
      // Persist + broadcast happens in the caller (writeFile,
      // applyRemoteUpdate, etc.) — the observer here is for
      // future hooks; we keep it cheap.
      void update;
    };
    doc.on("update", observer);
    this.docs.set(pathId, { doc, seqApplied, observer });
    return doc;
  }

  /**
   * Apply a remote update (from a WebSocket client OR a writeFile
   * transaction) to the cached doc, persist it as a new oplog
   * row, broadcast to all connected sockets except the origin,
   * and trigger compaction if thresholds are met.
   *
   * Returns the assigned seq number.
   */
  async applyRemoteUpdate(
    scope: VFSScope,
    userId: string,
    pathId: string,
    poolSize: number,
    update: Uint8Array,
    excludeSocket: WebSocket | null
  ): Promise<number> {
    // Ensure the doc is materialised.
    const doc = await this.getDoc(scope, pathId);

    // Apply locally. Origin 'remote' so any observers (none in
    // current code, but future-proof) can discriminate.
    Y.applyUpdate(doc, update, "remote");

    // Persist as op row.
    const seq = await appendUpdate(
      this.durableObject,
      scope,
      userId,
      pathId,
      poolSize,
      update,
      "op"
    );

    const cached = this.docs.get(pathId);
    if (cached) cached.seqApplied = seq;

    // Broadcast to other sockets on this path.
    this.broadcast(pathId, encodeUpdateMessage(update), excludeSocket);

    // Compaction trigger.
    const meta = getYjsMeta(this.durableObject, pathId);
    if (
      shouldCompact(meta.op_count_since_ckpt, meta.last_compact_at, Date.now())
    ) {
      // Fire-and-forget; failures don't break the live edit path.
      void this.compact(scope, userId, pathId, poolSize).catch(() => {
        /* swallow: compaction is opportunistic */
      });
    }

    return seq;
  }

  /**
   * Compact: emit a checkpoint encoding the current Y.Doc state,
   * then drop op rows with seq < checkpoint.seq. Returns the
   * number of ops reaped.
   *
   * Idempotent enough — concurrent invocations may both compact;
   * the second sees a higher last_checkpoint_seq and effectively
   * no-ops because there are no older ops left.
   */
  async compact(
    scope: VFSScope,
    userId: string,
    pathId: string,
    poolSize: number
  ): Promise<{ checkpointSeq: number; opsReaped: number }> {
    const doc = await this.getDoc(scope, pathId);
    const stateBytes = Y.encodeStateAsUpdate(doc);
    const ckSeq = await appendUpdate(
      this.durableObject,
      scope,
      userId,
      pathId,
      poolSize,
      stateBytes,
      "checkpoint"
    );
    const r = await dropOpsBefore(
      this.durableObject,
      scope,
      pathId,
      ckSeq
    );
    return { checkpointSeq: ckSeq, opsReaped: r.dropped };
  }

  /** Materialise → encode → return current doc state as bytes. */
  async readMaterialised(
    scope: VFSScope,
    pathId: string
  ): Promise<Uint8Array> {
    const doc = await this.getDoc(scope, pathId);
    // For Option A's writeFile semantics we expose the contents
    // of a Y.Text named "content". If the doc has none yet, an
    // empty buffer.
    const text = doc.getText("content");
    return new TextEncoder().encode(text.toString());
  }

  /**
   * Apply a Uint8Array as the new "content" Y.Text via a Yjs
   * transaction. This is what writeFile does when a yjs-mode
   * file is the target — it merges into the live CRDT instead
   * of replacing the storage.
   */
  async writeMaterialised(
    scope: VFSScope,
    userId: string,
    pathId: string,
    poolSize: number,
    bytes: Uint8Array
  ): Promise<void> {
    const doc = await this.getDoc(scope, pathId);
    const text = doc.getText("content");
    const newContent = new TextDecoder().decode(bytes);

    // Capture the update emitted by this transaction so we can
    // persist + broadcast it. We attach a one-shot observer.
    // Box the captured value so TS doesn't over-narrow across the
    // closure boundary.
    const capture: { update: Uint8Array | null } = { update: null };
    const onUpdate = (update: Uint8Array, origin: unknown) => {
      if (origin === "writeFile") capture.update = update;
    };
    doc.on("update", onUpdate);
    try {
      doc.transact(() => {
        text.delete(0, text.length);
        text.insert(0, newContent);
      }, "writeFile");
    } finally {
      doc.off("update", onUpdate);
    }
    const captured = capture.update;
    if (captured !== null && captured.byteLength > 0) {
      const update = captured;
      const seq = await appendUpdate(
        this.durableObject,
        scope,
        userId,
        pathId,
        poolSize,
        update,
        "op"
      );
      const cached = this.docs.get(pathId);
      if (cached) cached.seqApplied = seq;
      this.broadcast(pathId, encodeUpdateMessage(update), null);

      const meta = getYjsMeta(this.durableObject, pathId);
      if (
        shouldCompact(meta.op_count_since_ckpt, meta.last_compact_at, Date.now())
      ) {
        void this.compact(scope, userId, pathId, poolSize).catch(() => {});
      }
    }
  }

  // ── WebSocket session handling ────────────────────────────────────────

  /** Track a newly accepted (or rehydrated) socket. */
  registerSocket(pathId: string, ws: WebSocket): void {
    let set = this.sockets.get(pathId);
    if (!set) {
      set = new Set();
      this.sockets.set(pathId, set);
    }
    set.add(ws);
  }

  removeSocket(pathId: string, ws: WebSocket): void {
    const set = this.sockets.get(pathId);
    if (!set) return;
    set.delete(ws);
    if (set.size === 0) this.sockets.delete(pathId);
  }

  /** Broadcast bytes to all sockets attached to pathId, except `exclude`. */
  broadcast(pathId: string, msg: Uint8Array, exclude: WebSocket | null): void {
    const set = this.sockets.get(pathId);
    if (!set) return;
    for (const ws of set) {
      if (ws === exclude) continue;
      try {
        ws.send(msg);
      } catch {
        /* socket may be closed; cleanup happens on close event */
      }
    }
  }

  /**
   * Compose a Yjs sync-step-1 reply: encode our current state
   * vector. Client compares against theirs and sends sync-step-2.
   * Pure function, but lives here because it touches the cached
   * doc.
   */
  async syncStep1Reply(
    scope: VFSScope,
    pathId: string
  ): Promise<Uint8Array> {
    const doc = await this.getDoc(scope, pathId);
    const sv = Y.encodeStateVector(doc);
    return encodeSyncStep1(sv);
  }

  /**
   * Apply a sync-step-2 message body (which IS a Y.Doc update
   * encoded as bytes) via the standard remote-update path.
   */
  async applySyncStep2(
    scope: VFSScope,
    userId: string,
    pathId: string,
    poolSize: number,
    body: Uint8Array,
    fromSocket: WebSocket
  ): Promise<void> {
    await this.applyRemoteUpdate(
      scope,
      userId,
      pathId,
      poolSize,
      body,
      fromSocket
    );
  }
}

// ── Yjs sync-protocol message framing ───────────────────────────────────
//
// A trivially simple framing — first byte = message type, rest = payload.
// We don't use lib0/encoding to avoid pulling another dep in; the
// protocol bytes are hand-rolled and match the Yjs sync protocol's
// type tags so any standard y-websocket-compatible client can talk
// to us. (Awareness frames live on a separate type tag and are
// broadcast-only, never persisted — Phase 10 ships a stub.)

/** Frame an update as a single-byte-tagged message. */
export function encodeUpdateMessage(update: Uint8Array): Uint8Array {
  const out = new Uint8Array(update.byteLength + 1);
  out[0] = YJS_UPDATE;
  out.set(update, 1);
  return out;
}

/** Frame a sync-step-1 message (the requester's state vector). */
export function encodeSyncStep1(stateVector: Uint8Array): Uint8Array {
  const out = new Uint8Array(stateVector.byteLength + 1);
  out[0] = YJS_SYNC_STEP_1;
  out.set(stateVector, 1);
  return out;
}

/** Frame a sync-step-2 message (the diff bytes the requester needs). */
export function encodeSyncStep2(diff: Uint8Array): Uint8Array {
  const out = new Uint8Array(diff.byteLength + 1);
  out[0] = YJS_SYNC_STEP_2;
  out.set(diff, 1);
  return out;
}

export type DecodedYjsMessage =
  | { kind: "syncStep1"; stateVector: Uint8Array }
  | { kind: "syncStep2"; diff: Uint8Array }
  | { kind: "update"; update: Uint8Array }
  | { kind: "unknown"; tag: number };

/** Parse a single tagged message; tolerant of unknown tags. */
export function decodeYjsMessage(msg: Uint8Array): DecodedYjsMessage {
  if (msg.byteLength === 0) return { kind: "unknown", tag: -1 };
  const tag = msg[0];
  const body = msg.subarray(1);
  if (tag === YJS_SYNC_STEP_1) return { kind: "syncStep1", stateVector: body };
  if (tag === YJS_SYNC_STEP_2) return { kind: "syncStep2", diff: body };
  if (tag === YJS_UPDATE) return { kind: "update", update: body };
  return { kind: "unknown", tag };
}

/**
 * Reach into a yjs-mode file's storage to compute its content for
 * readFile. Cheap on warm cache (just `text.toString()`); slow on
 * cold load. Resilient to a non-yjs file being passed by accident
 * (returns empty bytes — the caller validates mode_yjs first).
 */
export async function readYjsAsBytes(
  durableObject: UserDO,
  scope: VFSScope,
  pathId: string
): Promise<Uint8Array> {
  return durableObject.yjsRuntime.readMaterialised(scope, pathId);
}

/**
 * Write bytes into a yjs-mode file via a CRDT transaction.
 * Broadcasts to live editors AND persists.
 */
export async function writeYjsBytes(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  pathId: string,
  poolSize: number,
  bytes: Uint8Array
): Promise<void> {
  return durableObject.yjsRuntime.writeMaterialised(
    scope,
    userId,
    pathId,
    poolSize,
    bytes
  );
}

/** Drop all yjs storage for a path (used by unlink hard-delete + dropVersions on yjs files). */
export async function purgeYjs(
  durableObject: UserDO,
  scope: VFSScope,
  pathId: string
): Promise<void> {
  // Drop every op + checkpoint via the standard cutoff helper
  // (cutoff = next_seq drops everything).
  const meta = durableObject.sql
    .exec("SELECT next_seq FROM yjs_meta WHERE path_id = ?", pathId)
    .toArray()[0] as { next_seq: number } | undefined;
  if (meta) {
    await dropOpsBefore(durableObject, scope, pathId, meta.next_seq);
    durableObject.sql.exec("DELETE FROM yjs_meta WHERE path_id = ?", pathId);
  }
}
