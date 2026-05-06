/**
 * Phase 38 — internal helpers shared between `sdk/src/vfs.ts`
 * (which provides `commitYjsSnapshot`) and `sdk/src/yjs.ts` (the
 * `openYDoc` runtime).
 *
 * Lives in its own file so `vfs.ts` (which is imported by every
 * SDK consumer) doesn't pay for any of the yjs-specific code path
 * unless `commitYjsSnapshot` is actually called. The peer dep
 * `yjs` itself is still lazy-loaded behind `await import("yjs")`
 * — only this magic-prefix helper is eagerly available.
 *
 * Mirror of the server-side helpers in
 * `worker/core/objects/user/yjs.ts`. The constants MUST stay
 * byte-identical across both ends; a divergence would cause a
 * snapshot write from a new SDK to be misinterpreted as legacy
 * UTF-8 text by the server.
 */

/**
 * 4-byte magic prefix identifying a `Y.encodeStateAsUpdate(doc)`
 * payload wrapped for the `writeFile` channel.
 *
 * Bytes: `0x59 0x4A 0x53 0x31` — ASCII "YJS1".
 *
 *  - No valid Yjs binary update or `encodeStateAsUpdate` output
 *    starts with these 4 bytes (Yjs updates start with a varint
 *    for the update format version, currently 0x00).
 *  - Plain ASCII so a hex-dump or `file(1)` inspection of a chunk
 *    on a ShardDO is self-describing.
 *  - Versioned (`YJS1` → potentially `YJS2`) for future evolution.
 *
 * Pairs with `hasYjsSnapshotMagic` (server) and
 * `wrapYjsSnapshot` (here) for round-trip detection.
 */
export const YJS_SNAPSHOT_MAGIC: Uint8Array = new Uint8Array([
  0x59, 0x4a, 0x53, 0x31,
]);

/**
 * Phase 38 — wrap raw `Y.encodeStateAsUpdate(doc)` bytes with the
 * snapshot magic prefix. The result is the byte payload to pass
 * to `vfs.writeFile(yjsPath, payload)` so the server applies it
 * via `Y.applyUpdate` instead of stuffing the bytes into
 * `Y.Text("content")`.
 */
export function wrapYjsSnapshot(updateBytes: Uint8Array): Uint8Array {
  const out = new Uint8Array(
    YJS_SNAPSHOT_MAGIC.byteLength + updateBytes.byteLength
  );
  out.set(YJS_SNAPSHOT_MAGIC, 0);
  out.set(updateBytes, YJS_SNAPSHOT_MAGIC.byteLength);
  return out;
}

/**
 * Phase 38 — true iff `bytes` starts with `YJS_SNAPSHOT_MAGIC`.
 * Cheap (4-byte prefix check); safe on short inputs (returns
 * false for `bytes.byteLength < 4`).
 *
 * Used by tests to assert that a payload destined for `writeFile`
 * is correctly snapshot-wrapped.
 */
export function hasYjsSnapshotMagic(bytes: Uint8Array): boolean {
  if (bytes.byteLength < YJS_SNAPSHOT_MAGIC.byteLength) return false;
  for (let i = 0; i < YJS_SNAPSHOT_MAGIC.byteLength; i++) {
    if (bytes[i] !== YJS_SNAPSHOT_MAGIC[i]) return false;
  }
  return true;
}
