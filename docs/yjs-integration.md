# Yjs integration — Tiptap, ProseMirror, Notion-style block editors

Mossaic supports per-file Yjs CRDT mode. Any path can be promoted to
yjs-mode via `vfs.setYjsMode(path, true)`; from then on the file's
content lives as a Yjs op log inside its UserDO and live editors
exchange standard Yjs sync frames over a Hibernation-API
WebSocket.

This document covers the **Phase 38** integration surface that
makes Mossaic a drop-in for Tiptap, ProseMirror, and Notion-style
block editors. The contract has three load-bearing entry points:

| API | Purpose |
|---|---|
| `openYDoc(vfs, path)` | live bidirectional WebSocket; the returned `handle.doc` is a real `Y.Doc` you mutate via standard Yjs APIs (`doc.getXmlFragment`, `doc.getMap`, `doc.getArray`, `doc.getText`, …). Updates stream to other connected editors automatically. |
| `vfs.commitYjsSnapshot(path, doc)` | one-shot write of the full Y.Doc state. Ideal for seeding a path with an offline-built doc, importing from another store, or marking a save point. |
| `vfs.readYjsSnapshot(path)` | one-shot read returning `Y.encodeStateAsUpdate(doc)` bytes; decode locally with `Y.applyUpdate(localDoc, bytes)` to recover every named shared type. |

The legacy `vfs.writeFile(yjsPath, "text")` / `vfs.readFile(yjsPath)`
contract is preserved byte-for-byte: bytes that don't carry the
4-byte snapshot magic prefix continue to set `Y.Text("content")`.
Existing CLI / SDK consumers keep working without code changes.

---

## 1. Why three entry points?

Different editor frameworks need different shapes of access:

* **Tiptap / ProseMirror** want a live `Y.Doc` whose
  `Y.XmlFragment("default")` they can hand to their `Collaboration`
  extension. They want bidirectional sync, awareness for cursors,
  and cheap incremental updates. → `openYDoc`.
* **Server-side rendering, link previews, search indexing** want a
  one-shot snapshot — the materialized state at this instant —
  without holding a WebSocket open. → `readYjsSnapshot`.
* **Importers / migrations / "copy from template"** want to
  initialize a path with a Y.Doc built offline. → `commitYjsSnapshot`.
* **Plain-text consumers** that don't care about CRDT semantics
  still get to call `vfs.writeFile(path, "text")` / `readFile(path)`
  on a yjs-mode path and see the `Y.Text("content")` round-trip.
  → legacy path.

All four paths share the same op log on the server. Edits made via
any path are visible to live editors via `openYDoc` immediately;
edits made via `openYDoc` are visible to the next `readYjsSnapshot`
or `readFile` call.

---

## 2. Tiptap integration

```ts
import { Editor } from "@tiptap/core";
import StarterKit from "@tiptap/starter-kit";
import { Collaboration } from "@tiptap/extension-collaboration";
import { CollaborationCursor } from "@tiptap/extension-collaboration-cursor";
import { createMossaicHttpClient } from "@mossaic/sdk/http";
import { openYDoc } from "@mossaic/sdk/yjs";

// 1. Connect to Mossaic.
const vfs = createMossaicHttpClient({
  url: "https://mossaic-core.example.workers.dev",
  apiKey: () => fetchVfsToken(),
});

// 2. Promote a path to yjs-mode (idempotent — safe on every load).
await vfs.writeFile("/docs/intro.md", "");
await vfs.setYjsMode("/docs/intro.md", true);

// 3. Open a live Y.Doc handle. The Y.Doc inside the handle is
//    pre-bound to the Mossaic op log; edits flow through the WS.
const handle = await openYDoc(vfs, "/docs/intro.md");
await handle.synced;

// 4. Hand the handle's Y.Doc to Tiptap. Tiptap will register a
//    `Y.XmlFragment("default")` and use it as the document model.
const editor = new Editor({
  extensions: [
    StarterKit.configure({ history: false }), // Yjs provides undo/redo
    Collaboration.configure({ document: handle.doc }),
    CollaborationCursor.configure({
      provider: { awareness: handle.awareness },
      user: { name: currentUser.name, color: currentUser.color },
    }),
  ],
});

// 5. Cleanup on unmount.
window.addEventListener("beforeunload", () => {
  void handle.close();
});
```

Tiptap stores its document tree in `Y.XmlFragment("default")` —
exactly the type Phase 38 added support for. Any number of
concurrent editors can join via `openYDoc` against the same path
and see each other's edits in real time. Cursors and selections
flow through the awareness channel.

---

## 3. ProseMirror integration (without Tiptap)

ProseMirror users binding directly via `y-prosemirror`:

```ts
import { ySyncPlugin, yCursorPlugin, yUndoPlugin } from "y-prosemirror";
import { EditorState } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { schema } from "prosemirror-schema-basic";
import { openYDoc } from "@mossaic/sdk/yjs";

const handle = await openYDoc(vfs, "/docs/notes.md");
await handle.synced;

const fragment = handle.doc.getXmlFragment("prosemirror");
const state = EditorState.create({
  schema,
  plugins: [
    ySyncPlugin(fragment),
    yCursorPlugin(handle.awareness),
    yUndoPlugin(),
  ],
});
const view = new EditorView(document.querySelector("#editor"), { state });
```

The shared-type name (`"prosemirror"` here, vs Tiptap's
`"default"`) is your choice — the only constraint is that every
peer uses the same name. Mossaic doesn't interpret the name at
all; it persists the entire Y.Doc.

---

## 4. Notion-style block editor

Block editors (Notion, Craft, Roam, Anytype) typically model the
document as a `Y.Map` of blocks plus a `Y.Array` for ordering:

```ts
import * as Y from "yjs";
import { openYDoc } from "@mossaic/sdk/yjs";

const handle = await openYDoc(vfs, "/pages/2026-roadmap.json");
await handle.synced;

// Each block is a Y.XmlFragment so it can hold its own marks /
// inline structures (mentions, links, …).
const blocks = handle.doc.getMap<Y.XmlFragment>("blocks");
const order = handle.doc.getArray<string>("order");
const meta = handle.doc.getMap<string>("meta");

// Add a block.
const blockId = crypto.randomUUID();
const blockBody = new Y.XmlFragment();
blockBody.insert(0, [
  Object.assign(new Y.XmlElement("paragraph"), {}),
]);
blocks.set(blockId, blockBody);
order.push([blockId]);

meta.set("title", "2026 Roadmap");
meta.set("emoji", "🗺️");

// Subscribe to remote edits (rendering layer).
order.observe(() => render(blocks, order));
blocks.observe(() => render(blocks, order));
```

Mossaic stores all three top-level types (`blocks`, `order`,
`meta`) plus any inline marks inside the block fragments. A
new client connecting via `openYDoc` or pulling a fresh state
via `readYjsSnapshot` recovers the full structure.

---

## 5. Snapshots — when not to use the live socket

If your code path doesn't need bidirectional updates — e.g. an
SSR endpoint rendering markdown for an unfurl preview, or a search
indexer pulling text out of every doc — open a one-shot snapshot
instead:

```ts
import * as Y from "yjs";

// Read.
const bytes = await vfs.readYjsSnapshot("/docs/intro.md");
const doc = new Y.Doc();
Y.applyUpdate(doc, bytes);

// Now use the doc however you need.
const xmlFragment = doc.getXmlFragment("default");
const text = doc.getText("content");
const meta = doc.getMap<string>("meta");
```

Or write — useful for migrations and "duplicate page" features:

```ts
import * as Y from "yjs";

const sourceBytes = await vfs.readYjsSnapshot("/templates/blank.md");
const dest = new Y.Doc();
Y.applyUpdate(dest, sourceBytes);
dest.getMap<string>("meta").set("title", "New Page");

await vfs.writeFile("/pages/new-page.md", "");
await vfs.setYjsMode("/pages/new-page.md", true);
await vfs.commitYjsSnapshot("/pages/new-page.md", dest);
```

`commitYjsSnapshot` writes a Y.Doc state update; if there are
already live editors on the path via `openYDoc`, they see the
new content merged into their local doc. The Yjs CRDT guarantees
convergence — applying the same update everywhere yields the
same state.

---

## 6. Versioning interaction

Mossaic supports per-tenant S3-style versioning. When versioning
is enabled, the Yjs surface composes:

* Each call to `handle.flush({ label, userVisible: true })` creates
  a `file_versions` row whose `inline_data` IS the
  `Y.encodeStateAsUpdate(doc)` snapshot wrapped with the
  `YJS_SNAPSHOT_MAGIC` 4-byte prefix. The wrapped bytes are stored
  verbatim, so a future `restoreVersion(v)` call gets the exact
  state at flush time.
* `vfs.listVersions(path)` returns the chain — including the labels
  passed to `flush`.
* `vfs.commitYjsSnapshot(path, doc)` does NOT itself create a
  version row (it writes through the CRDT op log; the op log is
  the live history). Use `flush({ userVisible: true })` for that.

Typical save-point pattern:

```ts
const handle = await openYDoc(vfs, "/docs/intro.md");
await handle.synced;

// ... user edits ...

// User clicks "Save snapshot" or "Mark as v2".
const result = await handle.flush({
  label: "v2 — added introduction",
  // userVisible defaults to true via flush
});
console.log(`saved as version ${result.versionId}`);
```

---

## 7. Encryption interaction

End-to-end encrypted yjs files are supported via the SDK's
`encryption: { masterKey, tenantSalt }` config. The SDK encrypts
outbound Yjs frames before they reach the WS and decrypts inbound
frames before applying them locally. The server stores envelope
bytes only — it cannot materialise an encrypted Y.Doc.

**Limitation**: `readYjsSnapshot` is **not available** on
encrypted yjs files (server can't materialise without the key).
Use `openYDoc` and read the doc locally:

```ts
const handle = await openYDoc(vfs, "/secret/notes.md");
await handle.synced;
const fragment = handle.doc.getXmlFragment("default"); // local Y.Doc
```

For server-side compaction on encrypted files, see
`vfs.compactYjs(path)` — it runs entirely client-side, decrypts
the op log locally, builds a fresh Y.Doc, encrypts the result,
and submits via a CAS-protected RPC.

---

## 8. Troubleshooting

**Symptom**: `EINVAL: file is not in yjs mode` when calling
`openYDoc` or `readYjsSnapshot`.
**Fix**: call `vfs.setYjsMode(path, true)` first (idempotent).

**Symptom**: `EACCES` from `readYjsSnapshot` on a known yjs file.
**Cause**: the file is encrypted; the server can't materialise.
**Fix**: use `openYDoc` and read the doc client-side.

**Symptom**: Tiptap shows empty editor when opening an existing
file.
**Cause**: `await handle.synced` was skipped. Tiptap mounts
before the initial sync round-trip completes; binding before
sync = empty doc.
**Fix**:
```ts
const handle = await openYDoc(vfs, path);
await handle.synced;        // ← required
mountEditor(handle.doc);
```

**Symptom**: Two clients see different content.
**Cause**: One client edited offline, then the network came back.
Yjs converges automatically once the WS reconnects, but if the
SDK's reconnect logic is opt-in (currently the consumer's
responsibility — see `handle.onClose` / `handle.onError`), a
consumer that doesn't reconnect will stay forked.
**Fix**: implement reconnect + replay using
`Y.encodeStateVector(localDoc)` → `Y.encodeStateAsUpdate(remoteDoc, sv)`
on the new socket.

**Symptom**: `vfs.writeFile(yjsPath, bytes)` doesn't update the
Y.Doc the way I expected.
**Cause**: the legacy contract is "bytes set Y.Text("content")".
If you want the bytes to be a state update, use
`commitYjsSnapshot`.

---

## 9. The snapshot magic prefix

For diagnostics and integration with existing storage tooling:

* The 4 bytes `0x59 0x4A 0x53 0x31` ("YJS1") at the start of a
  payload identify it as a Y.Doc snapshot wrapped for `writeFile`.
* `vfs.commitYjsSnapshot(path, doc)` adds the prefix automatically.
* The server-side `writeYjsBytes` (which both `commitYjsSnapshot`
  and the legacy `writeFile`-on-yjs path route through) checks the
  prefix and routes to `Y.applyUpdate` if present, else falls
  through to `Y.Text("content")` overwrite.
* `flush({ userVisible: true })` stores the prefix in the version
  row's `inline_data` so future tooling can detect "this is a
  snapshot, not a flat text version".

The exact constant is published from `sdk/src/yjs-internal.ts`:

```ts
import { YJS_SNAPSHOT_MAGIC } from "@mossaic/sdk/yjs"; // re-exported
console.log(Array.from(YJS_SNAPSHOT_MAGIC).map((b) => b.toString(16)));
// → ["59", "4a", "53", "31"]
```

---

## 10. What's next

Phase 38 ships full arbitrary-named-shared-type support. Open
questions tracked for follow-up phases:

1. **`restoreVersion` for yjs files** — currently creates a new
   `file_versions` row with the snapshot bytes but does NOT rewind
   the live Y.Doc. A clean Phase 39 fix routes restoreVersion-on-
   yjs through `writeYjsBytes(snapshotBytes)` so live editors see
   the rewind.
2. **`commitYjsSnapshot` ordering** — the snapshot write merges
   with the live doc; in the rare case where a client wants
   "discard live state + replace entirely", they need to delete
   and re-create. We may add an opt-in `replace: true` flag.
3. **Encrypted snapshot writes** — `commitYjsSnapshot` doesn't
   handle the encryption-aware wire format yet (`openYDoc`'s
   per-frame envelope encryption is the only encrypted path).
   For now, encrypted tenants must use `openYDoc` for all writes.
