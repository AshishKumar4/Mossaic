# Mossaic KV evaluation

**Phase 36 \u2014 deliberation document**, not implementation.

The user asked: should Mossaic adopt Cloudflare KV? This document
walks every potentially-cacheable surface, classifies the fit
(KV / Workers Cache / DO / nothing), and recommends an ordering.

Bottom line up front:

- **Workers Cache wins for byte-payload surfaces** keyed by
  `(file_id, updated_at)`. Phase 36 Theme B implements three
  (gallery thumbnail, gallery image, shared image). Adopting KV
  for these specific surfaces would be strictly worse \u2014 KV's
  per-key 25 MB limit, 60s eventual consistency window, and
  per-key write quota are misaligned with content-addressed
  bytes that Workers Cache handles in <5ms with no consistency
  drama.
- **KV wins for opaque key-value mappings that are read-mostly,
  written-rarely, and globally consistent reads aren't required.**
  Mossaic has very few such surfaces today.
- **Durable Objects continue to own the source of truth.**
  Anything that needs strong consistency (paths, versions,
  shard placement, quota) stays in UserDO/ShardDO.
- **Nothing else needs caching at the moment.** The Phase 33
  research plan classified ~12 read surfaces; Theme B implements
  the top three. The rest fall into "future Workers Cache
  candidates" or "structurally not cacheable".

Recommendation matrix at the end.

---

## 1. Cache-tier landscape inside a Cloudflare Worker

Mossaic runs entirely on the Cloudflare runtime, so the available
cache substrates are:

| Substrate | Strong consistency? | Write latency | Read latency | Per-value cap | Best for |
|---|---|---|---|---|---|
| Durable Object SQLite | Strong (single-writer per object) | ~10\u201320ms | ~1\u20135ms | ~10 GB per DO | source of truth, transactional reads |
| Workers Cache (caches.default) | None (per-colo cache; ~250 colos) | ~5ms put | ~1\u20135ms get | 512 MB | content-addressed byte payloads |
| Cloudflare KV | Eventually consistent (~60s globally) | 50\u2013500ms | ~5\u201320ms (cache hot), ~80ms (cold) | 25 MB / value, 512 B / key | rarely-changing config, per-tenant feature flags, opaque tokens |
| R2 | Strong (per-object) | 50\u2013200ms | 50\u2013200ms | 5 TB | bulk blob storage, paid by GB-month |
| D1 | Eventually consistent for replicas | ~10ms write | ~5\u201320ms read | 10 GB / database | analytical queries; not used by Mossaic |
| Vectorize | Eventually consistent | ~50ms write | ~50\u2013200ms search | per-vector budget | embeddings (Mossaic uses this for `/api/search`) |

Mossaic's invariants pin the choice on most surfaces:

- **Per-tenant strong consistency required**: writes must be
  reflected in the next read. UserDO is the only substrate that
  delivers this. KV's 60s eventual-consistency window means a
  user could read stale data after their own write \u2014
  unacceptable for a file system.
- **Content-addressed bytes are immutable**: chunk hashes
  (SHA-256) and version IDs (ULID) are baked into URLs.
  `Cache-Control: public, max-age=31536000, immutable` is
  achievable. Workers Cache thrives here.
- **Hot-path latency budget is tight**: every gallery scroll
  fans out 50+ thumbnail fetches; sub-10ms cache lookups are
  table stakes.

---

## 2. Per-surface analysis

The table below covers every read surface in Mossaic plus the
auth + share endpoints. Recommendation column is one of:

- **WC** \u2014 Workers Cache (caches.default).
- **KV** \u2014 Cloudflare KV.
- **DO** \u2014 keep in Durable Object (no separate cache layer).
- **None** \u2014 don't cache at all.

| # | Surface | Bytes? | Consistency need | Hit-rate estimate | Recommendation | Rationale |
|---|---|---|---|---|---|---|
| 1 | `/api/vfs/chunk/:fileId/:idx` | yes (1 MB) | strong (read-after-write) | high (90%+) | **WC** (already shipped) | content-addressed by hash; immutable; multipart-routes.ts:514. |
| 2 | `/api/vfs/readPreview` (variants) | yes (50\u2013500 KB) | strong | high (gallery scroll) | **WC** (Phase 33 plan, deferred) | variant cache table at preview-variants.ts:65 keys on (fileId, variant_kind, renderer_kind, version_id). |
| 3 | `/api/gallery/thumbnail/:fileId` | yes | strong | very high | **WC** (Phase 36 Theme B \u2014 SHIPPED) | edge-cache.ts wraps it. |
| 4 | `/api/gallery/image/:fileId` | yes | strong | medium | **WC** (Phase 36 Theme B \u2014 SHIPPED) | same pattern; 1h TTL. |
| 5 | `/api/shared/:token/image/:fileId` | yes | strong | very high (viral) | **WC** + edge tier (Phase 36 Theme B \u2014 SHIPPED) | public; doubles up with browser + edge. |
| 6 | `/api/vfs/readChunk` (POST) | yes | strong | medium | **WC** (Phase 33 deferred to 36b) | content-addressed; same pattern as #1. |
| 7 | `/api/vfs/readFile` (unencrypted) | yes (up to 100 MB) | strong | low (whole-file reads rare) | **WC** (Phase 33 deferred to 36b; encrypted skipped) | matches phase-32.6 readiness. |
| 8 | `/api/vfs/openManifest` (POST) | no (JSON) | strong | medium | **WC** short-TTL | one RPC saved per gallery scroll. |
| 9 | `/api/vfs/listFiles`, listdir, stat | no (JSON) | strong | low (mutates per write) | **DO** | invalidation cost outweighs hit. |
| 10 | `/api/vfs/listVersions`, fileInfo | no | strong | low | **DO** | mutates on dropVersions / writeFile. |
| 11 | `/api/auth/login`, signup, vfs-token | no | n/a (single-use) | structurally zero | **None** | tokens used once; caching is dangerous. |
| 12 | `/api/vfs/multipart/*` (sessions) | mixed | strong | structurally zero | **None** | one-shot bearer secrets. |
| 13 | `/api/vfs/yjs/ws` | live state | strong | n/a (WebSocket) | **None** | live duplex. |
| 14 | `/api/search/*` | mixed | eventually | low (per-query) | **None** | vector queries vary per query. |
| 15 | Per-tenant feature flags | no (small JSON) | eventually OK | high | **KV candidate** | rarely changes; eventual is fine. |
| 16 | Sharing-link metadata (when scaled) | no | eventually | medium | **KV candidate** (future) | viral lookup keyed by token. |
| 17 | Public gallery TOC (album manifests) | no | eventually | high | **KV candidate** (future) | when we host hundreds of albums. |

**Three candidate KV surfaces** (15, 16, 17) all share: small
opaque JSON, read-heavy, write-rare, tolerant of ~60s
inconsistency. They don't exist yet at scale; the question
isn't "should we use KV today" but "would we use KV when these
surfaces appear?" Answer: probably yes for #15 (feature flags
read on every request); maybe for #16/#17 (could equally live
in DO with read-through Workers Cache).

---

## 3. Why NOT KV for chunk bytes / preview bytes

Easily the most common temptation: "throw the chunks in KV". Why
this is wrong:

1. **25 MB cap**: Mossaic chunks are 1 MB by default but file
   variants can hit several MB after re-encoding. KV would force
   awkward sub-chunking.
2. **60s consistency window**: a user uploads a new version of
   `/photos/family.jpg`; for up to a minute, half the world sees
   the old bytes. Workers Cache's per-colo write is locally
   consistent within ms.
3. **Per-key write quota**: 1 write/sec/key. Chunk hashes are
   unique per content; not a hot key. But the chunk-existence
   gate during dedup checks ARE a hot key (one tenant uploading
   a thousand identical-hash chunks). Wrong shape for KV.
4. **Cost**: Workers Cache is bundled in the Workers plan;
   KV is metered per-read + per-write. At scale, KV reads of
   gallery thumbnails would dominate the bill.
5. **Latency**: KV is ~80ms cold; Workers Cache is <5ms. The
   colo-local cache is faster than KV's globally-replicated
   read because the round trip is microseconds, not
   milliseconds.

KV's consistency model is fine for "did this user enable
versioning", "what's the customer's tier", "what's this
sharing-token's resolved metadata". It's wrong for byte
payloads that need fresh-after-write semantics.

---

## 4. Why NOT KV for path resolution / file metadata

Even more tempting: "cache `appGetFilePath` results in KV". Don't:

1. **Inconsistency window**: file rename produces stale lookups
   for up to 60s. Users see "file not found" intermittently
   after they just renamed it.
2. **Coordination cost**: every write must invalidate every KV
   key that touched the affected pathId. Phase 36 Theme B
   solves this elegantly with `(file_id, updated_at)` keys
   that bust on every write \u2014 no explicit invalidation.
3. **DO + Workers Cache already wins**: appGetFilePath is a
   single SQL JOIN \u2014 ~1ms inside the UserDO. Adding KV in
   front saves at most a few ms but adds a stale-read failure
   mode.

If `appGetFilePath` ever shows up as a hotspot, the right answer
is to wrap it in **Workers Cache** with a short TTL and a per-
user namespace that includes `file_count` (cheap bust on write
because every recordWriteUsage call moves it). Don't reach for
KV.

---

## 5. KV surfaces worth a look in the future

If Mossaic grows the surfaces below into hot reads, KV becomes
defensible:

### 5.1 Per-tenant feature flags (KV-ready when they exist)

There's no per-tenant feature-flag system today. When one is
added (say, "tenant has access to advanced sharing"), it would
likely look like:

- 1 KV namespace, key = `flags:<userId>`, value = JSON object.
- Read on every `authMiddleware` call.
- Write only when admin flips a flag (low rate).
- 60s eventual consistency \u2014 fine; flag flips don't need to
  propagate within seconds.

KV cost at this shape: one read per authenticated request.
That's a lot of reads. The right pattern is "KV + Workers Cache
read-through"; KV is the source of truth, Workers Cache is the
per-colo cache with 60s TTL matching KV's window. This is the
canonical KV pattern.

### 5.2 Public sharing-token metadata (KV when scale demands)

Today `/api/shared/:token` decodes a base64 JSON token and looks
up via `appGetFilePath`. When Mossaic has thousands of public
albums, each share request could query KV instead:

- key = `share:<token>`, value = `{ userId, fileIds[], expiry }`.
- Reads scale globally; no per-DO bottleneck.
- Writes are rare (album creation).

But: the current implementation already works well; the JSON
token IS the authoritative source. Moving to KV adds an extra
hop; only worth it if the per-tenant DO becomes a bottleneck
for shared reads, which it isn't today.

### 5.3 Public album TOC (KV at hundreds-of-albums scale)

Same shape as 5.2. Defer until the load shows up.

---

## 6. Recommendation matrix

| Workload | Substrate | Why |
|---|---|---|
| Source of truth (paths, versions, quota, chunks) | DO | strong consistency mandatory |
| Content-addressed bytes (chunks, previews, thumbnails) | Workers Cache | content-addressed; <5ms; bust-via-key-shape |
| Variant rendering output (preview variants) | DO + Workers Cache | DO source of truth; WC for HTTP layer |
| Auth tokens / sessions | DO | strong + already there |
| Per-tenant feature flags (future) | KV + Workers Cache read-through | rarely written; tolerates 60s window |
| Public album TOC (future, at scale) | KV + Workers Cache read-through | viral reads from many colos |
| Search queries / vector results | Vectorize | already used; not cacheable per-query |
| Path metadata for hot fileIds | Workers Cache short-TTL | NOT KV \u2014 stale-after-write footgun |

---

## 7. Recommendation: don't add KV in Phase 36

The surfaces that would benefit are speculative (per-tenant
feature flags don't exist; public album TOC is a future-scale
problem). Phase 36 Theme B's Workers Cache implementation
covers the actual measurable wins today.

**When to revisit**: when one of these triggers fires:
- A genuine per-tenant feature-flag system is added (KV
  becomes the right home).
- Public sharing scales past ~100 simultaneous public albums
  with global readers (KV becomes the right home for
  share-token metadata).
- A read surface emerges that is global (not per-tenant), small
  (< 25 MB), tolerant of 60s windows, and read-heavy.

Until then, Workers Cache + DO is the right substrate
combination. This document exists so the next revisit doesn't
re-litigate the analysis from scratch.

---

## 8. Cross-references

- `worker/app/lib/edge-cache.ts` \u2014 the Phase 36 Theme B helper.
- `worker/app/routes/gallery.ts` \u2014 wires gallery thumbnail +
  image to Workers Cache.
- `worker/app/routes/shared.ts` \u2014 wires public shared image to
  Workers Cache.
- `worker/core/routes/multipart-routes.ts:514-654` \u2014 the
  pre-Phase-36 precedent (chunk download cache).
- `local/workers-cache-plan.md` \u2014 the Phase 33 research plan
  that informed Theme B's surface selection.
- `docs/scaling-roadmap.md` \u2014 phase boundaries.
