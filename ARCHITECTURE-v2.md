# Mossaic v2 — Pure Durable Object Architecture

> **Distributed file storage on Cloudflare Workers + Durable Objects + Vectorize + Workers AI**
>
> **No R2. Pure DO storage. Unlimited parallelism.**
>
> Version: 2.0 — March 2026
>
> This document is implementation-ready. A senior engineer should be able to build Mossaic from this specification alone.

---

## Table of Contents

1.  [DO Namespace Design](#1-do-namespace-design)
2.  [Chunking Strategy](#2-chunking-strategy)
3.  [Deterministic Shard Placement](#3-deterministic-shard-placement)
4.  [Pure DO Storage Architecture](#4-pure-do-storage-architecture)
5.  [UserDO Architecture](#5-userdo-architecture)
6.  [Parallel Transfer Protocol](#6-parallel-transfer-protocol)
7.  [Worker Router Design](#7-worker-router-design)
8.  [Semantic Search Architecture](#8-semantic-search-architecture)
9.  [Shared Albums & Permissions](#9-shared-albums--permissions)
10. [Cost Analysis](#10-cost-analysis)
11. [Performance Analysis](#11-performance-analysis)
12. [Data Flow Diagrams](#12-data-flow-diagrams)
13. [Scalability Limits & Mitigations](#13-scalability-limits--mitigations)
14. [Appendix: Key Type Definitions](#14-appendix-key-type-definitions)

---

## System Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         MOSSAIC v2 ARCHITECTURE                              │
│                                                                              │
│  ┌──────────┐    ┌──────────────────┐    ┌──────────────┐                    │
│  │  Client   │───▶│  Cloudflare      │───▶│   UserDO     │                    │
│  │  (Web /   │    │  Worker Router   │    │  (1 per user)│                    │
│  │   CLI)    │◀───│  (edge, global)  │◀───│   auth,      │                    │
│  └──────────┘    └────────┬─────────┘    │   metadata,  │                    │
│       │                   │              │   manifests,  │                    │
│       │                   │              │   albums,     │                    │
│       │                   │              │   search refs │                    │
│       │                   │              └──────┬───────┘                    │
│       │      ┌────────────┼────────────────┐    │                            │
│       │      │            │                │    │                            │
│       │      ▼            ▼            ... ▼    ▼                            │
│       │ ┌──────────┐ ┌──────────┐    ┌──────────┐                            │
│       │ │ ShardDO-0│ │ShardDO-1 │    │ShardDO-N │  ← UNLIMITED instances    │
│       │ │ (SQLite  │ │          │    │          │     per user namespace     │
│       │ │  BLOBs)  │ │          │    │          │                            │
│       │ └──────────┘ └──────────┘    └──────────┘                            │
│       │                                                                      │
│       │  Direct chunk fetch (client → Worker → ShardDO)                      │
│       │  Up to 256 concurrent connections                                    │
│       │                                                                      │
│       │         ┌──────────────┐                                             │
│       │         │  Vectorize   │                                             │
│       │         │  + Workers AI│                                             │
│       │         │  (search)    │                                             │
│       │         └──────────────┘                                             │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Design Principles

1. **Pure DO storage** — Every byte of chunk data lives in a Durable Object's SQLite database. No R2. No external blob stores. DOs are the single storage layer.
2. **Unlimited parallelism** — If a file has 1,000 chunks across 200 ShardDOs, the client can download from all 200 simultaneously. Parallelism scales with file size, bounded only by client bandwidth and browser connection limits (~256 concurrent fetches).
3. **Deterministic placement** — Given `(userId, fileId, chunkIndex, poolSize)`, any node computes the target ShardDO with zero lookups. Rendezvous hashing, computed in microseconds.
4. **Content-addressed** — SHA-256 chunk hashes enable deduplication and integrity verification.
5. **Infinite shard pool** — The ShardDO pool grows dynamically. 100 ShardDOs = 1 TB. 1,000 ShardDOs = 10 TB. No artificial ceiling.
6. **Throughput over cost** — DO storage at $0.20/GB is 13× more expensive than R2. The value proposition is **maximum throughput via unlimited parallel streams**, not minimum cost.

### What Changed from v1

| Aspect | v1 (Conservative) | v2 (Throughput-Maximized) |
|--------|-------------------|--------------------------|
| **Storage backend** | Hybrid DO + R2 (R2 for overflow) | **Pure DO. No R2.** |
| **Parallel connections** | 4-6 (capped) | **As many as beneficial** (20-100+) |
| **Shard pool** | 8 base, grows to ~80 | **Starts larger, grows to thousands** |
| **Chunk size** | Adaptive 1-64 MB | **1 MB fixed** (optimized for parallelism) |
| **Design goal** | Balance cost vs performance | **Maximum throughput, cost is acceptable** |
| **DO namespaces** | UserDO + StorageDO | **UserDO + ShardDO + ThumbnailDO** |
| **R2 dependency** | Critical (overflow storage) | **None** |

---

## 1. DO Namespace Design

### Three Durable Object Namespaces

Mossaic v2 uses exactly three DO namespaces. Each is a separate `DurableObjectNamespace` binding in `wrangler.toml`, each with its own class, its own unlimited instance pool.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DO NAMESPACE LAYOUT                              │
│                                                                        │
│  Namespace 1: USER_DO                                                  │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │ Exactly 1 instance per user                                    │    │
│  │ Name: "user:{userId}"                                          │    │
│  │                                                                │    │
│  │ Responsibilities:                                              │    │
│  │  • Authentication (sessions, password hashes)                  │    │
│  │  • File metadata & manifests (file_chunks table)               │    │
│  │  • Folder tree                                                 │    │
│  │  • Albums & album items                                        │    │
│  │  • Share tokens & permissions                                  │    │
│  │  • Search index references (embedding IDs)                     │    │
│  │  • Quota tracking & pool size                                  │    │
│  │  • Embedding generation queue (async via alarms)               │    │
│  │                                                                │    │
│  │ Storage: Metadata only. ~1 KB per file. 10 GB DO limit         │    │
│  │          → supports ~10M files per user.                       │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                        │
│  Namespace 2: SHARD_DO                                                 │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │ UNLIMITED instances per user                                   │    │
│  │ Name: "shard:{userId}:{shardIndex}"                            │    │
│  │                                                                │    │
│  │ Responsibilities:                                              │    │
│  │  • Store chunk data as SQLite BLOBs (≤ 2 MB per row)          │    │
│  │  • Content-addressed deduplication (hash-keyed)                │    │
│  │  • Reference counting for garbage collection                   │    │
│  │  • Capacity tracking (approach 10 GB → new shards absorb)     │    │
│  │  • Integrity verification (periodic scrub via alarms)          │    │
│  │                                                                │    │
│  │ Storage: Pure chunk data. Each DO holds up to ~10 GB.          │    │
│  │          100 DOs = 1 TB. 1,000 DOs = 10 TB.                   │    │
│  │          Pool grows automatically as user stores more data.    │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                        │
│  Namespace 3: THUMBNAIL_DO                                             │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │ 1 instance per user (or sharded for power users)               │    │
│  │ Name: "thumb:{userId}"                                         │    │
│  │                                                                │    │
│  │ Responsibilities:                                              │    │
│  │  • Store pre-generated thumbnails (256×256 WebP, ~10-30 KB)    │    │
│  │  • Gallery grid rendering without touching ShardDOs            │    │
│  │  • Thumbnail generation queue (async via alarms)               │    │
│  │  • Multiple thumbnail sizes: grid (256px), preview (1024px)    │    │
│  │                                                                │    │
│  │ Storage: Thumbnails only. 30 KB × 100K photos = 3 GB.         │    │
│  │          Shards to "thumb:{userId}:{N}" if > 10 GB.            │    │
│  └────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

### Why a Separate ThumbnailDO?

**The gallery problem:** When a user opens their photo library, the client must render a grid of 50-200 thumbnails immediately. Without ThumbnailDO:

```
Naive approach (no ThumbnailDO):
  50 thumbnails → 50 manifest lookups from UserDO → 50 chunk fetches from 50 ShardDOs
  = 101 DO round-trips for one gallery page

With ThumbnailDO:
  1 request to ThumbnailDO → batch fetch 50 thumbnails from SQLite
  = 1 DO round-trip for one gallery page
```

The ThumbnailDO is an optimization DO — it caches derived data (thumbnails) in a locality-optimized layout for the gallery's access pattern. The ShardDOs remain the source of truth for full-resolution chunk data.

**Thumbnail generation flow:**
1. After upload completes, UserDO's alarm fires
2. Alarm handler fetches the first chunk from the relevant ShardDO
3. Runs Workers AI image resize → 256×256 WebP thumbnail
4. Stores thumbnail in ThumbnailDO (keyed by `fileId`)
5. Optionally generates a 1024px preview variant

```typescript
// ThumbnailDO stores thumbnails as small BLOBs, keyed by fileId
// Each thumbnail is 10-30 KB — thousands fit easily in one DO

// ThumbnailDO SQLite schema
// CREATE TABLE thumbnails (
//   file_id    TEXT PRIMARY KEY,
//   grid_thumb BLOB NOT NULL,      -- 256×256 WebP, ~15 KB
//   preview    BLOB,               -- 1024×1024 WebP, ~80 KB (generated lazily)
//   width      INTEGER NOT NULL,   -- original image width
//   height     INTEGER NOT NULL,   -- original image height
//   created_at INTEGER NOT NULL
// );

// Gallery endpoint: fetch 50 thumbnails in one DO call
async function handleGalleryPage(
  thumbnailDO: DurableObjectStub,
  fileIds: string[]
): Promise<Response> {
  return thumbnailDO.fetch(new Request('http://internal/batch', {
    method: 'POST',
    body: JSON.stringify({ fileIds }),
  }));
  // Returns: [{fileId, thumbBase64, width, height}, ...] in one response
}
```

### Why NOT Other Namespaces

**CoordinatorDO — Rejected.** A coordinator would centralize decisions (e.g., "which shard is full?", "where should this chunk go?"). But Mossaic's placement is fully deterministic via rendezvous hashing — no coordination needed. The UserDO tracks pool size, and that's the only state the placement algorithm needs. Adding a coordinator creates a bottleneck and a single point of failure for zero benefit.

**SearchDO — Rejected.** Search is handled by Cloudflare Vectorize (managed vector database). There's no advantage to wrapping it in a DO. The Worker queries Vectorize directly, and the UserDO stores embedding metadata. A SearchDO would just be a passthrough that adds latency.

**QueueDO — Rejected.** Background work (embedding generation, thumbnail creation) is handled by UserDO alarms. DO alarms are Cloudflare's built-in job scheduling mechanism — they survive hibernation, they retry on failure, and they run within the DO's single-threaded context (no concurrency bugs). A separate QueueDO would duplicate functionality that alarms already provide.

### wrangler.toml Configuration

```toml
name = "mossaic"
main = "src/worker.ts"
compatibility_date = "2026-03-01"

# === Durable Object Bindings ===

[[durable_objects.bindings]]
name = "USER_DO"
class_name = "UserDO"

[[durable_objects.bindings]]
name = "SHARD_DO"
class_name = "ShardDO"

[[durable_objects.bindings]]
name = "THUMBNAIL_DO"
class_name = "ThumbnailDO"

[[migrations]]
tag = "v1"
new_sqlite_classes = ["UserDO", "ShardDO", "ThumbnailDO"]

# === AI & Search ===

[ai]
binding = "AI"

[[vectorize]]
binding = "VECTORIZE_INDEX"
index_name = "mossaic-embeddings"
```

### DO Instance Naming Convention

All DO instances use deterministic, human-readable names:

```
UserDO:      "user:{userId}"
ShardDO:     "shard:{userId}:{shardIndex}"    ← shardIndex is 0-indexed integer
ThumbnailDO: "thumb:{userId}"
             "thumb:{userId}:{shardN}"         ← if thumbnails exceed 10 GB

Examples:
  user:01HXYZ123ABC
  shard:01HXYZ123ABC:0
  shard:01HXYZ123ABC:1
  shard:01HXYZ123ABC:47
  shard:01HXYZ123ABC:999
  thumb:01HXYZ123ABC
```

Using `idFromName()` guarantees the same DO instance is always referenced for the same name. No random IDs, no lookups, no registry.

### Env Bindings

```typescript
interface Env {
  USER_DO: DurableObjectNamespace;
  SHARD_DO: DurableObjectNamespace;
  THUMBNAIL_DO: DurableObjectNamespace;
  AI: Ai;
  VECTORIZE_INDEX: VectorizeIndex;
}
```

---

## 2. Chunking Strategy

### Design Goal: Maximize Parallelism

In v1, chunk sizes were chosen for storage efficiency — large chunks reduce per-chunk metadata overhead. **In v2, chunk size is chosen to maximize the number of parallel streams.**

The insight: more chunks → more ShardDOs involved → more parallel connections → faster transfer. The chunk size must be small enough to spread across many DOs, but large enough that per-chunk overhead doesn't dominate.

### Hard Constraint: SQLite BLOB Limit

DO SQLite stores BLOBs up to **2 MB per row**. This is a hard platform limit. Every chunk must fit in a single row. Therefore:

```
MAXIMUM CHUNK SIZE = 2 MB (2,097,152 bytes)
```

No exceptions. No sub-chunking workarounds. The 2 MB limit is the ceiling.

### Decision: 1 MB Fixed Chunk Size

**Choice: Fixed 1 MB (1,048,576 bytes) chunks for all files > 1 MB.**

```
CHUNK_SIZE(file_size):
  if file_size ≤ 1 MB:    return file_size    (1 chunk — no splitting)
  else:                    return 1 MB          (always 1 MB chunks)
```

Why 1 MB and not 2 MB (the maximum)?

| Factor | 1 MB chunks | 2 MB chunks |
|--------|------------|------------|
| **100 MB file** | 100 chunks across ~50+ DOs | 50 chunks across ~25+ DOs |
| **1 GB file** | 1,000 chunks across ~200+ DOs | 500 chunks across ~100+ DOs |
| **Parallelism** | 2× more parallel streams | Half the parallelism |
| **Per-chunk overhead** | ~200 bytes metadata | ~200 bytes metadata |
| **Overhead ratio** | 200B / 1MB = 0.02% | 200B / 2MB = 0.01% |
| **SHA-256 hash time** | ~3 ms per chunk (SubtleCrypto) | ~6 ms per chunk |
| **SQLite BLOB headroom** | 50% under limit | At the limit |

**1 MB is the sweet spot.** The overhead difference between 1 MB and 2 MB is negligible (0.02% vs 0.01%), but 1 MB doubles the parallelism potential. It also leaves comfortable headroom below the 2 MB BLOB limit.

### Why Not Smaller? (256 KB, 512 KB)

Smaller chunks create more parallelism but introduce real overhead:

```
100 MB file at different chunk sizes:

256 KB chunks:  400 chunks, 400 × 200B metadata = 80 KB overhead
                400 SHA-256 hashes (client-side) ≈ 400 × 0.8ms = 320 ms
                400 DO round-trips for upload
                Per-chunk HTTP framing: 400 × ~500B headers = 200 KB wire overhead

512 KB chunks:  200 chunks, 200 × 200B = 40 KB overhead
                200 SHA-256 hashes ≈ 160 ms
                200 DO round-trips
                200 × ~500B = 100 KB wire overhead

1 MB chunks:    100 chunks, 100 × 200B = 20 KB overhead
                100 SHA-256 hashes ≈ 100 ms
                100 DO round-trips
                100 × ~500B = 50 KB wire overhead
```

At 256 KB, HTTP framing overhead becomes measurable. At 1 MB, the per-chunk overhead is negligible. And 100 chunks for a 100 MB file already provides more than enough parallelism — the client can't meaningfully use 400 simultaneous connections anyway (browser limit ~256, practical limit ~50-100).

### Why Fixed Size (Not Adaptive Tiers)

v1 used adaptive tiers: 1 MB for small files, 4 MB for medium, 16 MB for large, 64 MB for huge. This was designed to keep chunk counts manageable.

**v2 doesn't need manageable chunk counts.** The whole point is MORE chunks = MORE parallelism. A 5 GB file with 5,000 × 1 MB chunks across 500+ ShardDOs is desirable, not a problem. The client handles the concurrency adaptively (see Section 6).

Benefits of a single fixed chunk size:

1. **Simpler implementation** — No tier logic, no edge cases at tier boundaries
2. **Simpler manifest** — `chunkCount = ceil(fileSize / 1MB)`, no lookup table needed
3. **Predictable placement** — All chunks are the same size, so shard capacity planning is trivial
4. **Maximum parallelism** — Always the most chunks possible within the 1 MB sweet spot

### Chunk Count Examples

```
┌────────────────┬─────────┬──────────────┬──────────────────────────┐
│ File Size      │ Chunks  │ Unique DOs   │ Use Case                 │
│                │ (1 MB)  │ (est. @ 200  │                          │
│                │         │  pool size)  │                          │
├────────────────┼─────────┼──────────────┼──────────────────────────┤
│ 500 KB         │ 1       │ 1            │ Small thumbnail          │
│ 1 MB           │ 1       │ 1            │ Phone photo (HEIC)       │
│ 5 MB           │ 5       │ 5            │ Smartphone JPEG          │
│ 20 MB          │ 20      │ 19-20        │ DSLR JPEG                │
│ 50 MB          │ 50      │ 42-50        │ RAW photo                │
│ 100 MB         │ 100     │ 70-95        │ Short video clip         │
│ 500 MB         │ 500     │ 150-200      │ Long video               │
│ 1 GB           │ 1,024   │ 180-200      │ HD video                 │
│ 5 GB           │ 5,120   │ ~200 (pool)  │ 4K video                 │
│ 10 GB          │ 10,240  │ ~300 (pool)  │ Large archive            │
└────────────────┴─────────┴──────────────┴──────────────────────────┘

"Unique DOs" = number of distinct ShardDOs touched by this file.
Estimated via birthday problem: for C chunks across P shards,
  unique DOs ≈ P × (1 - (1 - 1/P)^C)
```

### Content Addressing

Every chunk is identified by its SHA-256 hash of the raw chunk data:

```typescript
type ChunkHash = string; // hex-encoded SHA-256, 64 chars

async function hashChunk(data: Uint8Array): Promise<ChunkHash> {
  const digest = await crypto.subtle.digest('SHA-256', data);
  return Array.from(new Uint8Array(digest))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
}
```

**Properties:**
- **Integrity**: On download, `SHA-256(received_data) === expected_hash` verifies no corruption
- **Deduplication**: Identical chunk data → same hash → stored once per ShardDO
- **Content-addressable**: The hash IS the chunk's identity. No UUIDs needed for chunks.

### File-Level Hash

Each file has a composite hash — the SHA-256 of the ordered concatenation of all chunk hashes:

```typescript
// File hash = SHA-256 of the ordered concatenation of chunk hashes
// Equivalent to a Merkle tree root with a flat structure
async function computeFileHash(chunkHashes: ChunkHash[]): Promise<ChunkHash> {
  const concat = chunkHashes.join('');
  const data = new TextEncoder().encode(concat);
  const digest = await crypto.subtle.digest('SHA-256', data);
  return Array.from(new Uint8Array(digest))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
}
```

**Use cases:**
- **Whole-file dedup**: If two uploads produce the same file hash, the second upload is a no-op (all chunks already exist)
- **Integrity**: After downloading all chunks, client can verify the reassembled file matches the expected file hash

### Chunk Specification Types

```typescript
interface ChunkSpec {
  index: number;        // 0-based chunk index within the file
  offset: number;       // byte offset in original file
  size: number;         // actual bytes (last chunk may be smaller than 1 MB)
  hash: ChunkHash;      // SHA-256 of chunk data
}

interface FileManifest {
  fileId: string;           // ULID (sortable, unique)
  fileName: string;         // original filename
  fileSize: number;         // total bytes
  fileHash: ChunkHash;      // SHA-256 of concatenated chunk hashes
  mimeType: string;         // detected MIME type
  chunkSize: number;        // always 1,048,576 (1 MB) for files > 1 MB
  chunkCount: number;       // ceil(fileSize / 1MB), or 1 for files ≤ 1 MB
  poolSize: number;         // ShardDO pool size at upload time
  chunks: ChunkSpec[];      // ordered list of chunk specs
  createdAt: number;        // Unix timestamp ms
  encryptionKey?: string;   // client-side encryption key (encrypted, optional)
}

// Pure function — no I/O needed
function computeChunkSpec(fileSize: number): { chunkSize: number; chunkCount: number } {
  const ONE_MB = 1_048_576;
  if (fileSize <= ONE_MB) {
    return { chunkSize: fileSize, chunkCount: 1 };
  }
  return { chunkSize: ONE_MB, chunkCount: Math.ceil(fileSize / ONE_MB) };
}
```

### Why Fixed-Size Over CDC (Content-Defined Chunking)

This decision is unchanged from v1 — the rationale is stronger in v2:

| Factor | Fixed-Size | CDC (FastCDC) |
|--------|-----------|---------------|
| **Determinism** | `chunk_count = ceil(file_size / 1MB)` — trivially predictable | Variable — must scan entire file to know chunk count |
| **Random access** | `chunk[i]` starts at `i × 1MB` — O(1) | Requires chunk index lookup |
| **Parallelism** | Client computes all chunk boundaries BEFORE upload begins | Must chunk sequentially (rolling hash is serial) |
| **DO placement** | Can compute DO targets before first byte is read | Must chunk first, then place |
| **Throughput** | No hash computation per byte — >10 GB/s chunking speed | FastCDC: ~800 MB/s native, ~50-100 MB/s in JS |
| **Simplicity** | Trivial | Moderate (Gear hash, masks, min/max) |

**The determinism requirement is non-negotiable.** The parallel transfer protocol needs to know chunk count and DO placement targets before reading any file data. CDC violates this.

---

## 3. Deterministic Shard Placement

### Algorithm: Rendezvous Hashing (HRW — Highest Random Weight)

**Choice: Rendezvous hashing for mapping `(userId, fileId, chunkIndex)` → ShardDO.**

Rendezvous hashing computes a score for every `(chunk, shard)` pair. The shard with the highest score wins. It's the same algorithm as v1, but now operating over a much larger shard pool.

### Why Rendezvous Hashing

| Property | Rendezvous (HRW) | Consistent Hash Ring | Jump Hash |
|----------|:-:|:-:|:-:|
| k-of-n selection | **Trivial** (top-k scores) | Complex (walk ring) | Not supported |
| Balance (no tuning) | **Perfect** | Poor without vnodes | Perfect |
| Arbitrary node removal | **Yes** | Yes | **No** |
| Memory overhead | O(n) server list | O(n × V) ring | O(1) |
| Implementation | **~20 lines** | ~100 lines + BST | ~5 lines |

Rendezvous hashing is optimal for Mossaic because:

1. **Natural scatter** — Each chunk of a file produces different scores, so consecutive chunks land on different ShardDOs. This is exactly RAID 0 striping via hashing.
2. **Zero metadata** — No ring, no virtual nodes. The shard pool is defined by a single integer (pool size).
3. **Perfect balance** — Each shard is equally likely to be selected. No tuning.
4. **Minimal disruption** — Adding a shard only moves ~1/(n+1) of existing chunks. Information-theoretically optimal.
5. **O(n) is acceptable** — For large pools (n = 200-1000), computing n MurmurHash3 values takes <1 ms on a Worker. This runs once per chunk placement, not per byte.

### Shard Pool: Per-User, Dynamically Growing, LARGE

Each user has their own pool of ShardDOs. The pool grows continuously based on total storage used. **There is no artificial ceiling.**

```
POOL_SIZE(total_storage_bytes):
  BASE_POOL = 32                         // minimum shards per user
  BYTES_PER_SHARD = 5 * 1024^3           // add 1 shard per 5 GB stored
  additional = floor(total_storage_bytes / BYTES_PER_SHARD)
  return BASE_POOL + additional
```

| Total Storage | Pool Size | Capacity Headroom | Parallelism Ceiling |
|--------------|-----------|-------------------|---------------------|
| 0 (new user) | 32 shards | 320 GB | 32 parallel streams |
| 10 GB | 34 shards | 340 GB | 34 parallel streams |
| 50 GB | 42 shards | 420 GB | 42 parallel streams |
| 100 GB | 52 shards | 520 GB | 52 parallel streams |
| 500 GB | 132 shards | 1.3 TB | 132 parallel streams |
| 1 TB | 232 shards | 2.3 TB | 232 parallel streams |
| 5 TB | 1,032 shards | 10.3 TB | ~256 (browser limit) |
| 10 TB | 2,032 shards | 20.3 TB | ~256 (browser limit) |

**Why start at 32?**

- With 1 MB chunks, a 100 MB file has 100 chunks. Across 32 shards, rendezvous hashing distributes them to ~28-32 unique DOs (birthday problem). This means the client can open ~30 parallel streams even for moderate files.
- 32 shards × 10 GB each = 320 GB capacity. This gives massive headroom for a new user — no pool growth needed until they exceed 320 GB of actual data.
- 32 MurmurHash3 computations per chunk placement: <0.05 ms. Negligible.

**Why not start even larger (e.g., 128)?**

- 128 hash computations per chunk is still fast (<0.2 ms), but the benefit is marginal. A 100 MB file already touches ~30 of 32 shards. At 128 shards, it'd touch ~63 — more parallel streams, but client bandwidth is the real bottleneck well before 63 concurrent connections.
- 32 is the point of diminishing returns for typical file sizes. The pool grows for users who need it.

**Why grow at 1 shard per 5 GB?**

- Each ShardDO has 10 GB capacity. At 1 shard per 5 GB stored, the pool capacity grows at 2× the data ingestion rate — always double the required space.
- This means: fill 5 GB → pool grows by 1 shard (10 GB capacity) → you can fill 10 more GB before you'd even theoretically run low.
- The growth rate also increases parallelism for users who store more data — exactly the users who benefit most from parallel transfers.

### Shard Pool Capacity vs Actual Usage

```
Capacity vs Usage over time (1 shard per 5 GB growth):

Capacity (GB)
  1000 ┤                                              ╱── total pool capacity
       │                                          ╱──╱     (pool_size × 10 GB)
   800 ┤                                      ╱──╱
       │                                  ╱──╱
   600 ┤                              ╱──╱
       │                          ╱──╱
   400 ┤                      ╱──╱
       │                  ╱──╱
   320 ┤─── base ────╱──╱            ← 32 base shards = 320 GB capacity
       │          ╱──╱
   200 ┤      ╱──╱
       │  ╱──╱
   100 ┤╱╱                    ╱─────── actual data stored
       │               ╱─────╱
    50 ┤          ╱────╱
       │     ╱───╱
     0 ┤────╱
       └──┬──┬──┬──┬──┬──┬──┬──┬──┬──
          0  10 20 30 40 50 60 70 80 90  ← Data stored (GB)

The pool always has at least 2× headroom over actual data stored.
```

### ShardDO ID Generation

ShardDO IDs are deterministic, derived from user ID + shard index:

```typescript
function shardDOName(userId: string, shardIndex: number): string {
  return `shard:${userId}:${shardIndex}`;
}

// The pool is always: shard indices [0, 1, 2, ..., poolSize - 1]
function getShardPool(userId: string, poolSize: number): string[] {
  return Array.from({ length: poolSize }, (_, i) => shardDOName(userId, i));
}
```

Using `env.SHARD_DO.idFromName(shardDOName(...))` guarantees the same DO instance for the same name. No random IDs, no lookup table.

### The Placement Function

```typescript
import { murmurhash3_x86_32 } from './hash'; // fast 32-bit hash

/**
 * Compute a deterministic score for placing a chunk on a specific shard.
 * Higher score = higher priority for placement.
 */
function placementScore(fileId: string, chunkIndex: number, shardId: string): number {
  const key = `${fileId}:${chunkIndex}:${shardId}`;
  return murmurhash3_x86_32(key);
}

/**
 * Determine which ShardDO holds chunk `chunkIndex` of file `fileId`.
 *
 * FULLY DETERMINISTIC: depends only on (userId, fileId, chunkIndex, poolSize).
 * No network calls. No state lookups. Any Worker computes the same answer.
 */
function placeChunk(
  userId: string,
  fileId: string,
  chunkIndex: number,
  poolSize: number
): { shardIndex: number; doName: string } {
  let bestShard = 0;
  let bestScore = -1;

  for (let shard = 0; shard < poolSize; shard++) {
    const shardId = shardDOName(userId, shard);
    const score = placementScore(fileId, chunkIndex, shardId);
    if (score > bestScore) {
      bestScore = score;
      bestShard = shard;
    }
  }

  return {
    shardIndex: bestShard,
    doName: shardDOName(userId, bestShard),
  };
}

/**
 * Place ALL chunks of a file across the shard pool.
 * Returns a map: chunkIndex → shardIndex.
 *
 * Key property: consecutive chunks scatter across different ShardDOs,
 * enabling parallel fetch from many DOs simultaneously.
 */
function placeFile(
  userId: string,
  fileId: string,
  chunkCount: number,
  poolSize: number
): Map<number, number> {
  const placement = new Map<number, number>();
  for (let i = 0; i < chunkCount; i++) {
    const { shardIndex } = placeChunk(userId, fileId, i, poolSize);
    placement.set(i, shardIndex);
  }
  return placement;
}

/**
 * Compute how many UNIQUE ShardDOs a file touches.
 * This determines the maximum useful parallelism for this file.
 */
function uniqueShardsForFile(
  userId: string,
  fileId: string,
  chunkCount: number,
  poolSize: number
): number {
  const shards = new Set<number>();
  for (let i = 0; i < chunkCount; i++) {
    const { shardIndex } = placeChunk(userId, fileId, i, poolSize);
    shards.add(shardIndex);
  }
  return shards.size;
}
```

### Distribution Analysis: RAID 0-Style Striping

Rendezvous hashing with MurmurHash3 produces uniformly distributed scores. For C chunks across P shards:

```
Expected chunks per shard = C / P

For a 100 MB file (100 × 1 MB chunks) across 32 shards:
  Expected per shard: 100/32 = 3.125 chunks
  Unique shards touched: ~32 × (1 - (1 - 1/32)^100) = ~32 × 0.96 = ~31 shards
  → Almost ALL 32 shards are involved → 31 parallel streams possible

For a 1 GB file (1,024 × 1 MB chunks) across 52 shards (100 GB user):
  Expected per shard: 1024/52 = 19.7 chunks
  Unique shards touched: ~52 × (1 - (1 - 1/52)^1024) = ~52 (all)
  → ALL 52 shards involved → 52 parallel streams

For a 50 MB file (50 × 1 MB chunks) across 32 shards:
  Expected per shard: 50/32 = 1.56 chunks
  Unique shards touched: ~32 × (1 - (1 - 1/32)^50) = ~32 × 0.80 = ~26 shards
  → 26 parallel streams for a 50 MB file
```

This is RAID 0 striping — data distributed across "disks" (DOs) for parallel I/O:

```
Rendezvous Hashing Distribution (100 MB = 100 chunks, 32 shards):

  Shard 0:  C3, C41, C78           (3 chunks)
  Shard 1:  C7, C22, C55, C91      (4 chunks)
  Shard 2:  C1, C34, C67           (3 chunks)
  Shard 3:  C12, C45, C89          (3 chunks)
  Shard 4:  C0, C38, C71           (3 chunks)
  ...
  Shard 30: C19, C52, C85          (3 chunks)
  Shard 31: C28, C63, C96          (3 chunks)

  Average: 3.125 chunks/shard. Std dev ≈ 1.7
  Range: 1-6 chunks per shard (99th percentile)

  → Download: client opens 31 parallel connections, one per unique shard
  → Each connection streams 3-4 chunks sequentially
  → Total time ≈ (100 MB / 31 parallel streams) / bandwidth + overhead
```

### Handling Pool Growth (Rebalancing)

When a user's storage triggers pool growth, the pool size increases. This changes the placement for some existing chunks.

**Solution: Lazy migration with fallback reads.**

```
On pool growth (32 → 33 shards):
  1. UserDO records: old_pool_size=32, new_pool_size=33, migration_epoch=now
  2. For WRITES: always use new_pool_size
  3. For READS:
     a. Compute placement with new_pool_size
     b. Attempt read from new placement
     c. If not found → compute with old_pool_size → read from old location
     d. If found at old location → background migrate to new location
  4. DO alarm gradually migrates remaining chunks
  5. When complete: clear old_pool_size
```

```typescript
interface PoolConfig {
  userId: string;
  currentPoolSize: number;
  previousPoolSize?: number;     // set during migration
  migrationStarted?: number;     // epoch ms
}

async function readChunkWithFallback(
  env: Env,
  config: PoolConfig,
  fileId: string,
  chunkIndex: number,
  chunkHash: string
): Promise<ArrayBuffer> {
  // Try current placement first
  const current = placeChunk(config.userId, fileId, chunkIndex, config.currentPoolSize);
  const currentDO = env.SHARD_DO.get(env.SHARD_DO.idFromName(current.doName));
  const data = await fetchChunkFromDO(currentDO, chunkHash);

  if (data) return data;

  // Fallback to previous placement during migration
  if (config.previousPoolSize) {
    const previous = placeChunk(config.userId, fileId, chunkIndex, config.previousPoolSize);
    if (previous.shardIndex !== current.shardIndex) {
      const prevDO = env.SHARD_DO.get(env.SHARD_DO.idFromName(previous.doName));
      const oldData = await fetchChunkFromDO(prevDO, chunkHash);
      if (oldData) {
        // Background: migrate chunk to new location (fire and forget)
        currentDO.fetch(new Request('http://internal/migrate', {
          method: 'PUT',
          headers: { 'X-Chunk-Hash': chunkHash },
          body: oldData,
        }));
        return oldData;
      }
    }
  }

  throw new Error(`Chunk not found: ${fileId}:${chunkIndex} (hash: ${chunkHash})`);
}
```

**Disruption analysis for pool growth 32 → 33:**

```
P(chunk moves) = 1 - P(original winner still wins among 33)
               = 1 - 32/33 ≈ 3.03%

For a user with 50 GB = 50,000 chunks:
  Expected chunks to migrate: 50,000 × 0.0303 = ~1,515 chunks = ~1.5 GB
  At 100 MB/s migration throughput: ~15 seconds

Compare v1's doubling (8 → 16): 50% of chunks moved.
Growing by 1 shard at a time is FAR less disruptive.
```

### Pool Growth Is Incremental (Not Doubling)

v1 doubled the pool (8 → 16 → 32), which moved 50% of chunks each time. v2 grows by 1 shard per 5 GB, which moves only ~1/(n+1) of chunks each time:

```
Pool growth disruption comparison:

Growth Event          Chunks Moved    Migration Data (50 GB user)
─────────────────────────────────────────────────────────────────
v1: 8 → 16           50.0%           25 GB
v1: 16 → 32          50.0%           25 GB

v2: 32 → 33          3.03%           1.5 GB
v2: 33 → 34          2.94%           1.5 GB
v2: 34 → 35          2.86%           1.4 GB
...
v2: 50 → 51          1.96%           1.0 GB

v2's incremental growth is dramatically less disruptive.
```

### Worked Example

```
User: user_01HXYZ
File: file_ABC123 (100 MB → 100 chunks of 1 MB)
Pool size: 32 shards

Placement (rendezvous hashing, illustrative):

Chunk  0 → Shard 14    Chunk 25 → Shard  7    Chunk 50 → Shard 22    Chunk 75 → Shard  3
Chunk  1 → Shard  7    Chunk 26 → Shard 19    Chunk 51 → Shard 11    Chunk 76 → Shard 28
Chunk  2 → Shard 28    Chunk 27 → Shard  0    Chunk 52 → Shard  5    Chunk 77 → Shard 15
Chunk  3 → Shard  3    Chunk 28 → Shard 23    Chunk 53 → Shard 30    Chunk 78 → Shard  0
Chunk  4 → Shard 21    Chunk 29 → Shard 11    Chunk 54 → Shard 17    Chunk 79 → Shard 21
...

Distribution across 32 shards:
  Shard  0: [C27, C78, C94]                     → 3 chunks
  Shard  1: [C8, C43]                           → 2 chunks
  Shard  2: [C17, C61, C88]                     → 3 chunks
  Shard  3: [C3, C36, C75]                      → 3 chunks
  ...
  Shard 14: [C0, C33, C58, C82]                 → 4 chunks
  ...
  Shard 31: [C15, C47, C99]                     → 3 chunks

  Unique shards: 31 of 32  (one shard got 0 chunks by chance)
  Min chunks/shard: 0       Max: 6       Average: 3.125

Download plan:
  Open 31 parallel connections (one per unique shard)
  Each connection fetches 2-6 chunks sequentially from its shard
  Total: 100 chunks × 1 MB = 100 MB across 31 parallel streams
  At 100 Mbps: ~100 MB / (31 × ~3 MB/s per stream) ≈ ~1 second
```

---

## 4. Pure DO Storage Architecture

### No R2. Period.

Every byte of user data lives in ShardDO SQLite databases. There is no R2 bucket, no external blob store, no overflow tier. The ShardDO pool IS the storage layer.

```
┌─────────────────────────────────────────────────────────────────────┐
│                     PURE DO STORAGE MODEL                           │
│                                                                     │
│  All chunk data stored as SQLite BLOBs inside ShardDOs              │
│                                                                     │
│  ShardDO-0              ShardDO-1              ShardDO-N            │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐      │
│  │ SQLite DB     │      │ SQLite DB     │      │ SQLite DB     │     │
│  │ ≤ 10 GB       │      │ ≤ 10 GB       │      │ ≤ 10 GB       │     │
│  │               │      │               │      │               │     │
│  │ ┌───────────┐ │      │ ┌───────────┐ │      │ ┌───────────┐ │     │
│  │ │ chunks    │ │      │ │ chunks    │ │      │ │ chunks    │ │     │
│  │ │           │ │      │ │           │ │      │ │           │ │     │
│  │ │ hash→BLOB │ │      │ │ hash→BLOB │ │      │ │ hash→BLOB │ │     │
│  │ │ (≤ 2 MB   │ │      │ │ each row) │ │      │ │           │ │     │
│  │ │ per row)  │ │      │ │           │ │      │ │           │ │     │
│  │ └───────────┘ │      │ └───────────┘ │      │ └───────────┘ │     │
│  │               │      │               │      │               │     │
│  │ ┌───────────┐ │      │ ┌───────────┐ │      │ ┌───────────┐ │     │
│  │ │ chunk_refs│ │      │ │ chunk_refs│ │      │ │ chunk_refs│ │     │
│  │ │ (GC refs) │ │      │ │           │ │      │ │           │ │     │
│  │ └───────────┘ │      │ └───────────┘ │      │ └───────────┘ │     │
│  │               │      │               │      │               │     │
│  │ capacity_used │      │ capacity_used │      │ capacity_used │     │
│  │ = 7.2 GB      │      │ = 4.8 GB      │      │ = 6.1 GB      │     │
│  └──────────────┘      └──────────────┘      └──────────────┘      │
│                                                                     │
│  No R2. No external storage. No overflow tier.                      │
│  Pool grows: more data → more ShardDOs → more parallel throughput.  │
└─────────────────────────────────────────────────────────────────────┘
```

### Why Pure DO Storage?

**The throughput argument:**

Every ShardDO is an independent SQLite database running on its own Durable Object instance. Each DO handles requests independently. When the client downloads a file, it opens parallel connections to many ShardDOs simultaneously. More ShardDOs = more independent I/O channels = more total throughput.

With R2, all reads go through a single regional blob store. You can issue parallel GET requests, but they all hit the same backend. R2's throughput scales with Cloudflare's infrastructure, but you don't control it.

With pure DO, every shard is a separate compute instance with its own SQLite database, its own memory, its own I/O path. You control the parallelism: more shards = more independent channels. This is true RAID 0 — each "disk" (DO) operates independently.

**The simplicity argument:**

- One storage layer, not two
- No R2 key management, no r2_key columns, no "is this chunk inline or in R2?" branching
- No R2 eviction logic, no LRU tracking, no spill-to-R2 alarms
- No R2 consistency concerns (R2 is eventually consistent for overwrites)
- Every chunk read is the same: `SELECT data FROM chunks WHERE hash = ?`

**The cost argument (honest):**

- DO storage: $0.20/GB/month
- R2 storage: $0.015/GB/month
- **DO is 13.3× more expensive per GB**
- But: R2 Class A operations (PUT) cost $4.50 per million. R2 Class B operations (GET) cost $0.36 per million. DO row reads cost $0.001 per million — 360× cheaper per read.
- For read-heavy workloads (photos: view many, upload once), DO reads are dramatically cheaper per operation.
- Full cost analysis in Section 10.

### ShardDO SQLite Schema

```sql
-- Main chunk storage: hash-keyed BLOBs
CREATE TABLE chunks (
  hash          TEXT PRIMARY KEY,      -- SHA-256 hex (64 chars)
  data          BLOB NOT NULL,         -- chunk bytes (≤ 2 MB per row, always inline)
  size          INTEGER NOT NULL,      -- chunk size in bytes
  ref_count     INTEGER NOT NULL DEFAULT 1,  -- number of files referencing this chunk
  created_at    INTEGER NOT NULL,      -- Unix timestamp ms
  last_read_at  INTEGER                -- Unix timestamp ms (for analytics/monitoring)
);

-- Track which files reference which chunks on this shard
-- Enables garbage collection when files are deleted
CREATE TABLE chunk_refs (
  chunk_hash    TEXT NOT NULL REFERENCES chunks(hash),
  file_id       TEXT NOT NULL,          -- the file that references this chunk
  chunk_index   INTEGER NOT NULL,       -- position within that file
  user_id       TEXT NOT NULL,          -- owner (for access control)
  PRIMARY KEY (chunk_hash, file_id, chunk_index)
);

CREATE INDEX idx_refs_file ON chunk_refs(file_id);

-- Shard capacity tracking (maintained in-memory, persisted periodically)
CREATE TABLE shard_meta (
  key           TEXT PRIMARY KEY,
  value         INTEGER NOT NULL
);
-- Keys: 'capacity_used_bytes', 'chunk_count', 'created_at'
```

**Schema differences from v1:**

- No `r2_key` column — chunks are always inline, never in R2
- No `checksum` column — the primary key `hash` IS the checksum (SHA-256 of the data)
- No `idx_chunks_size` or `idx_chunks_last_read` indexes — no LRU eviction needed (no R2 to evict to)
- `data` column is `NOT NULL` — every chunk has its data right here

### Writing Chunks

```typescript
// Inside ShardDO
async writeChunk(
  hash: string,
  data: ArrayBuffer,
  fileId: string,
  chunkIndex: number,
  userId: string
): Promise<{ status: 'created' | 'deduplicated'; bytesStored: number }> {

  // Check capacity first
  const capacityUsed = this.getCapacityUsed();
  const SOFT_LIMIT = 9n * 1024n * 1024n * 1024n; // 9 GB (1 GB headroom)

  if (capacityUsed + BigInt(data.byteLength) > SOFT_LIMIT) {
    // This shard is full. The placement algorithm should route elsewhere.
    // In practice, this is handled by the UserDO tracking shard capacities
    // and adjusting pool_size to grow past full shards.
    throw new Error(`Shard at capacity: ${capacityUsed} bytes`);
  }

  // Check for dedup (same hash = same data)
  const existing = this.sql.exec(
    `SELECT hash, ref_count FROM chunks WHERE hash = ?`, hash
  ).one();

  if (existing) {
    // Chunk exists — just add a reference
    this.sql.exec(
      `UPDATE chunks SET ref_count = ref_count + 1 WHERE hash = ?`, hash
    );
    this.sql.exec(
      `INSERT OR IGNORE INTO chunk_refs (chunk_hash, file_id, chunk_index, user_id)
       VALUES (?, ?, ?, ?)`,
      hash, fileId, chunkIndex, userId
    );
    return { status: 'deduplicated', bytesStored: 0 };
  }

  // New chunk — store it
  this.sql.exec(
    `INSERT INTO chunks (hash, data, size, ref_count, created_at)
     VALUES (?, ?, ?, 1, ?)`,
    hash, data, data.byteLength, Date.now()
  );

  // Record reference
  this.sql.exec(
    `INSERT INTO chunk_refs (chunk_hash, file_id, chunk_index, user_id)
     VALUES (?, ?, ?, ?)`,
    hash, fileId, chunkIndex, userId
  );

  // Update capacity tracking
  this.updateCapacity(data.byteLength);

  return { status: 'created', bytesStored: data.byteLength };
}
```

### Reading Chunks

Reading is simple — one SQLite query, one BLOB returned:

```typescript
async readChunk(hash: string): Promise<ArrayBuffer | null> {
  const row = this.sql.exec(
    `SELECT data FROM chunks WHERE hash = ?`, hash
  ).one();

  if (!row) return null;

  // Optional: update last_read_at for analytics (async, non-blocking)
  // this.sql.exec(`UPDATE chunks SET last_read_at = ? WHERE hash = ?`, Date.now(), hash);

  return row.data;
}
```

**That's it.** No "is this inline or in R2?" check. No R2 fetch fallback. No edge cache lookup. One query, one response.

### Streaming Chunk Reads

For the Worker to stream chunk data to the client without buffering:

```typescript
// Inside ShardDO fetch handler
async fetch(request: Request): Promise<Response> {
  const url = new URL(request.url);

  if (url.pathname.startsWith('/chunk/')) {
    const hash = url.pathname.split('/')[2];
    const data = await this.readChunk(hash);

    if (!data) {
      return new Response('Chunk not found', { status: 404 });
    }

    return new Response(data, {
      headers: {
        'Content-Type': 'application/octet-stream',
        'Content-Length': data.byteLength.toString(),
        'Cache-Control': 'public, max-age=31536000, immutable',
        // Content-addressed: same hash = same data forever
        'ETag': `"${hash}"`,
      },
    });
  }

  // ... other routes (write, delete, migrate, etc.)
}
```

**Cache-Control: immutable** — Because chunks are content-addressed (hash = identity), they never change. The browser and Cloudflare edge can cache them forever. This means repeated views of the same photo never hit the ShardDO after the first load.

### Deduplication

Dedup is per-ShardDO. Since rendezvous hashing deterministically maps a given chunk hash to the same shard (given the same pool size), identical chunks from different files land on the same ShardDO and are deduplicated.

```
File A (10 MB) and File B (10 MB) share 3 identical chunks:
  Chunk with hash "abc123" → rendezvous → Shard 7 (both files)
  Chunk with hash "def456" → rendezvous → Shard 14 (both files)
  Chunk with hash "789abc" → rendezvous → Shard 3 (both files)

Shard 7 stores "abc123" once with ref_count = 2
Shard 14 stores "def456" once with ref_count = 2
Shard 3 stores "789abc" once with ref_count = 2

Storage saved: 3 × 1 MB = 3 MB
```

**Cross-user dedup is NOT attempted.** Different users have different shard pools (different user IDs in the DO names). This is intentional:
- Security: user A's data is never stored in user B's DOs
- Isolation: user A's hot file doesn't slow down user B
- Simplicity: no shared reference counting across users

### Garbage Collection

When a file is deleted, its chunk references are removed. Chunks with `ref_count = 0` are garbage:

```typescript
async removeFileRefs(fileId: string): Promise<{ freedBytes: number }> {
  let freedBytes = 0;

  // Get all chunk refs for this file
  const refs = this.sql.exec(
    `SELECT chunk_hash FROM chunk_refs WHERE file_id = ?`, fileId
  ).toArray();

  for (const ref of refs) {
    // Remove the reference
    this.sql.exec(
      `DELETE FROM chunk_refs WHERE chunk_hash = ? AND file_id = ?`,
      ref.chunk_hash, fileId
    );

    // Decrement ref count
    this.sql.exec(
      `UPDATE chunks SET ref_count = ref_count - 1 WHERE hash = ?`,
      ref.chunk_hash
    );

    // Check if chunk is now garbage
    const chunk = this.sql.exec(
      `SELECT size FROM chunks WHERE hash = ? AND ref_count <= 0`,
      ref.chunk_hash
    ).one();

    if (chunk) {
      this.sql.exec(`DELETE FROM chunks WHERE hash = ?`, ref.chunk_hash);
      freedBytes += chunk.size;
    }
  }

  this.updateCapacity(-freedBytes);
  return { freedBytes };
}
```

### Integrity Verification (Bitrot Protection)

Periodic scrubbing verifies chunk integrity. Run via DO alarm, e.g., weekly:

```typescript
async scrubChunks(): Promise<{ checked: number; corrupted: number }> {
  let checked = 0, corrupted = 0;

  // Sample random chunks for verification
  const chunks = this.sql.exec(
    `SELECT hash, data FROM chunks ORDER BY RANDOM() LIMIT 500`
  ).toArray();

  for (const chunk of chunks) {
    const actualHash = await hashChunk(new Uint8Array(chunk.data));
    if (actualHash !== chunk.hash) {
      corrupted++;
      console.error(`CORRUPTION: chunk ${chunk.hash} actual hash: ${actualHash}`);
      // Mark as corrupted for re-upload from client
      // (client can re-derive chunk from original file)
    }
    checked++;
  }

  return { checked, corrupted };
}
```

### Shard Capacity Management

Each ShardDO tracks its own capacity. When a shard approaches 10 GB, it rejects new writes:

```typescript
class ShardDO {
  private capacityUsedBytes: number = 0;
  private initialized: boolean = false;

  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return;

    const meta = this.sql.exec(
      `SELECT value FROM shard_meta WHERE key = 'capacity_used_bytes'`
    ).one();

    this.capacityUsedBytes = meta ? Number(meta.value) : 0;
    this.initialized = true;
  }

  getCapacityUsed(): bigint {
    return BigInt(this.capacityUsedBytes);
  }

  updateCapacity(deltaBytes: number): void {
    this.capacityUsedBytes += deltaBytes;
    // Persist periodically (not every write — too expensive)
    // DO alarm persists every 60 seconds
    this.dirty = true;
  }

  // Capacity report (called by Worker to check shard health)
  async getCapacityReport(): Promise<ShardCapacityReport> {
    await this.ensureInitialized();
    return {
      capacityUsedBytes: this.capacityUsedBytes,
      capacityTotalBytes: 10 * 1024 * 1024 * 1024, // 10 GB
      chunkCount: this.sql.exec(`SELECT COUNT(*) as cnt FROM chunks`).one()!.cnt,
      isFull: this.capacityUsedBytes > 9 * 1024 * 1024 * 1024, // > 9 GB
    };
  }
}

interface ShardCapacityReport {
  capacityUsedBytes: number;
  capacityTotalBytes: number;
  chunkCount: number;
  isFull: boolean;
}
```

### What Happens When a Shard Is Full?

When a ShardDO approaches 10 GB, new chunks that would be routed there need to go somewhere else. The system handles this automatically:

1. **ShardDO rejects the write** (returns 507 Insufficient Storage)
2. **Worker retries on the next-best shard** (second-highest rendezvous score)
3. **UserDO grows the pool** (adds more shards so future placements spread more evenly)

```typescript
// In Worker: handle shard-full scenario
async function writeChunkWithOverflow(
  env: Env,
  userId: string,
  fileId: string,
  chunkIndex: number,
  chunkHash: string,
  chunkData: ArrayBuffer,
  poolSize: number
): Promise<{ shardIndex: number; status: string }> {

  // Get all shards ranked by rendezvous score for this chunk
  const ranked = rankShardsByScore(userId, fileId, chunkIndex, poolSize);

  for (const { shardIndex, doName } of ranked) {
    const shardDO = env.SHARD_DO.get(env.SHARD_DO.idFromName(doName));
    const response = await shardDO.fetch(new Request('http://internal/chunk', {
      method: 'PUT',
      headers: {
        'X-Chunk-Hash': chunkHash,
        'X-File-Id': fileId,
        'X-Chunk-Index': chunkIndex.toString(),
        'X-User-Id': userId,
      },
      body: chunkData,
    }));

    if (response.ok) {
      return { shardIndex, status: 'stored' };
    }

    if (response.status === 507) {
      // Shard full — try next ranked shard
      continue;
    }

    throw new Error(`Shard write failed: ${response.status}`);
  }

  throw new Error('All shards full — pool growth required');
}

// Rank all shards by rendezvous score (descending)
function rankShardsByScore(
  userId: string,
  fileId: string,
  chunkIndex: number,
  poolSize: number
): Array<{ shardIndex: number; doName: string; score: number }> {
  const results = [];
  for (let shard = 0; shard < poolSize; shard++) {
    const doName = shardDOName(userId, shard);
    const score = placementScore(fileId, chunkIndex, doName);
    results.push({ shardIndex: shard, doName, score });
  }
  return results.sort((a, b) => b.score - a.score);
}
```

**Important:** When a chunk ends up on a non-primary shard (due to overflow), the actual shard index is recorded in the UserDO's `file_chunks.shard_index` column. The manifest always reflects reality, not just the ideal placement.

---

## 5. UserDO Architecture

Each user gets exactly one UserDO, identified by `user:{userId}`. It is the **single source of truth** for that user's account: authentication, file metadata, album organization, sharing permissions, quota tracking, and background job coordination.

**The UserDO stores NO chunk data.** Only metadata. At ~1 KB per file, the 10 GB DO limit supports ~10 million files.

### UserDO SQLite Schema

```sql
----------------------------------------------------------------------
-- AUTHENTICATION
----------------------------------------------------------------------
CREATE TABLE auth (
  user_id       TEXT PRIMARY KEY,
  email         TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,          -- Argon2id hash
  salt          TEXT NOT NULL,          -- 16-byte random salt (hex)
  created_at    INTEGER NOT NULL,       -- Unix ms
  updated_at    INTEGER NOT NULL
);

CREATE TABLE sessions (
  session_id    TEXT PRIMARY KEY,       -- random 32-byte token (hex)
  user_id       TEXT NOT NULL REFERENCES auth(user_id),
  created_at    INTEGER NOT NULL,
  expires_at    INTEGER NOT NULL,       -- absolute expiry (Unix ms)
  ip_address    TEXT,
  user_agent    TEXT
);

CREATE INDEX idx_sessions_user ON sessions(user_id);
CREATE INDEX idx_sessions_expires ON sessions(expires_at);

----------------------------------------------------------------------
-- FILE METADATA
----------------------------------------------------------------------
CREATE TABLE files (
  file_id       TEXT PRIMARY KEY,       -- ULID (sortable, unique)
  user_id       TEXT NOT NULL,
  parent_id     TEXT REFERENCES folders(folder_id),  -- NULL = root
  file_name     TEXT NOT NULL,
  file_size     INTEGER NOT NULL,       -- bytes
  file_hash     TEXT NOT NULL,          -- SHA-256 of chunk hash list
  mime_type     TEXT NOT NULL,
  chunk_size    INTEGER NOT NULL,       -- always 1,048,576 for files > 1 MB
  chunk_count   INTEGER NOT NULL,       -- ceil(file_size / 1MB), or 1
  pool_size     INTEGER NOT NULL,       -- ShardDO pool size at upload time
  status        TEXT NOT NULL DEFAULT 'uploading',
                                        -- uploading | complete | failed | deleted
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL,
  deleted_at    INTEGER,                -- soft delete timestamp

  -- Image-specific metadata (populated after upload via AI pipeline)
  width         INTEGER,
  height        INTEGER,
  taken_at      INTEGER,                -- EXIF date taken (Unix ms)
  latitude      REAL,
  longitude     REAL,
  camera_model  TEXT,
  orientation   INTEGER,

  -- AI/Search metadata
  embedding_id  TEXT,                   -- Vectorize vector ID
  caption       TEXT,                   -- AI-generated caption
  labels        TEXT                    -- JSON array of detected labels
);

CREATE INDEX idx_files_user ON files(user_id, created_at DESC);
CREATE INDEX idx_files_parent ON files(parent_id);
CREATE INDEX idx_files_hash ON files(file_hash);
CREATE INDEX idx_files_status ON files(status);
CREATE INDEX idx_files_mime ON files(mime_type);
CREATE INDEX idx_files_taken ON files(taken_at);

-- File chunks manifest: ordered list of chunk hashes per file
-- This is the critical link between files and ShardDOs
CREATE TABLE file_chunks (
  file_id       TEXT NOT NULL REFERENCES files(file_id),
  chunk_index   INTEGER NOT NULL,
  chunk_hash    TEXT NOT NULL,          -- SHA-256 of chunk data
  chunk_size    INTEGER NOT NULL,       -- actual bytes (last chunk may be smaller)
  shard_index   INTEGER NOT NULL,       -- which ShardDO stores this chunk
  PRIMARY KEY (file_id, chunk_index)
);

----------------------------------------------------------------------
-- FOLDER TREE
----------------------------------------------------------------------
CREATE TABLE folders (
  folder_id     TEXT PRIMARY KEY,       -- ULID
  user_id       TEXT NOT NULL,
  parent_id     TEXT REFERENCES folders(folder_id),  -- NULL = root
  name          TEXT NOT NULL,
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL,
  UNIQUE(parent_id, name)               -- no duplicate names in same folder
);

CREATE INDEX idx_folders_parent ON folders(parent_id);

----------------------------------------------------------------------
-- ALBUMS
----------------------------------------------------------------------
CREATE TABLE albums (
  album_id      TEXT PRIMARY KEY,       -- ULID
  user_id       TEXT NOT NULL,
  name          TEXT NOT NULL,
  description   TEXT,
  cover_file_id TEXT REFERENCES files(file_id),
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
);

CREATE TABLE album_items (
  album_id      TEXT NOT NULL REFERENCES albums(album_id),
  file_id       TEXT NOT NULL REFERENCES files(file_id),
  sort_order    INTEGER NOT NULL DEFAULT 0,
  added_at      INTEGER NOT NULL,
  PRIMARY KEY (album_id, file_id)
);

CREATE INDEX idx_album_items_file ON album_items(file_id);

----------------------------------------------------------------------
-- SHARING & PERMISSIONS
----------------------------------------------------------------------
CREATE TABLE shares (
  share_id      TEXT PRIMARY KEY,       -- ULID
  owner_id      TEXT NOT NULL,          -- user who shared
  target_type   TEXT NOT NULL,          -- 'album' | 'file' | 'folder'
  target_id     TEXT NOT NULL,          -- album_id / file_id / folder_id
  share_type    TEXT NOT NULL,          -- 'link' | 'user'
  grantee_id    TEXT,                   -- target user_id (NULL for link shares)
  permission    TEXT NOT NULL DEFAULT 'viewer',
                                        -- 'viewer' | 'contributor' | 'owner'
  token         TEXT UNIQUE,            -- random token for link shares
  expires_at    INTEGER,               -- NULL = no expiry
  created_at    INTEGER NOT NULL
);

CREATE INDEX idx_shares_target ON shares(target_type, target_id);
CREATE INDEX idx_shares_grantee ON shares(grantee_id);
CREATE INDEX idx_shares_token ON shares(token);

----------------------------------------------------------------------
-- QUOTA & POOL MANAGEMENT
----------------------------------------------------------------------
CREATE TABLE quota (
  user_id       TEXT PRIMARY KEY,
  storage_used  INTEGER NOT NULL DEFAULT 0,    -- bytes currently stored
  storage_limit INTEGER NOT NULL DEFAULT 107374182400,
                                                -- 100 GB default limit
  file_count    INTEGER NOT NULL DEFAULT 0,
  pool_size     INTEGER NOT NULL DEFAULT 32,   -- current ShardDO pool size
  prev_pool_size INTEGER,                       -- set during migration
  migration_started_at INTEGER                  -- epoch ms, NULL when not migrating
);

----------------------------------------------------------------------
-- BACKGROUND JOB QUEUE (embedding generation, thumbnail creation)
----------------------------------------------------------------------
CREATE TABLE job_queue (
  job_id        TEXT PRIMARY KEY,       -- ULID
  job_type      TEXT NOT NULL,          -- 'embedding' | 'thumbnail' | 'migration'
  file_id       TEXT,                   -- target file (NULL for migration jobs)
  status        TEXT NOT NULL DEFAULT 'pending',
                                        -- pending | processing | complete | failed
  attempts      INTEGER NOT NULL DEFAULT 0,
  max_attempts  INTEGER NOT NULL DEFAULT 3,
  error         TEXT,                   -- last error message
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
);

CREATE INDEX idx_jobs_status ON job_queue(status, created_at);
CREATE INDEX idx_jobs_file ON job_queue(file_id);
```

### Auth System

```typescript
interface AuthResult {
  userId: string;
  sessionId: string;
  expiresAt: number;
}

// Signup flow
async signup(email: string, password: string): Promise<AuthResult> {
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const passwordHash = await argon2id(password, salt, {
    memory: 19456,  // 19 MiB (fits in DO 128 MB)
    iterations: 2,
    parallelism: 1,
  });

  const userId = ulid();
  const sessionId = hex(crypto.getRandomValues(new Uint8Array(32)));
  const now = Date.now();

  this.sql.exec('BEGIN TRANSACTION');
  this.sql.exec(
    `INSERT INTO auth (user_id, email, password_hash, salt, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?)`,
    userId, email, passwordHash, hex(salt), now, now
  );
  this.sql.exec(
    `INSERT INTO quota (user_id) VALUES (?)`, userId
  );
  this.sql.exec(
    `INSERT INTO sessions (session_id, user_id, created_at, expires_at)
     VALUES (?, ?, ?, ?)`,
    sessionId, userId, now, now + 30 * 24 * 60 * 60 * 1000  // 30 days
  );
  this.sql.exec('COMMIT');

  return { userId, sessionId, expiresAt: now + 30 * 86400000 };
}

// Login flow
async login(email: string, password: string): Promise<AuthResult> {
  const user = this.sql.exec(
    `SELECT user_id, password_hash, salt FROM auth WHERE email = ?`, email
  ).one();

  if (!user) throw new Error('Invalid credentials');
  if (!await argon2idVerify(user.password_hash, password, user.salt)) {
    throw new Error('Invalid credentials');
  }

  const sessionId = hex(crypto.getRandomValues(new Uint8Array(32)));
  const now = Date.now();
  this.sql.exec(
    `INSERT INTO sessions (session_id, user_id, created_at, expires_at)
     VALUES (?, ?, ?, ?)`,
    sessionId, user.user_id, now, now + 30 * 86400000
  );

  return { userId: user.user_id, sessionId, expiresAt: now + 30 * 86400000 };
}

// Session validation (called on every authenticated request)
async validateSession(sessionId: string): Promise<string | null> {
  const session = this.sql.exec(
    `SELECT user_id FROM sessions WHERE session_id = ? AND expires_at > ?`,
    sessionId, Date.now()
  ).one();
  return session?.user_id ?? null;
}
```

### File Manifest Retrieval

The `file_chunks` table is the critical link between the file tree and physical chunk storage. On download, the UserDO returns the full manifest:

```typescript
async getFileManifest(fileId: string): Promise<FileManifest> {
  const file = this.sql.exec(
    `SELECT * FROM files WHERE file_id = ? AND status = 'complete'`, fileId
  ).one();
  if (!file) throw new Error('File not found');

  const chunks = this.sql.exec(
    `SELECT chunk_index, chunk_hash, chunk_size, shard_index
     FROM file_chunks
     WHERE file_id = ?
     ORDER BY chunk_index`,
    fileId
  ).toArray();

  return {
    fileId: file.file_id,
    fileName: file.file_name,
    fileSize: file.file_size,
    fileHash: file.file_hash,
    mimeType: file.mime_type,
    chunkSize: file.chunk_size,
    chunkCount: file.chunk_count,
    poolSize: file.pool_size,
    chunks: chunks.map(c => ({
      index: c.chunk_index,
      offset: c.chunk_index * file.chunk_size,
      size: c.chunk_size,
      hash: c.chunk_hash,
      shardIndex: c.shard_index,
    })),
    createdAt: file.created_at,
  };
}
```

**Key difference from v1:** The manifest now includes `shardIndex` for each chunk. The client uses this to open direct connections to the correct ShardDOs. The client does NOT need to recompute placement — the manifest tells it exactly where each chunk lives.

### Quota and Pool Management

```typescript
// Pool size formula (must match Section 3)
function computePoolSize(storageUsedBytes: number): number {
  const BASE_POOL = 32;
  const BYTES_PER_SHARD = 5 * 1024 * 1024 * 1024; // 5 GB
  const additional = Math.floor(storageUsedBytes / BYTES_PER_SHARD);
  return BASE_POOL + additional;
}

async checkQuota(additionalBytes: number): Promise<boolean> {
  const quota = this.sql.exec(
    `SELECT storage_used, storage_limit FROM quota WHERE user_id = ?`, this.userId
  ).one();
  return (quota.storage_used + additionalBytes) <= quota.storage_limit;
}

async updateUsage(deltaBytes: number, deltaFiles: number): Promise<void> {
  this.sql.exec(
    `UPDATE quota SET
       storage_used = storage_used + ?,
       file_count = file_count + ?
     WHERE user_id = ?`,
    deltaBytes, deltaFiles, this.userId
  );

  // Check if pool needs to grow
  const quota = this.sql.exec(
    `SELECT storage_used, pool_size FROM quota WHERE user_id = ?`, this.userId
  ).one();

  const newPoolSize = computePoolSize(Number(quota.storage_used));
  if (newPoolSize > quota.pool_size) {
    // Start migration: record old pool size, update to new
    this.sql.exec(
      `UPDATE quota SET
         prev_pool_size = pool_size,
         pool_size = ?,
         migration_started_at = ?
       WHERE user_id = ?`,
      newPoolSize, Date.now(), this.userId
    );

    // Schedule migration alarm
    this.enqueueJob('migration', null);
  }
}
```

### Background Job Processing (DO Alarms)

The UserDO uses Cloudflare DO alarms for all async work: embedding generation, thumbnail creation, chunk migration.

```typescript
async enqueueJob(jobType: string, fileId: string | null): Promise<void> {
  const now = Date.now();
  this.sql.exec(
    `INSERT INTO job_queue (job_id, job_type, file_id, status, created_at, updated_at)
     VALUES (?, ?, ?, 'pending', ?, ?)`,
    ulid(), jobType, fileId, now, now
  );

  // Ensure alarm is set (idempotent — if already set, this is a no-op conceptually)
  await this.ctx.storage.setAlarm(Date.now() + 1000);
}

async alarm(): Promise<void> {
  // Process up to 5 pending jobs per alarm fire
  const jobs = this.sql.exec(
    `SELECT * FROM job_queue
     WHERE status = 'pending' AND attempts < max_attempts
     ORDER BY created_at ASC
     LIMIT 5`
  ).toArray();

  for (const job of jobs) {
    this.sql.exec(
      `UPDATE job_queue SET status = 'processing', attempts = attempts + 1,
       updated_at = ? WHERE job_id = ?`,
      Date.now(), job.job_id
    );

    try {
      switch (job.job_type) {
        case 'embedding':
          await this.generateEmbedding(job.file_id);
          break;
        case 'thumbnail':
          await this.generateThumbnail(job.file_id);
          break;
        case 'migration':
          await this.runMigrationBatch();
          break;
      }

      this.sql.exec(
        `UPDATE job_queue SET status = 'complete', updated_at = ? WHERE job_id = ?`,
        Date.now(), job.job_id
      );
    } catch (error) {
      this.sql.exec(
        `UPDATE job_queue SET status = 'failed', error = ?, updated_at = ?
         WHERE job_id = ?`,
        (error as Error).message, Date.now(), job.job_id
      );
    }
  }

  // If more pending jobs, schedule another alarm
  const remaining = this.sql.exec(
    `SELECT COUNT(*) as cnt FROM job_queue
     WHERE status IN ('pending', 'failed') AND attempts < max_attempts`
  ).one();

  if (remaining && remaining.cnt > 0) {
    await this.ctx.storage.setAlarm(Date.now() + 2000);
  }
}
```

---

## 6. Parallel Transfer Protocol

### Design Philosophy: Unlimited Parallelism

v1 capped parallelism at 4-6 connections. v2 removes all artificial caps. The concurrency is determined by:

```
concurrency = min(
  chunk_count,                        // can't exceed number of chunks
  unique_DOs_involved,                // can't exceed number of distinct shards
  client_bandwidth / per_chunk_throughput,  // don't exceed bandwidth
  BROWSER_CONNECTION_LIMIT            // ~256 for modern browsers
)
```

**For a 1 GB file:** 1,024 chunks across ~52 shards → client can run 52 concurrent chunk downloads. At 100 Mbps, this saturates bandwidth with ~13 streams (1 MB chunks at ~8 MB/s per stream). The client adaptively finds the sweet spot.

**For a 100 MB file:** 100 chunks across ~31 shards → 31 concurrent downloads. Even 20 Mbps bandwidth is saturated by 3-4 streams.

The protocol never artificially limits what the network can handle.

### Upload Flow

```
┌───────────────────────────────────────────────────────────────────────────┐
│                          UPLOAD PROTOCOL                                  │
│                                                                          │
│  Client                     Worker Router              ShardDOs           │
│    │                            │                         │               │
│    │ 1. POST /upload/init       │                         │               │
│    │    {name, size, mime}      │                         │               │
│    │───────────────────────────►│                         │               │
│    │                            │ 2. → UserDO:            │               │
│    │                            │    checkQuota()         │               │
│    │                            │    computeChunkSpec()   │               │
│    │                            │    createFile(status=   │               │
│    │                            │      'uploading')       │               │
│    │                            │                         │               │
│    │ 3. ◄── {fileId,            │                         │               │
│    │    chunkSize: 1MB,         │                         │               │
│    │    chunkCount, poolSize,   │                         │               │
│    │    uploadToken}            │                         │               │
│    │◄───────────────────────────│                         │               │
│    │                            │                         │               │
│    │ 4. Client-side:            │                         │               │
│    │    split file → chunks     │                         │               │
│    │    SHA-256 each chunk      │                         │               │
│    │    placeChunk() for each   │                         │               │
│    │    group by target shard   │                         │               │
│    │                            │                         │               │
│    │ 5. PUT /upload/chunk × N PARALLEL (N = adaptive)     │               │
│    │───────────────────────────►│──────────────────────►│ Shard-14      │
│    │───────────────────────────►│──────────────────────►│ Shard-7       │
│    │───────────────────────────►│──────────────────────►│ Shard-28      │
│    │───────────────────────────►│──────────────────────►│ Shard-3       │
│    │───────────────────────────►│──────────────────────►│ Shard-21      │
│    │  ... up to 30-50 parallel  │  ... each to correct   │               │
│    │                            │      ShardDO            │               │
│    │                            │                         │               │
│    │ 6. ◄── chunk confirmations (parallel, as they complete)              │
│    │◄───────────────────────────│◄─────────────────────│                 │
│    │                            │                         │               │
│    │ 7. POST /upload/complete   │                         │               │
│    │    {fileId, chunkHashes[]} │                         │               │
│    │───────────────────────────►│ 8. → UserDO:            │               │
│    │                            │    verifyAllChunks()    │               │
│    │                            │    updateFile→complete  │               │
│    │                            │    updateQuota()        │               │
│    │                            │    enqueue(embedding)   │               │
│    │                            │    enqueue(thumbnail)   │               │
│    │                            │                         │               │
│    │ 9. ◄── 201 Created         │                         │               │
│    │◄───────────────────────────│                         │               │
└───────────────────────────────────────────────────────────────────────────┘
```

### Client-Side Upload Orchestrator

```typescript
interface UploadConfig {
  fileId: string;
  userId: string;
  chunkSize: number;       // always 1,048,576
  chunkCount: number;
  poolSize: number;
  uploadToken: string;
}

async function uploadFile(file: File, config: UploadConfig): Promise<void> {
  const { fileId, chunkSize, chunkCount, poolSize, uploadToken } = config;

  // ── Phase 1: Chunk, hash, and compute placement ──
  // This can be done incrementally via ReadableStream for large files

  const chunkPlans: ChunkUploadPlan[] = [];

  for (let i = 0; i < chunkCount; i++) {
    const offset = i * chunkSize;
    const size = Math.min(chunkSize, file.size - offset);
    const data = await file.slice(offset, offset + size).arrayBuffer();
    const hash = await hashChunk(new Uint8Array(data));
    const { shardIndex, doName } = placeChunk(config.userId, fileId, i, poolSize);
    chunkPlans.push({ index: i, hash, data, size, shardIndex, doName });
  }

  // ── Phase 2: Adaptive parallel upload ──
  const uploader = new AdaptiveUploader({
    initialConcurrency: 20,   // start with 20 parallel uploads
    maxConcurrency: 100,       // scale up to 100 if bandwidth allows
    minConcurrency: 4,         // never go below 4
    uploadToken,
    fileId,
    userId: config.userId,
  });

  await uploader.uploadAll(chunkPlans);

  // ── Phase 3: Finalize ──
  await fetch('/api/upload/complete', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${uploadToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      fileId,
      chunkHashes: chunkPlans.map(c => c.hash),
    }),
  });
}

interface ChunkUploadPlan {
  index: number;
  hash: string;
  data: ArrayBuffer;
  size: number;
  shardIndex: number;
  doName: string;
}
```

### Adaptive Concurrency Controller

The client dynamically adjusts concurrency based on observed throughput. This is the core mechanism that replaces v1's fixed 4-6 connections.

```typescript
class AdaptiveUploader {
  private concurrency: number;
  private maxConcurrency: number;
  private minConcurrency: number;
  private inFlight = new Set<Promise<void>>();
  private queue: ChunkUploadPlan[] = [];
  private completedChunks = 0;
  private totalBytes = 0;
  private startTime = 0;

  // Throughput tracking (sliding window)
  private recentThroughputs: number[] = [];  // bytes/sec per recent chunk
  private windowSize = 10;

  constructor(config: {
    initialConcurrency: number;
    maxConcurrency: number;
    minConcurrency: number;
    uploadToken: string;
    fileId: string;
    userId: string;
  }) {
    this.concurrency = config.initialConcurrency;
    this.maxConcurrency = config.maxConcurrency;
    this.minConcurrency = config.minConcurrency;
  }

  async uploadAll(chunks: ChunkUploadPlan[]): Promise<void> {
    this.queue = [...chunks];
    this.startTime = performance.now();

    while (this.queue.length > 0 || this.inFlight.size > 0) {
      // Fill up to current concurrency level
      while (this.inFlight.size < this.concurrency && this.queue.length > 0) {
        const chunk = this.queue.shift()!;
        const promise = this.uploadOneChunk(chunk)
          .then(() => {
            this.inFlight.delete(promise);
            this.completedChunks++;
            this.adjustConcurrency();
          })
          .catch((err) => {
            this.inFlight.delete(promise);
            // Retry: push back to queue with exponential backoff
            this.queue.push(chunk);
            this.concurrency = Math.max(
              this.minConcurrency,
              Math.floor(this.concurrency * 0.75)
            );
          });
        this.inFlight.add(promise);
      }

      // Wait for at least one to complete
      if (this.inFlight.size > 0) {
        await Promise.race(this.inFlight);
      }
    }
  }

  private async uploadOneChunk(chunk: ChunkUploadPlan): Promise<void> {
    const startMs = performance.now();

    const response = await fetch('/api/upload/chunk', {
      method: 'PUT',
      headers: {
        'Authorization': `Bearer ${this.uploadToken}`,
        'X-File-Id': this.fileId,
        'X-Chunk-Index': chunk.index.toString(),
        'X-Chunk-Hash': chunk.hash,
        'X-Shard-Index': chunk.shardIndex.toString(),
        'X-Pool-Size': this.poolSize.toString(),
      },
      body: chunk.data,
    });

    if (!response.ok) throw new Error(`Chunk upload failed: ${response.status}`);

    const elapsedMs = performance.now() - startMs;
    const throughput = chunk.size / (elapsedMs / 1000); // bytes/sec
    this.recordThroughput(throughput);
  }

  private recordThroughput(bps: number): void {
    this.recentThroughputs.push(bps);
    if (this.recentThroughputs.length > this.windowSize) {
      this.recentThroughputs.shift();
    }
  }

  /**
   * Adaptive concurrency adjustment.
   *
   * Strategy: Additive Increase / Multiplicative Decrease (AIMD)
   * - If average throughput per stream is stable/increasing → add 1 stream
   * - If average throughput per stream drops significantly → halve concurrency
   * - Never go below minConcurrency or above maxConcurrency
   */
  private adjustConcurrency(): void {
    if (this.recentThroughputs.length < this.windowSize) return;

    const avgRecent = average(this.recentThroughputs.slice(-5));
    const avgOlder = average(this.recentThroughputs.slice(0, 5));

    if (avgRecent >= avgOlder * 0.9) {
      // Throughput holding steady or improving → try more concurrency
      this.concurrency = Math.min(this.concurrency + 1, this.maxConcurrency);
    } else if (avgRecent < avgOlder * 0.7) {
      // Throughput dropped >30% → back off
      this.concurrency = Math.max(
        Math.floor(this.concurrency * 0.5),
        this.minConcurrency
      );
    }
    // Otherwise: no change (stable within 10-30% band)
  }
}

function average(arr: number[]): number {
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}
```

### Download Flow

```
┌───────────────────────────────────────────────────────────────────────────┐
│                         DOWNLOAD PROTOCOL                                 │
│                                                                          │
│  Client                     Worker Router              ShardDOs           │
│    │                            │                         │               │
│    │ 1. GET /files/{fileId}     │                         │               │
│    │───────────────────────────►│ 2. → UserDO:            │               │
│    │                            │    getFileManifest()    │               │
│    │                            │                         │               │
│    │ 3. ◄── manifest {          │                         │               │
│    │    chunks: [{hash,         │                         │               │
│    │     shardIndex, size}...]} │                         │               │
│    │◄───────────────────────────│                         │               │
│    │                            │                         │               │
│    │ 4. Client groups chunks by shard, opens parallel     │               │
│    │    connections to ALL relevant shards                 │               │
│    │                            │                         │               │
│    │ GET /chunks/{shard}/{hash} ────────────────────────►│ Shard-14      │
│    │ GET /chunks/{shard}/{hash} ────────────────────────►│ Shard-7       │
│    │ GET /chunks/{shard}/{hash} ────────────────────────►│ Shard-28      │
│    │ GET /chunks/{shard}/{hash} ────────────────────────►│ Shard-3       │
│    │ GET /chunks/{shard}/{hash} ────────────────────────►│ Shard-21      │
│    │  ... 20-50+ concurrent     │                         │               │
│    │                            │                         │               │
│    │ 5. ◄── chunk data (parallel, out of order)           │               │
│    │◄──────────────────────────────────────────────────│                 │
│    │                            │                         │               │
│    │ 6. Client reassembles:     │                         │               │
│    │    verify SHA-256 per chunk│                         │               │
│    │    write to buffer/disk    │                         │               │
│    │    in chunk order          │                         │               │
└───────────────────────────────────────────────────────────────────────────┘
```

### Client-Side Download Orchestrator

```typescript
async function downloadFile(fileId: string, auth: AuthContext): Promise<Blob> {
  // 1. Get manifest
  const manifestRes = await fetch(`/api/files/${fileId}`, {
    headers: { Authorization: `Bearer ${auth.token}` },
  });
  const manifest: FileManifest = await manifestRes.json();

  // 2. Group chunks by shard for connection reuse
  const byShardIndex = groupBy(manifest.chunks, c => c.shardIndex);
  const uniqueShards = Object.keys(byShardIndex).length;

  // 3. Compute initial concurrency
  const initialConcurrency = Math.min(
    manifest.chunkCount,
    uniqueShards,
    30  // start conservative, scale up adaptively
  );

  // 4. Download all chunks with adaptive parallelism
  const downloader = new AdaptiveDownloader({
    initialConcurrency,
    maxConcurrency: Math.min(uniqueShards * 2, 100), // up to 2 per shard
    minConcurrency: 4,
    auth,
    userId: manifest.userId,
  });

  const chunkBuffers = await downloader.downloadAll(manifest.chunks);

  // 5. Verify and reassemble
  const parts: ArrayBuffer[] = new Array(manifest.chunkCount);
  for (const [index, buffer] of chunkBuffers) {
    const actualHash = await hashChunk(new Uint8Array(buffer));
    if (actualHash !== manifest.chunks[index].hash) {
      throw new Error(`Chunk ${index} integrity failed`);
    }
    parts[index] = buffer;
  }

  return new Blob(parts, { type: manifest.mimeType });
}

class AdaptiveDownloader {
  // Same AIMD pattern as AdaptiveUploader
  // Fetches chunks from: GET /api/chunks/{userId}/{shardIndex}/{chunkHash}
  // Each fetch is a single HTTP request → response with chunk data

  async downloadAll(
    chunks: ChunkSpec[]
  ): Promise<Map<number, ArrayBuffer>> {
    const results = new Map<number, ArrayBuffer>();
    const queue = [...chunks];

    // Sort by shard index to maximize connection reuse
    // (HTTP/2 multiplexes requests to same origin on one connection)
    queue.sort((a, b) => a.shardIndex - b.shardIndex);

    while (queue.length > 0 || this.inFlight.size > 0) {
      while (this.inFlight.size < this.concurrency && queue.length > 0) {
        const chunk = queue.shift()!;
        const promise = this.downloadOneChunk(chunk)
          .then((data) => {
            results.set(chunk.index, data);
            this.inFlight.delete(promise);
            this.adjustConcurrency();
          })
          .catch(() => {
            this.inFlight.delete(promise);
            queue.push(chunk); // retry
            this.concurrency = Math.max(
              this.minConcurrency,
              Math.floor(this.concurrency * 0.75)
            );
          });
        this.inFlight.add(promise);
      }

      if (this.inFlight.size > 0) {
        await Promise.race(this.inFlight);
      }
    }

    return results;
  }

  private async downloadOneChunk(chunk: ChunkSpec): Promise<ArrayBuffer> {
    const url = `/api/chunks/${this.userId}/${chunk.shardIndex}/${chunk.hash}`;
    const response = await fetch(url, {
      headers: { Authorization: `Bearer ${this.auth.token}` },
    });

    if (!response.ok) throw new Error(`Chunk fetch failed: ${response.status}`);
    return response.arrayBuffer();
  }
}
```

### Server-Side Streaming Download (Alternative)

For clients that want a single HTTP response (e.g., browser `<img>` tag, `<video>` tag, `curl`), the Worker can orchestrate the parallel fetch internally and stream a single response:

```typescript
async function handleStreamDownload(
  manifest: FileManifest,
  env: Env,
  userId: string
): Promise<Response> {
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();

  // Orchestrate parallel chunk fetches, write in order
  (async () => {
    const PREFETCH = 10; // prefetch 10 chunks ahead
    const pending = new Map<number, Promise<ArrayBuffer>>();

    for (let i = 0; i < manifest.chunkCount; i++) {
      // Prefetch upcoming chunks
      for (let j = i; j < Math.min(i + PREFETCH, manifest.chunkCount); j++) {
        if (!pending.has(j)) {
          const chunk = manifest.chunks[j];
          const shardDO = env.SHARD_DO.get(
            env.SHARD_DO.idFromName(shardDOName(userId, chunk.shardIndex))
          );
          pending.set(j, shardDO.fetch(
            new Request(`http://internal/chunk/${chunk.hash}`)
          ).then(r => r.arrayBuffer()));
        }
      }

      // Write chunk i (blocks if client backpressures)
      const data = await pending.get(i)!;

      // Verify integrity
      const actualHash = await hashChunk(new Uint8Array(data));
      if (actualHash !== manifest.chunks[i].hash) {
        throw new Error(`Chunk ${i} integrity check failed`);
      }

      await writer.write(new Uint8Array(data));
      pending.delete(i);
    }
    await writer.close();
  })();

  return new Response(readable, {
    headers: {
      'Content-Type': manifest.mimeType,
      'Content-Length': manifest.fileSize.toString(),
      'Content-Disposition': `attachment; filename="${manifest.fileName}"`,
    },
  });
}
```

### Concurrency Sweet Spot Analysis

How many parallel connections actually help?

```
Throughput vs Concurrency (100 Mbps = 12.5 MB/s client bandwidth)

Assumptions:
  - 1 MB chunks
  - DO read latency: 10 ms (SQLite BLOB fetch)
  - Worker proxy overhead: 2 ms
  - HTTP round-trip: 20 ms
  - Per-chunk total latency: ~32 ms
  - Per-chunk throughput: 1 MB / 0.032s = 31.25 MB/s (exceeds bandwidth)
  - Effective per-stream throughput (bandwidth-limited): ~12.5 MB/s / N streams

Throughput (MB/s)
  12.5 ┤                ●────●────●────●────●────●  bandwidth cap
       │            ●───
  10.0 ┤        ●──
       │      ●─
   7.5 ┤    ●─
       │   ●
   5.0 ┤  ●
       │ ●
   2.5 ┤●
       │
   0.0 ┤
       └──┬──┬──┬──┬──┬──┬──┬──┬──┬──
          1  2  4  6  8 12 16 20 30 50
              Number of Parallel Streams

At 100 Mbps:
  N=1:   ~3.1 MB/s   (latency-limited: 1MB/32ms overhead per chunk)
  N=2:   ~6.1 MB/s
  N=4:   ~10.8 MB/s
  N=6:   ~12.5 MB/s  ← bandwidth saturated at only 6 streams!
  N=8:   ~12.5 MB/s
  N=50:  ~12.5 MB/s  (no additional benefit)

At 1 Gbps:
  N=1:   ~3.1 MB/s   (still latency-limited)
  N=4:   ~12.2 MB/s
  N=8:   ~24.0 MB/s
  N=16:  ~46.0 MB/s
  N=32:  ~82.0 MB/s
  N=40:  ~100 MB/s   ← bandwidth saturated at 40 streams
  N=50:  ~105 MB/s   (marginal gains from reduced idle time)
  N=100: ~110 MB/s

At 10 Gbps (data center):
  N=1:   ~3.1 MB/s
  N=10:  ~30 MB/s
  N=50:  ~140 MB/s
  N=100: ~260 MB/s
  N=200: ~460 MB/s   ← still scaling at 200 streams!

Key insight: the faster the client, the more parallel streams help.
Mossaic's adaptive controller finds the optimal N automatically.
```

### Protocol: HTTP/2 Multiplexing

**Decision: HTTP/2 for all chunk transfers.**

```
┌────────────────────────────────────────────────────────────────────┐
│ Why HTTP/2 (not HTTP/1.1 or WebSocket)                             │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  HTTP/1.1 × N connections:                                         │
│    ✗ Each connection needs its own TLS handshake (~100ms)          │
│    ✗ 50 connections = 50 TLS handshakes = 5 seconds overhead       │
│    ✗ Browser limits ~6 connections per origin (HTTP/1.1)           │
│                                                                    │
│  HTTP/2 (single connection, multiplexed):                          │
│    ✓ ONE TLS handshake for ALL chunk requests                      │
│    ✓ Up to 256 concurrent streams on one connection                │
│    ✓ HPACK compresses repeated headers (auth tokens)               │
│    ✓ Cloudflare edge handles HTTP/2 natively                       │
│    ✓ Browser's fetch() API handles multiplexing transparently      │
│    ✗ Head-of-line blocking on single TCP stream (solved by HTTP/3) │
│                                                                    │
│  WebSocket:                                                        │
│    ✓ Persistent connection, low overhead per message                │
│    ✗ Requires custom framing protocol                              │
│    ✗ No native request/response semantics                          │
│    ✗ Harder to debug, no browser dev tools request inspection      │
│    ✗ Cloudflare DO WebSocket hibernation adds complexity           │
│                                                                    │
│  Decision: HTTP/2 now, HTTP/3 (QUIC) as upgrade path              │
│    HTTP/3 eliminates HoL blocking via independent QUIC streams     │
└────────────────────────────────────────────────────────────────────┘
```

### Upload Resume

Uploads can be interrupted and resumed. The server tracks per-chunk status:

```typescript
// Ask server which chunks already exist
async function getUploadStatus(
  fileId: string,
  auth: AuthContext
): Promise<Set<number>> {
  const response = await fetch(`/api/upload/status/${fileId}`, {
    headers: { Authorization: `Bearer ${auth.token}` },
  });
  const { completedChunks } = await response.json();
  return new Set(completedChunks); // chunk indices already stored
}

// Client resumes by skipping completed chunks
async function resumeUpload(file: File, config: UploadConfig): Promise<void> {
  const completed = await getUploadStatus(config.fileId, auth);
  const chunkPlans = computeAllChunkPlans(file, config);
  const remaining = chunkPlans.filter(c => !completed.has(c.index));

  const uploader = new AdaptiveUploader({ /* ... */ });
  await uploader.uploadAll(remaining);

  // Finalize
  await fetch('/api/upload/complete', { /* ... */ });
}
```

### Download Resume (Range Requests)

Standard HTTP Range headers enable resumable downloads:

```
GET /api/files/{fileId}/stream
Range: bytes=5242880-

→ Server maps byte 5,242,880 to chunk index 5 (offset 0 within chunk)
→ Starts streaming from chunk 5 onward
→ Returns 206 Partial Content
```

### Error Handling and Retry

```typescript
const RETRY_CONFIG = {
  maxRetries: 3,
  baseDelay: 500,    // ms
  maxDelay: 5000,    // ms
  backoffFactor: 2,
};

async function fetchChunkWithRetry(
  url: string,
  attempt: number = 0
): Promise<ArrayBuffer> {
  try {
    const response = await fetch(url, { headers: { /* auth */ } });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.arrayBuffer();
  } catch (error) {
    if (attempt >= RETRY_CONFIG.maxRetries) throw error;

    // Exponential backoff with jitter
    const delay = Math.min(
      RETRY_CONFIG.baseDelay * Math.pow(RETRY_CONFIG.backoffFactor, attempt),
      RETRY_CONFIG.maxDelay
    ) * (0.5 + Math.random() * 0.5);

    await new Promise(r => setTimeout(r, delay));
    return fetchChunkWithRetry(url, attempt + 1);
  }
}
```

### Progress Tracking

```typescript
interface TransferProgress {
  fileId: string;
  direction: 'upload' | 'download';
  totalChunks: number;
  completedChunks: number;
  failedChunks: number;
  bytesTransferred: number;
  bytesTotal: number;
  activeConcurrency: number;     // current number of parallel streams
  throughputBps: number;          // current throughput (EWMA)
  estimatedRemainingMs: number;
}

function onChunkComplete(
  chunk: ChunkSpec,
  progress: TransferProgress,
  elapsedMs: number
): void {
  progress.completedChunks++;
  progress.bytesTransferred += chunk.size;

  const instantBps = chunk.size / (elapsedMs / 1000);
  progress.throughputBps = ewma(progress.throughputBps, instantBps, 0.3);

  const remaining = progress.bytesTotal - progress.bytesTransferred;
  progress.estimatedRemainingMs = (remaining / progress.throughputBps) * 1000;

  emit('transfer:progress', progress);
}

function ewma(current: number, sample: number, alpha: number): number {
  return alpha * sample + (1 - alpha) * current;
}
```

---
