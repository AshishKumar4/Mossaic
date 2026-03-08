# Mossaic Next-Gen Storage Engine — Architecture Document

> A distributed chunked file storage system on Cloudflare Workers + Durable Objects.
>
> **Version**: 2.0 | **Date**: March 2026

---

## Table of Contents

1. [Current System Analysis](#1-current-system-analysis)
2. [Adaptive Chunk Sizing](#2-adaptive-chunk-sizing)
3. [AIMD Concurrency Control](#3-aimd-concurrency-control)
4. [Shard Architecture](#4-shard-architecture)
5. [Fault Tolerance & Erasure Coding](#5-fault-tolerance--erasure-coding)
6. [Transfer Pipeline Optimizations](#6-transfer-pipeline-optimizations)
7. [Next-Gen Design](#7-next-gen-design)
8. [Implementation Roadmap](#8-implementation-roadmap)

---

## 1. Current System Analysis

### 1.1 System Parameters

| Parameter | Value | Source | Status |
|-----------|-------|--------|--------|
| `CHUNK_SIZE` | 1,048,576 (1 MB) | `shared/constants.ts` | **Active** — hardcoded |
| Upload concurrency | 6 | `src/hooks/use-upload.ts:82` | **Hardcoded** |
| Download concurrency | 6 | `src/hooks/use-download.ts:58` | **Hardcoded** |
| `MAX_UPLOAD_CONCURRENCY` | 50 | `shared/constants.ts` | **Defined but NEVER IMPORTED** |
| `INITIAL_UPLOAD_CONCURRENCY` | 20 | `shared/constants.ts` | **Defined but NEVER IMPORTED** |
| `MIN_UPLOAD_CONCURRENCY` | 4 | `shared/constants.ts` | **Defined but NEVER IMPORTED** |
| `BASE_POOL_SIZE` | 32 | `shared/constants.ts` | Active |
| `EXTRA_SHARD_INTERVAL` | 5 GB | `shared/constants.ts` | Active |
| Max retries | 3 | `src/hooks/use-upload.ts` | Active |
| Retry backoff | 500ms × 2^attempt | `src/hooks/use-upload.ts` | Active |
| DO SQLite blob limit | ~2 MB | Platform constraint | Hard limit |
| DO storage limit | 10 GB per DO | Platform constraint | Hard limit |

### 1.2 Architecture Overview

```
┌─────────────┐     ┌──────────────────┐     ┌────────────────────┐
│  React UI   │────▶│  CF Worker (Hono) │────▶│  Durable Objects   │
│  (Vite)     │     │  Routes + Auth    │     │  ShardDO (SQLite)  │
└─────────────┘     └──────────────────┘     └────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    │  Shard Pool (32+)  │
                    │  Rendezvous Hash   │
                    │  MurmurHash3       │
                    └───────────────────┘
```

**Upload flow:**
1. Client splits file into fixed 1 MB chunks
2. Computes SHA-256 hash per chunk (content-addressing)
3. Uploads 6 chunks concurrently via `PUT /api/upload/chunk`
4. Worker routes each chunk to a shard via rendezvous hashing
5. ShardDO stores chunk blob in SQLite, dedup via ref-counting
6. Client calls `POST /api/upload/complete` to finalize

**Download flow:**
1. Client fetches metadata: `GET /api/download/{fileId}/metadata`
2. Downloads 6 chunks concurrently: `GET /api/download/{fileId}/chunk/{index}`
3. Reassembles in order

### 1.3 Problems

**Fixed chunk size wastes resources at scale:**
- A 1 GB file generates 1,000 chunks → 1,001 HTTP requests (including init)
- Each request carries ~500 bytes of HTTP overhead (headers, TLS)
- At 1 MB chunks: 0.05% overhead. But the real cost is **request latency** — 1,000 sequential round-trips dominate wall time

**Hardcoded concurrency cannot adapt:**
- 6 concurrent workers is a guess. On fast networks, this leaves bandwidth on the table.
- On slow/congested networks, 6 concurrent uploads can overwhelm the connection, causing timeouts and retries.
- The codebase *defines* `MAX_UPLOAD_CONCURRENCY=50` and friends but **never imports them** — the frontend hooks hardcode `6`.

**Static shard pool:**
- 32 base shards is reasonable for small deployments but doesn't consider load distribution.
- No rebalancing mechanism — hot shards stay hot.
- No health-aware routing — a slow DO gets the same traffic as a fast one.

---

## 2. Adaptive Chunk Sizing

### 2.1 BitTorrent Piece Sizing Strategy

BitTorrent's piece size selection (BEP 3, libtorrent) is the most battle-tested algorithm for chunked file distribution. The strategy:

- **Target piece count**: 1,000–4,000 pieces per torrent
- **Piece sizes are powers of 2**: 16 KB, 32 KB, ..., 16 MB
- **Selection**: `piece_size = 2^ceil(log2(file_size / 1500))`
- **Clamp**: min 16 KB, max 16 MB

**Rationale**: Too few pieces = poor parallelism and coarse-grained fault recovery. Too many = excessive metadata overhead (each piece has a 20-byte SHA-1 hash in the .torrent file).

| File Size | BitTorrent Piece Size | Piece Count |
|-----------|----------------------|-------------|
| 1 MB      | 1 KB (clamped to 16 KB) | 64       |
| 10 MB     | 8 KB                 | 1,280       |
| 100 MB    | 128 KB               | 800         |
| 1 GB      | 1 MB                 | 1,024       |
| 10 GB     | 8 MB                 | 1,280       |
| 100 GB    | 16 MB (capped)       | 6,400       |

### 2.2 Content-Defined Chunking (FastCDC)

Traditional fixed-size chunking breaks when files are modified — inserting a single byte shifts all subsequent chunk boundaries, invalidating every downstream hash. **Content-Defined Chunking (CDC)** solves this by placing chunk boundaries at content-dependent positions.

**FastCDC algorithm** (Wen Xia et al., 2020):
1. Slide a window over the file content
2. Compute a rolling hash (Gear hash, faster than Rabin)
3. When `hash & mask == 0`, place a chunk boundary
4. The mask determines average chunk size: `mask = (1 << bits) - 1` where `avg_size = 2^bits`

**Pros**: Excellent dedup ratio for versioned files (only modified chunks re-upload).
**Cons**: Variable chunk sizes complicate storage (can't predict chunk count), CPU overhead for hashing.

**Recommendation for Mossaic**: Defer CDC to Phase 5. Our current SHA-256 content-addressing already provides per-chunk dedup. CDC adds value only for versioned/edited files — a future use case.

### 2.3 Our Adaptive Chunk Tiers

Given the Durable Object SQLite blob limit of ~2 MB, we can't use BitTorrent-scale piece sizes. Our tiers:

```typescript
function getChunkSize(fileSize: number): number {
  if (fileSize <= 1_048_576) return fileSize;          // ≤1 MB: single chunk
  if (fileSize <= 67_108_864) return 1_048_576;        // 1-64 MB: 1 MB chunks
  if (fileSize <= 536_870_912) return 1_572_864;       // 64-512 MB: 1.5 MB chunks
  return 2_097_152;                                     // 512 MB+: 2 MB chunks
}
```

**Design rationale:**

| File Size | Chunk Size | Chunk Count | HTTP Requests | vs V1 (1 MB fixed) |
|-----------|-----------|-------------|---------------|---------------------|
| 1 MB      | 1 MB      | 1           | 2             | Same |
| 10 MB     | 1 MB      | 10          | 11            | Same |
| 50 MB     | 1 MB      | 50          | 51            | Same |
| 100 MB    | 1.5 MB    | 67          | 68            | **33% fewer requests** |
| 500 MB    | 2 MB      | 250         | 251           | **50% fewer requests** |
| 1 GB      | 2 MB      | 512         | 513           | **50% fewer requests** |
| 5 GB      | 2 MB      | 2,560       | 2,561         | **50% fewer requests** |

**Key insight**: For files under 64 MB, 1 MB chunks are already optimal — the HTTP overhead of 10-64 requests is negligible. The wins come at 100 MB+ where reducing request count by 33-50% directly translates to lower wall time.

The 2 MB upper bound is a hard constraint from DO SQLite. If Cloudflare raises this limit, we should scale to 4-8 MB chunks for multi-GB files.

---

## 3. AIMD Concurrency Control

### 3.1 The Problem

Fixed concurrency (6 workers) is a static guess that can't adapt to:
- **Network conditions**: WiFi vs fiber, congested vs idle
- **Server load**: DO response times vary with shard utilization
- **File characteristics**: Many small chunks vs few large chunks

### 3.2 TCP Congestion Control for HTTP

TCP's congestion control (RFC 5681) solves an identical problem: how many segments to send before waiting for ACKs. We adapt this for HTTP chunk uploads.

**State variables:**
- `cwnd` (congestion window): max concurrent requests, float
- `ssthresh` (slow-start threshold): transition point, initially 32
- `srtt` (smoothed RTT): exponential moving average of request latency
- `rttvar` (RTT variance): variance estimate for timeout calculation

**Phases:**

```
                    ┌─────────────────┐
                    │   Slow Start    │  cwnd doubles per RTT
                    │   cwnd < ssth   │  (exponential growth)
                    └────────┬────────┘
                             │ cwnd >= ssthresh
                    ┌────────▼────────┐
                    │   Congestion    │  cwnd += 1/cwnd per success
                    │   Avoidance     │  (linear growth)
                    └────────┬────────┘
                             │ timeout or failure
                    ┌────────▼────────┐
                    │  Multiplicative │  ssthresh = cwnd/2
                    │  Decrease       │  cwnd = max(cwnd/2, minCwnd)
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  Recovery /     │  back to slow start or
                    │  Slow Start     │  congestion avoidance
                    └─────────────────┘
```

### 3.3 Implementation

```typescript
class AIMDController {
  private cwnd = 4;           // Start conservative
  private ssthresh = 32;      // Slow-start threshold
  private srtt = 0;           // Smoothed RTT (ms)
  private rttvar = 0;         // RTT variance (ms)
  private minCwnd = 2;
  private maxCwnd = 64;
  private rttInitialized = false;

  onSuccess(rttMs: number): void {
    // Update RTT estimates (Jacobson/Karels, RFC 6298)
    if (!this.rttInitialized) {
      this.srtt = rttMs;
      this.rttvar = rttMs / 2;
      this.rttInitialized = true;
    } else {
      const alpha = 0.125;  // 1/8
      const beta = 0.25;    // 1/4
      this.rttvar = (1 - beta) * this.rttvar + beta * Math.abs(this.srtt - rttMs);
      this.srtt = (1 - alpha) * this.srtt + alpha * rttMs;
    }

    // Window growth
    if (this.cwnd < this.ssthresh) {
      // Slow start: exponential growth (double per RTT)
      this.cwnd += 1;  // +1 per ACK ≈ doubles per RTT
    } else {
      // Congestion avoidance: linear growth
      this.cwnd += 1 / this.cwnd;
    }

    this.cwnd = Math.min(this.cwnd, this.maxCwnd);
  }

  onFailure(): void {
    this.ssthresh = Math.max(this.cwnd / 2, this.minCwnd);
    this.cwnd = Math.max(this.cwnd / 2, this.minCwnd);
  }

  getMaxConcurrency(): number {
    return Math.max(this.minCwnd, Math.min(Math.floor(this.cwnd), this.maxCwnd));
  }

  getRTO(): number {
    // Retransmission timeout = SRTT + 4 * RTTVAR, minimum 1000ms
    if (!this.rttInitialized) return 5000;
    return Math.max(1000, this.srtt + 4 * this.rttvar);
  }
}
```

### 3.4 Jacobson/Karels RTT Estimation

The RTT estimator (RFC 6298) is critical for setting accurate timeouts:

```
On first sample R:
  SRTT  = R
  RTTVAR = R/2
  RTO = SRTT + 4 * RTTVAR

On subsequent samples R:
  RTTVAR = (1 - β) * RTTVAR + β * |SRTT - R|     where β = 1/4
  SRTT   = (1 - α) * SRTT   + α * R               where α = 1/8
  RTO    = SRTT + max(G, 4 * RTTVAR)               where G = clock granularity
```

**Why this matters**: Fixed timeouts (e.g., 30s) are either too aggressive (kill slow-but-valid requests) or too lenient (waste time on dead connections). Adaptive RTO tracks actual server performance and reacts to degradation within 4-8 samples.

### 3.5 Endgame Mode

Borrowed from BitTorrent: when >90% of chunks are uploaded, the remaining chunks become the bottleneck. In endgame mode:

1. Send duplicate requests for all remaining chunks
2. Cancel duplicates when the primary succeeds
3. Accept whichever response arrives first

This eliminates tail latency from slow/lost requests at the cost of ~10% extra bandwidth for the last few chunks — a worthwhile trade.

```typescript
function shouldEnterEndgame(completed: number, total: number): boolean {
  return total > 10 && completed / total > 0.9;
}
```

### 3.6 Benchmark Results

POC benchmarks comparing V1 (fixed 6 concurrent, 1 MB chunks) vs V2 (AIMD + adaptive chunks):

| File Size | Upload Δ | Download Δ | Notes |
|-----------|----------|-----------|-------|
| 1 MB      | **+14%** | **+64%** | Single chunk, AIMD overhead minimal |
| 10 MB     | -3%      | -4%      | Same chunk size, AIMD still ramping |
| 50 MB     | **+13%** | -2%      | Modest upload gains |
| 100 MB    | **+40%** | **+36%** | Sweet spot: 33% fewer chunks + AIMD |
| 500 MB    | **+50%** | **+54%** | AIMD + larger chunks scale well |

**Key findings:**
- **Large files (100MB+) see 36-54% improvement** across both upload and download
- **100 MB is the sweet spot**: 67 × 1.5 MB chunks vs 100 × 1 MB = 33% fewer requests + AIMD = 1.40× upload, 1.36× download
- AIMD concurrency scaling is the primary win for downloads

---

## 4. Shard Architecture

### 4.1 Current: Rendezvous Hashing

Mossaic uses rendezvous hashing (Highest Random Weight) for chunk→shard placement:

```typescript
// shared/placement.ts
function selectShard(chunkHash: string, shardIds: string[]): string {
  let bestShard = shardIds[0];
  let bestScore = -Infinity;
  for (const shard of shardIds) {
    const score = murmur3(chunkHash + shard);
    if (score > bestScore) {
      bestScore = score;
      bestShard = shard;
    }
  }
  return bestShard;
}
```

**Pros**: Minimal disruption when adding/removing shards (only 1/n keys move). Simple. Deterministic.
**Cons**: O(n) per lookup — must hash against every shard. Fine for 32 shards, problematic at 1,000+.

### 4.2 Alternative: Jump Consistent Hashing

Jump consistent hash (Lamping & Veach, 2014, Google) provides:
- **O(1) computation** — ~5ns per lookup vs ~500ns for rendezvous at 32 shards
- **Minimal disruption** — adding shard N+1 moves exactly 1/(N+1) keys
- **Zero memory** — no shard list needed, just the count

```typescript
function jumpConsistentHash(key: bigint, numBuckets: number): number {
  let b = -1n;
  let j = 0n;
  while (j < BigInt(numBuckets)) {
    b = j;
    key = (key * 2862933555777941757n + 1n) & 0xFFFFFFFFFFFFFFFFn;
    j = BigInt(Math.floor(
      (Number(b) + 1) * Number(1n << 31n) / Number((key >> 33n) + 1n)
    ));
  }
  return Number(b);
}
```

**Limitation**: Only supports appending shards (bucket 0..N-1). Removing arbitrary shards requires a remapping layer.

**Recommendation**: Keep rendezvous hashing for now (32-128 shards, O(n) is fine). Migrate to jump consistent hash if shard count exceeds 256.

### 4.3 Dynamic Pool Scaling

Current formula: `shards = 32 + floor(totalStorage / 5GB)`

**Proposed**: Scale based on both storage AND request rate:

```typescript
function computePoolSize(totalStorageGB: number, requestsPerSecond: number): number {
  const storageShards = Math.floor(totalStorageGB / 5) + 32;
  const loadShards = Math.ceil(requestsPerSecond / 100);  // ~100 rps per shard
  return Math.max(storageShards, loadShards);
}
```

### 4.4 Load-Aware Routing

Instead of pure hash-based placement, weight shards by health:

```typescript
function selectShardWeighted(
  chunkHash: string,
  shards: Array<{ id: string; latencyMs: number; utilizationPct: number }>
): string {
  let bestShard = shards[0];
  let bestScore = -Infinity;

  for (const shard of shards) {
    const hashScore = murmur3(chunkHash + shard.id);
    const healthWeight = 1.0
      - 0.3 * (shard.latencyMs / 1000)        // Penalize slow shards
      - 0.7 * (shard.utilizationPct / 100);    // Penalize full shards
    const score = hashScore * Math.max(0.1, healthWeight);

    if (score > bestScore) {
      bestScore = score;
      bestShard = shard;
    }
  }
  return bestShard.id;
}
```

**Trade-off**: Weighted routing breaks deterministic placement — you can't recompute the shard for a chunk without knowing the health state at write time. Solution: store the shard assignment in file metadata (which we already do).

---

## 5. Fault Tolerance & Erasure Coding

### 5.1 Why Erasure Coding on Durable Objects?

DOs are already replicated by Cloudflare (multi-AZ). So why add erasure coding?

**Not for durability — for read parallelism and availability.**

With RS(6,4) encoding (6 total fragments, any 4 sufficient to reconstruct):
- Read from 6 shards in parallel, use the first 4 responses
- Tolerate 2 slow/unavailable shards without retry
- P99 latency drops because you're racing 6 sources instead of depending on 1

### 5.2 Reed-Solomon RS(N+K, N)

Reed-Solomon codes work over GF(2^8) (Galois Field with 256 elements):

```
Original data:    [D1] [D2] [D3] [D4]     — 4 data chunks
Encoded:          [D1] [D2] [D3] [D4] [P1] [P2]  — 4 data + 2 parity

Any 4 of the 6 fragments can reconstruct the original data.
```

**Parameters for Mossaic:**
- `N = 4` data fragments (configurable)
- `K = 2` parity fragments (configurable)
- Overhead: 50% extra storage (2/4)
- Benefit: tolerate 2 fragment losses, read from fastest 4 of 6

**Storage overhead analysis:**

| Scheme | Storage Overhead | Read Parallelism | Fault Tolerance |
|--------|-----------------|-------------------|-----------------|
| No coding (current) | 0% | 1 source per chunk | 0 (retry on fail) |
| Simple replication (2×) | 100% | 2 sources per chunk | 1 failure |
| RS(6,4) | 50% | 6 sources, need 4 | 2 failures |
| RS(8,4) | 100% | 8 sources, need 4 | 4 failures |

RS(6,4) gives better fault tolerance than 2× replication at half the storage cost.

### 5.3 Implementation Considerations

**Chunk-level vs file-level coding:**
- **File-level** (encode the whole file into N+K fragments): simpler, but requires reading the entire file before any fragment is useful
- **Chunk-level** (encode each chunk independently): each chunk becomes N+K sub-chunks, allows streaming decode. More metadata.

**Recommendation**: File-level RS for files < 100 MB, chunk-level RS for larger files (allows streaming reconstruction).

**GF(2^8) math in JavaScript:**
```typescript
// Galois Field multiplication via log/exp tables
const GF_EXP = new Uint8Array(512);  // Anti-log table
const GF_LOG = new Uint8Array(256);  // Log table

function gfMul(a: number, b: number): number {
  if (a === 0 || b === 0) return 0;
  return GF_EXP[(GF_LOG[a] + GF_LOG[b]) % 255];
}

function gfDiv(a: number, b: number): number {
  if (b === 0) throw new Error("Division by zero");
  if (a === 0) return 0;
  return GF_EXP[(GF_LOG[a] - GF_LOG[b] + 255) % 255];
}
```

Phase 6 deliverable. Not needed for initial V2 launch.

---

## 6. Transfer Pipeline Optimizations

### 6.1 Zero-Copy Streaming

Current implementation buffers entire chunks in memory before uploading. For large chunks (1.5-2 MB), this wastes memory:

```typescript
// Current: Buffer entire chunk, then upload
const chunkBuffer = file.slice(offset, offset + chunkSize);
const hash = await sha256(chunkBuffer);
await uploadChunk(chunkBuffer, hash);

// Proposed: Stream with tee for hash + upload
const chunkStream = file.stream().pipeThrough(new SliceTransform(offset, chunkSize));
const [hashStream, uploadStream] = chunkStream.tee();
const [hash, _] = await Promise.all([
  sha256Stream(hashStream),
  uploadChunkStream(uploadStream)
]);
```

**Browser limitation**: `File.slice()` is already lazy (doesn't copy), so the win is mainly on the hash computation side — streaming SHA-256 instead of buffering.

### 6.2 Chunk Batching

For many small chunks, HTTP/2 multiplexing helps but doesn't eliminate per-request overhead. Batch multiple chunks into a single multipart request:

```
POST /api/upload/batch
Content-Type: multipart/form-data

--boundary
Content-Disposition: form-data; name="chunk-0"
X-Chunk-Index: 0
X-Chunk-Hash: abc123...
[chunk data]
--boundary
Content-Disposition: form-data; name="chunk-1"
X-Chunk-Index: 1
X-Chunk-Hash: def456...
[chunk data]
--boundary--
```

**Trade-off**: Larger requests = less granular error recovery. If a batch fails, all chunks in the batch must be retried. Optimal batch size: 4-8 chunks or 8 MB total, whichever comes first.

### 6.3 Download Prefetch

When downloading, predict which chunks will be needed next and prefetch:

```typescript
class PrefetchController {
  private prefetchWindow = 4;  // Prefetch N chunks ahead
  private cache = new Map<number, Promise<ArrayBuffer>>();

  async getChunk(index: number, total: number): Promise<ArrayBuffer> {
    // Trigger prefetch for upcoming chunks
    for (let i = index + 1; i < Math.min(index + this.prefetchWindow, total); i++) {
      if (!this.cache.has(i)) {
        this.cache.set(i, this.fetchChunk(i));
      }
    }

    // Return current chunk (may already be prefetched)
    if (!this.cache.has(index)) {
      this.cache.set(index, this.fetchChunk(index));
    }
    return this.cache.get(index)!;
  }
}
```

Combined with AIMD concurrency control, prefetch ensures the download pipeline stays full even during congestion window adjustments.

---

## 7. Next-Gen Design

### 7.1 System Overview

```
┌─────────────────────────────────────────────────────────────┐
│                       Client (Browser)                       │
│                                                              │
│  ┌──────────────┐  ┌───────────────┐  ┌──────────────────┐ │
│  │ Chunk Sizer  │  │ AIMD Control  │  │ Transfer Engine  │ │
│  │ (adaptive)   │──│ (cwnd, RTT)   │──│ (upload/download)│ │
│  └──────────────┘  └───────────────┘  └──────────────────┘ │
│          │                  │                    │            │
│          │         ┌────────▼────────┐          │            │
│          │         │ Endgame Mode    │          │            │
│          │         │ (>90% complete) │          │            │
│          │         └─────────────────┘          │            │
└──────────┼──────────────────────────────────────┼────────────┘
           │                                      │
    ┌──────▼──────────────────────────────────────▼──────┐
    │                  CF Worker (Hono)                    │
    │                                                      │
    │  ┌──────────────┐  ┌─────────────┐  ┌────────────┐ │
    │  │ Pool Manager │  │ Load-Aware  │  │ Batch API  │ │
    │  │ (dynamic)    │──│ Router      │──│ Endpoint   │ │
    │  └──────────────┘  └─────────────┘  └────────────┘ │
    └─────────────────────────┬────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              │               │               │
        ┌─────▼─────┐  ┌─────▼─────┐  ┌─────▼─────┐
        │ ShardDO-0 │  │ ShardDO-1 │  │ ShardDO-N │
        │ SQLite    │  │ SQLite    │  │ SQLite    │
        │ ≤10GB     │  │ ≤10GB     │  │ ≤10GB     │
        └───────────┘  └───────────┘  └───────────┘
```

### 7.2 Client-Side Components

**AdaptiveChunkSizer**: Determines chunk size based on file size tiers. Stateless, pure function.

**AIMDController**: Manages concurrency window. One instance per upload/download operation. Reports metrics for observability.

**TransferEngine**: Orchestrates chunk upload/download with concurrency control. Handles retries, endgame mode, progress reporting.

### 7.3 Server-Side Components

**PoolManager**: Maintains shard health metrics, handles dynamic scaling, provides weighted shard selection.

**BatchEndpoint**: Accepts multi-chunk uploads in a single request, reduces HTTP overhead for small files.

**ShardDO**: Unchanged — stores chunk blobs in SQLite with SHA-256 content addressing and ref-counting.

---

## 8. Implementation Roadmap

### Phase 1: Adaptive Chunk Sizing (1-2 days)
- Implement `getChunkSize()` tier function in `shared/chunking.ts`
- Update `use-upload.ts` to use adaptive sizing
- Update `use-download.ts` to read chunk size from metadata
- Update worker routes to accept variable chunk sizes
- **Risk**: Low. Backward-compatible if metadata stores chunk size.

### Phase 2: AIMD Concurrency Control (2-3 days)
- Implement `AIMDController` class in `shared/aimd.ts`
- Replace hardcoded `6` in upload/download hooks
- Add RTT tracking and observability
- Wire up endgame mode for uploads
- **Risk**: Medium. Needs careful testing — bad AIMD params can make things worse.

### Phase 3: Transfer Pipeline (1-2 days)
- Streaming SHA-256 computation
- Download prefetch controller
- Chunk batching API endpoint
- **Risk**: Low. Additive improvements.

### Phase 4: Dynamic Shard Pool (2-3 days)
- Load-aware shard routing
- Health metrics collection in PoolManager DO
- Auto-scaling rules based on storage + request rate
- **Risk**: Medium. Shard rebalancing during live operation.

### Phase 5: Content-Defined Chunking (3-5 days)
- Implement FastCDC (Gear hash, normalized chunking)
- Variable chunk size metadata storage
- Cross-file dedup via content-addressed chunks
- **Risk**: High. Major metadata schema change.

### Phase 6: Erasure Coding (5-7 days)
- GF(2^8) math library
- RS encoder/decoder
- Fragment distribution across shards
- Reconstruction logic in download path
- **Risk**: High. Complex math, significant storage overhead.

**Recommended priority**: Phase 1 → Phase 2 → Phase 4 → Phase 3 → Phase 5 → Phase 6

Phases 1-2 deliver the biggest wins (benchmark-proven 1.4-1.5× improvement at 100MB+) with the lowest risk. Phase 4 (dynamic shards) is important for production scaling. Phases 5-6 are advanced features for later.

---

## Appendix: Glossary

| Term | Definition |
|------|-----------|
| AIMD | Additive Increase / Multiplicative Decrease — congestion control algorithm |
| CDC | Content-Defined Chunking — chunk boundaries determined by content hash |
| cwnd | Congestion window — maximum concurrent in-flight requests |
| DO | Durable Object — Cloudflare's stateful edge compute primitive |
| FastCDC | Fast Content-Defined Chunking — improved CDC with gear hash |
| GF(2^8) | Galois Field with 256 elements — finite field for Reed-Solomon math |
| RS(N+K, N) | Reed-Solomon code — N data + K parity fragments, any N sufficient |
| RTT | Round-Trip Time — time from request send to response received |
| RTO | Retransmission Timeout — when to consider a request lost |
| ssthresh | Slow-Start Threshold — cwnd value where slow-start transitions to congestion avoidance |
| SRTT | Smoothed RTT — exponential moving average of RTT samples |

---

*Document generated from deep research into BitTorrent (BEP 3), TCP congestion control (RFC 5681, RFC 6298), Jump Consistent Hashing (Lamping & Veach 2014), FastCDC (Xia et al. 2020), and Reed-Solomon coding theory. Benchmarked against live Mossaic deployment.*
