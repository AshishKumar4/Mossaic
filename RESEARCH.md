# Mossaic Deep Research: Distributed Chunked Storage Systems

> Comprehensive technical research for the Mossaic project — a distributed chunked storage system built on Cloudflare Durable Objects and R2.
>
> Compiled: March 7, 2026

---

## Executive Summary

This document consolidates deep technical research across eight domains critical to the design of Mossaic, a distributed chunked storage system built on Cloudflare's edge infrastructure (Durable Objects, R2, and Workers). The research spans foundational protocols, storage theory, production system analysis, and platform-specific constraints, synthesized into a coherent architecture proposal.

**Chunking and Data Distribution.** BitTorrent's piece model demonstrates that power-of-2 chunk sizes between 64 KiB and 16 MiB, selected adaptively based on file size, provide the best balance of metadata overhead and transfer parallelism. RAID striping theory further confirms that distributing chunks across independent storage nodes in a round-robin fashion yields near-linear throughput scaling, with the critical caveat that parity-based redundancy introduces write amplification that must be managed through full-stripe writes. For Mossaic, content-defined chunking (CDC) using the FastCDC algorithm with Gear hashing is recommended over fixed-size chunking. FastCDC achieves ~1 GB/s throughput while providing 75-95% deduplication ratios — critical for photo storage workloads where re-uploads and EXIF modifications are common. A 128 KB average chunk size balances deduplication granularity against Durable Object operation costs.

**Redundancy and Fault Tolerance.** Erasure coding using Reed-Solomon codes over GF(2^8) provides dramatically better storage efficiency than replication: an RS(6,4) configuration tolerates 2 simultaneous node failures at only 1.5x storage overhead, compared to 3x for triple replication with equivalent fault tolerance. Cauchy Reed-Solomon is preferred over Vandermonde for its XOR-only encoding, achieving 2-10x faster parity computation — essential given Cloudflare Workers' limited CPU budget. The research across five production systems (IPFS, Ceph, GFS/Colossus, Haystack/f4, MinIO) consistently validates erasure coding as the industry standard for warm and cold data, with replication reserved for hot-path access.

**Chunk Placement.** Rendezvous hashing (Highest Random Weight) emerges as the optimal placement algorithm for mapping chunks to Durable Objects. It naturally selects the top-N DOs for any file — exactly what erasure coding requires — with zero metadata overhead, perfect load balance, and minimal disruption when DOs are added or removed. The algorithm is trivially implementable (~20 lines of TypeScript) and executes in under 1ms for clusters of up to 500 DOs.

**Transfer Optimization.** Parallel chunk transfers using 4-6 concurrent connections, managed by an AIMD-style adaptive concurrency controller, maximize throughput across varying network conditions. Amdahl's Law analysis shows that with ~7% serial overhead (metadata fetch, reassembly), 4-8 connections capture most of the available parallelism. HTTP/2 multiplexing is preferred for small chunks and metadata, while WebSocket connections to Durable Objects excel for large streaming uploads. HTTP/3 (QUIC) is the ideal long-term target, eliminating TCP head-of-line blocking while retaining multiplexing benefits.

**Platform Constraints Drive Architecture.** The most consequential finding is the Cloudflare DO pricing and capability analysis: DO storage costs $0.20/GB/month — 13x more expensive than R2 at $0.015/GB/month. Combined with DOs' single-threaded execution model (~1,000 req/s ceiling per instance), this definitively dictates a hybrid architecture: **R2 for bulk chunk data, DOs for metadata and coordination**. This hybrid approach reduces costs from ~$203/month to ~$25/month per TB while eliminating single-threaded read bottlenecks. Content-addressed R2 keys (`chunks/{sha256}`) enable natural deduplication and CDN caching, allowing the read path to bypass DOs entirely.

**Recommended Architecture.** Mossaic should adopt a layered design: content-addressed CIDs (inspired by IPFS) for chunk identity, rendezvous hashing for deterministic placement, FastCDC for intelligent chunking, Cauchy Reed-Solomon RS(6,4) for erasure coding, R2 for bulk storage, and Durable Objects for manifests, coordination, and deduplication indexes. This architecture achieves 1.5x storage overhead, tolerates 2 concurrent failures, provides sub-100ms read latency at the edge, and costs approximately $25/month per TB at rest.

---

## Table of Contents

- [Executive Summary](#executive-summary)
- [Section 1: BitTorrent Protocol](#section-1-bittorrent-protocol)
  - [1.1 Overview](#11-overview)
  - [1.2 File Chunking: How Files Are Split Into Pieces](#12-file-chunking-how-files-are-split-into-pieces)
  - [1.3 Piece Length Selection: The Math](#13-piece-length-selection-the-math)
  - [1.4 Piece Selection Algorithms](#14-piece-selection-algorithms)
  - [1.5 Parallel Downloading: Choking/Unchoking Algorithm](#15-parallel-downloading-chokingunchoking-algorithm)
  - [1.6 Tit-for-Tat Economics](#16-tit-for-tat-economics)
  - [1.7 Piece Hash Verification](#17-piece-hash-verification)
  - [1.8 Parallel Download from Multiple Peers](#18-parallel-download-from-multiple-peers)
  - [1.9 Mossaic Recommendation: What We Can Borrow](#19-mossaic-recommendation-what-we-can-borrow)
  - [References (BitTorrent)](#references-bittorrent)
- [Section 2: RAID Striping](#section-2-raid-striping)
  - [2.1 Overview](#21-overview)
  - [2.2 RAID 0: Striping](#22-raid-0-striping)
  - [2.3 RAID 5: Distributed Parity](#23-raid-5-distributed-parity)
  - [2.4 RAID 6: Dual Parity (Reed-Solomon)](#24-raid-6-dual-parity-reed-solomon)
  - [2.5 Stripe Width vs. Stripe Unit Size Optimization](#25-stripe-width-vs-stripe-unit-size-optimization)
  - [2.6 Write Penalty Summary](#26-write-penalty-summary)
  - [2.7 RAID Layout Comparison Diagram](#27-raid-layout-comparison-diagram)
  - [2.8 Array Failure Probability Math](#28-array-failure-probability-math)
  - [2.9 Mossaic Recommendation: How RAID-Like Striping Maps to DOs](#29-mossaic-recommendation-how-raid-like-striping-maps-to-dos)
  - [References (RAID)](#references-raid)
- [Section 3: Erasure Coding (Reed-Solomon)](#section-3-erasure-coding-reed-solomon)
  - [3.1 Fundamental Math: Galois Field Arithmetic — GF(2^8)](#31-fundamental-math-galois-field-arithmetic--gf28)
  - [3.2 Reed-Solomon Encoding: How (k, m) Works](#32-reed-solomon-encoding-how-k-m-works)
  - [3.3 Cauchy RS vs Vandermonde RS](#33-cauchy-rs-vs-vandermonde-rs)
  - [3.4 The (n, k) Parameter Space](#34-the-n-k-parameter-space)
  - [3.5 Decoding / Reconstruction](#35-decoding--reconstruction)
  - [3.6 Comparison: Erasure Coding vs. Replication](#36-comparison-erasure-coding-vs-replication)
  - [3.7 Real-World Usage](#37-real-world-usage)
  - [3.8 Mossaic Recommendation](#38-mossaic-recommendation)
  - [References (Erasure Coding)](#references-erasure-coding)
- [Section 4: Consistent Hashing / Rendezvous Hashing](#section-4-consistent-hashing--rendezvous-hashing)
  - [4.1 The Problem: Mapping Chunks to DOs](#41-the-problem-mapping-chunks-to-dos)
  - [4.2 Classic Consistent Hashing](#42-classic-consistent-hashing)
  - [4.3 Rendezvous Hashing (HRW)](#43-rendezvous-hashing-hrw)
  - [4.4 Jump Consistent Hash](#44-jump-consistent-hash)
  - [4.5 Multi-Probe Consistent Hashing](#45-multi-probe-consistent-hashing)
  - [4.6 Mapping (file_id, chunk_index) to DO_id](#46-mapping-file_id-chunk_index-to-do_id)
  - [4.7 Adding / Removing DOs: Disruption Analysis](#47-adding--removing-dos-disruption-analysis)
  - [4.8 Comparison Table](#48-comparison-table)
  - [4.9 Mossaic Recommendation: Rendezvous Hashing (HRW)](#49-mossaic-recommendation-rendezvous-hashing-hrw)
  - [References (Hashing)](#references-hashing)
- [Section 5: Content-Defined Chunking](#section-5-content-defined-chunking)
  - [5.1 Fixed-Size vs Variable-Size Chunking](#51-fixed-size-vs-variable-size-chunking)
  - [5.2 The Content Shift Problem](#52-the-content-shift-problem)
  - [5.3 Rolling Hash Functions](#53-rolling-hash-functions)
  - [5.4 Rabin Fingerprinting](#54-rabin-fingerprinting)
  - [5.5 Buzhash and Other Rolling Hash Alternatives](#55-buzhash-and-other-rolling-hash-alternatives)
  - [5.6 FastCDC Algorithm](#56-fastcdc-algorithm)
  - [5.7 The Math: Expected Chunk Size, Min/Max, Bit-Masking](#57-the-math-expected-chunk-size-minmax-bit-masking)
  - [5.8 Deduplication Ratios: CDC vs Fixed-Size](#58-deduplication-ratios-cdc-vs-fixed-size)
  - [5.9 Practical Chunk Size Ranges](#59-practical-chunk-size-ranges)
  - [5.10 Mossaic Recommendation](#510-mossaic-recommendation)
  - [References (Content-Defined Chunking)](#references-content-defined-chunking)
- [Section 6: Parallel Transfer Optimization](#section-6-parallel-transfer-optimization)
  - [6.1 Optimal Concurrent Connections](#61-optimal-concurrent-connections)
  - [6.2 Bandwidth-Delay Product (BDP)](#62-bandwidth-delay-product-bdp)
  - [6.3 TCP Window Scaling (RFC 7323)](#63-tcp-window-scaling-rfc-7323)
  - [6.4 HTTP/2 vs HTTP/1.1 for Chunk Transfers](#64-http2-vs-http11-for-chunk-transfers)
  - [6.5 Chunk Pipeline Scheduling](#65-chunk-pipeline-scheduling)
  - [6.6 Adaptive Concurrency](#66-adaptive-concurrency)
  - [6.7 Amdahl's Law for Downloads](#67-amdahls-law-for-downloads)
  - [6.8 WebSocket vs HTTP for Chunk Streaming](#68-websocket-vs-http-for-chunk-streaming)
  - [6.9 Mossaic Recommendation](#69-mossaic-recommendation)
  - [References (Parallel Transfer)](#references-parallel-transfer)
- [Section 7: Existing Systems Study](#section-7-existing-systems-study)
  - [7.1 IPFS (InterPlanetary File System)](#71-ipfs-interplanetary-file-system)
  - [7.2 Ceph](#72-ceph)
  - [7.3 Google GFS / Colossus](#73-google-gfs--colossus)
  - [7.4 Facebook Haystack / f4](#74-facebook-haystack--f4)
  - [7.5 MinIO](#75-minio)
  - [7.6 Comparison Table](#76-comparison-table)
  - [7.7 Synthesis: Key Lessons for Mossaic](#77-synthesis-key-lessons-for-mossaic)
- [Section 8: Cloudflare DO Constraints](#section-8-cloudflare-do-constraints)
  - [8.1 DO Storage Limits](#81-do-storage-limits)
  - [8.2 DO Pricing Model](#82-do-pricing-model)
  - [8.3 DO Concurrency Model](#83-do-concurrency-model)
  - [8.4 DO Hibernation](#84-do-hibernation)
  - [8.5 Geographic Locality](#85-geographic-locality)
  - [8.6 Architecture Implications for Mossaic](#86-architecture-implications-for-mossaic)
  - [8.7 Cost Analysis](#87-cost-analysis)
  - [8.8 Design Recommendations for Mossaic](#88-design-recommendations-for-mossaic)
  - [8.9 Risk Factors & Mitigations](#89-risk-factors--mitigations)
  - [8.10 Summary](#810-summary)
- [Section 9: Unified Mossaic Architecture Recommendations](#section-9-unified-mossaic-architecture-recommendations)

---

## Section 1: BitTorrent Protocol

BitTorrent is a peer-to-peer file distribution protocol designed by Bram Cohen (2001). Its core innovation: when multiple downloads of the same file happen concurrently, downloaders upload to each other, allowing the file source to support massive numbers of downloaders with only a modest increase in load. The protocol is formalized across several BEP (BitTorrent Enhancement Proposal) documents, with BEP 3 as the foundational spec and BEP 52 introducing v2 with SHA-256 and Merkle trees.

### 1.1 Overview

### 1.2 File Chunking: How Files Are Split Into Pieces

#### The Piece Model (BEP 3)

Files are split into fixed-size **pieces**, all the same length except possibly the last one (which may be truncated). Each piece is identified by its zero-based index and verified by a SHA-1 hash (SHA-256 in v2).

```
File (total_length bytes):
+--------+--------+--------+--------+-----+--------+
| Piece  | Piece  | Piece  | Piece  | ... | Piece  |
|   0    |   1    |   2    |   3    |     |  N-1   |
+--------+--------+--------+--------+-----+--------+
|<- piece_length ->|                       |<-trunc |

Number of pieces = ceil(total_length / piece_length)
```

**Multi-file torrents** treat all files as a single concatenated byte stream. Files are laid out in the order they appear in the metainfo, and piece boundaries may span file boundaries:

```
File A (1500 bytes) + File B (2500 bytes), piece_length = 1024:

Piece 0: [FileA bytes 0-1023]
Piece 1: [FileA bytes 1024-1499 | FileB bytes 0-523]
Piece 2: [FileB bytes 524-1547]
Piece 3: [FileB bytes 1548-2499]  (truncated, 952 bytes)
```

In BEP 52 (v2), each file is aligned to piece boundaries (with padding), and each file has its own Merkle hash tree with a branching factor of 2, constructed from 16 KiB leaf blocks. This enables per-file deduplication across different torrents.

#### Pieces vs. Blocks

A critical distinction:

- **Piece**: A chunk described in the metainfo file, verified by a hash. Typical sizes: 256 KB - 16 MB.
- **Block**: A sub-piece unit requested between peers on the wire. Standard size: **16 KiB (2^14 bytes)**. Connections requesting > 16 KiB are closed by most implementations.

```
Piece (e.g., 256 KiB = 262,144 bytes):
+-------+-------+-------+-------+--- ... ---+-------+
| Block | Block | Block | Block |           | Block |
|  0    |  1    |  2    |  3    |           |  15   |
+-------+-------+-------+-------+--- ... ---+-------+
|<-16KiB->|

Blocks per piece = piece_length / 16384
                 = 262144 / 16384
                 = 16 blocks
```

### 1.3 Piece Length Selection: The Math

#### BEP 3 Specification

> "piece length is almost always a power of two, most commonly 2^18 = 256 K (BitTorrent prior to version 3.2 uses 2^20 = 1 M as default)."

#### Tradeoffs

| Factor | Small Pieces | Large Pieces |
|--------|-------------|--------------|
| .torrent file size | Larger (20 bytes SHA-1 per piece) | Smaller |
| Piece availability | Better (finer granularity) | Worse |
| Hash verification overhead | More frequent checks | Fewer checks |
| Waste on last piece | Less waste | More waste |
| Protocol overhead | Higher (more HAVE messages) | Lower |
| Peer diversity per piece | More peers can contribute | Fewer peers |

#### The Metainfo Size Constraint

Historically, piece size was chosen to keep the .torrent file under ~50-75 KB. The `pieces` field is a concatenation of 20-byte SHA-1 hashes:

```
metainfo_overhead = num_pieces * 20 bytes
                  = ceil(file_size / piece_length) * 20

For a 4 GB file with 256 KiB pieces:
  num_pieces = ceil(4 * 2^30 / 2^18) = 16,384 pieces
  hash_data  = 16,384 * 20 = 327,680 bytes ≈ 320 KiB

For a 4 GB file with 4 MiB pieces:
  num_pieces = ceil(4 * 2^30 / 2^22) = 1,024 pieces
  hash_data  = 1,024 * 20 = 20,480 bytes ≈ 20 KiB
```

#### Common Piece Sizes in Practice

| File Size Range | Recommended Piece Size | Pieces Count |
|----------------|----------------------|-------------|
| < 50 MB | 32 KiB - 64 KiB | ~800 - 1,600 |
| 50 MB - 150 MB | 64 KiB - 128 KiB | ~400 - 2,400 |
| 150 MB - 350 MB | 128 KiB - 256 KiB | ~600 - 2,800 |
| 350 MB - 512 MB | 256 KiB | ~1,400 - 2,000 |
| 512 MB - 1 GB | 512 KiB | ~1,000 - 2,000 |
| 1 GB - 2 GB | 512 KiB - 1 MiB | ~1,000 - 4,000 |
| 2 GB+ | 1 MiB - 2 MiB | ~1,000 - 2,000+ |
| 8-10 GB+ | 512 KiB (best practice) | ~16,000 - 20,000 |

Current best practice from the community spec: **keep piece size to 512 KiB or less for torrents around 8-10 GB**, even if it results in a larger .torrent file, as this produces a more efficient swarm.

#### BEP 52 (v2) Changes

- Piece length **must be a power of two** and **at least 16 KiB**.
- Each file is aligned to piece boundaries (padding between files).
- Merkle tree hashes (SHA-256) are used instead of flat hash lists, meaning the .torrent file itself doesn't need to contain all leaf hashes — only the root hash per file. Leaf hashes are exchanged between peers on demand.

### 1.4 Piece Selection Algorithms

#### 1.4.1 Random First Piece

When a peer has **no** complete pieces yet, it cannot upload anything. Getting a complete piece ASAP is critical to participate in tit-for-tat. Strategy: pick a **random** piece to download first. Random selection maximizes the chance that nearby peers can supply blocks quickly, since common pieces are on more peers.

#### 1.4.2 Rarest-First (Strict Priority after first piece)

The primary piece selection algorithm in BitTorrent. After obtaining the first piece:

1. Each client maintains a count of piece availability across all connected peers (from initial bitfields + HAVE messages).
2. The piece with the **lowest availability count** (rarest) is selected for download next.
3. **Ties are broken randomly** among equally-rare pieces to prevent herding.

```
Peer Availability Map (example, 5 peers):

Piece:  | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
--------|---|---|---|---|---|---|---|---|
Peer A: | 1 | 1 | 0 | 1 | 1 | 0 | 1 | 0 |
Peer B: | 1 | 0 | 0 | 1 | 1 | 1 | 0 | 0 |
Peer C: | 1 | 1 | 1 | 0 | 1 | 0 | 1 | 0 |
Peer D: | 0 | 1 | 0 | 1 | 1 | 0 | 0 | 1 |
Peer E: | 1 | 0 | 0 | 0 | 1 | 0 | 0 | 0 |
--------|---|---|---|---|---|---|---|---|
Count:  | 4 | 3 | 1 | 3 | 5 | 1 | 2 | 1 |

Rarest pieces (count=1): {2, 5, 7} → pick randomly among these
```

**Why rarest-first works:**

- **Prevents piece extinction**: If only one peer has a piece and goes offline, it's lost. Prioritizing rare pieces replicates them across the swarm.
- **Increases upload opportunities**: Having rare pieces makes a peer more valuable to others → better reciprocation.
- **Equalizes piece distribution**: Over time, all pieces approach equal availability.

Research reference: Arnaud Legout et al., "Rarest First and Choke Algorithms Are Enough" (IMC 2006) demonstrated that rarest-first alone achieves near-optimal piece distribution in most scenarios.

#### 1.4.3 Endgame Mode

When a download is **almost complete** and all remaining pieces have been requested:

1. Send requests for **all** missing blocks to **all** peers that have them.
2. When any block arrives, send **cancel** messages to all other peers for that block.

```
Normal Mode:                    Endgame Mode:
  Block X → requested from      Block X → requested from
  Peer A only                   Peer A, B, C, D simultaneously
                                First response wins;
                                cancel sent to losers
```

**Thresholds** (implementation-specific, not standardized):
- Some clients enter endgame when all pieces have been requested.
- Others wait until `blocks_remaining < blocks_in_transit` and `blocks_remaining <= 20`.
- Best practice: keep pending block count low (1-2 blocks per request) to minimize wasted bandwidth.

### 1.5 Parallel Downloading: Choking/Unchoking Algorithm

The choking algorithm is the **economic engine** of BitTorrent — it determines who uploads to whom, enforcing reciprocity.

#### Connection State Machine

Every connection has **4 bits of state**:

```
                    LOCAL SIDE         REMOTE SIDE
                 +--------------+  +--------------+
                 | am_choking   |  | peer_choking |
                 | am_interested|  | peer_interested|
                 +--------------+  +--------------+

Initial state: am_choking=1, am_interested=0,
               peer_choking=1, peer_interested=0

Data flows when: am_interested=1 AND peer_choking=0  (download)
            or:  peer_interested=1 AND am_choking=0   (upload)
```

#### The Choking Algorithm (Deployed)

**Goals:**
1. Cap simultaneous uploads (good TCP performance)
2. Avoid fibrillation (rapid choke/unchoke oscillation)
3. Reciprocate to peers who let us download (tit-for-tat)
4. Discover better peers (optimistic unchoking)

**Rules:**

```
Every 10 seconds:
  1. Calculate upload rates from all connected interested peers
  2. Unchoke the TOP 4 peers by download rate (they become "downloaders")
     - If we have the complete file: use upload rate instead
  3. Choke everyone else (except optimistic unchoke slot)

Every 30 seconds:
  4. Rotate the optimistic unchoke:
     - Select ONE random peer to unchoke regardless of rate
     - New connections are 3x more likely to be chosen
     - If interested, this peer counts as one of the 4 downloaders
```

**Why 4 simultaneous uploads?** TCP congestion control degrades badly with too many concurrent connections. 4 is empirically a good balance between throughput and fairness.

#### Anti-Snubbing

If a peer receives no data from a connection for > 60 seconds, it considers itself "snubbed." Response: stop uploading to that peer (except via optimistic unchoke). This can temporarily create multiple optimistic unchoke slots, speeding recovery.

### 1.6 Tit-for-Tat Economics

BitTorrent's choking algorithm implements a variant of **tit-for-tat** from game theory:

```
Classical Tit-for-Tat:     BitTorrent's Version:
  Round 1: Cooperate         Round 1: Optimistic unchoke (cooperate first)
  Round N: Copy opponent     Round N: Unchoke best 4 uploaders (reciprocate)
           from round N-1              + 1 random (explore)
```

**Key economic properties:**

1. **Reciprocity**: Peers that upload fast to us get unchoked (we upload back). This creates bilateral exchanges between peers with good links.

2. **Optimistic exploration**: The random unchoke (every 30s) ensures:
   - New peers can bootstrap (they have nothing to trade initially).
   - Better partnerships are discovered over time.
   - The system doesn't get stuck in local optima.

3. **Pareto efficiency**: The mechanism tends toward states where no peer can improve without worsening another — upload capacity is allocated to maximize total swarm throughput.

4. **Incentive compatibility**: Free-riders (peers that only download) are naturally penalized — they only receive data via optimistic unchoke slots, getting ~1/5 the bandwidth of cooperating peers.

5. **Seeder behavior**: When a peer has the complete file, it switches from download-rate-based unchoking to upload-rate-based unchoking, distributing data to peers that will spread it most efficiently.

```
Bandwidth Allocation (Leecher with 5 Mbps upload):
+------------------------------------------+
| Peer A (best reciprocator): ~1.25 Mbps   |  ← unchoked (slot 1)
| Peer B (2nd best):          ~1.25 Mbps   |  ← unchoked (slot 2)
| Peer C (3rd best):          ~1.25 Mbps   |  ← unchoked (slot 3)
| Peer D (4th best):          ~1.00 Mbps   |  ← unchoked (slot 4)
| Peer E (random):            ~0.25 Mbps   |  ← optimistic unchoke
| Peers F-Z:                   0 Mbps      |  ← choked
+------------------------------------------+
```

### 1.7 Piece Hash Verification

#### BEP 3 (v1): Flat SHA-1

Each piece has exactly one 20-byte SHA-1 hash stored in the `pieces` field of the metainfo:

```
pieces = SHA1(piece_0) || SHA1(piece_1) || ... || SHA1(piece_N-1)
       = <20 bytes>    || <20 bytes>    || ... || <20 bytes>
Total: N * 20 bytes

Verification:
  received_piece → SHA1(received_piece) == expected_hash[piece_index]
  If match: piece is valid, announce HAVE to all peers
  If mismatch: discard piece, re-request from different peer
```

**Verification granularity issue**: A single corrupted byte in a 1 MiB piece forces re-download of the entire piece. This is why smaller pieces improve resilience.

#### BEP 52 (v2): Merkle Tree with SHA-256

Each file has a binary Merkle tree constructed from 16 KiB leaf blocks:

```
                    Root Hash (pieces_root)
                   /                        \
              H(01)                          H(23)
             /     \                        /     \
         H(0)     H(1)                 H(2)      H(3)=0
          |        |                    |          |
       SHA256   SHA256              SHA256      (padding)
      [0-16K]  [16K-32K]          [32K-44K]

Leaf hashes: SHA-256 of each 16 KiB block
Inner nodes: SHA-256(left_child || right_child)
Root: stored in metainfo as `pieces root` (32 bytes per file)
```

**Advantages of Merkle approach:**
- Peers can exchange hash proofs incrementally (don't need all hashes upfront)
- Verification at 16 KiB granularity (vs. full piece size in v1)
- The .torrent file is much smaller (only root hashes, not all piece hashes)
- Hash layers are exchanged via `hash request` / `hashes` wire messages

### 1.8 Parallel Download from Multiple Peers

```
Downloading Piece 42 (256 KiB = 16 blocks of 16 KiB):

  Client
    |
    |--- REQUEST(42, offset=0,    len=16384) → Peer A
    |--- REQUEST(42, offset=16384, len=16384) → Peer B
    |--- REQUEST(42, offset=32768, len=16384) → Peer A
    |--- REQUEST(42, offset=49152, len=16384) → Peer C
    |        ...
    |--- REQUEST(42, offset=245760,len=16384) → Peer B
    |
    |←-- PIECE(42, offset=16384, data...) from Peer B     ✓
    |←-- PIECE(42, offset=0,     data...) from Peer A     ✓
    |←-- PIECE(42, offset=49152, data...) from Peer C     ✓
    |        ...
    |
    | All 16 blocks received → SHA1(assembled_piece) == hash[42]?
    | If YES → send HAVE(42) to all peers
```

**Pipelining**: Clients maintain a queue of outstanding requests per connection (typically 5-10 requests). This hides round-trip latency and keeps the TCP pipe full:

```
Without pipelining:          With pipelining:
  REQ → ... → PIECE          REQ₁ → REQ₂ → REQ₃ → REQ₄ → REQ₅ →
  REQ → ... → PIECE          ←PIECE₁ ←PIECE₂ ←PIECE₃ REQ₆ → ...
  REQ → ... → PIECE
  (idle gaps between)         (continuous data flow)
```

Optimal queue depth depends on link BDP (bandwidth-delay product):

```
queue_depth = ceil(bandwidth * round_trip_time / block_size)

Example: 50 Mbps link, 50ms RTT, 16 KiB blocks:
  BDP = 50,000,000 * 0.050 / 8 = 312,500 bytes
  queue_depth = ceil(312,500 / 16,384) ≈ 20 requests
```

### 1.9 Mossaic Recommendation: What We Can Borrow

The BitTorrent protocol offers several directly applicable patterns for Mossaic's chunk distribution across Durable Objects:

#### 1.9.1 Chunk Sizing Strategy

**Borrow**: Power-of-2 piece sizes with size-adaptive selection.

For Mossaic, we should define chunk sizes based on total file size, similar to BitTorrent's piece length selection. However, our constraints differ:
- **No .torrent file size constraint** (metadata is in DOs, not a downloadable file)
- **DO memory limits** (~128 MB) constrain maximum chunk size
- **Subrequest limits** (1000 subrequests per Worker invocation) constrain minimum chunk size (too many chunks = too many DO invocations)

```
Recommended Mossaic chunk sizes:
  File Size < 1 MB:      64 KiB chunks  → max 16 chunks
  File Size 1-16 MB:    256 KiB chunks  → max 64 chunks
  File Size 16-256 MB:    1 MiB chunks  → max 256 chunks
  File Size 256 MB-4 GB:  4 MiB chunks  → max 1024 chunks
  File Size > 4 GB:      16 MiB chunks  → scale as needed
```

#### 1.9.2 Hash Verification

**Borrow**: Per-chunk hash verification, preferably with SHA-256 (matching BEP 52 / v2).

Store the hash tree in a manifest DO. On chunk retrieval, verify `SHA-256(chunk_data) == manifest.chunk_hashes[i]`. This provides:
- Integrity verification against storage corruption
- Tamper detection
- Content-addressable chunk identification (enables deduplication)

Consider adopting the **Merkle tree** approach from BEP 52: store only the root hash in the manifest, compute proof paths on demand. This allows verification of partial downloads without the full hash list.

#### 1.9.3 Parallel Retrieval

**Borrow**: Parallel block fetching from multiple sources with pipelining.

When a client requests a file from Mossaic, the coordinating Worker should:
1. Look up the chunk manifest (which DO holds which chunk)
2. Issue **parallel** fetch requests to multiple DOs simultaneously
3. Reassemble chunks as they arrive (not necessarily in order)
4. Verify hashes on reassembly

This mirrors BitTorrent's parallel block downloading but with DOs as "peers":

```
Client → Worker (coordinator)
            |
            |--- fetch chunk 0 → DO-A
            |--- fetch chunk 1 → DO-B   (parallel)
            |--- fetch chunk 2 → DO-C   (parallel)
            |--- fetch chunk 3 → DO-A   (parallel)
            |
            ← stream reassembled response to client
```

#### 1.9.4 Chunk Placement Diversity (Inspired by Rarest-First)

**Borrow**: The rarest-first principle as a **placement** strategy (not selection).

When storing chunks across DOs, ensure that no single DO failure causes total data loss. Distribute chunks across multiple DOs such that each chunk exists on at least R DOs (replication factor). Prioritize placing replicas on DOs that have the fewest chunks of this file (analogous to rarest-first in reverse — "emptiest DO first").

#### 1.9.5 Endgame Mode for Latency-Sensitive Reads

**Borrow**: Redundant requests for tail latency reduction.

For the last few chunks (or for latency-sensitive reads), issue requests to **multiple replicas** of each chunk simultaneously and take the first response. This is exactly BitTorrent's endgame mode applied to reads:

```
Endgame read (p99 latency optimization):
  chunk_5 → request DO-A replica AND DO-B replica
  First response wins → cancel/ignore the other
```

#### 1.9.6 Flow Control (Inspired by Choking)

**Borrow**: Rate limiting and connection management concepts.

DOs have limited throughput. Implement a flow control mechanism where:
- Hot DOs (high request rate) can "choke" lower-priority requests
- A coordinator can redirect reads to less-loaded replica DOs
- This prevents any single DO from becoming a bottleneck

### References (BitTorrent)

| Source | URL |
|--------|-----|
| BEP 3: The BitTorrent Protocol Specification | https://www.bittorrent.org/beps/bep_0003.html |
| BEP 52: The BitTorrent Protocol Specification v2 | https://www.bittorrent.org/beps/bep_0052.html |
| BitTorrent Community Specification (wiki.theory.org) | https://wiki.theory.org/BitTorrentSpecification |
| BitTorrent Economics Paper (Bram Cohen) | http://bittorrent.org/bittorrentecon.pdf |
| Legout et al., "Rarest First and Choke Algorithms Are Enough" (IMC 2006) | http://hal.inria.fr/inria-00000156/en |
| Wikipedia: BitTorrent | https://en.wikipedia.org/wiki/BitTorrent |
| BEP 6: Fast Extension (reject messages) | https://www.bittorrent.org/beps/bep_0006.html |

---

## Section 2: RAID Striping

### 2.1 Overview

RAID (Redundant Array of Independent Disks) is a data storage virtualization technology that combines multiple physical disk drives into one or more logical units for the purposes of data redundancy, performance improvement, or both. Originally proposed by Patterson, Gibson, and Katz at UC Berkeley in 1988, RAID replaces a "SLED" (Single Large Expensive Disk) with an array of inexpensive disks.

The three fundamental RAID techniques are:
- **Striping** (RAID 0): Distributing data across drives for throughput
- **Mirroring** (RAID 1): Duplicating data for redundancy
- **Parity** (RAID 5/6): Computing error-correction data for fault tolerance

### 2.2 RAID 0: Striping

#### How It Works

RAID 0 splits data evenly across N disks in a round-robin fashion. Data is divided into fixed-size **stripe units** (also called "chunks" or "strip size"), and consecutive units are placed on consecutive disks.

```
RAID 0 with 4 disks, stripe unit = 64 KiB:

         Disk 0      Disk 1      Disk 2      Disk 3
        +--------+  +--------+  +--------+  +--------+
Row 0:  |  A0    |  |  A1    |  |  A2    |  |  A3    |
        +--------+  +--------+  +--------+  +--------+
Row 1:  |  A4    |  |  A5    |  |  A6    |  |  A7    |
        +--------+  +--------+  +--------+  +--------+
Row 2:  |  A8    |  |  A9    |  |  A10   |  |  A11   |
        +--------+  +--------+  +--------+  +--------+
        |  ...   |  |  ...   |  |  ...   |  |  ...   |

Stripe = one full row across all disks
       = N × stripe_unit = 4 × 64 KiB = 256 KiB

Mapping: logical_block B is on:
  disk   = B mod N
  offset = floor(B / N) × stripe_unit_size
```

#### The Parallel I/O Throughput Math

The key performance property of RAID 0: reads and writes to different disks happen **concurrently**.

```
Single disk throughput: T₁
N disks in RAID 0:     T_total ≈ N × T₁  (theoretical max)

Example: 4 SSDs each doing 500 MB/s sequential read
  RAID 0 theoretical: 4 × 500 = 2,000 MB/s

For random I/O (IOPS):
  Single disk IOPS: I₁
  N-disk RAID 0:    I_total ≈ N × I₁
  
Example: 4 NVMe SSDs each doing 100K random read IOPS
  RAID 0 theoretical: 4 × 100K = 400K IOPS
```

**Caveats:**
- Controller overhead reduces actual throughput below N×
- Small I/Os that fit within a single stripe unit don't benefit from parallelism
- Sequential reads only benefit when the access spans multiple stripe units

#### Stripe Unit Sizing

The stripe unit size (sometimes called "chunk size") is critical:

| Stripe Unit | Sequential Perf | Random Perf | Notes |
|-------------|----------------|-------------|-------|
| Small (4-16 KiB) | Better (more parallel) | Worse (one I/O hits multiple disks) | High parallelism for large reads |
| Medium (64-256 KiB) | Good balance | Good balance | Common default |
| Large (512 KiB - 1 MiB) | Worse for small files | Better (I/O stays on one disk) | Good for database workloads |

**Optimal stripe unit formula** (rule of thumb):

```
stripe_unit ≈ typical_IO_size / N

For a workload doing 256 KiB reads on 4 disks:
  stripe_unit = 256 KiB / 4 = 64 KiB
  → Each read touches all 4 disks in parallel
```

#### Capacity and Fault Tolerance

```
Usable capacity:  C_total = N × C_disk  (100% efficient)
Space efficiency:  1.0  (no redundancy overhead)
Fault tolerance:   NONE — any single disk failure destroys the array
Failure probability: P_fail = 1 - (1 - p)^N  (increases with N)

Example: 4 disks each with 2% annual failure rate
  P_array_fail = 1 - (1 - 0.02)^4 = 1 - 0.98^4 = 1 - 0.922 = 0.078 ≈ 7.8%
```

### 2.3 RAID 5: Distributed Parity

#### How It Works

RAID 5 uses block-level striping with **distributed parity**. For every stripe (row across disks), one stripe unit is dedicated to parity, computed as the XOR of all other stripe units in that row. The parity rotates across disks to avoid a single bottleneck.

```
RAID 5 with 4 disks (Left-Asymmetric layout):

         Disk 0      Disk 1      Disk 2      Disk 3
        +--------+  +--------+  +--------+  +--------+
Row 0:  |  D0    |  |  D1    |  |  D2    |  | [P0]   |
        +--------+  +--------+  +--------+  +--------+
Row 1:  |  D3    |  |  D4    |  | [P1]   |  |  D5    |
        +--------+  +--------+  +--------+  +--------+
Row 2:  |  D6    |  | [P2]   |  |  D7    |  |  D8    |
        +--------+  +--------+  +--------+  +--------+
Row 3:  | [P3]   |  |  D9    |  |  D10   |  |  D11   |
        +--------+  +--------+  +--------+  +--------+
Row 4:  |  D12   |  |  D13   |  |  D14   |  | [P4]   |
        +--------+  +--------+  +--------+  +--------+

[Px] = parity block for row x
Parity rotates: row 0 on disk 3, row 1 on disk 2, row 2 on disk 1, etc.
```

#### XOR-Based Parity Calculation

For a stripe with data blocks D₀, D₁, ..., D_{N-2} (N-1 data blocks + 1 parity block):

```
P = D₀ ⊕ D₁ ⊕ D₂ ⊕ ... ⊕ D_{N-2}

where ⊕ = bitwise XOR

Example (byte-level):
  D₀ = 10110011
  D₁ = 01101010
  D₂ = 11001100
  P  = 10110011 ⊕ 01101010 ⊕ 11001100
     = 00010101

Verification: D₀ ⊕ D₁ ⊕ D₂ ⊕ P = 00000000  (all zeros = valid)
```

#### Single Disk Failure Reconstruction

If any one disk fails, its data can be reconstructed from the remaining disks:

```
If Disk 1 fails (containing D₁):
  D₁ = D₀ ⊕ D₂ ⊕ P
     = 10110011 ⊕ 11001100 ⊕ 00010101
     = 01101010  ✓  (matches original D₁)

This works because XOR is its own inverse:
  If P = D₀ ⊕ D₁ ⊕ D₂
  Then D₁ = D₀ ⊕ D₂ ⊕ P = D₀ ⊕ D₂ ⊕ (D₀ ⊕ D₁ ⊕ D₂) = D₁

Generalizing: any missing element = XOR of all other elements
```

#### Capacity and Performance

```
Usable capacity:    C_total = (N - 1) × C_disk
Space efficiency:   (N - 1) / N
                    3 disks: 66.7%
                    4 disks: 75%
                    8 disks: 87.5%
Fault tolerance:    1 disk failure

Read throughput:    ≈ (N - 1) × T₁  (only data disks contribute)
Write throughput:   More complex — see write penalty
```

#### The Write Penalty

Every write to a RAID 5 array requires updating the parity. Two strategies:

**1. Read-Modify-Write (small writes):**
```
To update one data block D_old → D_new:
  1. Read old data:    D_old          (1 read)
  2. Read old parity:  P_old          (1 read)
  3. Compute new parity:
     P_new = P_old ⊕ D_old ⊕ D_new   (XOR)
  4. Write new data:   D_new          (1 write)
  5. Write new parity: P_new          (1 write)

Total: 2 reads + 2 writes = 4 I/O operations per logical write
Write penalty = 4  (compared to 1 for RAID 0)
```

**2. Reconstruct-Write (large writes spanning most of stripe):**
```
To update most blocks in a stripe:
  1. Read all data blocks NOT being written  (N-2 reads worst case)
  2. Compute new parity from all new data
  3. Write all new data + parity            (N writes)

Better when writing > N/2 blocks in a stripe
```

**Effective write throughput:**
```
For small random writes:
  T_write = T₁ × N / 4    (each write costs 4 I/Os spread across disks)

For full-stripe writes:
  T_write ≈ (N-1) × T₁    (no read penalty — compute parity inline)
```

### 2.4 RAID 6: Dual Parity (Reed-Solomon)

#### How It Works

RAID 6 extends RAID 5 with a **second parity block** per stripe, allowing survival of **two** simultaneous disk failures. It requires at least 4 disks.

```
RAID 6 with 5 disks:

         Disk 0      Disk 1      Disk 2      Disk 3      Disk 4
        +--------+  +--------+  +--------+  +--------+  +--------+
Row 0:  |  D0    |  |  D1    |  |  D2    |  | [P0]   |  | [Q0]   |
        +--------+  +--------+  +--------+  +--------+  +--------+
Row 1:  |  D3    |  |  D4    |  | [P1]   |  | [Q1]   |  |  D5    |
        +--------+  +--------+  +--------+  +--------+  +--------+
Row 2:  |  D6    |  | [P2]   |  | [Q2]   |  |  D7    |  |  D8    |
        +--------+  +--------+  +--------+  +--------+  +--------+
Row 3:  | [P3]   |  | [Q3]   |  |  D9    |  |  D10   |  |  D11   |
        +--------+  +--------+  +--------+  +--------+  +--------+
Row 4:  | [Q4]   |  |  D12   |  |  D13   |  |  D14   |  | [P4]   |
        +--------+  +--------+  +--------+  +--------+  +--------+

[Px] = P parity (XOR, same as RAID 5)
[Qx] = Q parity (Reed-Solomon / Galois field)
```

#### P + Q Parity Calculations

**P parity** (same as RAID 5):
```
P = D₀ ⊕ D₁ ⊕ D₂ ⊕ ... ⊕ D_{N-3}

Simple XOR across all data blocks in the stripe.
```

**Q parity** (Reed-Solomon in GF(2⁸)):
```
Q = g⁰·D₀ ⊕ g¹·D₁ ⊕ g²·D₂ ⊕ ... ⊕ g^{N-3}·D_{N-3}

where:
  g = generator element of GF(2⁸), typically g = 2
  · = multiplication in GF(2⁸)
  ⊕ = addition in GF(2⁸) = bitwise XOR

Each byte of each block is treated as an element of GF(2⁸).
Multiplication by 2 in GF(2⁸) is a left shift + conditional XOR
with the irreducible polynomial (typically 0x1D for the polynomial
x⁸ + x⁴ + x³ + x² + 1 used by Linux md-raid).
```

The Linux kernel uses H. Peter Anvin's optimized approach, which requires only addition (XOR) and multiplication by 2 in GF(2⁸), making it efficient with SIMD instructions (SSSE3, AVX2).

#### Two-Disk Failure Reconstruction

With P and Q, we can reconstruct any two missing blocks. Three cases:

**Case 1: Two data disks fail (D_x, D_y lost)**
```
Using P: D_x ⊕ D_y = P ⊕ (all other surviving data blocks) = known value K_p
Using Q: g^x·D_x ⊕ g^y·D_y = Q ⊕ (all other g^i·D_i terms) = known value K_q

This gives two equations in two unknowns in GF(2⁸):
  D_x ⊕ D_y = K_p
  g^x·D_x ⊕ g^y·D_y = K_q

Solution:
  D_y = (K_q ⊕ g^x · K_p) / (g^y ⊕ g^x)
  D_x = K_p ⊕ D_y

Division in GF(2⁸) is multiplication by the inverse.
```

**Case 2: One data disk and P disk fail**
```
Reconstruct data from Q and remaining data, then recompute P.
```

**Case 3: One data disk and Q disk fail**
```
Reconstruct data from P (same as RAID 5), then recompute Q.
```

#### Capacity and Write Penalty

```
Usable capacity:    C_total = (N - 2) × C_disk
Space efficiency:   (N - 2) / N
                    4 disks: 50%
                    5 disks: 60%
                    8 disks: 75%
Fault tolerance:    2 disk failures

Write penalty (small writes):
  Read old data + Read old P + Read old Q = 3 reads
  Write new data + Write new P + Write new Q = 3 writes
  Total = 6 I/O operations per logical write
  Write penalty = 6  (vs 4 for RAID 5, vs 1 for RAID 0)
```

### 2.5 Stripe Width vs. Stripe Unit Size Optimization

**Definitions:**
```
Stripe unit size:  Size of each individual block on a single disk (e.g., 64 KiB)
Stripe width:      Total data across one full stripe = stripe_unit × N_data_disks
Full stripe size:  Stripe width + parity = stripe_unit × N_total_disks

RAID 0: stripe_width = stripe_unit × N
RAID 5: stripe_width = stripe_unit × (N - 1)
RAID 6: stripe_width = stripe_unit × (N - 2)
```

**Optimization considerations:**

```
Workload-Based Selection:
+-------------------+------------------------+-------------------------+
| Workload          | Optimal Stripe Unit     | Why                     |
+-------------------+------------------------+-------------------------+
| Large sequential  | Small (16-64 KiB)       | Max parallelism across  |
| (video streaming) |                         | all disks per I/O       |
+-------------------+------------------------+-------------------------+
| Small random      | Large (256 KiB-1 MiB)   | Single I/O stays on one |
| (database OLTP)   |                         | disk; minimizes seeks   |
+-------------------+------------------------+-------------------------+
| Mixed             | Medium (64-256 KiB)     | Balance between both    |
+-------------------+------------------------+-------------------------+

Full-Stripe Write Threshold:
  When writing ≥ stripe_width bytes aligned to stripe boundary,
  the write penalty drops to 0 (no reads needed, parity computed inline).
  This is the optimal case for RAID 5/6 writes.
```

### 2.6 Write Penalty Summary

```
+--------+------------------+--------------------+-------------------+
| Level  | Small Write      | Full-Stripe Write  | Read              |
|        | (I/Os per write) | (I/Os per write)   | (I/Os per read)   |
+--------+------------------+--------------------+-------------------+
| RAID 0 | 1                | 1                  | 1                 |
| RAID 1 | 2 (write both)   | 2                  | 1 (read either)   |
| RAID 5 | 4 (2R + 2W)      | 1 per disk         | 1                 |
| RAID 6 | 6 (3R + 3W)      | 1 per disk         | 1                 |
+--------+------------------+--------------------+-------------------+

Effective Random Write IOPS:
  RAID 0: N × I₁
  RAID 1: N/2 × I₁  (for 2-way mirror)
  RAID 5: N × I₁ / 4
  RAID 6: N × I₁ / 6
```

### 2.7 RAID Layout Comparison Diagram

```
═══════════════════════════════════════════════════════════════════════
                         RAID LEVEL COMPARISON
═══════════════════════════════════════════════════════════════════════

RAID 0 (Striping):          RAID 1 (Mirroring):
  D0  D1  D2  D3              D0  D0
  D4  D5  D6  D7              D1  D1
  D8  D9  D10 D11             D2  D2
  Cap: 100%  Fault: 0         Cap: 50%  Fault: N-1

RAID 5 (Distributed Parity):
  D0   D1   D2   P0       ← parity rotates across disks
  D3   D4   P1   D5
  D6   P2   D7   D8
  P3   D9   D10  D11
  Cap: (N-1)/N  Fault: 1 disk

RAID 6 (Dual Distributed Parity):
  D0   D1   D2   P0   Q0  ← two parity blocks per stripe
  D3   D4   P1   Q1   D5
  D6   P2   Q2   D7   D8
  P3   Q3   D9   D10  D11
  Q4   D12  D13  D14  P4
  Cap: (N-2)/N  Fault: 2 disks

═══════════════════════════════════════════════════════════════════════
```

### 2.8 Array Failure Probability Math

Given N disks each with independent failure rate r:

```
RAID 0: P_fail = 1 - (1-r)^N
  Any single failure → total loss

RAID 5: P_fail = 1 - (1-r)^N - N·r·(1-r)^{N-1}
  = P(≥2 failures) = 1 - P(0 failures) - P(exactly 1 failure)
  
  Example: N=5, r=5%
  P_fail = 1 - 0.95^5 - 5×0.05×0.95^4
         = 1 - 0.7738 - 0.2036
         = 0.0226 ≈ 2.3%

RAID 6: P_fail = 1 - (1-r)^N - N·r·(1-r)^{N-1} - C(N,2)·r²·(1-r)^{N-2}
  = P(≥3 failures)
  
  Example: N=5, r=5%
  P_fail = 1 - 0.95^5 - 5×0.05×0.95^4 - 10×0.05²×0.95³
         = 1 - 0.7738 - 0.2036 - 0.0214
         = 0.0012 ≈ 0.12%
```

### 2.9 Mossaic Recommendation: How RAID-Like Striping Maps to DOs

Mossaic distributes file chunks across Cloudflare Durable Objects. Here's how RAID concepts map to our architecture:

#### 2.9.1 RAID 0 Striping → Basic Chunk Distribution

**Borrow**: Round-robin data distribution across DOs for parallel throughput.

The most direct analogy: treat each Durable Object as a "disk" in RAID 0. Stripe chunks across DOs in a round-robin pattern:

```
File (16 MiB, chunk_size = 1 MiB):

  DO-0    DO-1    DO-2    DO-3    DO-4    DO-5    DO-6    DO-7
  C0      C1      C2      C3      C4      C5      C6      C7
  C8      C9      C10     C11     C12     C13     C14     C15

Parallel fetch: request all 16 chunks simultaneously
  → bounded by slowest DO response (like RAID 0 bounded by slowest disk)
```

**Key difference from RAID 0**: DOs are not "disks" with fixed throughput — they have variable latency based on geographic proximity and load. Our "stripe width" (number of DOs per file) should be tuned to the file size, not fixed.

```
Recommended DO fan-out:
  File < 1 MiB:    1-2 DOs (overhead of coordination exceeds benefit)
  File 1-16 MiB:   4-8 DOs
  File 16-256 MiB:  8-16 DOs
  File > 256 MiB:  16-32 DOs (bounded by subrequest limits)
```

#### 2.9.2 RAID 5 Parity → Erasure-Coded Redundancy

**Borrow**: XOR parity for single-DO failure resilience.

Add one parity chunk per stripe for single-DO-failure tolerance:

```
File (4 chunks) with RAID 5-style parity:

  DO-A    DO-B    DO-C    DO-D (parity)
  C0      C1      C2      P0 = C0 ⊕ C1 ⊕ C2

If DO-B is unavailable:
  C1 = C0 ⊕ C2 ⊕ P0   (reconstruct from remaining)

Overhead: 1 extra DO per stripe = 1/(N-1) overhead
  4 DOs: 33% overhead for 1-failure tolerance
  8 DOs: 14% overhead for 1-failure tolerance
```

**Advantages for Mossaic**:
- XOR parity is extremely cheap to compute (unlike Reed-Solomon)
- Only 1/(N-1) storage overhead (much cheaper than full replication)
- If a DO is temporarily unavailable (not permanently dead), parity allows reconstruction without waiting

#### 2.9.3 RAID 6 Dual Parity → Two-Failure Tolerance

**Borrow**: For critical data, use two parity chunks per stripe.

```
File (4 chunks) with RAID 6-style dual parity:

  DO-A    DO-B    DO-C    DO-D     DO-E     DO-F
  C0      C1      C2      C3       P0       Q0

P0 = C0 ⊕ C1 ⊕ C2 ⊕ C3
Q0 = g⁰·C0 ⊕ g¹·C1 ⊕ g²·C2 ⊕ g³·C3  (GF(2⁸))

If DO-A and DO-C both unavailable:
  → Solve 2 equations in 2 unknowns to reconstruct C0 and C2

Overhead: 2/(N-2)
  6 DOs: 50% overhead for 2-failure tolerance
  10 DOs: 25% overhead for 2-failure tolerance
```

**When to use in Mossaic**: For files that must survive any two concurrent DO failures — e.g., paid user data, critical metadata manifests.

**Practical simplification**: Rather than implementing full GF(2⁸) Reed-Solomon, consider using a simple erasure coding library (e.g., based on Cauchy matrices or Liberation codes) that provides `k` data + `m` parity with configurable `m`.

#### 2.9.4 Stripe Unit Sizing → Chunk Size Selection

**Borrow**: Match chunk size to typical access pattern.

| Access Pattern | Recommended Chunk Size | Analogy |
|---------------|----------------------|---------|
| Full file download | Larger chunks (1-4 MiB) | Like large stripe units for sequential I/O |
| Range requests / seeking | Smaller chunks (64-256 KiB) | Like small stripe units for random I/O |
| Streaming media | Medium chunks (256 KiB - 1 MiB) | Balance latency and throughput |

#### 2.9.5 Write Penalty → Write Amplification in DO Updates

**Borrow**: Understand the read-modify-write cost.

When a file is partially updated in Mossaic, we face the same write amplification as RAID 5:

```
Without parity: Update chunk C2 → 1 DO write
With RAID 5 parity: Update chunk C2 →
  1. Read old C2 from DO-C
  2. Read old P from DO-D
  3. Compute P_new = P_old ⊕ C2_old ⊕ C2_new
  4. Write C2_new to DO-C
  5. Write P_new to DO-D
  Total: 2 reads + 2 writes = 4 DO operations (write penalty = 4)

With full stripe rewrite (all chunks change):
  Just compute parity from all new chunks, write everything.
  No reads needed → write penalty = 1 per DO
```

**Mossaic optimization**: For append-only or write-once workloads (which is the common case for file storage), always write full stripes. This eliminates the write penalty entirely. Only use read-modify-write for rare in-place updates.

#### 2.9.6 Failure Recovery → DO Unavailability Handling

**Borrow**: Proactive scrubbing and lazy reconstruction.

Like RAID, Mossaic should:
1. **Periodically verify** chunk integrity (scrubbing) — read each chunk, verify its hash
2. **On read failure**, attempt parity reconstruction instead of returning an error
3. **Lazy rebuild** — when a DO is detected as permanently failed, reconstruct its chunks onto a new DO in the background
4. **Prioritize critical chunks** — chunks with reduced redundancy (already lost one replica/parity) should be rebuilt first (analogous to RAID rebuild priority)

### References (RAID)

| Source | URL |
|--------|-----|
| Patterson, Gibson, Katz, "A Case for Redundant Arrays of Inexpensive Disks (RAID)" (SIGMOD 1988) | https://www.cs.cmu.edu/~garth/RAIDpaper/Patterson88.pdf |
| Wikipedia: Standard RAID Levels | https://en.wikipedia.org/wiki/Standard_RAID_levels |
| Wikipedia: RAID | https://en.wikipedia.org/wiki/RAID |
| Wikipedia: Reed-Solomon Error Correction | https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction |
| Chen et al., "RAID: High-Performance, Reliable Secondary Storage" (ACM Computing Surveys, 1994) | https://doi.org/10.1145/176979.176981 |
| OSTEP Chapter 38: Redundant Arrays of Inexpensive Disks (RAIDs) | http://pages.cs.wisc.edu/~remzi/OSTEP/file-raid.pdf |
| SNIA: Common RAID Disk Data Format (DDF) Standard | http://www.snia.org/tech_activities/standards/curr_standards/ddf/ |
| H. Peter Anvin, Linux RAID 6 implementation | https://github.com/koverstreet/bcachefs-tools/blob/master/raid/raid.c |
| Plank, "Erasure Codes for Storage Systems: A Brief Primer" (USENIX ;login:) | https://www.usenix.org/system/files/login/articles/10_plank-online.pdf |
| Plank, "The RAID-6 Liberation Codes" (FAST '08) | https://www.usenix.org/legacy/event/fast08/tech/full_papers/plank/plank_html |

---

## Section 3: Erasure Coding (Reed-Solomon)

### 3.1 Fundamental Math: Galois Field Arithmetic — GF(2^8)

Reed-Solomon codes operate over **finite fields** (Galois Fields). For byte-oriented
storage systems, the field of choice is **GF(2^8)** — a field with exactly 256 elements,
one for each possible byte value (0x00 – 0xFF).

#### Why GF(2^8)?

- Each element is exactly **one byte** — maps perfectly to storage
- The field has exactly 256 elements (2^8), matching the byte alphabet
- Addition is **bitwise XOR** — extremely fast on all hardware
- Used by AES (Rijndael), RAID-6, and virtually all storage erasure codes

#### Field Construction

GF(2^8) is constructed as the set of polynomials of degree < 8 over GF(2),
modulo an **irreducible polynomial** of degree 8.

Common irreducible polynomials for GF(2^8):

```
p(x) = x^8 + x^4 + x^3 + x^2 + 1    (0x11D) — used by many RS implementations
p(x) = x^8 + x^4 + x^3 + x   + 1    (0x11B) — used by AES/Rijndael
```

Each element is a polynomial represented as a byte. For example:

```
0x53 = 01010011 = x^6 + x^4 + x + 1
0xCA = 11001010 = x^7 + x^6 + x^3 + x
```

#### Addition in GF(2^8)

Addition (and subtraction — they are identical in characteristic 2) is **XOR**:

```
a + b = a XOR b

Example:
  0x53 + 0xCA = 01010011 XOR 11001010 = 10011001 = 0x99
```

This is because polynomial coefficients are in GF(2), where 1+1 = 0.

#### Multiplication in GF(2^8)

Multiplication is polynomial multiplication modulo the irreducible polynomial.
In practice, this is done via **log/antilog tables** for O(1) lookup:

```
1. Precompute tables:
   - exp_table[i] = g^i  for i = 0..254  (g = generator, e.g. 0x03)
   - log_table[a] = i    such that g^i = a

2. Multiply:
   a * b = exp_table[(log_table[a] + log_table[b]) % 255]

3. Special case: if a == 0 or b == 0, result is 0
```

The **Russian Peasant Algorithm** provides an alternative without tables:

```c
uint8_t gmul(uint8_t a, uint8_t b) {
    uint8_t p = 0;
    while (a && b) {
        if (b & 1) p ^= a;           // polynomial addition
        if (a & 0x80)                 // if x^7 term exists
            a = (a << 1) ^ 0x1D;     // reduce mod p(x)
        else
            a <<= 1;
        b >>= 1;
    }
    return p;
}
```

#### Multiplicative Inverse

Every non-zero element has a multiplicative inverse:

```
a^(-1) = a^(254)   (by Fermat's little theorem in GF(2^8))

Or via log tables:
a^(-1) = exp_table[(255 - log_table[a]) % 255]
```

### 3.2 Reed-Solomon Encoding: How (k, m) Works

Reed-Solomon is a **Maximum Distance Separable (MDS)** code, meaning it achieves
the theoretical optimum: from any k of n = k + m encoded chunks, the original
k data chunks can be perfectly reconstructed.

#### Systematic Encoding

In a **systematic** RS(n, k) code with n = k + m:

```
Input:   D_0, D_1, ..., D_{k-1}    (k data chunks)
Output:  D_0, D_1, ..., D_{k-1},   (k data chunks, unchanged)
         P_0, P_1, ..., P_{m-1}    (m parity chunks)
```

```
                    Encoding Matrix (n x k)
                    ┌                     ┐   ┌      ┐     ┌      ┐
                    │  1   0   0  ...  0   │   │ D_0  │     │ D_0  │
                    │  0   1   0  ...  0   │   │ D_1  │     │ D_1  │
                    │  0   0   1  ...  0   │   │ D_2  │     │ D_2  │
                    │  .   .   .  ...  .   │ x │  .   │  =  │  .   │
        Identity    │  0   0   0  ...  1   │   │  .   │     │  .   │
        ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ │   │ D_{k-1}│   │D_{k-1}│
        Parity      │ p00 p01 p02 ... p0k │   └      ┘     │ P_0  │
        rows        │ p10 p11 p12 ... p1k │                 │ P_1  │
                    │  .   .   .  ...  .   │                 │  .   │
                    │ pm0 pm1 pm2 ... pmk │                 │ P_{m-1}│
                    └                     ┘                 └      ┘

        [  I_k  ]       [ D ]       [ D ]
        [ ───── ] x     [   ]   =   [ ─ ]
        [  P    ]                    [ P ]
```

Each parity chunk P_j is computed as a linear combination over GF(2^8):

```
P_j = p_{j,0} * D_0  +  p_{j,1} * D_1  + ... +  p_{j,k-1} * D_{k-1}
```

where all arithmetic is in GF(2^8), and `+` is XOR, `*` is GF multiply.

#### The Encoding Matrix

The encoding matrix is typically constructed from either a **Vandermonde matrix**
or a **Cauchy matrix** over GF(2^8).

**Vandermonde-based encoding matrix:**

```
         ┌                                        ┐
         │  1     1      1      1    ...  1        │
     A = │  1    a_1    a_1^2  a_1^3 ... a_1^{k-1}│
         │  1    a_2    a_2^2  a_2^3 ... a_2^{k-1}│
         │  ...                                    │
         │  1    a_m    a_m^2  a_m^3 ... a_m^{k-1}│
         └                                        ┘

    where a_1, a_2, ..., a_m are distinct non-zero elements of GF(2^8)
```

The systematic generator is then formed by [I_k; A] where A is the parity part.

### 3.3 Cauchy RS vs Vandermonde RS

Both constructions produce valid MDS codes, but they have different computational
properties.

#### Vandermonde Reed-Solomon

```
Matrix element:   V_{i,j} = a_i^j

Pros:
  - Straightforward polynomial evaluation
  - Well-understood, classical construction
  - Easy to make systematic via Gaussian elimination

Cons:
  - Encoding requires general GF multiply (log/exp tables)
  - Higher computational cost for parity generation
  - The generator matrix must be carefully constructed to remain invertible
```

#### Cauchy Reed-Solomon

```
Matrix element:   C_{i,j} = 1 / (x_i - y_j)

where x_i and y_j are distinct elements of GF(2^8)

Key property: ANY square submatrix of a Cauchy matrix is invertible
(as long as all x_i are distinct and all y_j are distinct, and x_i ≠ y_j)
```

```
Pros:
  - Guaranteed invertibility of every submatrix (no careful construction needed)
  - The encoding matrix can be transformed so that parity rows contain
    only binary (0/1) entries after applying a diagonal scaling
  - With the Bloemer-Kalfane-Karp-Welch optimization, Cauchy RS encoding
    can be reduced to XOR-only operations for the parity computation
  - XOR-only encoding is 2-10x faster than general GF multiply

Cons:
  - Slightly more complex initial matrix construction
  - The XOR schedule requires precomputation
```

**Why Cauchy RS is preferred for storage systems:**

```
┌─────────────────────────────┬──────────────┬───────────────┐
│ Property                    │ Vandermonde  │ Cauchy RS     │
├─────────────────────────────┼──────────────┼───────────────┤
│ Encoding operations         │ GF multiply  │ XOR only      │
│ Speed (relative)            │ 1x           │ 2-10x faster  │
│ Submatrix invertibility     │ Must verify  │ Guaranteed    │
│ SIMD friendliness           │ Moderate     │ Excellent     │
│ Used by                     │ Classic impl │ ISA-L, Jerasure│
│ Suitable for storage        │ Yes          │ Yes (better)  │
└─────────────────────────────┴──────────────┴───────────────┘
```

### 3.4 The (n, k) Parameter Space

The total number of chunks is n = k + m, where:
- **k** = data chunks (original data, split evenly)
- **m** = parity/coding chunks (redundancy)
- **n** = total chunks

#### Storage Overhead

```
Storage overhead ratio = n / k = (k + m) / k = 1 + m/k

Overhead percentage = m/k × 100%

Examples:
  (6,4):   6/4 = 1.50x  → 50% overhead
  (9,6):   9/6 = 1.50x  → 50% overhead
  (14,10): 14/10 = 1.40x → 40% overhead
  (20,17): 20/17 = 1.18x → 18% overhead
```

#### Common Configurations

```
┌──────────┬───┬───┬─────────┬──────────────┬───────────────────────────┐
│ Config   │ k │ m │ n/k     │ Fault Tol.   │ Used By                   │
├──────────┼───┼───┼─────────┼──────────────┼───────────────────────────┤
│ (4,2)    │ 2 │ 2 │ 2.00x   │ 2 failures   │ Small clusters, tutorials │
│ (6,4)    │ 4 │ 2 │ 1.50x   │ 2 failures   │ RAID-6, small deployments │
│ (9,6)    │ 6 │ 3 │ 1.50x   │ 3 failures   │ Azure LRC (inner code)    │
│ (10,4)   │ 10│ 4 │ 1.40x   │ 4 failures   │ HDFS-RAID (Facebook)      │
│ (12,4)   │ 12│ 4 │ 1.33x   │ 4 failures   │ Google Colossus           │
│ (14,10)  │ 10│ 4 │ 1.40x   │ 4 failures   │ Facebook f4, HDFS-EC      │
│ (16,12)  │ 12│ 4 │ 1.33x   │ 4 failures   │ Azure Storage (LRC)       │
│ (20,17)  │ 17│ 3 │ 1.18x   │ 3 failures   │ Backblaze Vaults           │
└──────────┴───┴───┴─────────┴──────────────┴───────────────────────────┘
```

#### Parameter Space Diagram

```
    Fault tolerance (m)
    │
  6 │                              ← Very high reliability
    │                                 (but high overhead)
  5 │
    │
  4 │           ●(14,10)  ●(10,4) ●(12,4)  ●(16,12)
    │
  3 │    ●(9,6)                    ●(20,17)
    │
  2 │  ●(6,4)  ●(4,2)
    │
  1 │  ← Minimal (parity only)
    │
    └────┬────┬────┬────┬────┬────┬────┬───── Data chunks (k)
         2    4    6    8   10   12   14   17

    Sweet spot: k=4..10, m=2..4
```

### 3.5 Decoding / Reconstruction

When up to m chunks are lost (data or parity), reconstruction works by
**matrix inversion** over GF(2^8).

#### Step-by-step Reconstruction

```
1. Identify which chunks survived (need at least k of n)
2. Select any k surviving chunks
3. Extract the corresponding k rows from the encoding matrix → submatrix M
4. Compute M^(-1) (inverse over GF(2^8))
5. Multiply: [D_0, D_1, ..., D_{k-1}] = M^(-1) × [surviving chunks]
```

#### Visual Example: RS(6,4) with 2 lost chunks

```
Original encoding:                 After losing chunks 1, 4:

  ┌ 1 0 0 0 ┐   ┌ D0 ┐   ┌ C0 ┐      Available:
  │ 0 1 0 0 │   │ D1 │   │ C1 │        C0 = D0      (data)
  │ 0 0 1 0 │ × │ D2 │ = │ C2 │   ✓    C2 = D2      (data)
  │ 0 0 0 1 │   │ D3 │   │ C3 │        C3 = D3      (data)
  │ a b c d │   └    ┘   │ C4 │   ✓    C5 = P1      (parity)
  │ e f g h │             │ C5 │
  └         ┘             └    ┘   Lost: C1 (=D1), C4 (=P0)

Recovery matrix (rows 0,2,3,5 of encoding matrix):
  ┌ 1 0 0 0 ┐ -1    ┌ C0 ┐     ┌ D0 ┐
  │ 0 0 1 0 │    ×   │ C2 │  =  │ D1 │  ← recovered!
  │ 0 0 0 1 │        │ C3 │     │ D2 │
  │ e f g h │        │ C5 │     │ D3 │
  └         ┘        └    ┘     └    ┘
```

#### Reconstruction Cost

```
Decoding cost = O(k^2) GF(2^8) operations for matrix inversion
              + O(k × chunk_size) for matrix-vector multiply

For RS(14,10) with 1MB chunks:
  - Matrix inversion: ~100 GF multiplies (negligible)
  - Data recovery: 10 × 1MB × GF-multiply = ~10MB of work
  - With SIMD: easily saturates disk/network I/O
```

### 3.6 Comparison: Erasure Coding vs. Replication

```
┌────────────────────┬─────────────────┬─────────────────────────┐
│ Property           │ 3x Replication  │ RS(14,10) Erasure Code  │
├────────────────────┼─────────────────┼─────────────────────────┤
│ Storage overhead   │ 3.00x (200%)    │ 1.40x (40%)             │
│ Fault tolerance    │ 2 failures      │ 4 failures              │
│ Read latency       │ Low (any copy)  │ Low (any k of n chunks) │
│ Write latency      │ Low (3 writes)  │ Higher (encoding + n wr)│
│ Repair bandwidth   │ Copy 1 replica  │ Read k chunks, compute  │
│ Repair I/O         │ 1x data size    │ k/1 × chunk size        │
│ Implementation     │ Trivial         │ Complex (GF arithmetic) │
│ Min nodes for      │ 3               │ 14                      │
│   full protection  │                 │                         │
│ Space for 1 PB     │ 3 PB            │ 1.4 PB                  │
│ Cost savings       │ baseline        │ 53% less storage        │
└────────────────────┴─────────────────┴─────────────────────────┘
```

**Key tradeoff:** Erasure coding saves 50-65% storage vs 3x replication while
providing **better** fault tolerance, at the cost of higher compute during
writes and repairs.

### 3.7 Real-World Usage

#### HDFS (Hadoop Distributed File System)

- **HDFS-EC** (Erasure Coding): RS(6,3) and RS(10,4) are default policies
- Facebook used RS(10,4) for HDFS — 1.4x overhead, tolerates 4 failures
- Previously used 3x replication (3.0x overhead) for warm data
- Saved ~50% storage across their data centers
- Ref: "A Tale of Two Erasure Codes in HDFS" (FAST '15)

#### Ceph

- Uses **CRUSH** algorithm for placement + erasure coding
- Supports RS and locally repairable codes (LRC)
- Common config: RS(8,3) or RS(4,2) for pools
- Plugin architecture: supports ISA-L (Intel) for SIMD-optimized encoding

#### Azure Storage

- Uses **Local Reconstruction Codes (LRC)** — a generalization of RS
- Configuration: (12,2,2) LRC — 12 data, 2 local parity, 2 global parity
- Provides 16 chunks total, tolerates 4 failures
- LRC advantage: most repairs only need to read a local group (6 chunks)
  instead of all 12 data chunks
- Ref: "Erasure Coding in Windows Azure Storage" (USENIX ATC '12)

#### Backblaze Vaults

- Uses RS(20,17) — 17 data shards + 3 parity shards
- 1.18x storage overhead — very efficient
- Can survive loss of any 3 Storage Pods simultaneously
- Open-sourced their Java RS implementation on GitHub
- Encodes at ~149 MB/s per core (Intel Xeon E5-1620, 3.7GHz)
- Ref: https://github.com/Backblaze/JavaReedSolomon

#### Facebook f4 (Warm/Cold Storage)

- RS(14,10) for cold blob storage
- 1.4x overhead vs 3.6x for their prior warm storage
- Saved hundreds of petabytes of raw storage

### 3.8 Mossaic Recommendation

#### Recommended Configuration: RS(6,4) — Adaptive

For Mossaic's Durable Object (DO) topology:

```
Primary configuration:   RS(6,4)  — k=4 data, m=2 parity
  - 6 total DOs per file
  - 1.5x storage overhead (50%)
  - Tolerates loss of any 2 DOs
  - Balanced: not too many DOs, good fault tolerance

Secondary (large files):  RS(10,6) — k=6 data, m=4 parity
  - 10 total DOs per file
  - 1.67x storage overhead (67%)
  - Tolerates loss of any 4 DOs
  - Better for critical data / cross-region deployment
```

#### Why RS(6,4) for Mossaic:

```
1. DO TOPOLOGY FIT
   - Cloudflare DOs run in ~30 Colo locations globally
   - 6 DOs is small enough to place across diverse locations
   - Not so many that placement becomes constrained

2. CHUNK SIZE
   - For a 4MB file: each chunk = 1MB (4 data + 2 parity)
   - For a 64MB file: each chunk = 16MB
   - Fits well within DO storage limits (up to 1GB/DO typical)

3. REPAIR BANDWIDTH
   - To repair 1 lost chunk: read 4 chunks, XOR-compute 1 new chunk
   - Repair fan-in of 4 is manageable over Cloudflare's backbone

4. ENCODING COST
   - Cauchy RS with XOR-only encoding
   - 4 data chunks × 2 parity = 8 XOR passes per byte
   - At ~10 GB/s XOR throughput: encoding is not the bottleneck
   - DO Workers have limited CPU; XOR-only is critical

5. COMPARISON TO ALTERNATIVES
   ┌──────────┬──────────┬──────────┬──────────┐
   │          │ RS(6,4)  │ RS(10,6) │ 3x Repl  │
   ├──────────┼──────────┼──────────┼──────────┤
   │ Overhead │ 1.50x    │ 1.67x    │ 3.00x    │
   │ Fault T. │ 2        │ 4        │ 2        │
   │ # DOs    │ 6        │ 10       │ 3        │
   │ Repair   │ Read 4   │ Read 6   │ Copy 1   │
   │ Encoding │ Fast     │ Moderate │ None     │
   └──────────┴──────────┴──────────┴──────────┘
```

#### Implementation Strategy:

```
1. Use Cauchy Reed-Solomon (not Vandermonde)
   - XOR-only encoding for speed on DO Workers
   - Pre-compute the encoding schedule at startup

2. Library options for JS/WASM:
   - Port leopard-rs (Rust) to WASM for DO Workers
   - Or use a pure JS GF(2^8) implementation with SIMD.js
   - Encoding/decoding tables: only 512 bytes (exp + log tables)

3. Chunk layout:
   file_id + chunk_index → DO_id  (see Section 4 for mapping)
   Each DO stores exactly one chunk of each file it participates in

4. Degraded reads:
   - Request all 6 chunks in parallel
   - Return file from first 4 responses (any 4 of 6 suffice)
   - Provides natural tail-latency hedging
```

### References (Erasure Coding)

| # | Reference | URL |
|---|-----------|-----|
| 1 | Reed & Solomon, "Polynomial Codes over Certain Finite Fields" (1960) | https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction |
| 2 | Wikipedia: Erasure Code | https://en.wikipedia.org/wiki/Erasure_code |
| 3 | Wikipedia: Finite Field Arithmetic | https://en.wikipedia.org/wiki/Finite_field_arithmetic |
| 4 | Backblaze: Reed-Solomon Erasure Coding (open source) | https://www.backblaze.com/blog/reed-solomon/ |
| 5 | GitHub: Backblaze JavaReedSolomon | https://github.com/Backblaze/JavaReedSolomon |
| 6 | "A Tale of Two Erasure Codes in HDFS" (FAST '15) | https://www.usenix.org/conference/fast15/technical-sessions/presentation/xia |
| 7 | "Erasure Coding in Windows Azure Storage" (USENIX ATC '12) | https://www.usenix.org/conference/atc12/technical-sessions/presentation/huang |
| 8 | Plank, "Jerasure: Erasure Coding Library" | http://jerasure.org/ |
| 9 | Cauchy Matrix (Wikipedia) | https://en.wikipedia.org/wiki/Cauchy_matrix |
| 10 | Intel ISA-L (Intelligent Storage Acceleration Library) | https://github.com/intel/isa-l |

---

## Section 4: Consistent Hashing / Rendezvous Hashing

### 4.1 The Problem: Mapping Chunks to DOs

In Mossaic, we need a function:

```
f(file_id, chunk_index) → DO_id
```

This function must:
1. Be **deterministic** — any node computes the same answer without coordination
2. Provide **uniform distribution** — chunks spread evenly across DOs
3. Be **minimally disruptive** — adding/removing a DO moves as few chunks as possible
4. Support **k-of-n placement** — for erasure coding, each file's chunks go to different DOs

### 4.2 Classic Consistent Hashing

#### The Hash Ring (Karger et al., 1997)

```
                        0 / 2^32
                     ●──────────●
                  ╱                 ╲
               ╱                       ╲
            ●                             ●
        S3 ●                               ● S1
            ●                             ●
               ╲                       ╱
                  ╲                 ╱
                     ●──────────●
                         S2

    Keys hash to positions on the ring.
    Each key maps to the first server found clockwise.
```

**Algorithm:**
1. Hash each server to a position on a circular number space [0, 2^32)
2. Hash each key to the same space
3. Walk clockwise from the key's position to find the first server

```
key_position = hash(key) mod 2^32
server = first server clockwise from key_position
```

**Properties:**
- When a server is removed, only its keys move (to the next clockwise server)
- When a server is added, only keys between it and its predecessor move
- Expected keys moved when adding server to n servers: K/n (where K = total keys)

#### Virtual Nodes

Without virtual nodes, load balance is poor. With n servers and no virtual nodes,
the expected load on the most-loaded server is O(log n / n) of total load.

**Solution:** Map each physical server to V virtual positions on the ring:

```
for v in 0..V:
    position = hash(server_id + ":" + v) mod 2^32
    ring.insert(position, server_id)
```

With V = 150-200 virtual nodes per server, load variance drops to ~5-10%.

#### Complexity

```
┌────────────────────┬──────────────────────┐
│ Operation          │ Complexity           │
├────────────────────┼──────────────────────┤
│ Key lookup         │ O(log(n * V))        │
│ Add server         │ O(V * log(n * V))    │
│ Remove server      │ O(V * log(n * V))    │
│ Memory             │ O(n * V)             │
│ Keys moved on add  │ K / n (optimal)      │
└────────────────────┴──────────────────────┘
```

### 4.3 Rendezvous Hashing (HRW — Highest Random Weight)

Rendezvous hashing (Thaler & Ravishankar, 1996) takes a fundamentally different
approach: for each key, compute a score for every server and pick the highest.

#### Algorithm

```python
def assign(key, servers):
    best_server = None
    best_score  = -1
    for server in servers:
        score = hash(key + server.id)     # combined hash
        if score > best_score:
            best_score = score
            best_server = server
    return best_server
```

#### Visual

```
Key: "file_42:chunk_3"

    Server    hash(key, server)    Score
    ──────    ─────────────────    ─────
    DO_A      hash(key, "A")       0.31
    DO_B      hash(key, "B")       0.87  ← WINNER (highest)
    DO_C      hash(key, "C")       0.45
    DO_D      hash(key, "D")       0.12
    DO_E      hash(key, "E")       0.73

    → Chunk assigned to DO_B
```

#### Properties

1. **Perfect load balance** — each server equally likely to win (uniform hash)
2. **Minimal disruption** — removing server S only moves keys that were on S
3. **No metadata** — no ring, no virtual nodes, no storage
4. **Distributed k-agreement** — to pick k servers, take the top-k scores:

```python
def assign_k(key, servers, k):
    scored = [(hash(key, s.id), s) for s in servers]
    scored.sort(reverse=True)
    return [s for _, s in scored[:k]]
```

This is exactly what we need for erasure coding: pick k+m = n distinct DOs!

#### Minimal Disruption Math

When removing 1 server from n servers:
- Each key had a 1/n chance of being assigned to the removed server
- Only those keys move (to the server with the next-highest score)
- Expected keys moved: **K/n** — this is information-theoretically optimal

When adding 1 server to n servers:
- The new server "wins" for ~K/(n+1) keys
- Those keys are taken equally from all existing servers
- Expected keys moved: **K/(n+1)** — also optimal

#### Complexity

```
┌────────────────────┬────────────────┐
│ Operation          │ Complexity     │
├────────────────────┼────────────────┤
│ Key lookup         │ O(n)           │
│ Add server         │ O(n) per key   │
│ Remove server      │ O(n) per key   │
│ Memory             │ O(n) (server list) │
│ Keys moved on add  │ K/(n+1)        │
└────────────────────┴────────────────┘
```

The O(n) per lookup is fine for moderate n (< 1000 servers). For very large n,
hierarchical HRW achieves O(log n).

### 4.4 Jump Consistent Hash

Jump consistent hash (Lamping & Veach, Google, 2014) is an O(1) memory, O(log n) time algorithm with perfect
balance. It requires buckets to be numbered 0..n-1 (no arbitrary removal).

#### Algorithm (5 lines of code)

```c
int32_t JumpConsistentHash(uint64_t key, int32_t num_buckets) {
    int64_t b = -1, j = 0;
    while (j < num_buckets) {
        b = j;
        key = key * 2862933555777941757ULL + 1;
        j = (int64_t)((b + 1) * (double)(1LL << 31) /
                       (double)((key >> 33) + 1));
    }
    return (int32_t)b;
}
```

#### How It Works

The key insight: consider adding buckets one at a time. When going from n to n+1
buckets, each key should move to the new bucket with probability 1/(n+1).

```
Buckets:  1  →  2  →  3  →  4  →  5  ...

Key K:    0     0     2     2     4
          └─────┘     └─────┘     └── moved to bucket 4
          stayed      moved to 2

At each step n→n+1:
  P(key moves) = 1/(n+1)
  P(key stays) = n/(n+1)
```

The algorithm simulates this process efficiently by "jumping" over steps
where the key doesn't move, using a pseudo-random number generator seeded
by the key.

#### Properties

```
- Perfect balance: max load = 1/n * K (exactly uniform)
- O(log n) expected time (jumps skip most buckets)
- O(1) memory (no data structure needed)
- Monotone: adding a bucket only moves keys TO the new bucket

Limitation: buckets must be numbered 0..n-1
  - Cannot remove bucket 3 from {0,1,2,3,4} without renumbering
  - Only supports adding/removing the LAST bucket
  - Not suitable for arbitrary server failure
```

### 4.5 Multi-Probe Consistent Hashing

Multi-probe consistent hashing (Appleton & O'Reilly, Google, 2015) improves on classic
consistent hashing by **probing multiple hash positions** per key.

#### Algorithm

```
1. Each server gets exactly ONE position on the ring (no virtual nodes)
2. For each key, compute k probe positions:
     probe_i = hash(key, i) for i in 0..k-1
3. For each probe, find the next clockwise server
4. The server found most frequently (or nearest among probes) wins
```

#### Balance vs. Probes

```
┌────────────┬──────────────────┐
│ # Probes   │ Peak-to-avg load │
├────────────┼──────────────────┤
│ 1          │ O(log n)         │
│ 2          │ ~2.20            │
│ 3          │ ~1.64            │
│ 7          │ ~1.28            │
│ 21         │ ~1.05            │
└────────────┴──────────────────┘
```

**Tradeoff:** More probes = better balance but more computation. With 21 probes,
load is within 5% of perfect, using O(1) memory per server (vs O(V) for virtual
nodes).

### 4.6 Mapping (file_id, chunk_index) to DO_id

For Mossaic, we need to map composite keys to DOs. Here's how each scheme handles
the specific requirement of placing n = k + m chunks for one file onto n distinct DOs.

#### Using Rendezvous Hashing (Recommended)

```python
def place_file_chunks(file_id, k, m, all_dos):
    """Assign n = k+m chunks to distinct DOs using HRW."""
    n = k + m
    composite_key = file_id   # use file_id, NOT per-chunk keys

    # Score every DO for this file
    scores = []
    for do in all_dos:
        score = hash(file_id + do.id)
        scores.append((score, do))

    # Sort by score descending, take top n
    scores.sort(reverse=True)
    selected_dos = [do for _, do in scores[:n]]

    # Assignment:
    # selected_dos[0..k-1]   → data chunks 0..k-1
    # selected_dos[k..n-1]   → parity chunks 0..m-1
    return selected_dos
```

```
Key insight: use the FILE ID (not chunk index) as the HRW key.
The top-n DOs for that file become the placement group.
Chunk i always goes to selected_dos[i].

This gives us:
  ✓ Deterministic: any node computes the same placement
  ✓ Distinct DOs: the top-n are guaranteed distinct
  ✓ Uniform: each DO equally likely to be in any file's group
  ✓ Minimal disruption: removing a DO only affects files on that DO
```

#### Using Consistent Hashing

```
1. Place all DOs on ring (with virtual nodes)
2. Hash the file_id to ring position P
3. Walk clockwise, collecting n distinct physical DOs
4. Assign data chunk i to the i-th distinct DO found
```

This works but is more complex and doesn't give as clean a k-selection
as HRW.

#### Using Jump Hash

Not directly suitable because:
- Requires sequential bucket IDs (DOs have arbitrary IDs)
- Cannot handle DO removal without renumbering
- Would need a wrapper layer that adds complexity

### 4.7 Adding / Removing DOs: Disruption Analysis

```
Scenario: N = 100 DOs, each file uses n = 6 DOs, 10,000 files

Adding 1 DO (N: 100 → 101):
┌─────────────────────┬──────────┬──────────────┬──────────────┐
│ Scheme              │ Files    │ Chunks moved │ % disruption │
│                     │ affected │              │              │
├─────────────────────┼──────────┼──────────────┼──────────────┤
│ Rendezvous (HRW)    │ ~594     │ ~594         │ 5.9%         │
│ Consistent hash     │ ~594     │ ~594         │ 5.9%         │
│ Jump hash           │ ~594     │ ~594         │ 5.9%         │
│ Simple modulo       │ ~9,900   │ ~50,000      │ 99%          │
└─────────────────────┴──────────┴──────────────┴──────────────┘

All three consistent schemes move ~K/(N+1) keys ≈ 10000/101 ≈ 99 files
that have the new DO in their top-6. Each affected file moves 1 chunk.
Total: ~99 × 6 possible, but only ~594 chunk-level remappings.

Removing 1 DO (N: 100 → 99):
  - Files on removed DO: ~10000 × 6/100 = ~600 files affected
  - Each loses 1 chunk, which remaps to the next-best DO
  - Triggers 1 erasure-code repair per affected file
```

### 4.8 Comparison Table

```
┌──────────────────────┬────────────┬────────────┬────────────┬────────────────┐
│ Property             │ Consistent │ Rendezvous │ Jump Hash  │ Multi-Probe CH │
│                      │ Hash+Vnodes│ (HRW)      │ (Google)   │ (Google 2015)  │
├──────────────────────┼────────────┼────────────┼────────────┼────────────────┤
│ Lookup time          │ O(log nV)  │ O(n)       │ O(log n)   │ O(k log n)     │
│ Memory               │ O(nV)      │ O(n)       │ O(1)       │ O(n)           │
│ Balance (no tuning)  │ Poor       │ Perfect    │ Perfect    │ Tunable        │
│ Balance (with tuning)│ Good (V≥150│ Perfect    │ Perfect    │ Good (k≥21)    │
│ Add node             │ K/n moved  │ K/n moved  │ K/n moved  │ K/n moved      │
│ Remove arbitrary node│ Yes        │ Yes        │ NO *       │ Yes            │
│ Select k-of-n        │ Complex    │ Trivial ** │ N/A        │ Complex        │
│ Implementation       │ Moderate   │ Simple     │ Trivial    │ Moderate       │
│ Weighted nodes       │ Via vnodes │ Via log    │ Not native │ Via positions  │
│ Metadata             │ Ring+BST   │ None       │ None       │ Ring           │
├──────────────────────┼────────────┼────────────┼────────────┼────────────────┤
│ Best for Mossaic?    │ No         │ YES ✓      │ No         │ Maybe          │
└──────────────────────┴────────────┴────────────┴────────────┴────────────────┘

*  Jump hash only supports appending/removing the last bucket
** HRW: just take top-k scores — perfect for erasure coding placement
```

### 4.9 Mossaic Recommendation: Rendezvous Hashing (HRW)

#### Why HRW is the best fit for Mossaic

```
1. ERASURE CODING SYNERGY
   HRW naturally selects the top-n DOs for a file — exactly what
   RS(6,4) needs. No second algorithm needed for placement groups.

2. ZERO METADATA
   No ring, no virtual nodes, no BST. The DO list is the only state.
   Perfect for a system where DOs are managed by Cloudflare.

3. SIMPLICITY
   ~20 lines of code. Easy to audit, test, and understand.
   Critical for a system built on Cloudflare Workers (limited runtime).

4. PERFECT BALANCE
   Each DO equally likely to be in any file's placement group.
   No tuning parameters (unlike virtual node count).

5. GRACEFUL DO CHANGES
   When a DO is added/removed, only affected files remap.
   The "next best" DO is always well-defined.

6. O(n) IS FINE
   For Mossaic, n = number of DOs ≈ 50-500.
   Computing 500 hashes is <1ms on a Worker.
   No need for O(log n) — the constant factor matters more.
```

#### Recommended Implementation

```typescript
// Mossaic chunk placement using Rendezvous Hashing

interface DONode {
  id: string;     // Durable Object ID
  weight: number; // Capacity weight (default 1.0)
}

function hashScore(fileId: string, doId: string): number {
  // Use a fast, well-distributed hash (e.g., xxHash, MurmurHash3)
  const combined = `${fileId}:${doId}`;
  return murmur3_128(combined);  // 128-bit hash as float in (0,1)
}

function weightedScore(fileId: string, node: DONode): number {
  const score = hashScore(fileId, node.id);
  // Weighted HRW: -weight / ln(score)
  // This ensures P(node wins) ∝ node.weight
  return -node.weight / Math.log(score);
}

function placeFileChunks(
  fileId: string,
  k: number,        // data chunks (e.g., 4)
  m: number,        // parity chunks (e.g., 2)
  allDOs: DONode[]  // all available DOs
): DONode[] {
  const n = k + m;

  // Score all DOs for this file
  const scored = allDOs.map(node => ({
    node,
    score: weightedScore(fileId, node)
  }));

  // Sort descending by score
  scored.sort((a, b) => b.score - a.score);

  // Top n DOs form the placement group
  return scored.slice(0, n).map(s => s.node);
  // [0..k-1] → data chunks
  // [k..n-1] → parity chunks
}

// Lookup: which DO has chunk i of file F?
function lookupChunkDO(
  fileId: string,
  chunkIndex: number,
  k: number,
  m: number,
  allDOs: DONode[]
): DONode {
  const placement = placeFileChunks(fileId, k, m, allDOs);
  return placement[chunkIndex];
}
```

#### Weighted HRW for Heterogeneous DOs

If some DOs have more capacity:

```
Standard HRW:  score(file, DO) = hash(file, DO)
               → each DO equally likely to win

Weighted HRW:  score(file, DO) = -weight / ln(hash(file, DO))
               → P(DO wins) ∝ DO.weight

Example: DO_A has weight=2, DO_B has weight=1
  → DO_A receives ~2x more file assignments than DO_B

The logarithmic transform (Schindelhauer & Schomaker, 2005) ensures:
  - Changing one node's weight doesn't affect other nodes' assignments
  - Only the minimal number of keys move when weights change
```

### Cross-Topic Integration: Erasure Coding + Chunk Placement

The erasure coding and hashing topics combine naturally in Mossaic:

```
┌─────────────────────────────────────────────────────────────┐
│                    FILE UPLOAD FLOW                          │
│                                                             │
│  1. Client uploads file F                                   │
│  2. Split F into k=4 data chunks: D0, D1, D2, D3           │
│  3. Compute m=2 parity chunks: P0, P1  (Cauchy RS)         │
│  4. Use HRW to select 6 DOs:                               │
│       placement = HRW(file_id, all_DOs, n=6)               │
│  5. Store:                                                  │
│       placement[0] ← D0    placement[3] ← D3               │
│       placement[1] ← D1    placement[4] ← P0               │
│       placement[2] ← D2    placement[5] ← P1               │
│                                                             │
│                    FILE READ FLOW                            │
│                                                             │
│  1. Compute placement = HRW(file_id, all_DOs, n=6)         │
│  2. Request all 6 chunks in parallel                        │
│  3. First 4 responses arrive → reconstruct immediately      │
│     (if all 4 data chunks arrive, no RS decode needed)      │
│  4. If <4 data chunks, use RS decode with any 4 of 6       │
│                                                             │
│                    DO FAILURE FLOW                           │
│                                                             │
│  1. DO_X goes down                                          │
│  2. For each file with a chunk on DO_X:                     │
│     a. Recompute placement without DO_X                     │
│     b. The chunk slides to the next-best DO (HRW property)  │
│     c. Read 4 surviving chunks → RS decode → re-encode      │
│     d. Store repaired chunk on the new DO                   │
└─────────────────────────────────────────────────────────────┘
```

This architecture gives Mossaic:
- **1.5x storage overhead** (vs 3x for replication)
- **Tolerance of 2 simultaneous DO failures** per file
- **No central metadata** for chunk placement (fully deterministic)
- **Tail-latency hedging** (read 6 in parallel, need any 4)
- **Minimal repair disruption** when DOs change

### References (Hashing)

| # | Reference | URL |
|---|-----------|-----|
| 1 | Karger et al., "Consistent Hashing and Random Trees" (STOC 1997) | https://doi.org/10.1145/258533.258660 |
| 2 | Thaler & Ravishankar, "A Name-Based Mapping Scheme for Rendezvous" (1996) | http://www.eecs.umich.edu/techreports/cse/96/CSE-TR-316-96.pdf |
| 3 | Wikipedia: Consistent Hashing | https://en.wikipedia.org/wiki/Consistent_hashing |
| 4 | Wikipedia: Rendezvous Hashing | https://en.wikipedia.org/wiki/Rendezvous_hashing |
| 5 | Lamping & Veach, "A Fast, Minimal Memory, Consistent Hash Algorithm" (2014) | https://arxiv.org/abs/1406.2294 |
| 6 | Appleton & O'Reilly, "Multi-Probe Consistent Hashing" (2015) | https://arxiv.org/abs/1505.00062 |
| 7 | Schindelhauer & Schomaker, "Weighted Distributed Hash Tables" (2005) | https://doi.org/10.1145/1073814.1073836 |
| 8 | Amazon Dynamo Paper, "Dynamo: Amazon's Highly Available Key-value Store" | https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf |
| 9 | DeCandia et al., "Dynamo" (SOSP 2007) | https://doi.org/10.1145/1323293.1294281 |
| 10 | GitHub Load Balancer (uses Rendezvous Hashing) | https://github.blog/2016-09-22-introducing-glb/ |

---

## Section 5: Content-Defined Chunking

### 5.1 Fixed-Size vs Variable-Size Chunking

#### Fixed-Size Chunking (FSC)

The simplest approach: split data into equal-sized blocks at regular byte offsets.

```
File (16 bytes):  [A B C D | E F G H | I J K L | M N O P]
                  chunk 0    chunk 1    chunk 2    chunk 3
                  (4 bytes each)
```

**Advantages:**
- Trivially simple to implement — just divide at every N-th byte
- Predictable, uniform chunk sizes
- No hash computation overhead — O(1) per boundary
- Easy to calculate offsets for random access: `chunk_index = byte_offset / chunk_size`

**Disadvantages:**
- Catastrophic deduplication failure on insertions/deletions (see §5.2)
- All chunk boundaries shift when a single byte is inserted at the beginning
- No content awareness — identical data at different offsets produces different chunks

#### Variable-Size / Content-Defined Chunking (CDC)

Chunk boundaries are determined by the **content itself**, using a rolling hash over a sliding window. When the hash matches a predetermined condition, a boundary is declared.

```
File:  [A B C D E|F G H I J K L|M N O P Q R|S T U]
       chunk 0    chunk 1        chunk 2      chunk 3
       (5 bytes)  (7 bytes)      (6 bytes)    (3 bytes)
```

**Advantages:**
- Robust to insertions, deletions, and modifications (boundaries self-heal)
- High deduplication ratio across file versions
- Inter-file deduplication: identical regions in different files produce identical chunks
- Critical for backup systems (restic, Borg), sync tools (rsync), and storage dedup

**Disadvantages:**
- Computational overhead from rolling hash (mitigated by Gear hash)
- Variable chunk sizes require metadata to track boundaries
- Chunk size variance can complicate storage allocation
- More complex implementation

#### Comparison Table

| Property                  | Fixed-Size Chunking | Content-Defined Chunking |
|---------------------------|--------------------:|-------------------------:|
| Throughput                | ~10 GB/s+           | 300 MB/s–1.2 GB/s (FastCDC) |
| Dedup ratio (1-byte edit) | ~0% (all chunks change) | ~95–99% (only affected chunks change) |
| Implementation complexity | Trivial             | Moderate                 |
| Chunk size variance       | Zero                | Controlled by min/max    |
| Random access             | O(1) by offset      | Requires index lookup    |
| Use cases                 | Block devices, RAID | Backup, sync, dedup, CAS |

### 5.2 The Content Shift Problem

This is the fundamental motivation for CDC. Consider a file split into fixed 8-byte chunks:

```
ORIGINAL FILE:
Bytes:    [A B C D E F G H | I J K L M N O P | Q R S T U V W X]
Chunks:    chunk_0 (8B)       chunk_1 (8B)       chunk_2 (8B)

INSERT byte 'Z' at position 0:
Bytes:    [Z A B C D E F G | H I J K L M N O | P Q R S T U V W | X]
Chunks:    chunk_0' (8B)      chunk_1' (8B)      chunk_2' (8B)    chunk_3'

Result: ALL chunks are different! Zero deduplication despite only 1 byte changing.
```

With CDC, boundaries depend on local content, not global offsets:

```
ORIGINAL FILE (CDC boundaries marked by content-dependent hash match):
Bytes:    [A B C D E|F G H I J K L|M N O P Q R|S T U V W X]
Chunks:    chunk_0    chunk_1        chunk_2      chunk_3

INSERT byte 'Z' at position 0:
Bytes:    [Z A B C D E|F G H I J K L|M N O P Q R|S T U V W X]
Chunks:    chunk_0'     chunk_1        chunk_2      chunk_3
                        ^^ SAME ^^     ^^ SAME ^^   ^^ SAME ^^

Result: Only chunk_0 changes! The rest are identical → high deduplication.
```

**Why this works:** CDC boundaries are determined by the content within the rolling hash window. Once the modified region passes out of the window (typically 32–64 bytes), the hash values reconverge to the same sequence, producing identical boundary positions relative to the local content.

The "resynchronization distance" after a modification is bounded by:
- **Minimum chunk size** (if enforced), plus
- **Window size** of the rolling hash (typically 32–64 bytes)

In practice, a single-byte insertion only invalidates 1–2 chunks.

### 5.3 Rolling Hash Functions

A **rolling hash** computes a hash over a sliding window of bytes. The key property is that when the window shifts by one byte, the new hash can be computed incrementally in O(1) from the previous hash, rather than recomputing from scratch.

#### General Framework

```
Window of size w slides through the data byte-by-byte:

Data:     ... b[i-w] b[i-w+1] ... b[i-1] b[i] b[i+1] ...
               \_________________________/
                    window at position i

Hash update:
  H(i+1) = f(H(i), b[i-w], b[i+1])
            ^^^^   ^^^^^^^^  ^^^^^^^
            old    byte out  byte in
```

All rolling hashes support this O(1) update. They differ in:
- **Mathematical basis** (polynomial, cyclic, lookup-table)
- **Collision resistance** (uniformity of distribution)
- **Computational cost** (multiplications vs. XOR/shift)
- **Window size** requirements

### 5.4 Rabin Fingerprinting

#### Mathematical Foundation

Rabin fingerprinting treats the input as a polynomial over **GF(2)** (the Galois field with elements {0, 1}), where arithmetic is performed modulo an irreducible polynomial `p(x)` of degree `k`.

Given a byte sequence `b[0], b[1], ..., b[n-1]`, interpret the entire bit stream as a polynomial:

```
M(x) = m_{n-1} · x^{n-1} + m_{n-2} · x^{n-2} + ... + m_1 · x + m_0
```

where `m_i` are individual bits. The Rabin fingerprint is:

```
R(M) = M(x) mod p(x)     over GF(2)
```

#### Rolling Update Formula

For a window of `w` bytes sliding through the data:

```
fingerprint(b[i+1..i+w]) = 
    ( fingerprint(b[i..i+w-1]) · x    -- shift left (multiply by x)
      ⊕ b[i+w]                         -- XOR in new byte
      ⊕ b[i] · x^w                     -- XOR out old byte
    ) mod p(x)
```

All operations are in GF(2): addition is XOR, multiplication is shift-and-XOR.

#### Boundary Determination

After computing the fingerprint `F` at each byte position, a chunk boundary is declared when:

```
F mod D == r
```

where:
- `D` = divisor (determines average chunk size; `D = 2^n` for power-of-2 sizes)
- `r` = a fixed target remainder (often `r = 0`, i.e., check lowest bits)

Equivalently, using bit masking:

```
if (fingerprint & MASK) == 0:
    declare chunk boundary
```

where `MASK = D - 1` (e.g., `MASK = 0x1FFF` for 8 KB average chunks, since 2^13 = 8192).

#### Properties

- **Window size**: Typically 48–64 bytes (restic uses 64 bytes)
- **Polynomial degree**: 53–64 bits for the irreducible polynomial
- **Throughput**: ~200–400 MB/s (limited by GF(2) polynomial division)
- **Distribution**: Provably uniform given a random irreducible polynomial
- **Chunk size range**: restic uses 512 KiB–8 MiB with Rabin fingerprinting

#### Example: restic's Implementation

```
Window size:  64 bytes
Hash bits:    64-bit Rabin fingerprint
Boundary:     lowest 21 bits == 0  →  average chunk ~2 MiB
Min chunk:    512 KiB
Max chunk:    8 MiB
```

### 5.5 Buzhash and Other Rolling Hash Alternatives

#### Buzhash (Cyclic Polynomial Hashing)

Buzhash avoids multiplications entirely, using only **bitwise rotation (circular shift)** and **XOR**. It relies on a pre-computed substitution table `S[]` mapping byte values to random L-bit integers.

**Hash definition over window `b[0..w-1]`:**

```
H = rot^{w-1}(S[b[0]]) ⊕ rot^{w-2}(S[b[1]]) ⊕ ... ⊕ rot(S[b[w-2]]) ⊕ S[b[w-1]]
```

where `rot^k` means circular left-shift by `k` bits.

**Rolling update (sliding window right by one byte):**

```
H_new = rot(H_old) ⊕ rot^w(S[b_out]) ⊕ S[b_in]
```

- `rot(H_old)` — rotate old hash left by 1
- `rot^w(S[b_out])` — remove contribution of exiting byte
- `S[b_in]` — add contribution of entering byte

**Properties:**
- Only XOR and bit-rotation — extremely fast on modern CPUs
- Strongly universal (pairwise independent)
- No multiplication or modular arithmetic
- Borg backup uses Buzhash with 4095-byte window, 512 KiB–8 MiB chunk range

#### Gear Hash (Used by FastCDC)

The simplest and fastest rolling hash for CDC purposes. Uses a pre-computed random lookup table `Gear[256]` of 64-bit values.

**Hash computation (no explicit window):**

```
fp = (fp << 1) + Gear[byte]
```

That's it — one shift and one addition per byte. No window tracking, no byte removal.

**Key insight:** Because the hash uses a left-shift, older bytes naturally lose influence as they shift out of the 64-bit register. The effective window is implicitly ~64 bytes wide.

**Properties:**
- ~10x faster than Rabin fingerprinting
- No explicit window management
- Uniform distribution of hash values
- Used by FastCDC, achieving 1+ GB/s chunking throughput

#### Comparison of Rolling Hash Functions

| Hash Function        | Operations/byte       | Throughput     | Window     | Used By         |
|---------------------:|----------------------:|---------------:|-----------:|----------------:|
| Rabin Fingerprint    | Shift, XOR, mod p(x) | ~200–400 MB/s  | Explicit   | restic, LBFS    |
| Buzhash              | Rotate, XOR           | ~400–600 MB/s  | Explicit   | Borg, attic     |
| Gear Hash            | Shift, Add            | ~800–1200 MB/s | Implicit   | FastCDC         |
| Adler-32 (moving sum)| Add, Subtract, Mod    | ~500 MB/s      | Explicit   | rsync           |

### 5.6 FastCDC Algorithm

#### Overview

**FastCDC** (Wen Xia et al., USENIX ATC 2016) is the state-of-the-art content-defined chunking algorithm. It achieves ~10x higher throughput than Rabin-based CDC while maintaining equivalent deduplication ratios.

#### Three Key Innovations

**1. Gear-Based Rolling Hash**

Instead of Rabin fingerprinting (requires polynomial division over GF(2)), FastCDC uses the Gear hash:

```
fp = (fp << 1) + Gear[src[i]]
```

- `Gear[256]` is a pre-computed table of 256 random 64-bit values
- One left-shift + one addition per byte
- No modular arithmetic, no window management
- Implicit window: bits naturally fall off the left side of the 64-bit register

**2. Normalized Chunking (Adaptive Hash Judgment)**

**Problem:** Standard CDC produces a geometric (exponential) distribution of chunk sizes. Many chunks are much smaller or larger than the expected average, wasting storage metadata on tiny chunks and reducing deduplication on huge chunks.

**Solution:** FastCDC uses **two different masks** to bias the distribution toward the desired average:

```
Given: expected average chunk size = A
       minimum chunk size = MinSize
       maximum chunk size = MaxSize

Phase 1 (MinSize → A): Use a HARDER mask (more bits must match)
    MaskS — has more '1' bits → fewer boundaries → chunks grow longer

Phase 2 (A → MaxSize): Use an EASIER mask (fewer bits must match)
    MaskL — has fewer '1' bits → more boundaries → chunks end sooner
```

**ASCII Diagram — Normalized vs Standard CDC chunk size distribution:**

```
Probability
  |
  |  *                           Standard CDC (exponential/geometric)
  |   *
  |    **
  |      ***
  |         ******
  |               *****************
  +-----|---------|---------|--------→ Chunk Size
       Min    Expected    Max

Probability
  |
  |         ****
  |        *    *                    Normalized CDC (FastCDC)
  |       *      *                   (more uniform, peaks near expected)
  |      *        **
  |    **           ***
  |  **                ******
  +-----|---------|---------|--------→ Chunk Size
       Min    Expected    Max
```

The normalized approach produces chunks that cluster more tightly around the expected average, yielding:
- More uniform chunk sizes (better storage allocation)
- Higher deduplication ratio (more consistent boundaries)

**3. Minimum Chunk Skip (Cut-Point Skipping)**

FastCDC skips hash computation entirely for the first `MinSize` bytes of each chunk. Since no boundary can occur before `MinSize`, there's no point computing the hash there.

```
Data: |<-- MinSize (skip) -->|<-- hash and check -->|
      |  no hash computed    | fp = (fp<<1)+Gear[b] |
      |  i increments only   | check (fp & Mask)==0  |
```

This reduces the number of hash operations by `MinSize / ExpectedSize` fraction (e.g., if MinSize = ExpectedSize/4, saves ~25% of hash operations).

#### FastCDC Pseudocode (Basic Version)

```
algorithm FastCDC(src, n):
    MinSize ← 2 KB            // minimum chunk size
    MaxSize ← 64 KB           // maximum chunk size
    Mask   ← 0x0000d93003530000  // bit mask for boundary detection

    fp ← 0                    // fingerprint accumulator
    i  ← 0                    // byte index

    if n ≤ MinSize: return n  // data smaller than minimum → one chunk

    if n ≥ MaxSize: n ← MaxSize  // cap at maximum

    // Phase 1: Skip first MinSize bytes (no boundary possible)
    while i < MinSize:
        fp ← (fp << 1) + Gear[src[i]]
        i ← i + 1

    // Phase 2: Scan for boundary
    while i < n:
        fp ← (fp << 1) + Gear[src[i]]
        if !(fp & Mask):      // if masked bits are all zero
            return i           // declare boundary
        i ← i + 1

    return i                   // hit MaxSize → forced boundary
```

#### FastCDC with Normalized Chunking (Full Version)

```
algorithm FastCDC_Normalized(src, n):
    MinSize  ← expected_size / 4
    MaxSize  ← expected_size * 8
    NormSize ← expected_size     // the transition point

    MaskS ← harder_mask          // more bits → fewer matches (for small region)
    MaskL ← easier_mask          // fewer bits → more matches (for large region)

    fp ← 0
    i  ← 0

    if n ≤ MinSize: return n
    if n ≥ MaxSize: n ← MaxSize

    // Skip MinSize
    while i < MinSize:
        fp ← (fp << 1) + Gear[src[i]]
        i ← i + 1

    // Region 1: MinSize → NormSize — use HARDER mask (grow chunks)
    barrier ← min(NormSize, n)
    while i < barrier:
        fp ← (fp << 1) + Gear[src[i]]
        if !(fp & MaskS):
            return i
        i ← i + 1

    // Region 2: NormSize → MaxSize — use EASIER mask (encourage boundaries)
    while i < n:
        fp ← (fp << 1) + Gear[src[i]]
        if !(fp & MaskL):
            return i
        i ← i + 1

    return i
```

#### Mask Design

For an expected average chunk size of `A = 2^n`:

| Mask     | Bits set | Probability of match | Purpose              |
|---------:|---------:|---------------------:|---------------------:|
| MaskS    | n + 1    | 1/2^(n+1)           | Harder — grow chunks |
| MaskL    | n - 1    | 1/2^(n-1)           | Easier — end chunks  |

The combination produces a distribution that peaks near the expected size `A` rather than the exponential decay of standard CDC.

### 5.7 The Math: Expected Chunk Size, Min/Max, Bit-Masking

#### Expected Chunk Size from Bit Masking

The fundamental equation of CDC chunk sizing:

```
Expected chunk size = 1 / P(boundary)
```

If we declare a boundary when `(hash & MASK) == 0`, and the hash is uniformly distributed:

```
P(boundary at any byte) = 1 / 2^k

where k = number of '1' bits in MASK (popcount(MASK))

Therefore:
    E[chunk_size] = 2^k bytes
```

**Examples:**

| Bits in mask (k) | E[chunk_size] | Practical use            |
|-----------------:|--------------:|-------------------------:|
| 10               | 1 KB          | Fine-grained dedup       |
| 12               | 4 KB          | Block-level dedup        |
| 13               | 8 KB          | General purpose (FastCDC default) |
| 16               | 64 KB         | Larger files             |
| 20               | 1 MB          | Very large files         |
| 21               | 2 MB          | restic default           |

#### Chunk Size Distribution (Standard CDC)

Without min/max constraints, standard CDC produces a **geometric distribution**:

```
P(chunk_size = s) = (1 - p)^{s-1} · p

where p = 1/2^k = probability of boundary at each byte

Mean:     E[S] = 1/p = 2^k
Variance: Var[S] = (1-p)/p^2 ≈ 1/p^2 = 2^{2k}
Std Dev:  σ ≈ 1/p = E[S]

Coefficient of Variation = σ/μ ≈ 1 (very high variance!)
```

This means the standard deviation equals the mean — chunks vary wildly. A 4 KB expected size produces chunks from 1 byte to 100+ KB regularly.

#### Min/Max Constraints

To control variance, CDC implementations enforce:

```
MinSize ≤ chunk_size ≤ MaxSize
```

**Implementation:**
- **MinSize**: Skip first MinSize bytes (don't check for boundaries)
- **MaxSize**: Force a boundary at MaxSize if none found

Common ratios:
```
MinSize = E[chunk_size] / 4    (or E/2)
MaxSize = E[chunk_size] * 4    (or E * 8)
```

**Effect on actual average:**

With min/max constraints, the actual average chunk size shifts slightly from the theoretical `2^k`:

```
E_actual ≈ MinSize + (E_theoretical - MinSize) · (1 - e^{-(MaxSize-MinSize)/E_theoretical})
```

In practice, the shift is small when `MaxSize >> E_theoretical >> MinSize`.

#### Normalized Chunking Math (FastCDC)

FastCDC's two-mask approach modifies the expected size calculation:

```
Region 1 (MinSize → NormSize): P₁ = 1/2^{k+1}   (harder mask, fewer boundaries)
Region 2 (NormSize → MaxSize): P₂ = 1/2^{k-1}   (easier mask, more boundaries)
```

The probability of a chunk ending at size `s`:

```
For MinSize < s ≤ NormSize:
    P(S = s) = (1 - P₁)^{s - MinSize - 1} · P₁

For NormSize < s ≤ MaxSize:
    P(S = s) = (1 - P₁)^{NormSize - MinSize} · (1 - P₂)^{s - NormSize - 1} · P₂
```

This creates a distribution that peaks near `NormSize` (the expected average), rather than decaying exponentially from `MinSize`.

#### Bit-Mask Construction

Masks are typically constructed with `k` bits set in specific (non-contiguous) positions for better hash distribution:

```
For 8 KB average (k=13):
    MaskS (k+1=14 bits): 0x0003590703530000  // 14 bits set
    MaskL (k-1=12 bits): 0x0000d90003530000  // 12 bits set

For 16 KB average (k=14):
    MaskS (15 bits):     0x0003590703530000
    MaskL (13 bits):     0x0000d90003530000
```

The bits are chosen to be non-contiguous and spread across the 64-bit hash value to minimize correlation effects.

### 5.8 Deduplication Ratios: CDC vs Fixed-Size

#### Theoretical Analysis

**Fixed-size chunking after a single-byte insertion at position 0:**

```
Number of changed chunks = ceil(file_size / chunk_size)  // ALL chunks
Dedup ratio = 0%
```

**CDC after a single-byte insertion at position 0:**

```
Number of changed chunks = 1–2 (the chunk containing the insertion + possibly next)
Dedup ratio ≈ 1 - 2/N  where N = total chunks
For a 100 MB file with 8 KB chunks:  N ≈ 12,800
Dedup ratio ≈ 99.98%
```

#### Empirical Results (from FastCDC paper and related work)

| Scenario                      | Fixed-Size Dedup | CDC Dedup  | Notes                        |
|-------------------------------|:----------------:|:----------:|------------------------------|
| Identical file                | 100%             | 100%       | Both perfect                 |
| 1 byte inserted at start     | ~0%              | ~99.9%     | FSC catastrophic failure     |
| 1 byte modified in middle    | ~50%             | ~99.9%     | FSC: all chunks after change |
| 10% random modifications     | ~10%             | ~85–90%    | CDC degrades gracefully      |
| File appended (10% growth)   | ~90%             | ~90–95%    | FSC okay for append-only     |
| File copied & renamed        | 100%             | 100%       | Both detect identical data   |
| Concat(file, "foo", file)    | ~50%             | ~95%+      | CDC handles shifted copies   |

#### Deduplication Ratio Formula

```
Dedup Ratio = 1 - (unique_chunks / total_chunks)

    or equivalently:

Dedup Ratio = 1 - (new_storage_needed / total_data_size)
```

For CDC with expected chunk size `E` and a modification of `m` bytes:

```
Expected new chunks ≈ ceil(m / E) + 2  (modification spans + boundary disruption)
Dedup ratio ≈ 1 - (ceil(m/E) + 2) / (file_size/E)
            = 1 - (m + 2E) / file_size
```

#### Real-World Deduplication Performance

From the FastCDC paper (USENIX ATC '16), testing on Linux kernel source tarballs:

```
Dataset: Linux kernel source tarballs (versions 3.0–3.7)
         ~95 MB per tarball, 8 versions

CDC dedup ratio:    87.3% (with 8 KB average chunk)
FSC dedup ratio:    81.2% (with 8 KB fixed chunks)
Difference:         CDC saves ~6% more storage

For web server VM images (with frequent small changes):
CDC dedup ratio:    92.1%
FSC dedup ratio:    67.4%
Difference:         CDC saves ~25% more storage
```

#### Empirical Benchmarks: Dedup Ratio vs Chunk Size (ronomon/deduplication)

Real-world measurements from the `ronomon/deduplication` FastCDC implementation (Intel Xeon E3-1230 V2 @ 3.30GHz, 64 x 4 MB files with synthetic modifications):

```
┌──────────────┬────────────┬───────────────┬───────────────────────┐
│ Avg Chunk    │ Dedup      │ JS Throughput │ Native (C++) Throughput│
│ Size         │ Ratio      │ (MB/s)        │ (MB/s)                │
├──────────────┼────────────┼───────────────┼───────────────────────┤
│   2 KB       │  97.96%    │   160         │  1,029                │
│   4 KB       │  97.40%    │   193         │  1,074                │
│   8 KB       │  96.75%    │   211         │  1,087                │
│  16 KB       │  95.19%    │   223         │  1,091                │
│  32 KB       │  89.73%    │   228         │  1,105                │
│  64 KB       │  84.11%    │   230         │  1,100                │
│ 128 KB       │  75.44%    │   233         │  1,100                │
└──────────────┴────────────┴───────────────┴───────────────────────┘

Note: Native throughput includes SHA-256 hashing overhead.
      JS throughput is for the pure JavaScript reference implementation.
```

**Key takeaways:**
- Dedup ratio degrades roughly linearly with log₂(chunk_size)
- Throughput is essentially constant for native C++ (dominated by SHA-256)
- JS reference achieves 160–233 MB/s — comparable to Cloudflare Workers performance
- **Sweet spot for Mossaic: 64–128 KB** — 75–84% dedup with manageable metadata

### 5.9 Practical Chunk Size Ranges

#### Tradeoff Space

```
                     Metadata Overhead
                           ▲
                           │
                    High   │  ●  256B chunks
                           │    ● 1KB
                           │      ● 4KB
                           │        ● 8KB (sweet spot for dedup)
                           │          ● 16KB
                           │            ● 64KB
                           │              ● 256KB
                           │                ● 1MB (sweet spot for large files)
                    Low    │                  ● 8MB
                           └──────────────────────────────────→
                          High                              Low
                                    Dedup Ratio
```

**Smaller chunks → higher dedup ratio but more metadata overhead**
**Larger chunks → lower metadata overhead but lower dedup ratio**

#### Recommended Ranges by Workload

| Workload               | Expected Chunk Size | Min      | Max      | Rationale                      |
|:-----------------------|:-------------------:|:--------:|:--------:|:-------------------------------|
| Source code / text     | 4–8 KB              | 1 KB     | 32 KB    | Small files, line-level changes |
| Documents (Office/PDF) | 8–32 KB             | 2 KB     | 128 KB   | Mixed binary/text content      |
| VM/container images    | 64–256 KB           | 16 KB    | 1 MB     | Large files, block-level changes |
| **Photos (JPEG/HEIC)** | **64–256 KB**       | **16 KB** | **1 MB** | **Most photos are 2–20 MB**    |
| Video files            | 256 KB–1 MB         | 64 KB    | 4 MB     | Very large, sequential access  |
| Database backups       | 64–256 KB           | 16 KB    | 1 MB     | Record-level changes           |
| General blob storage   | 64–128 KB           | 16 KB    | 512 KB   | Good balance for mixed content |
| Backup systems (restic)| ~1–2 MB             | 512 KB   | 8 MB     | Very large datasets            |

#### Chunk Size vs Metadata Overhead

For a 10 MB file:

| Avg Chunk Size | ~Number of Chunks | Metadata (32B/chunk) | Overhead |
|:--------------:|:-----------------:|:--------------------:|:--------:|
| 4 KB           | 2,560             | 80 KB                | 0.8%     |
| 8 KB           | 1,280             | 40 KB                | 0.4%     |
| 64 KB          | 160               | 5 KB                 | 0.05%    |
| 256 KB         | 40                | 1.25 KB              | 0.01%    |
| 1 MB           | 10                | 320 B                | 0.003%   |

Metadata per chunk typically includes: content hash (32B SHA-256), offset, length, and chunk ID.

#### Photo/Blob Specific Analysis

For Mossaic's photo storage use case:

```
Typical photo sizes:
  - Smartphone JPEG:    3–8 MB
  - DSLR RAW:          25–60 MB
  - HEIC/HEIF:         1–4 MB
  - Edited/export:     5–30 MB

With 64 KB average chunks:
  - 5 MB photo  →  ~80 chunks
  - 20 MB photo → ~320 chunks
  - Metadata overhead: ~0.05%
  - Good dedup when same photo stored at multiple resolutions
  - Good dedup when metadata (EXIF) changes but image data stays same

With 256 KB average chunks:
  - 5 MB photo  →  ~20 chunks
  - 20 MB photo →  ~80 chunks
  - Metadata overhead: ~0.01%
  - Fewer Durable Object instances needed
  - Still reasonable dedup for large identical regions
```

### 5.10 Mossaic Recommendation

#### Context

Mossaic is a distributed chunked storage system on **Cloudflare Durable Objects**, primarily targeting **photo and blob storage**. Key constraints:

1. **Durable Objects** have per-object storage limits and per-request pricing
2. Each chunk likely maps to one or more Durable Object operations
3. Fewer, larger chunks = fewer DO operations = lower cost
4. Photos are mostly write-once, read-many (WORM) — dedup matters less than for backup systems
5. Dedup opportunities exist: same photo at multiple resolutions, re-uploads, shared albums
6. Network transfer: larger chunks reduce HTTP request overhead on Cloudflare's edge

#### Recommendation: FastCDC with Gear Hash

**Algorithm: FastCDC (normalized, gear-based)**

**Rationale:**
1. **FastCDC** is the clear winner — 10x faster than Rabin, equivalent dedup ratio
2. **Gear hash** requires minimal computation: one shift + one add per byte
3. **Normalized chunking** produces more uniform chunk sizes, which is ideal for Durable Object storage allocation
4. Widely implemented and battle-tested (used in numerous production dedup systems)
5. Simple to implement in TypeScript/JavaScript for Workers runtime

#### Recommended Parameters

```
┌─────────────────────────────────────────────────────┐
│  MOSSAIC FastCDC CONFIGURATION                      │
├─────────────────────────────────────────────────────┤
│                                                     │
│  Expected average chunk:  128 KB  (2^17)            │
│  Minimum chunk size:       32 KB  (expected / 4)    │
│  Maximum chunk size:        1 MB  (expected * 8)    │
│  Normalization size:      128 KB  (= expected)      │
│                                                     │
│  Rolling hash:            Gear hash (64-bit)        │
│  MaskS (hard):            18 bits  (2^18 = 256 KB)  │
│  MaskL (easy):            16 bits  (2^16 = 64 KB)   │
│                                                     │
│  Content hash:            SHA-256 (for chunk ID)    │
│  Chunk addressing:        Content-addressable (CAS) │
│                                                     │
│  Per-chunk metadata:      ~64 bytes                 │
│    - SHA-256 hash:        32 bytes                  │
│    - Chunk length:         4 bytes                  │
│    - Offset in file:       8 bytes                  │
│    - Flags/reserved:       4 bytes                  │
│    - Chunk ID (derived):  16 bytes                  │
│                                                     │
└─────────────────────────────────────────────────────┘
```

#### Why 128 KB Average?

| Factor                        | Impact                                              |
|:------------------------------|:----------------------------------------------------|
| Durable Object operations     | 128 KB → ~40–80 chunks per photo → manageable       |
| Network efficiency            | 128 KB per request is efficient on Cloudflare edge   |
| Dedup potential               | Still fine-grained enough to dedup shared regions    |
| Metadata overhead             | ~0.05% — negligible                                  |
| Worker memory                 | 128 KB chunks fit comfortably in 128 MB worker limit |
| R2 storage alignment          | R2 has no minimum object size; any chunk size works  |

#### Why NOT Fixed-Size for Mossaic?

Even for a primarily photo-storage system:

1. **Re-uploads with EXIF changes**: Users re-upload the same photo with modified metadata (rotation, GPS strip, etc.). EXIF is at the start of JPEG files → fixed-size shifts all boundaries → zero dedup. CDC handles this gracefully — only the first 1–2 chunks change.

2. **Multiple export sizes**: Same photo exported at different quality levels may share large identical DCT blocks. CDC can identify these overlapping regions.

3. **Future flexibility**: If Mossaic extends to documents, archives, or general blobs, CDC's dedup advantage becomes critical.

4. **Cost**: On Cloudflare's pricing, storing duplicate data costs real money. Even a 10–20% dedup improvement on a large photo library pays for the CDC computation overhead many times over.

#### Implementation Sketch (TypeScript for Cloudflare Workers)

```typescript
// Pre-computed Gear table: 256 random 64-bit values
// (Use BigInt or two 32-bit integers for 64-bit arithmetic in JS)
const GEAR: BigInt[] = [/* 256 random 64-bit values */];

const MIN_SIZE  = 32 * 1024;     // 32 KB
const MAX_SIZE  = 1024 * 1024;   // 1 MB
const NORM_SIZE = 128 * 1024;    // 128 KB
const MASK_S    = 0x0003590703530000n;  // ~18 bits (harder)
const MASK_L    = 0x0000d90003530000n;  // ~16 bits (easier)

function fastcdc(src: Uint8Array): number {
    const n = Math.min(src.length, MAX_SIZE);
    if (n <= MIN_SIZE) return n;

    let fp = 0n;
    let i = 0;

    // Skip MinSize
    while (i < MIN_SIZE) {
        fp = ((fp << 1n) + GEAR[src[i]]) & 0xFFFFFFFFFFFFFFFFn;
        i++;
    }

    // Region 1: MinSize → NormSize (harder mask)
    const barrier = Math.min(NORM_SIZE, n);
    while (i < barrier) {
        fp = ((fp << 1n) + GEAR[src[i]]) & 0xFFFFFFFFFFFFFFFFn;
        if (!(fp & MASK_S)) return i;
        i++;
    }

    // Region 2: NormSize → MaxSize (easier mask)
    while (i < n) {
        fp = ((fp << 1n) + GEAR[src[i]]) & 0xFFFFFFFFFFFFFFFFn;
        if (!(fp & MASK_L)) return i;
        i++;
    }

    return i;
}
```

> **Note:** For production, consider using WebAssembly for the hot loop since BigInt operations in JavaScript are significantly slower than native 64-bit arithmetic. A Rust-compiled WASM module can achieve near-native FastCDC throughput (~800 MB/s+) vs ~50–100 MB/s with JS BigInt.

#### Architecture Integration

```
┌──────────────────────────────────────────────────────────────┐
│                      CLIENT UPLOAD                           │
│                                                              │
│  1. Client uploads photo via Workers API                     │
│  2. Worker receives ReadableStream                           │
│  3. FastCDC chunks the stream (128 KB avg)                   │
│  4. SHA-256 each chunk → content hash = chunk ID             │
│  5. Check if chunk exists (Durable Object lookup)            │
│                                                              │
│     ┌─────────┐   chunks    ┌──────────────────────────┐     │
│     │ Worker  │────────────▶│ Chunk Durable Objects    │     │
│     │ FastCDC │             │ (one per unique chunk)   │     │
│     └─────────┘             └──────────────────────────┘     │
│          │                                                   │
│          │ manifest          ┌──────────────────────────┐    │
│          └──────────────────▶│ File Manifest DO         │    │
│                              │ (ordered list of chunk   │    │
│                              │  hashes for this file)   │    │
│                              └──────────────────────────┘    │
└──────────────────────────────────────────────────────────────┘
```

### References (Content-Defined Chunking)

#### Primary Papers

1. **FastCDC** — Wen Xia, Yukun Zhou, Hong Jiang, Dan Feng, Yu Hua, Yuchong Hu, Qing Liu, Yucheng Zhang. "FastCDC: A Fast and Efficient Content-Defined Chunking Approach for Data Deduplication." *USENIX ATC '16*, 2016.
   - https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia

2. **FastCDC (Extended, IEEE TPDS)** — Wen Xia, Xiangyu Zou, Hong Jiang, et al. "The Design of Fast Content-Defined Chunking for Data Deduplication Based Storage Systems." *IEEE Transactions on Parallel and Distributed Systems*, 31(9), 2020.
   - https://doi.org/10.1109/TPDS.2020.2984632

3. **Rabin Fingerprinting** — Michael O. Rabin. "Fingerprinting by Random Polynomials." Center for Research in Computing Technology, Harvard University, 1981.
   - http://www.xmailserver.org/rabin.pdf

4. **LBFS** — Athicha Muthitacharoen, Benjie Chen, David Mazières. "A Low-Bandwidth Network File System." *ACM SOSP '01*, 2001.
   - https://pdos.csail.mit.edu/papers/lbfs:sosp01/lbfs.pdf

5. **Ddelta / Gear Hash** — Wen Xia, Hong Jiang, Dan Feng, Lei Tian, Min Fu, Yukun Zhou. "Ddelta: A Deduplication-Inspired Fast Delta Compression Approach." *Performance Evaluation*, 79, 2014.
   - https://doi.org/10.1016/j.peva.2014.07.016

6. **QuickCDC** — Zhi Xu, Wenlong Zhang. *IEEE Access*, 2021.
   - https://ieeexplore.ieee.org/document/9644788

7. **RapidCDC** — Fan Ni, Song Jiang. *ACM SoCC '19*, 2019.
   - https://dl.acm.org/doi/10.1145/3357223.3362731

#### Implementations & Tools

8. **restic** — CDC implementation using Rabin fingerprinting.
   - Blog: https://restic.net/blog/2015-09-12/restic-foundation1-cdc/
   - Code: https://github.com/restic/chunker

9. **Borg Backup** — Uses Buzhash for chunking.
   - Docs: https://borgbackup.readthedocs.io/en/stable/internals/data-structures.html#chunker-details

10. **FastCDC-rs** — Rust implementation of FastCDC.
    - https://crates.io/crates/fastcdc

11. **ronomon/deduplication** — JavaScript CDC implementation.
    - https://github.com/ronomon/deduplication

#### Background

12. **Rolling Hash (Wikipedia)** — https://en.wikipedia.org/wiki/Rolling_hash

13. **Content-Defined Chunking Introduction** — Josh Leeb's blog series.
    - https://joshleeb.com/posts/content-defined-chunking.html

14. **Daniel Lemire, Owen Kaser** — "Recursive n-gram hashing is pairwise independent, at best." *Computer Speech & Language*, 24(4), 2010.
    - https://arxiv.org/abs/0705.4676

15. **Primary Data Deduplication** — Dutch T. Meyer, William J. Bolosky. *USENIX ATC '12*. Recommends 64 KB average chunk size for optimal combined dedup + compression.
    - https://www.usenix.org/system/files/conference/atc12/atc12-final293.pdf

---

## Section 6: Parallel Transfer Optimization

### 6.1 Optimal Concurrent Connections

#### Why 4–8 Parallel Connections Maximize Throughput

A single TCP connection is fundamentally limited by its congestion window (cwnd). After the initial three-way handshake, TCP slow start begins with a small cwnd (typically 10 MSS segments = ~14.6 KB on modern Linux) and doubles each RTT until packet loss occurs. On a high-bandwidth link with non-trivial latency, a single connection may never fully saturate the pipe before the transfer completes.

Opening **N parallel connections** provides several compounding advantages:

| Benefit | Mechanism |
|---|---|
| **Aggregate cwnd** | Each connection maintains an independent congestion window. N connections yield up to N × cwnd_max aggregate window. |
| **Loss isolation** | Packet loss on connection A triggers congestion response only on A. Connections B–N continue at full rate. |
| **Slow-start bypass** | N connections exit slow start N-independently, reaching full throughput faster in aggregate. |
| **Fair-share gaming** | In shared-bottleneck scenarios, N connections capture a larger share of available bandwidth (ethically debatable but practically relevant). |

#### Why Not More Than 8?

Diminishing returns set in due to:

- **Server-side resource exhaustion**: Each connection consumes file descriptors, memory buffers, and TLS state (~50 KB/connection for TLS 1.3 session state).
- **Client-side overhead**: Each connection requires a thread or event-loop slot, socket buffers, and reassembly state.
- **Network congestion**: Too many connections in a shared bottleneck can trigger queue overflow and synchronized packet loss across all connections, collapsing aggregate throughput.
- **Diminishing marginal throughput**: Once aggregate cwnd exceeds the bottleneck bandwidth-delay product, additional connections add overhead without throughput gain.

**Empirical research** consistently shows that 4–8 connections is the sweet spot for most CDN-to-client transfers. Beyond 8, the overhead-to-gain ratio worsens rapidly. Browsers historically defaulted to 6 connections per origin (HTTP/1.1 era) for precisely this reason.

#### Per-Connection Congestion Window Independence

Each TCP connection runs its own instance of the congestion control algorithm (Cubic, BBR, Reno, etc.). This means:

```
Connection 1: cwnd = 42 segments  (steady state)
Connection 2: cwnd = 10 segments  (just recovered from loss)
Connection 3: cwnd = 65 segments  (growing aggressively)
Connection 4: cwnd = 55 segments  (steady state)
─────────────────────────────────────────────────
Aggregate:    cwnd = 172 segments (~251 KB effective window)
```

This independence is the primary reason parallel connections outperform a single connection: the **aggregate window is more stable** because individual loss events don't crash the entire transfer. The coefficient of variation of aggregate throughput decreases roughly as `1/√N`.

### 6.2 Bandwidth-Delay Product (BDP)

#### The Fundamental Formula

```
BDP = Bandwidth × RTT
```

The bandwidth-delay product represents the **maximum amount of data that can be "in flight"** (sent but not yet acknowledged) on a network path at any given time. It defines the optimal TCP window size needed to fully utilize a link.

#### Why a Single TCP Connection Underutilizes High-BDP Links

A TCP sender can only have `window_size` bytes in flight at any time. If `window_size < BDP`, the sender must idle waiting for ACKs, leaving bandwidth unused.

#### Worked Examples

**Example 1: Residential User Downloading from Cloudflare**

```
Bandwidth: 100 Mbps = 12.5 MB/s
RTT:       40 ms (US East Coast ↔ Cloudflare edge)
BDP:       12.5 MB/s × 0.040 s = 500 KB
```

A single TCP connection needs a 500 KB receive window to saturate this link. With the default Linux receive buffer of 128 KB, a single connection can only achieve:

```
Max throughput = 128 KB / 0.040 s = 3.2 MB/s = 25.6 Mbps  (25% utilization!)
```

With **4 parallel connections**, each with a 128 KB window:

```
Aggregate window = 4 × 128 KB = 512 KB > 500 KB BDP
→ Full link utilization achieved
```

**Example 2: Cross-Continental Transfer**

```
Bandwidth: 1 Gbps = 125 MB/s
RTT:       120 ms (US West Coast ↔ Europe via Cloudflare)
BDP:       125 MB/s × 0.120 s = 15 MB
```

A single connection with a 1 MB window achieves only:

```
Max throughput = 1 MB / 0.120 s = 8.33 MB/s = 66.7 Mbps  (6.7% utilization!)
```

You would need **15 connections** each with a 1 MB window, or TCP window scaling to reach a 15 MB window on a single connection.

**Example 3: Mobile User (High Latency)**

```
Bandwidth: 50 Mbps = 6.25 MB/s
RTT:       80 ms (4G/LTE typical)
BDP:       6.25 MB/s × 0.080 s = 500 KB
```

Mobile networks compound the problem with variable latency. The effective BDP fluctuates, making adaptive parallel connections even more valuable.

#### BDP Table for Common Scenarios

| Scenario | Bandwidth | RTT | BDP | Min Window Needed |
|---|---|---|---|---|
| Local edge (same city) | 100 Mbps | 5 ms | 62.5 KB | 62.5 KB |
| Domestic CDN | 100 Mbps | 40 ms | 500 KB | 500 KB |
| Cross-continent | 100 Mbps | 120 ms | 1.5 MB | 1.5 MB |
| Fiber + cross-continent | 1 Gbps | 120 ms | 15 MB | 15 MB |
| Mobile 4G domestic | 50 Mbps | 80 ms | 500 KB | 500 KB |
| Satellite (LEO/Starlink) | 200 Mbps | 40 ms | 1 MB | 1 MB |
| Satellite (GEO) | 50 Mbps | 600 ms | 3.75 MB | 3.75 MB |

### 6.3 TCP Window Scaling (RFC 7323)

#### The 64 KB Problem

The original TCP specification (RFC 793) defines the receive window field as a 16-bit unsigned integer, yielding a **maximum window of 65,535 bytes (64 KB)**. This was generous in 1981 but is catastrophically small for modern networks.

With a 64 KB window:

```
Max throughput = 65,535 bytes / RTT

At 10 ms RTT:  → 6.5 MB/s  = 52 Mbps
At 40 ms RTT:  → 1.6 MB/s  = 13 Mbps
At 120 ms RTT: → 0.53 MB/s = 4.3 Mbps   ← Barely usable!
```

#### The Window Scale Option

RFC 7323 (originally RFC 1323, updated in 2014) introduces a **window scale factor** negotiated during the TCP three-way handshake via TCP options:

```
SYN:     [Window Scale: shift_count]
SYN-ACK: [Window Scale: shift_count]
```

The `shift_count` is a value from 0 to 14, meaning the 16-bit window field is left-shifted by up to 14 bits:

```
Effective window = window_field × 2^shift_count
Maximum window   = 65,535 × 2^14 = 1,073,725,440 bytes ≈ 1 GB
```

#### Throughput Formula

The fundamental throughput limit imposed by the receive window:

```
Max Throughput = Receive Window Size / RTT
```

This is a hard ceiling. No amount of bandwidth can overcome a window-limited connection.

#### Window Scaling in Practice

| OS | Default rmem_max | Typical Scale Factor | Effective Max Window |
|---|---|---|---|
| Linux (modern) | 6 MB (autotuned) | 7–9 | 4–6 MB |
| macOS | 4 MB | 6–8 | 2–4 MB |
| Windows 10/11 | Autotuned | Variable | Up to 16 MB |
| Cloudflare Workers | Managed | Managed by runtime | Varies |

#### Interaction with Mossaic

Cloudflare Durable Objects sit behind Cloudflare's edge network, which handles TCP termination. The client-to-edge TCP connection is where window scaling matters most. Cloudflare's edge servers are tuned with large receive buffers, so the limiting factor is typically the **client's** receive window.

For Mossaic:
- Clients downloading chunks benefit from window scaling since each chunk fetch is a discrete HTTP response.
- Short-lived chunk transfers may not reach optimal window sizes if slow start hasn't completed — **another argument for parallel connections** that amortize slow start across multiple chunks.
- Persistent connections (HTTP/2, WebSocket) avoid repeated slow start, allowing the window to grow and stabilize.

### 6.4 HTTP/2 vs HTTP/1.1 for Chunk Transfers

#### HTTP/2 Multiplexing

HTTP/2 allows **multiple logical streams** over a single TCP connection. For Mossaic chunk transfers, this means:

```
┌─────────────────────────────────────────────┐
│              Single TCP Connection           │
│                                              │
│  Stream 1: GET /chunk/abc123  ──────►  data  │
│  Stream 3: GET /chunk/def456  ──────►  data  │
│  Stream 5: GET /chunk/ghi789  ──────►  data  │
│  Stream 7: GET /chunk/jkl012  ──────►  data  │
│                                              │
│  All streams share one congestion window     │
│  All streams share one TLS session           │
└─────────────────────────────────────────────┘
```

**Benefits of HTTP/2 for chunk transfers:**

| Advantage | Detail |
|---|---|
| **Single TLS handshake** | Saves 1–2 RTTs per additional connection (TLS 1.3 = 1 RTT, TLS 1.2 = 2 RTTs) |
| **Shared congestion state** | One well-tuned connection vs. N connections fighting each other |
| **Header compression (HPACK)** | Repeated headers (auth tokens, content-type) compressed across streams |
| **Stream prioritization** | Can prioritize urgent chunks over prefetched ones |
| **Lower server resources** | One connection per client instead of 4–8 |
| **No connection limits** | Browser limits (6/origin for H1) don't apply |

#### Head-of-Line (HoL) Blocking: The Critical Tradeoff

HTTP/2's Achilles heel is **TCP-level head-of-line blocking**. Because all streams share one TCP connection, a single lost packet blocks **all streams** until retransmission completes:

```
HTTP/1.1 (4 connections):
  Conn 1: ████████░░████████████████  ← Loss on conn 1
  Conn 2: ██████████████████████████  ← Unaffected
  Conn 3: ██████████████████████████  ← Unaffected
  Conn 4: ██████████████████████████  ← Unaffected
  Aggregate: 75% throughput maintained during loss event

HTTP/2 (1 connection):
  Stream 1: ████████░░░░░░████████████  ← Packet loss
  Stream 2: ████████░░░░░░████████████  ← BLOCKED (same TCP conn)
  Stream 3: ████████░░░░░░████████████  ← BLOCKED (same TCP conn)
  Stream 4: ████████░░░░░░████████████  ← BLOCKED (same TCP conn)
  Aggregate: 0% throughput during loss recovery
```

#### When Multiple HTTP/1.1 Connections Beat HTTP/2

| Scenario | Winner | Reason |
|---|---|---|
| **Lossy network (>1% loss)** | HTTP/1.1 × 4–6 | Loss isolation prevents total stall |
| **High-BDP path** | HTTP/1.1 × N | Multiple cwnd instances fill the pipe faster |
| **Large chunk transfers (>1 MB)** | HTTP/1.1 × 4 | Each connection saturates on one chunk |
| **Low-latency, reliable link** | HTTP/2 | Multiplexing overhead savings dominate |
| **Many small chunks (<64 KB)** | HTTP/2 | Header compression + no connection overhead |
| **Mixed chunk sizes** | Hybrid | H2 for metadata/small, H1 for large bulk |

#### HTTP/3 (QUIC) — The Future

HTTP/3 over QUIC solves HoL blocking at the transport layer. Each QUIC stream has independent loss recovery. This gives the **multiplexing benefits of HTTP/2** without the **HoL blocking penalty**. Cloudflare supports HTTP/3 natively, making this the ideal long-term protocol for Mossaic.

### 6.5 Chunk Pipeline Scheduling

#### The Core Idea

Don't wait for chunk N to be fully processed before requesting chunk N+1. **Overlap network I/O with processing** to hide latency.

#### Pipeline Architecture

```
Time ──────────────────────────────────────────────────────►

                    SEQUENTIAL (Naive)
  ┌──────────┐┌────────┐┌──────────┐┌────────┐┌──────────┐
  │ Fetch C1 ││Process ││ Fetch C2 ││Process ││ Fetch C3 │ ...
  │ (network)││  C1    ││ (network)││  C2    ││ (network)│
  └──────────┘└────────┘└──────────┘└────────┘└──────────┘
  Total time = N × (fetch_time + process_time)


                    PIPELINED (Optimized)
  ┌──────────┐┌──────────┐┌──────────┐┌──────────┐
  │ Fetch C1 ││ Fetch C2 ││ Fetch C3 ││ Fetch C4 │
  └────┬─────┘└────┬─────┘└────┬─────┘└────┬─────┘
       │     ┌─────┴────┐┌────┴─────┐┌────┴─────┐
       │     │Process C1││Process C2││Process C3│ ...
       │     └──────────┘└──────────┘└──────────┘
  Total time ≈ N × max(fetch_time, process_time) + one_fetch
  Speedup ≈ (fetch + process) / max(fetch, process)
```

#### Detailed Pipeline with Parallel Fetches

```
Time ──────────────────────────────────────────────────────────►

Connection 1: [Fetch C1]         [Fetch C5]         [Fetch C9]
Connection 2:   [Fetch C2]         [Fetch C6]         [Fetch C10]
Connection 3:     [Fetch C3]         [Fetch C7]
Connection 4:       [Fetch C4]         [Fetch C8]
              ─────────────────────────────────────────────────
Processing:         [Dec C1][Dec C2][Dec C3][Dec C4][Dec C5]...
              ─────────────────────────────────────────────────
Reassembly:              [Asm C1,C2] [Asm C3,C4] [Asm C5,C6]...
              ─────────────────────────────────────────────────
Output:                       [Write partial] [Write partial]...
```

#### Implementation Strategy for Mossaic

```typescript
// Conceptual pipeline scheduler
class ChunkPipelineScheduler {
  private fetchQueue: ChunkRequest[] = [];
  private processQueue: ArrayBuffer[] = [];
  private maxConcurrentFetches = 4;
  private prefetchAhead = 2; // Fetch 2 chunks ahead of processing

  async run(chunkIds: string[]): Promise<void> {
    // Fill the pipeline: start fetching first N chunks immediately
    const fetchers = new Array(this.maxConcurrentFetches)
      .fill(null)
      .map((_, i) => this.fetchLoop(i));

    // Process chunks as they arrive (in order)
    const processor = this.processLoop();

    await Promise.all([...fetchers, processor]);
  }

  private async fetchLoop(connectionId: number): Promise<void> {
    while (this.fetchQueue.length > 0) {
      const chunk = this.fetchQueue.shift()!;
      const data = await this.fetchChunk(chunk, connectionId);
      this.processQueue.push(data);  // Hand off to processing
    }
  }

  private async processLoop(): Promise<void> {
    // Process in arrival order, overlapping with ongoing fetches
    for await (const chunk of this.processQueue) {
      await this.decrypt(chunk);
      await this.verify(chunk);
      await this.reassemble(chunk);
    }
  }
}
```

#### Pipeline Efficiency

The pipeline efficiency depends on the ratio of fetch time to process time:

```
Let F = fetch time per chunk
Let P = process time per chunk (decrypt, verify, reassemble)
Let N = number of chunks

Sequential:  T = N × (F + P)
Pipelined:   T ≈ N × max(F, P) + min(F, P)
Speedup:     S ≈ (F + P) / max(F, P)

If F = P:    S ≈ 2.0×  (perfect overlap)
If F = 3P:   S ≈ 1.33× (network-bound)
If P = 3F:   S ≈ 1.33× (compute-bound)
```

For Mossaic, chunk decryption (AES-256-GCM) and hash verification (SHA-256) are fast relative to network fetch, so the system is typically **network-bound** and the pipeline mainly helps by keeping all connections busy.

### 6.6 Adaptive Concurrency

#### AIMD-Style Connection Management

Additive Increase, Multiplicative Decrease (AIMD) — the same algorithm that governs TCP congestion control — can be applied at the **application layer** to manage the number of concurrent chunk-fetch connections.

#### Algorithm

```
Initialize:
  connections = 4          (starting concurrency)
  min_connections = 1
  max_connections = 12
  throughput_history = []
  measurement_interval = 2 seconds

Every measurement_interval:
  current_throughput = bytes_received / elapsed_time
  throughput_history.append(current_throughput)

  if current_throughput > previous_throughput × 1.05:
    // Throughput increased — Additive Increase
    connections = min(connections + 1, max_connections)
  elif current_throughput < previous_throughput × 0.85:
    // Throughput dropped significantly — Multiplicative Decrease
    connections = max(floor(connections × 0.5), min_connections)
  else:
    // Stable — maintain current level
    no change

  previous_throughput = current_throughput
```

#### State Diagram

```
                    ┌──────────────┐
                    │  Start (N=4) │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
               ┌────│   Measure    │────┐
               │    │  Throughput  │    │
               │    └──────┬───────┘    │
               │           │            │
        TP increased    Stable     TP decreased
        (>5% gain)                 (>15% drop)
               │           │            │
        ┌──────▼──────┐    │    ┌───────▼──────┐
        │ N = N + 1   │    │    │ N = N × 0.5  │
        │ (Additive   │    │    │ (Multiplicat. │
        │  Increase)  │    │    │  Decrease)    │
        └──────┬──────┘    │    └───────┬──────┘
               │           │            │
               └───────────┴────────────┘
                           │
                     Wait interval
                           │
                    (loop back to Measure)
```

#### Why AIMD Works for Chunk Transfers

1. **Additive increase** is conservative: adding one connection at a time probes for available bandwidth without flooding the network.
2. **Multiplicative decrease** reacts aggressively to congestion: halving connections quickly reduces load when the network is struggling.
3. **Convergence**: AIMD provably converges to a fair and efficient equilibrium in shared-bottleneck scenarios.
4. **Responsiveness**: For mobile users whose bandwidth fluctuates (WiFi ↔ cellular handoff), AIMD adapts within seconds.

#### Practical Considerations for Mossaic

- **Measurement noise**: Use exponential moving average (EMA) of throughput with α = 0.3 to smooth out jitter.
- **Connection startup cost**: Don't add a connection if the current transfer is nearly complete. Set a minimum remaining-bytes threshold.
- **Per-connection metrics**: Track individual connection throughput. If one connection is consistently slow (bad DO region routing), replace it rather than adding more.
- **Cooldown period**: After a multiplicative decrease, wait 2–3 measurement intervals before allowing increase again to prevent oscillation.

```typescript
// Adaptive concurrency controller
class AdaptiveConcurrencyController {
  private concurrency = 4;
  private readonly min = 1;
  private readonly max = 12;
  private ewmaThroughput = 0;
  private readonly alpha = 0.3;
  private cooldownRemaining = 0;

  onMeasurement(bytesPerSecond: number): number {
    const smoothed = this.alpha * bytesPerSecond +
                     (1 - this.alpha) * this.ewmaThroughput;

    if (this.cooldownRemaining > 0) {
      this.cooldownRemaining--;
    } else if (smoothed > this.ewmaThroughput * 1.05) {
      // Additive increase
      this.concurrency = Math.min(this.concurrency + 1, this.max);
    } else if (smoothed < this.ewmaThroughput * 0.85) {
      // Multiplicative decrease
      this.concurrency = Math.max(
        Math.floor(this.concurrency * 0.5),
        this.min
      );
      this.cooldownRemaining = 3;
    }

    this.ewmaThroughput = smoothed;
    return this.concurrency;
  }
}
```

### 6.7 Amdahl's Law for Downloads

#### The Formula

Amdahl's Law defines the theoretical maximum speedup achievable by parallelizing a task:

```
Speedup(N) = 1 / (S + P/N)
```

Where:
- **S** = fraction of the task that is inherently serial (cannot be parallelized)
- **P** = fraction of the task that is parallelizable (P = 1 - S)
- **N** = number of parallel workers (connections)

#### Application to Chunk Downloads

A Mossaic file download has both serial and parallel components:

**Serial overhead (S):**
- Initial metadata fetch (chunk manifest, encryption keys)
- Final reassembly / concatenation of chunks into output file
- Integrity verification of the complete file (if applicable)
- Connection setup (amortized across many chunks)

**Parallelizable portion (P):**
- Individual chunk fetches from Durable Objects
- Per-chunk decryption
- Per-chunk integrity verification

#### Worked Example

Assume a 100 MB file split into 100 × 1 MB chunks:

```
Metadata fetch:      50 ms   (serial)
Chunk fetches:     2000 ms   (parallelizable — 100 chunks × 20ms each, sequentially)
Per-chunk decrypt:  500 ms   (parallelizable — 100 × 5ms)
Reassembly:         100 ms   (serial)
Final verification:  50 ms   (serial)
────────────────────────────
Total sequential:  2700 ms

S = (50 + 100 + 50) / 2700 = 200/2700 ≈ 0.074  (7.4%)
P = 2500/2700 ≈ 0.926  (92.6%)
```

| Connections (N) | Speedup | Time | Utilization |
|---|---|---|---|
| 1 | 1.00× | 2700 ms | 100% |
| 2 | 1.85× | 1459 ms | 92.5% |
| 4 | 3.32× | 813 ms | 83.0% |
| 6 | 4.50× | 600 ms | 75.0% |
| 8 | 5.41× | 499 ms | 67.7% |
| 12 | 6.67× | 405 ms | 55.6% |
| 16 | 7.47× | 361 ms | 46.7% |
| 32 | 8.76× | 308 ms | 27.4% |
| ∞ | 13.5× | 200 ms | →0% |

#### The Diminishing Returns Cliff

```
Speedup
  14× ┤                                          ──── theoretical max (1/S)
      │
  12× ┤
      │
  10× ┤
      │                              ·····················
   8× ┤                     ····
      │                ···
   6× ┤           ···
      │        ··
   4× ┤     ··
      │   ··
   2× ┤  ·
      │ ·
   0× ┤·
      └─┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──
        1  2  4  6  8  10 12 14 16 18 20 24 28 32    ∞
                    Number of Connections (N)
```

#### Key Insight for Mossaic

**The serial overhead (S) is the speed limit**. To maximize parallel gains:

1. **Minimize metadata fetch time**: Cache the chunk manifest. Pre-compute chunk locations.
2. **Overlap reassembly with downloads**: Write chunks to their final positions as they arrive (random-access output) rather than waiting for sequential reassembly.
3. **Pipeline verification**: Verify each chunk independently on arrival rather than verifying the complete file at the end.
4. **Reduce connection setup**: Use persistent connections (keep-alive, H2, WebSocket) to amortize handshake costs.

### 6.8 WebSocket vs HTTP for Chunk Streaming

#### HTTP (Request-Response) Model

```
Client                        Durable Object
  │                                │
  │─── GET /chunk/abc123 ────────►│
  │◄── 200 OK + chunk data ──────│
  │                                │
  │─── GET /chunk/def456 ────────►│
  │◄── 200 OK + chunk data ──────│
  │                                │
  (Per-request overhead: headers, framing)
```

**Per-request overhead:**
- HTTP/1.1: ~200–800 bytes of headers per request/response
- HTTP/2: ~20–50 bytes (HPACK compressed)
- TLS record framing: ~29 bytes per record

#### WebSocket (Persistent Bidirectional) Model

```
Client                        Durable Object
  │                                │
  │─── HTTP Upgrade ──────────────►│
  │◄── 101 Switching Protocols ───│
  │                                │
  │═══ WebSocket Connection ══════│
  │                                │
  │─── {type: "fetch", id: "abc"} ►│
  │◄── [binary chunk data] ───────│
  │─── {type: "fetch", id: "def"} ►│
  │◄── [binary chunk data] ───────│
  │                                │
  (Per-message overhead: 2-14 bytes framing)
```

#### Comparison

| Metric | HTTP/1.1 | HTTP/2 | WebSocket |
|---|---|---|---|
| **Per-message overhead** | 200–800 B | 20–50 B | 2–14 B |
| **Connection setup** | TCP + TLS per conn | TCP + TLS once | TCP + TLS + Upgrade once |
| **Server push** | No | Yes (deprecated) | Yes (native) |
| **Bidirectional** | No | Limited | Full |
| **Streaming** | Chunked encoding | Streams | Native frames |
| **Multiplexing** | No | Yes | Manual |
| **Browser support** | Universal | Universal | Universal |
| **Cloudflare DO support** | Yes | Yes (edge) | **Yes (native)** |
| **Connection lifecycle** | Short/keep-alive | Long-lived | Long-lived |

#### WebSocket Advantages for Mossaic

1. **Minimal framing overhead**: For a 1 MB chunk, HTTP headers add 0.05% overhead vs WebSocket's 0.001%. For a 4 KB chunk, HTTP headers add 10–20% overhead vs WebSocket's 0.2%.

2. **Server-initiated push**: A Durable Object can proactively push chunk data to the client without waiting for a request. This enables speculative prefetching:
   ```
   Client: "I want chunks 1–10"
   DO:     Starts sending chunk 1 immediately
   DO:     Sends chunk 2 without waiting for client to ask
   ...
   DO:     Sends chunk 10
   ```

3. **Persistent connection to DO**: Each Durable Object has a unique WebSocket endpoint. A persistent WebSocket connection avoids:
   - Repeated TLS handshakes
   - Repeated TCP slow start
   - Repeated DO routing lookups

4. **Backpressure signaling**: WebSocket flow control allows the client to signal when it's overwhelmed, causing the DO to slow down.

#### WebSocket Disadvantages

1. **No built-in multiplexing**: Unlike HTTP/2, WebSocket requires application-level multiplexing if you want multiple logical streams.
2. **Proxy/firewall issues**: Some corporate proxies don't support WebSocket upgrades (though this is increasingly rare).
3. **No built-in compression negotiation**: Must implement per-message compression (permessage-deflate) or handle at application level.
4. **Connection management complexity**: Must handle reconnection, heartbeats, and state synchronization manually.

#### Cloudflare Durable Objects WebSocket Support

Durable Objects have **first-class WebSocket support** via the Hibernatable WebSocket API:

- DOs can accept WebSocket connections and hold them open
- The Hibernation API allows DOs to sleep while WebSocket connections remain open, reducing costs
- Each DO can handle multiple concurrent WebSocket connections
- Binary message support enables efficient chunk transfer without Base64 encoding

This makes WebSocket a natural fit for Mossaic's chunk transfer protocol, especially for:
- Long-running upload/download sessions
- Real-time progress reporting
- Bidirectional chunk negotiation (client can report which chunks it already has)

### 6.9 Mossaic Recommendation

#### Recommended Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Mossaic Client                        │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │         Adaptive Concurrency Controller          │    │
│  │         (AIMD, target: 4–6 connections)          │    │
│  └──────────────┬──────────────────────────────┬────┘    │
│                 │                              │         │
│  ┌──────────────▼──────┐    ┌─────────────────▼─────┐   │
│  │   HTTP/2 Fetchers   │    │  WebSocket Channels   │   │
│  │  (small chunks,     │    │  (large streaming     │   │
│  │   metadata, index)  │    │   uploads, real-time) │   │
│  └──────────────┬──────┘    └─────────────────┬─────┘   │
│                 │                              │         │
│  ┌──────────────▼──────────────────────────────▼─────┐   │
│  │              Chunk Pipeline Scheduler              │   │
│  │  (prefetch ahead, overlap decrypt with fetch)     │   │
│  └──────────────┬────────────────────────────────────┘   │
│                 │                                        │
│  ┌──────────────▼────────────────────────────────────┐   │
│  │              Reassembly Buffer                     │   │
│  │  (random-access write, streaming output)          │   │
│  └───────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

#### Specific Recommendations

**1. Use 4–6 parallel fetch connections per client**

- Start at 4, use AIMD to adapt between 2 and 8.
- This range balances BDP coverage, loss isolation, and resource efficiency.
- For Cloudflare edge servers with ~5–40 ms RTT from most clients, 4 connections provide sufficient aggregate window for 100+ Mbps links.

**2. HTTP/2 for small chunks and metadata**

- Chunk sizes ≤ 256 KB: Use HTTP/2 multiplexing over a single connection.
- HPACK compression eliminates redundant headers across chunk requests.
- Stream prioritization allows metadata and manifest fetches to preempt bulk data.

**3. Consider WebSockets for large streaming uploads**

- Uploads of large files (multi-GB) benefit from persistent WebSocket connections to the target DOs.
- Server-initiated push enables speculative pre-confirmation.
- Hibernatable WebSocket API keeps costs low during pauses.

**4. Adaptive concurrency with AIMD**

- Measurement interval: 2 seconds
- Additive increase: +1 connection when throughput grows >5%
- Multiplicative decrease: halve connections when throughput drops >15%
- Use EWMA (α = 0.3) to smooth throughput measurements
- Cooldown: 3 intervals after decrease before allowing increase

**5. Pipeline chunk processing**

- Maintain a prefetch buffer of 2–3 chunks ahead of the processing cursor.
- Decrypt and verify each chunk immediately upon arrival (don't wait for all chunks).
- Write completed chunks to their final position in the output buffer (random-access).
- Stream output to the caller as soon as sequential chunks are available.

#### Configuration Matrix

| Transfer Type | Protocol | Concurrency | Pipeline Depth | Chunk Size |
|---|---|---|---|---|
| Small file download (<1 MB) | HTTP/2 | 1–2 | 1 | 256 KB |
| Medium file download (1–100 MB) | HTTP/2 | 4 (adaptive) | 2–3 | 1 MB |
| Large file download (>100 MB) | HTTP/2 × 4 or H1 × 4 | 4–6 (adaptive) | 3–4 | 4 MB |
| Small file upload | HTTP/2 POST | 1 | 1 | 256 KB |
| Large file upload | WebSocket | 2–4 | 2 | 4 MB |
| Real-time streaming | WebSocket | 1–2 | 1 | 64 KB |

#### Performance Targets

| Scenario | Target Throughput | Max Latency (TTFB) |
|---|---|---|
| Edge-local (same city) | 80%+ of link speed | < 50 ms |
| Domestic CDN | 60%+ of link speed | < 100 ms |
| Cross-continent | 40%+ of link speed | < 250 ms |
| Mobile (4G) | 50%+ of link speed | < 200 ms |

#### Future Considerations

1. **HTTP/3 (QUIC)**: When Cloudflare Workers/DO fully support QUIC end-to-end, migrate to HTTP/3 to eliminate TCP HoL blocking while retaining multiplexing.
2. **0-RTT resumption**: Both TLS 1.3 and QUIC support 0-RTT resumption. For repeat clients, connection setup overhead drops to near zero.
3. **Multipath**: QUIC multipath (draft-ietf-quic-multipath) would allow using WiFi + cellular simultaneously.
4. **Regional chunk placement**: Use Cloudflare's global network to place chunk replicas near requesting clients, reducing RTT and thus BDP requirements.
5. **Compression**: For compressible content, apply per-chunk compression (zstd streaming) before encryption to reduce transfer size.

### References (Parallel Transfer)

- RFC 7323 — TCP Extensions for High Performance (Window Scaling, Timestamps)
- RFC 5681 — TCP Congestion Control (Slow Start, Congestion Avoidance, Fast Retransmit)
- RFC 9000 — QUIC: A UDP-Based Multiplexed and Secure Transport
- RFC 7540 — Hypertext Transfer Protocol Version 2 (HTTP/2)
- RFC 9114 — HTTP/3
- Cloudflare Durable Objects Documentation — WebSocket Hibernation API
- Amdahl, G. M. (1967) — "Validity of the single processor approach to achieving large scale computing capabilities"
- Mathis, M., Semke, J., Mahdavi, J., Ott, T. (1997) — "The macroscopic behavior of the TCP congestion avoidance algorithm" — ACM CCR

---

## Section 7: Existing Systems Study

### 7.1 IPFS (InterPlanetary File System)

#### Overview

IPFS is a peer-to-peer, content-addressed hypermedia distribution protocol. Created by Juan Benet (Protocol Labs, 2015), IPFS fundamentally changes how data is located: instead of asking *where* data lives (location-addressed, like HTTP URLs), you ask for data by *what it is* (content-addressed, via cryptographic hash).

#### Content Addressing via CID

Every piece of data in IPFS is identified by its **Content Identifier (CID)** — a cryptographic hash of the content itself.

```
CID = Multicodec + Multihash(content)

Example CID (v1):
  bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3okuez5j7lnk334
  └── base32 ──┘└── codec ──┘└── SHA-256 hash of content ──────┘
```

Key properties of content addressing:
- **Self-certifying**: The address *is* the verification — if the hash matches, the data is authentic
- **Immutable**: Changing content changes the hash, producing a new CID
- **Deduplication for free**: Identical content always produces the same CID, regardless of who stores it or when
- **Location-independent**: Any node holding the data can serve it; no single point of failure

#### Merkle DAG (Directed Acyclic Graph)

Files in IPFS are represented as a **Merkle DAG** — a tree structure where:

```
                  ┌─────────────┐
                  │  Root CID   │  ← CID of entire file
                  │  (QmXyz...) │
                  └──────┬──────┘
                    ┌────┴────┐
               ┌────┴───┐ ┌──┴─────┐
               │ Block 1 │ │ Block 2 │  ← Intermediate nodes
               │ CID_A   │ │ CID_B   │
               └────┬────┘ └────┬────┘
              ┌─────┼─────┐    ┌┴────┐
           ┌──┴─┐┌──┴─┐┌──┴─┐┌┴───┐
           │Leaf││Leaf││Leaf││Leaf │  ← Raw data chunks
           │ 1  ││ 2  ││ 3  ││ 4  │
           └────┘└────┘└────┘└────┘
```

- Files are split into **chunks** (default 256KB via the `size-262144` chunker)
- Each chunk is hashed to produce a leaf CID
- Intermediate nodes link to child CIDs; their own CID is the hash of those links
- The **root CID** represents the entire file — changing any byte changes the root
- **UnixFS** is the protobuf-based format used to represent files and directories on top of the Merkle DAG

This structure enables:
- **Partial verification**: You can verify any subtree independently
- **Incremental transfer**: Only fetch blocks you don't already have
- **Deduplication across files**: Shared subtrees are stored once

#### Bitswap Protocol

Bitswap is IPFS's block exchange protocol — how peers actually trade data:

```
Peer A                          Peer B
  │                                │
  │──── WANT_HAVE(CID_1) ────────>│  "Do you have this block?"
  │<─── HAVE(CID_1) ──────────────│  "Yes, I do"
  │──── WANT_BLOCK(CID_1) ───────>│  "Send it to me"
  │<─── BLOCK(CID_1, data) ───────│  "Here's the data"
  │                                │
```

- **Tit-for-tat**: Peers preferentially serve those who serve them (inspired by BitTorrent)
- **Wantlist management**: Each peer maintains a list of CIDs it wants and broadcasts to connected peers
- **Session-based**: Related block requests are grouped into sessions for efficiency
- **Peer discovery**: Uses a Kademlia-based DHT to find peers that have specific CIDs

#### Pinning and Garbage Collection

IPFS nodes by default cache data they retrieve but may garbage-collect it. **Pinning** marks data as "keep forever":
- Local pins: stored on the node itself
- Remote pinning services: Pinata, Filebase, web3.storage delegate persistence to infrastructure providers
- Filecoin: Incentivized persistence layer — miners are paid to store pinned data

#### Strengths and Weaknesses

| Strengths | Weaknesses |
|-----------|------------|
| Content addressing provides natural dedup | No built-in persistence guarantees (must pin) |
| Merkle DAG enables partial verification | DHT lookups can be slow (seconds) |
| Censorship resistant by design | No native erasure coding |
| Works across untrusted peers | Bitswap overhead for small files |
| Mature ecosystem and tooling | IPNS (mutable names) is slow |

#### Mossaic Takeaway

**Content addressing is the right primitive for deduplication and integrity verification.** Mossaic should adopt CID-style content identifiers — the hash of content serves as both its address and its integrity proof. Merkle trees enable verification of individual chunks without downloading the entire object. However, IPFS's lack of built-in persistence and erasure coding means Mossaic needs to add those layers independently.

### 7.2 Ceph

#### Overview

Ceph is an open-source, unified distributed storage system providing object, block, and file storage on a single platform. Originally developed by Sage Weil as his PhD thesis at UC Santa Cruz (2006).

#### CRUSH Algorithm (Controlled Replication Under Scalable Hashing)

The defining innovation of Ceph is **CRUSH** — a pseudo-random, deterministic data placement algorithm that eliminates the need for a central lookup table:

```
CRUSH(object_hash, cluster_map, placement_rules) → [OSD_primary, OSD_replica1, OSD_replica2]
```

How CRUSH works:
1. **Input**: Object name is hashed to a 32-bit value
2. **Placement Group mapping**: Hash maps object → one of N placement groups (PGs)
   ```
   PG_id = hash(object_name) mod num_PGs
   ```
3. **CRUSH tree traversal**: The PG ID is fed into CRUSH along with the cluster map (a hierarchy of buckets: root → datacenter → rack → host → OSD). CRUSH walks the tree, selecting an OSD at each level using a pseudo-random function seeded by the PG ID.
4. **Output**: An ordered list of OSDs responsible for that PG

```
                    ┌─────────┐
                    │  Root   │
                    └────┬────┘
               ┌─────────┼─────────┐
          ┌────┴───┐ ┌───┴────┐ ┌──┴─────┐
          │ DC-1   │ │ DC-2   │ │ DC-3   │    ← Datacenters
          └───┬────┘ └───┬────┘ └───┬────┘
           ┌──┴──┐    ┌──┴──┐    ┌──┴──┐
          Rack-1 Rack-2 ...  ...  ...  ...     ← Racks
           ┌┴┐    ┌┴┐
         H1  H2  H3  H4                        ← Hosts
         │   │   │   │
        OSD OSD OSD OSD                        ← Object Storage Daemons
```

Key properties of CRUSH:
- **Deterministic**: Any client with the cluster map computes the same placement — no central directory needed
- **Pseudo-random**: Distributes data uniformly across OSDs
- **Topology-aware**: Placement rules enforce fault domains (e.g., "no two replicas on the same rack")
- **Minimal data movement**: Adding/removing an OSD only moves data proportional to that OSD's share

#### Placement Groups (PGs)

PGs are the critical **indirection layer** between objects and OSDs:

```
Objects (millions)  ──→  Placement Groups (thousands)  ──→  OSDs (hundreds)

  obj_1 ─┐                   ┌── PG 1.0 ──→ [OSD.4, OSD.12, OSD.7]
  obj_2 ──┼──→ hash mod N ──→├── PG 1.1 ──→ [OSD.1, OSD.9, OSD.15]
  obj_3 ──┤                   ├── PG 1.2 ──→ [OSD.3, OSD.11, OSD.6]
  obj_4 ──┘                   └── PG 1.3 ──→ [OSD.8, OSD.2, OSD.14]
  ...                              ...
```

#### Erasure Coding in Ceph

Ceph supports erasure-coded pools as an alternative to replication:

```
Replication (3×):     [D] [D] [D]           → 3× overhead
Erasure Code (4+2):   [D1][D2][D3][D4][P1][P2]  → 1.5× overhead
                       └── 4 data ──┘└ 2 parity ┘
                       Can lose any 2 shards and reconstruct
```

#### Mossaic Takeaway

**CRUSH-like deterministic placement maps directly to DO addressing.** Instead of maintaining a central directory, Mossaic can compute placement from the content hash and a cluster topology map. This eliminates a metadata bottleneck and enables any node to independently determine where data lives.

### 7.3 Google GFS / Colossus

#### GFS (First Generation, 2003)

```
                    ┌──────────────┐
                    │   GFS Master │  ← Single master: all metadata
                    │  (metadata)  │    - file→chunk mapping
                    └──────┬───────┘    - chunk→chunkserver mapping
                           │            - namespace, ACLs
              ┌────────────┼────────────┐
         ┌────┴────┐  ┌────┴────┐  ┌────┴────┐
         │ Chunk   │  │ Chunk   │  │ Chunk   │
         │ Server 1│  │ Server 2│  │ Server 3│
         └─────────┘  └─────────┘  └─────────┘
              │             │             │
         [64MB chunks] [64MB chunks] [64MB chunks]
```

Key design decisions:
- **64MB chunk size**: Reduces metadata, amortizes TCP overhead, enables sequential reads
- **Single master**: Simple but bottleneck at scale (~100M files limit)
- **3× replication**: Simple, fast reads, but expensive (3× raw storage cost)

#### Colossus (Second Generation, 2010+)

Colossus addressed every major GFS limitation:

- **Distributed Metadata**: Curators (stateless) + BigTable-backed metadata (vs single master)
- **Variable Chunk Sizes**: 1MB to 8MB (vs fixed 64MB)
- **Reed-Solomon Erasure Coding**: Reduced overhead from 3.0× to ~1.5×

#### Colossus Encoding Schemes (Approximate)

| Scheme | Data Shards | Parity Shards | Overhead | Fault Tolerance |
|--------|-------------|---------------|----------|-----------------|
| RS(3,3) | 3 | 3 | 2.0× | Any 3 failures |
| RS(6,3) | 6 | 3 | 1.5× | Any 3 failures |
| RS(9,3) | 9 | 3 | 1.33× | Any 3 failures |

#### Mossaic Takeaway

**Start with replication for simplicity, plan migration to Reed-Solomon for cost savings at scale.** Google's evolution from GFS to Colossus is the exact path Mossaic should follow:
1. **Phase 1** (MVP): 3× replication — simple, fast, easy to reason about
2. **Phase 2** (scale): RS(4,2) or RS(6,3) — 1.5× overhead with same fault tolerance
3. **Phase 3** (optimization): Variable encoding schemes based on data temperature

### 7.4 Facebook Haystack / f4

#### Overview

Haystack (2010) and f4 (2014) are Facebook's purpose-built photo storage systems. Haystack was designed to solve a specific problem: POSIX filesystems are terrible at serving billions of small, write-once, read-often files (photos).

#### The Problem Haystack Solved

```
POSIX photo read (NFS):
  1. Open directory → inode lookup (disk seek)
  2. Read directory entry → find file inode (disk seek)
  3. Read file inode → get data block pointers (disk seek)
  4. Read data blocks (disk seek)
  = 3-4 disk seeks per photo (at ~10ms each = 30-40ms)
```

#### The Volume Abstraction

Haystack eliminates per-file metadata by packing photos into large **volumes** (100GB files):

```
Volume File (on disk):
┌─────────┬─────────┬─────────┬─────────┬──────┐
│ Photo 1 │ Photo 2 │ Photo 3 │ Photo 4 │ ...  │  ← Sequential on disk
│ offset:0│ off:120K│ off:350K│ off:480K│      │
└─────────┴─────────┴─────────┴─────────┴──────┘

Volume Index (in memory):
┌──────────┬────────┬──────┐
│ photo_key│ offset │ size │
├──────────┼────────┼──────┤
│ 12345    │ 0      │ 120K │
│ 12346    │ 120K   │ 230K │
│ 12347    │ 350K   │ 130K │
│ 12348    │ 480K   │ 95K  │
└──────────┴────────┴──────┘
```

#### f4: Warm/Cold Tiering

```
Photo lifecycle:
  Day 1-7:    HOT    → Haystack (3× replication, 3.0× overhead)
  Day 7-90:   WARM   → f4 (XOR coding, 1.4× overhead)
  Day 90+:    COLD   → f4 (Reed-Solomon, 1.14× overhead)
```

| Tier | Method | Overhead | Relative Cost |
|------|--------|----------|---------------|
| Hot | 3× replication | 3.0× | 1.0× (baseline) |
| Warm | XOR coding | ~1.4× | 0.47× |
| Cold | Reed-Solomon | ~1.14× | 0.38× |

#### Mossaic Takeaway

**Haystack's direct-offset approach is ideal for Mossaic's storage layer.** Mossaic's data shares Haystack's key property: **write-once, read-many**. Key adoption points:
- **Volume-based storage**: Pack multiple objects into large volume files
- **In-memory index**: `hash → (volume_id, offset, size)` — one disk seek per read
- **Temperature-based tiering**: Hot = replication; warm/cold = erasure coding

### 7.5 MinIO

#### Overview

MinIO is a high-performance, S3-compatible object storage system designed for cloud-native workloads. It's open source (AGPLv3), written in Go, and designed for simplicity.

#### Per-Object Erasure Coding

MinIO's key differentiator: erasure coding is applied **per-object**, not per-volume or per-disk:

```
Example with 16 drives, EC(8,8):
  ┌─────────────────────────────────────────────┐
  │ Object "photo.jpg" (1MB)                     │
  │                                               │
  │  Encode with RS(8,8):                         │
  │  [D1][D2][D3][D4][D5][D6][D7][D8]  ← data   │
  │  [P1][P2][P3][P4][P5][P6][P7][P8]  ← parity │
  │                                               │
  │  Can lose ANY 8 of 16 drives                  │
  └─────────────────────────────────────────────┘
```

#### Bitrot Protection

Every object shard is protected with **HighwayHash** verification:

```
Write path:
  1. Object data arrives
  2. RS encode → data shards + parity shards
  3. Each shard: hash = HighwayHash(shard_data)
  4. Write shard + hash to disk

Read path:
  1. Read shard from disk
  2. Compute HighwayHash(shard_data)
  3. Compare with stored hash
  4. If mismatch → shard is corrupt → reconstruct from EC
  5. Automatically heal corrupted shard (write back correct data)
```

#### Mossaic Takeaway

**Per-object erasure coding and S3-compatible API are the right choices for Mossaic.** MinIO demonstrates that per-object EC is practical and provides superior granularity compared to per-volume approaches.

### 7.6 Comparison Table

| Feature | IPFS | Ceph | GFS/Colossus | Haystack/f4 | MinIO |
|---------|------|------|-------------|-------------|-------|
| **Primary Use** | Decentralized file sharing | General distributed storage | Google internal FS | Facebook photo storage | S3-compatible object store |
| **Addressing** | Content-addressed (CID) | CRUSH deterministic | Master-directed | Volume + offset | Bucket/key (S3) |
| **Metadata** | DHT (distributed) | CRUSH + Monitors | Single master (GFS) / Distributed (Colossus) | Directory service | Inline with data |
| **Chunk Size** | 256KB default | 4MB default | 64MB (GFS) / 1-8MB (Colossus) | Entire photo (variable) | Entire object or multipart 5MB+ |
| **Redundancy** | Replication (pinning) | Replication or EC | 3× (GFS) / RS (Colossus) | 3× hot / XOR warm / RS cold | Per-object EC |
| **Storage Overhead** | N× (number of pins) | 3× (replicated) or 1.5× (EC) | 3× → ~1.5× | 3× → 1.4× → 1.14× | 1.33×–2.0× |
| **Integrity** | Merkle DAG (hash tree) | Scrubbing + checksums | Chunk checksums | Cookie verification | HighwayHash per-shard |
| **Recovery** | Re-pin from other peers | Automated PG re-replication | Chunk re-replication | Volume-level re-replication | Per-object EC rebuild |
| **Deduplication** | Native (content-addressed) | Manual/application-level | None | None | None built-in |
| **API Standard** | IPFS HTTP Gateway | RADOS / S3 (via RGW) | Proprietary internal | Proprietary internal | S3-compatible |
| **Scale** | Millions of nodes (P2P) | Exabytes (enterprise) | Exabytes (Google) | Hundreds of PB (Facebook) | Petabytes |
| **Open Source** | Yes (MIT/Apache) | Yes (LGPL) | No | No | Yes (AGPLv3) |

### 7.7 Synthesis: Key Lessons for Mossaic

#### What Mossaic Should Adopt

| Lesson | Source System | Application to Mossaic |
|--------|-------------|----------------------|
| **Content addressing** | IPFS | Use content hashes as identifiers — natural dedup, self-verifying |
| **Merkle DAG integrity** | IPFS | Large objects as chunk trees — partial verification, incremental sync |
| **CRUSH-like placement** | Ceph | Deterministic `f(hash, topology) → node_list` — no central directory |
| **Placement groups** | Ceph | Group objects for manageable replication/recovery units |
| **Start replication, migrate to RS** | GFS → Colossus | Phase 1: 3× replication. Phase 2: RS(4,2) at 1.5× overhead |
| **Volume-based packing** | Haystack | Pack objects into volume files — eliminate per-file metadata overhead |
| **Direct offset reads** | Haystack | `(volume_id, offset, size)` → single disk seek per read |
| **Temperature tiering** | f4 | Hot=replication, warm=XOR, cold=RS — cost optimization |
| **Per-object EC** | MinIO | Each object independently erasure-coded — granular recovery |
| **Bitrot protection** | MinIO | Hash every shard, verify on read, auto-heal on mismatch |
| **S3-compatible API** | MinIO | External API should be S3-compatible for ecosystem leverage |

#### Mossaic's Hybrid Architecture (Recommended)

```
Layer 1 — Addressing:     Content-addressed CIDs (from IPFS)
Layer 2 — Placement:      CRUSH-like deterministic mapping (from Ceph)
Layer 3 — Physical Store: Volume-packed objects with offset index (from Haystack)
Layer 4 — Redundancy:     Per-object RS encoding (from MinIO)
Layer 5 — Tiering:        Hot/warm/cold with variable encoding (from f4)
Layer 6 — API:            S3-compatible external interface (from MinIO)
Layer 7 — Integrity:      Merkle DAG + per-shard hashing (from IPFS + MinIO)
```

#### What Mossaic Should NOT Adopt

| Anti-pattern | Source | Why Not |
|-------------|--------|---------|
| Bitswap tit-for-tat | IPFS | Mossaic is federated, not adversarial P2P |
| Single metadata master | GFS | Known bottleneck; use distributed approach from day 1 |
| Fixed 64MB chunks | GFS | Objects vary in size; need variable chunking |
| POSIX filesystem per-object | Pre-Haystack | Too many metadata seeks; use volume packing |
| AGPLv3 license constraints | MinIO | Mossaic should use MIT/Apache-licensed components |

---

## Section 8: Cloudflare DO Constraints

### 8.1 DO Storage Limits

#### SQLite-Backed DOs (Recommended — New Default)

| Resource                    | Limit                                       |
|-----------------------------|---------------------------------------------|
| Storage per DO              | **10 GB**                                   |
| Storage per account         | **Unlimited** (paid plan)                   |
| Number of DOs per account   | **Unlimited**                               |
| Max row/BLOB/string size    | **2 MB**                                    |
| Max columns per table       | 100                                         |
| Max rows per table          | Unlimited (within 10 GB per-DO limit)       |
| Max SQL statement length    | 100 KB                                      |
| DO classes per account      | 500 (paid) / 100 (free)                     |

#### Key-Value Backed DOs (Legacy)

| Resource                    | Limit                                       |
|-----------------------------|---------------------------------------------|
| Storage per DO              | **Unlimited**                               |
| Storage per account         | **50 GB** (can request increase)            |
| Key size                    | **2 KiB** (2048 bytes)                      |
| Value size                  | **128 KiB** (131,072 bytes)                 |

#### Implications for Mossaic

- **SQLite backend is superior** for chunked storage: 2 MB max BLOB vs 128 KiB max value in KV.
- A single DO can hold up to **10 GB** of chunks — enough for thousands of 1 MB chunks or millions of small metadata entries.
- With unlimited DOs per account, horizontal scaling is effectively unbounded.
- **Key insight**: The 2 MB BLOB limit in SQLite means chunks up to ~2 MB can be stored inline. Larger chunks must be split or stored in R2.

### 8.2 DO Pricing Model

#### Compute Costs

| Component       | Free Tier (daily)     | Paid Plan                                          |
|-----------------|-----------------------|----------------------------------------------------|
| Requests        | 100,000/day           | 1M included, then **$0.15 per million**            |
| Duration        | 13,000 GB-s/day       | 400,000 GB-s included, then **$12.50/M GB-s**      |
| WebSocket msgs  | Counted as requests   | 20:1 billing ratio (20 WS msgs = 1 billed request) |

> **Note**: Each DO is allocated 128 MB memory for billing purposes, regardless of actual usage.
> Duration = wall-clock seconds × 0.128 GB.

#### Storage Costs (SQLite Backend)

| Component       | Free Tier             | Paid Plan                                          |
|-----------------|-----------------------|----------------------------------------------------|
| Rows read       | 5M/day                | 25B/month included, then **$0.001/million rows**   |
| Rows written    | 100K/day              | 50M/month included, then **$1.00/million rows**    |
| Stored data     | 5 GB total            | 5 GB-month included, then **$0.20/GB-month**       |

#### Cost Per Chunk (Mossaic Estimates)

Assuming **1 MB chunks** stored in SQLite-backed DOs:

```
WRITE one 1 MB chunk:
  - 1 row written                          = $1.00 / 1M rows = $0.000001
  - 1 request to DO                        = $0.15 / 1M      = $0.00000015
  - Storage: 1 MB × $0.20/GB-month         = $0.000195/month
  ─────────────────────────────────────────
  Total write cost per chunk:              ~$0.000001
  Ongoing storage per chunk:               ~$0.0002/month

READ one 1 MB chunk:
  - 1 row read                             = $0.001 / 1M rows = $0.000000001
  - 1 request to DO                        = $0.15 / 1M       = $0.00000015
  ─────────────────────────────────────────
  Total read cost per chunk:               ~$0.00000015
```

**Verdict**: DO storage at $0.20/GB-month is expensive for bulk data. Compare to R2 at $0.015/GB-month — **13× cheaper**.

### 8.3 DO Concurrency Model

#### Single-Threaded Execution

Durable Objects are **inherently single-threaded**. Only one JavaScript execution context runs at a time per DO instance.

```
                    ┌─────────────────────────┐
  Request A ──────►│                           │
  Request B ──────►│   Queue    ──►  [DO]      │──► Response A
  Request C ──────►│  (FIFO)        (single    │──► Response B
                    │               threaded)   │──► Response C
                    └─────────────────────────┘
```

**Key properties**:
- Requests are queued if the DO is busy processing another request.
- Soft throughput limit: **~1,000 requests/second** per individual DO.
- Overloaded DOs return `overloaded` error to callers.
- Async I/O (fetch, storage calls) does NOT block the thread — other requests CAN interleave at `await` points.

#### Input Gate / Output Gate

DOs provide **automatic consistency guarantees** through gates:

- **Input Gate**: After a storage write, no new requests are delivered until the write is confirmed durable.
- **Output Gate**: After a storage write, outgoing fetch/responses are held until the write is confirmed.

```
  Request arrives ──► Input Gate ──► Handler executes
                                         │
                                    storage.put()
                                         │
                                    Output Gate ──► Response sent
                                    (waits for     (only after
                                     durability)    confirmed)
```

#### Implications for Hot Chunks

If a single chunk is extremely popular, all reads route to **one DO** — creating a bottleneck:

```
  1000 concurrent readers
         │
         ▼
  ┌──────────────┐
  │  Chunk DO    │  ◄── Single-threaded bottleneck!
  │  (1 instance)│      Max ~1000 req/s
  └──────────────┘
```

**Solutions**:
1. **Read-replica fan-out**: Replicate hot chunks across N DOs, route reads randomly.
2. **Cache in front**: Use Workers KV or R2 cache to serve reads without hitting the DO.
3. **Don't store data in DOs at all**: Use R2 for data, DOs only for metadata/coordination.

### 8.4 DO Hibernation

#### Lifecycle

```
  Request ──► DO Created/Woken ──► Active ──► Idle ──► Hibernated ──► Evicted
                                     │                      │
                                     │                      ├── WebSockets survive!
                                     │                      └── In-memory state LOST
                                     │
                                     └── Not billed for duration while
                                         hibernation-eligible (even before
                                         actual hibernation occurs)
```

#### Key Facts

| Property                    | Value                                          |
|-----------------------------|-------------------------------------------------|
| Time to hibernation         | **Seconds after last activity** (not billed while eligible) |
| Cold start latency          | **~50-100ms** (varies by region/load)           |
| In-memory state             | **Lost on hibernation** — must reload from storage |
| WebSocket connections       | **Survive hibernation** (Hibernatable WebSocket API) |
| Alarm wake-ups              | Supported — schedule future invocations         |
| Duration billing            | **Stops** when hibernation-eligible             |

#### Hibernatable WebSocket API

```javascript
// WebSocket handlers that survive hibernation
export class ChunkCoordinator extends DurableObject {
  async webSocketMessage(ws, message) {
    // Called when message arrives — DO wakes from hibernation
    // Process message, then DO can hibernate again
  }

  async webSocketClose(ws, code, reason, wasClean) {
    // Called when WebSocket closes
  }
}
```

- Without hibernation API: DO stays active (and billed) for entire WebSocket lifetime.
- With hibernation API: DO sleeps between messages — **massive cost savings**.

#### Alarms for Background Work

```javascript
export class ChunkManager extends DurableObject {
  async alarm() {
    // Runs at scheduled time — even if no active connections
    // Use for: garbage collection, compaction, replication
    await this.compactChunkIndex();

    // Schedule next alarm
    await this.ctx.storage.setAlarm(Date.now() + 3600_000); // 1 hour
  }
}
```

- Alarm handler wall time limit: **15 minutes**.
- Use for: periodic compaction, chunk replication, index rebuilding, GC of orphaned chunks.

### 8.5 Geographic Locality

#### DO Placement Model

Each DO instance runs in **exactly one data center** globally. It does not replicate.

```
                    ┌──────────────────────────────────────────┐
                    │           Cloudflare Global Network       │
                    │                                          │
  US User ─────────┼──► [Edge Worker] ──► DO (lives in US-East)│
                    │                         ▲                │
  EU User ─────────┼──► [Edge Worker] ────────┘                │
                    │        (cross-region latency!)           │
                    │                                          │
  Asia User ───────┼──► [Edge Worker] ────────┘                │
                    │        (even more latency!)              │
                    └──────────────────────────────────────────┘
```

#### Placement Rules

| Scenario                        | DO Location                                    |
|---------------------------------|-------------------------------------------------|
| First request (no hint)         | Near the first caller's location                |
| With `locationHint`             | Attempts to place near specified region          |
| After creation                  | **Fixed** — does not migrate                    |

Available location hints: `wnam`, `enam`, `weur`, `eeur`, `apac`, `oc`, `afr`, `me`.

#### Latency Implications

```
  Chunk DO in US-East
  ├── US-East reader:    ~5ms   (local)
  ├── US-West reader:    ~40ms  (cross-country)
  ├── EU reader:         ~80ms  (transatlantic)
  └── Asia reader:       ~200ms (transpacific)
```

#### Solutions

1. **R2/KV Cache Layer**: Cache chunk data at the edge. R2 already has global distribution.
2. **Regional Replica DOs**: Create read-only copies in different regions.
3. **Strategic Placement**: Use `locationHint` to place DOs near their primary consumers.
4. **Tiered Architecture**: Metadata DOs per-region, bulk data in R2 (globally cached).

### 8.6 Architecture Implications for Mossaic

#### Core Design Decision: What Goes in DOs vs R2?

```
┌─────────────────────────────────────────────────────────────────┐
│                     MOSSAIC ARCHITECTURE                        │
│                                                                 │
│  ┌─────────────┐    ┌──────────────────┐    ┌───────────────┐  │
│  │   Client     │───►│  Edge Worker      │───►│  Router DO    │  │
│  │  (Browser/   │    │  (Cloudflare      │    │  (Metadata +  │  │
│  │   CLI)       │◄───│   Workers)        │◄───│   Routing)    │  │
│  └─────────────┘    └──────────────────┘    └───────┬───────┘  │
│                                                      │          │
│                              ┌───────────────────────┼────────┐ │
│                              │                       │        │ │
│                              ▼                       ▼        │ │
│                     ┌──────────────┐       ┌──────────────┐   │ │
│                     │  Manifest DO │       │   R2 Bucket   │   │ │
│                     │  (Index,     │       │  (Bulk chunk  │   │ │
│                     │   Tree,      │       │   storage)    │   │ │
│                     │   Metadata)  │       │               │   │ │
│                     └──────────────┘       └──────────────┘   │ │
│                              │                       ▲        │ │
│                              │    chunk refs         │        │ │
│                              └───────────────────────┘        │ │
│                                                               │ │
│                     ┌──────────────────────────────────────┐  │ │
│                     │  Coordination DOs (per-shard)        │  │ │
│                     │  - Lock management                   │  │ │
│                     │  - Write ordering                    │  │ │
│                     │  - Deduplication index                │  │ │
│                     │  - Garbage collection state           │  │ │
│                     └──────────────────────────────────────┘  │ │
└───────────────────────────────────────────────────────────────┘ │
```

#### Hybrid Architecture: R2 for Data, DOs for Coordination

| Concern              | Durable Objects                        | R2                                    |
|----------------------|----------------------------------------|---------------------------------------|
| Storage cost         | $0.20/GB/month                         | **$0.015/GB/month** (13× cheaper)     |
| Egress cost          | Included in request pricing            | **Free** (zero egress fees)           |
| Max object size      | 2 MB (SQLite BLOB)                     | **5 TB** per object                   |
| Global distribution  | Single region                          | **Globally distributed** (cached)     |
| Consistency          | **Strong** (single-threaded + gates)   | Eventual (strong read-after-write)    |
| Throughput           | ~1,000 req/s per DO                    | **Very high** (distributed)           |
| Random access        | **Excellent** (SQL queries)            | Range requests only                   |
| Transactions         | **Yes** (SQLite transactions)          | No                                    |

#### Recommended DO Roles in Mossaic

**1. Manifest DO (per-repository/dataset)**

```
┌─────────────────────────────────────────┐
│            Manifest DO                   │
│                                          │
│  SQLite Tables:                          │
│  ┌────────────────────────────────────┐  │
│  │ chunks                             │  │
│  │  hash TEXT PRIMARY KEY             │  │
│  │  r2_key TEXT                       │  │
│  │  size INTEGER                      │  │
│  │  ref_count INTEGER                 │  │
│  │  created_at INTEGER               │  │
│  └────────────────────────────────────┘  │
│  ┌────────────────────────────────────┐  │
│  │ trees                              │  │
│  │  path TEXT                         │  │
│  │  hash TEXT (FK → chunks)           │  │
│  │  mode INTEGER                      │  │
│  └────────────────────────────────────┘  │
│  ┌────────────────────────────────────┐  │
│  │ refs                               │  │
│  │  name TEXT PRIMARY KEY             │  │
│  │  target_hash TEXT                  │  │
│  └────────────────────────────────────┘  │
│                                          │
│  Storage: ~10-100 MB for large repos     │
│  Well within 10 GB limit                 │
└─────────────────────────────────────────┘
```

**2. Coordination DO (per-shard or per-write-session)**

```
Purpose: Serialize concurrent writes, dedup detection, lock management

┌──────────────────────────────────────┐
│        Coordination DO                │
│                                       │
│  - Write lock acquisition             │
│  - Chunk dedup check (bloom filter)   │
│  - Upload session tracking            │
│  - Garbage collection scheduling      │
│                                       │
│  Lightweight — minimal storage        │
│  Hibernates between write sessions    │
└──────────────────────────────────────┘
```

**3. Index/Router DO (global or per-region)**

```
Purpose: Route chunk requests to the right R2 key/location

┌──────────────────────────────────────┐
│         Index DO                      │
│                                       │
│  SQLite: hash → r2_bucket/key mapping │
│                                       │
│  Can shard by hash prefix:           │
│  - DO_0x00-0x3F (quarter of space)   │
│  - DO_0x40-0x7F                      │
│  - DO_0x80-0xBF                      │
│  - DO_0xC0-0xFF                      │
└──────────────────────────────────────┘
```

### 8.7 Cost Analysis

#### Scenario: 1 Million 1 MB Files (1 TB Total)

**Option A: Store Everything in DO Storage**

```
Storage: 1,000 GB × $0.20/GB-month                    = $200.00/month
Writes: 1,000,000 rows × $1.00/M + 1M requests × $0.15/M = $1.15
Reads (10M/month): 10M rows × $0.001/M + 10M req × $0.15/M = $1.51
                                                ─────────────
TOTAL (Option A):                              ~$202.66/month
```

**Option B: Hybrid — R2 for Data, DOs for Metadata**

```
R2 Storage: 1,000 GB × $0.015/GB-month                = $15.00/month
R2 Operations: 1M writes × $4.50/M + 10M reads × $0.36/M = $8.10
DO Storage (metadata ~50 MB): 0.05 GB × $0.20/GB      = $0.01/month
DO Requests: 11M × $0.15/M                             = $1.65
                                                ─────────────
TOTAL (Option B):                              ~$24.76/month
```

**Option C: R2 Only (No DOs)**

```
R2 Storage: 1,000 GB × $0.015/GB-month                = $15.00/month
R2 Operations: 1M writes × $4.50/M + 10M reads × $0.36/M = $8.10
                                                ─────────────
TOTAL (Option C):                              ~$23.10/month
```

#### Cost Comparison Summary

```
┌─────────────────────────────────────────────────────────────┐
│              MONTHLY COST: 1 TB, 10M reads                  │
│                                                             │
│  Option A: All in DOs         ████████████████████  $202.66 │
│  Option B: Hybrid (DO+R2)     ██░                   $24.76  │
│  Option C: R2 Only            ██░                   $23.10  │
│                                                             │
│  Option B adds ~$1.66/month for DO coordination             │
│  but provides: transactions, strong consistency,            │
│  dedup index, write serialization, GC coordination          │
│                                                             │
│  ► RECOMMENDED: Option B (Hybrid)                           │
└─────────────────────────────────────────────────────────────┘
```

#### Cost at Scale

| Scale          | DO Only     | Hybrid (DO+R2) | R2 Only    |
|----------------|-------------|-----------------|------------|
| 1 TB (1M files)| $203/mo     | **$25/mo**      | $23/mo     |
| 10 TB          | $2,003/mo   | **$175/mo**     | $154/mo    |
| 100 TB         | $20,003/mo  | **$1,520/mo**   | $1,504/mo  |
| 1 PB           | $200,003/mo | **$15,020/mo**  | $15,004/mo |

### 8.8 Design Recommendations for Mossaic

#### Architecture Decision Record

```
DECISION: Hybrid R2 + Durable Objects Architecture

  ┌──────────────────────────────────────────────────────────┐
  │                    Cloudflare Workers                     │
  │                    (Edge — Global)                        │
  │                         │                                │
  │           ┌─────────────┼──────────────┐                 │
  │           │             │              │                 │
  │           ▼             ▼              ▼                 │
  │    ┌───────────┐ ┌───────────┐ ┌────────────┐           │
  │    │ Manifest  │ │ Coord DO  │ │ Index DOs  │           │
  │    │ DOs       │ │ (write    │ │ (hash→R2   │           │
  │    │ (per-repo │ │  locks,   │ │  key map,  │           │
  │    │  trees,   │ │  sessions,│ │  sharded   │           │
  │    │  refs,    │ │  dedup)   │ │  by hash   │           │
  │    │  commits) │ │           │ │  prefix)   │           │
  │    └─────┬─────┘ └─────┬─────┘ └─────┬──────┘           │
  │          │             │             │                   │
  │          └─────────────┼─────────────┘                   │
  │                        │                                 │
  │                        ▼                                 │
  │              ┌──────────────────┐                        │
  │              │       R2         │                        │
  │              │  (Bulk chunk     │                        │
  │              │   storage)       │                        │
  │              │  Key: chunks/{h} │                        │
  │              │  $0.015/GB/mo    │                        │
  │              │  Zero egress     │                        │
  │              └──────────────────┘                        │
  └──────────────────────────────────────────────────────────┘
```

#### Data Flow: Write Path

```
  Client                Worker              Coord DO            R2
    │                     │                    │                 │
    │── PUT /chunk ──────►│                    │                 │
    │   (hash + data)     │                    │                 │
    │                     │── acquire lock ───►│                 │
    │                     │◄── lock granted ───│                 │
    │                     │── dedup check ────►│                 │
    │                     │◄── not found ──────│                 │
    │                     │── PUT chunks/{h} ──────────────────►│
    │                     │◄── 200 OK ─────────────────────────│
    │                     │── record chunk ───►│                 │
    │                     │◄── confirmed ──────│                 │
    │                     │── release lock ───►│                 │
    │◄── 201 Created ─────│                    │                 │
```

#### Data Flow: Read Path (Optimized)

```
  Client                Worker              R2 (+ edge cache)
    │                     │                    │
    │── GET /chunk/{h} ──►│                    │
    │                     │── GET chunks/{h} ─►│
    │                     │◄── 200 + data ─────│  (cache hit: ~5ms)
    │◄── 200 + data ──────│                    │  (cache miss: ~50ms)

  NOTE: Read path SKIPS DOs entirely!
  DOs only involved in writes and metadata queries.
```

#### Key Design Principles

1. **DOs for coordination, R2 for data** — 13× cheaper storage, no single-threaded bottleneck on reads.
2. **Content-addressed keys in R2** — `chunks/{sha256}` enables natural deduplication and cache-friendliness.
3. **Shard Index DOs by hash prefix** — Distribute metadata load across multiple DOs.
4. **Hibernation everywhere** — All DOs should use Hibernatable WebSocket API and hibernate aggressively.
5. **Alarms for background work** — GC, compaction, replication checks run on alarm schedules.
6. **Location hints for write affinity** — Place Coord DOs near primary write sources.
7. **Skip DOs on read path** — Read directly from R2 with edge caching.
8. **Variable-size CDC chunks (~1 MB average)** — Good dedup granularity without excessive metadata overhead.

### 8.9 Risk Factors & Mitigations

| Risk                                    | Impact   | Mitigation                                           |
|-----------------------------------------|----------|------------------------------------------------------|
| Single DO overloaded (>1000 rps)        | High     | Shard by hash prefix; cache reads via R2             |
| DO cold start latency (~50-100ms)       | Medium   | Keep hot DOs warm with alarms; cache reads in R2     |
| DO placed in wrong region               | Medium   | Use `locationHint`; regional replica DOs             |
| 10 GB per-DO storage limit              | Low      | Shard metadata across multiple DOs                   |
| SQLite 2 MB BLOB limit                  | Low      | Store chunks in R2 (no size limit); DOs for metadata |
| DO storage pricing changes              | Medium   | Architecture already minimizes DO storage usage      |
| Single point of failure (Coord DO)      | Medium   | Coordinator is per-shard; losing one ≠ losing all    |
| Network partition between DO and R2     | Low      | Retry with backoff; R2 is highly available           |

### 8.10 Summary

```
┌──────────────────────────────────────────────────────────────┐
│                   KEY TAKEAWAYS                               │
│                                                              │
│  1. DO Storage is 13× more expensive than R2                 │
│     → Use R2 for bulk chunk data                             │
│                                                              │
│  2. DOs are single-threaded with ~1000 rps limit             │
│     → Never put hot-path reads through a single DO           │
│                                                              │
│  3. DOs provide strong consistency + transactions            │
│     → Perfect for metadata, indexes, coordination            │
│                                                              │
│  4. Hibernation makes DOs nearly free when idle              │
│     → Design for bursty coordination, not steady-state       │
│                                                              │
│  5. SQLite backend is the future (recommended by CF)         │
│     → Use SQLite for structured metadata in DOs              │
│                                                              │
│  6. Hybrid architecture is optimal for Mossaic:              │
│     R2 (data) + DOs (metadata/coordination)                  │
│     → ~$25/month for 1 TB vs ~$203/month all-in-DO           │
│                                                              │
│  7. Content-addressed R2 keys enable natural dedup           │
│     and CDN caching — skip DOs on the read path              │
└──────────────────────────────────────────────────────────────┘
```

---

## Section 9: Unified Mossaic Architecture Recommendations

This section synthesizes the individual recommendations from all eight research domains into a single, coherent architecture proposal for Mossaic. Where individual sections made recommendations in isolation, this section resolves conflicts between them and presents a unified design that accounts for all constraints simultaneously.

### 9.1 The Overarching Constraint: Cloudflare's Platform Economics

The most consequential finding across all research is that **Cloudflare's pricing model dictates the architecture**. Durable Object storage costs $0.20/GB/month — 13x more than R2's $0.015/GB/month. This single fact overrides several recommendations made in earlier sections when viewed in isolation:

- **Section 1 (BitTorrent)** recommended storing chunks directly in DOs. This is economically infeasible for bulk data. Chunks must reside in R2.
- **Section 2 (RAID)** mapped DOs as "disks" in a RAID array. In practice, R2 objects serve as the "disks," and DOs serve as the RAID controller (metadata, parity coordination).
- **Section 3 (Erasure Coding)** proposed RS(6,4) with 6 DOs per file. This should be reinterpreted: 6 R2 objects per file (data + parity shards), with a single Manifest DO tracking the mapping.

The unified architecture is therefore: **R2 for all bulk chunk data, DOs exclusively for metadata, coordination, and deduplication indexes.**

### 9.2 The Storage Layer: R2 with Content-Addressed Keys

Every chunk produced by the chunking pipeline is stored in R2 with a content-addressed key: `chunks/{sha256_hex}`. This design draws from IPFS (Section 7.1) for content addressing, MinIO (Section 7.5) for per-object erasure coding, and Haystack (Section 7.4) for write-once storage semantics.

**Chunk sizing** resolves a tension between Sections 1, 2, and 5. BitTorrent analysis (Section 1) recommended adaptive chunk sizes from 64 KiB to 16 MiB based on file size. RAID analysis (Section 2) recommended matching chunk size to access patterns. CDC analysis (Section 5) recommended 128 KB average with FastCDC. The unified recommendation is:

- **Primary chunking: FastCDC with 128 KB average** (Section 5's recommendation). This provides ~75-84% deduplication while keeping chunk counts manageable. For a 5 MB photo, this produces ~40 chunks with negligible metadata overhead.
- **Fallback for very large files (>100 MB)**: Increase the CDC average to 1 MB to reduce chunk counts below the Worker subrequest limit. This aligns with Section 1's size-adaptive approach.
- **No fixed-size chunking**: Even though Section 2 analyzed fixed-size RAID striping, CDC's deduplication advantages (Section 5) outweigh the simplicity of fixed-size blocks for Mossaic's photo-centric workload.

### 9.3 Redundancy: Erasure Coding on R2 Objects

Section 3 recommended Cauchy Reed-Solomon RS(6,4) — 4 data shards plus 2 parity shards — at 1.5x storage overhead. Section 2 validated this from the RAID perspective, showing that dual-parity schemes provide excellent fault tolerance. Section 7 confirmed this is industry standard (Ceph, Colossus, f4, MinIO all use RS coding for warm/cold data). Section 8 adds the cost constraint: at R2 pricing ($0.015/GB), 1.5x overhead costs $0.0225/GB — still dramatically cheaper than storing a single copy in DOs ($0.20/GB).

The unified erasure coding strategy:

```
Tier 1 — Hot data (first 7 days):
  3x replication in R2 (simple, fast reads from any copy)
  Overhead: 3.0x = $0.045/GB/month
  Rationale: Simplicity during peak access period; matches GFS/Colossus Phase 1

Tier 2 — Warm data (7-90 days):
  RS(6,4) erasure coding — 4 data + 2 parity shards in R2
  Overhead: 1.5x = $0.0225/GB/month
  Rationale: Good balance of redundancy and cost; matches Section 3's primary recommendation

Tier 3 — Cold data (90+ days):
  RS(10,4) erasure coding — or even simpler XOR parity groups
  Overhead: 1.4x or lower
  Rationale: Matches Facebook f4's cold tier approach (Section 7.4)
```

This tiering approach, drawn from Haystack/f4 (Section 7.4) and Colossus (Section 7.3), automatically reduces storage costs as data ages. The Manifest DO tracks each chunk's current tier and triggers tier transitions via scheduled alarms (Section 8.4).

### 9.4 Chunk Placement: Rendezvous Hashing over R2 Key Space

Section 4 recommended rendezvous hashing (HRW) for mapping files to DOs. In the unified architecture, HRW serves a modified role: it determines which **R2 key prefixes** (and thus which geographic regions or erasure coding groups) each file's chunks belong to. The algorithm remains identical — score all candidate locations for a file ID, take the top N — but the "locations" are now R2 placement groups rather than individual DOs.

For metadata DOs (manifests, coordination), HRW still applies directly: `file_id → manifest DO` mapping uses HRW to distribute metadata load across a pool of Manifest DOs.

### 9.5 The Coordination Layer: Durable Objects

DOs serve three roles, as identified in Section 8.6:

1. **Manifest DOs** (per-repository): Store the chunk tree (file → ordered list of chunk hashes), reference tracking, and commit history. SQLite tables provide transactional updates and efficient queries. Estimated storage: 10-100 MB per repository, well within the 10 GB DO limit.

2. **Coordination DOs** (per-shard): Handle write serialization, deduplication checking (via bloom filters), and upload session management. These DOs hibernate aggressively between write sessions, keeping costs near zero during read-dominated periods.

3. **Index DOs** (sharded by hash prefix): Map content hashes to R2 keys. Sharded across 16-256 DOs by the first 1-2 bytes of the hash, ensuring no single DO handles more than ~1/16th of the keyspace.

### 9.6 Transfer Protocol: Adaptive Parallel Fetches

Section 6 provided detailed transfer optimization analysis. Combined with Section 8's DO constraints, the unified transfer strategy is:

- **Downloads (read path)**: Fetch chunks directly from R2 via Workers, bypassing DOs entirely. Use HTTP/2 multiplexing for small files, 4-6 parallel HTTP connections (AIMD-managed) for large files. R2's global CDN caching handles geographic locality (resolving Section 8.5's latency concerns). This eliminates the DO single-threaded bottleneck for reads.

- **Uploads (write path)**: Client streams data to an Edge Worker, which runs FastCDC chunking, computes SHA-256 hashes, checks deduplication against the Coordination DO, writes new chunks to R2, and updates the Manifest DO. For large uploads, WebSocket connections to the Coordination DO (using the Hibernatable API) provide persistent sessions with minimal billing.

- **Pipeline scheduling**: The Worker maintains a 2-3 chunk prefetch buffer. Chunks are verified (SHA-256) on arrival and written to R2 in parallel. The Manifest DO is updated transactionally once all chunks are confirmed stored.

### 9.7 Integrity and Verification

Sections 1, 5, and 7 all independently recommended SHA-256 content hashing. The unified integrity strategy combines:

- **Per-chunk SHA-256** (from BitTorrent BEP 52 and IPFS CIDs): Every chunk's hash is its identity. Verification is automatic on retrieval — if the hash doesn't match, the chunk is corrupt.
- **Merkle tree** (from IPFS and BEP 52): The Manifest DO stores a Merkle tree of chunk hashes. The root hash represents the entire file. This enables partial verification and incremental sync without downloading all chunks.
- **Bitrot detection** (from MinIO): On read, Workers verify chunk integrity against the stored hash. Corrupted chunks trigger automatic reconstruction from erasure coding parity (Section 3) and self-healing writes to R2.

### 9.8 Complete Architecture Diagram

```
┌───────────────────────────────────────────────────────────────────┐
│                         MOSSAIC UNIFIED ARCHITECTURE               │
│                                                                   │
│  ┌──────────┐                                                     │
│  │  Client   │──── HTTPS ────┐                                    │
│  └──────────┘                │                                    │
│                              ▼                                    │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │              Cloudflare Edge Workers (Global)               │   │
│  │                                                            │   │
│  │  ┌─────────────┐  ┌──────────────┐  ┌──────────────────┐  │   │
│  │  │ FastCDC      │  │ SHA-256      │  │ Adaptive         │  │   │
│  │  │ Chunker      │  │ Verifier     │  │ Concurrency Ctrl │  │   │
│  │  │ (128KB avg)  │  │              │  │ (AIMD, 4-6 conn) │  │   │
│  │  └──────────────┘  └──────────────┘  └──────────────────┘  │   │
│  └──────────┬─────────────────┬───────────────┬───────────────┘   │
│             │                 │               │                    │
│       ┌─────▼──────┐   ┌─────▼──────┐  ┌─────▼──────────┐        │
│       │ Manifest   │   │ Coord DO   │  │   R2 Bucket     │        │
│       │ DOs        │   │ (write     │  │  (Bulk data)    │        │
│       │ (Merkle    │   │  locks,    │  │                 │        │
│       │  tree,     │   │  dedup,    │  │  chunks/{hash}  │        │
│       │  refs)     │   │  sessions) │  │  $0.015/GB/mo   │        │
│       │            │   │            │  │  Zero egress    │        │
│       │ via HRW    │   │ Hibernates │  │  CDN cached     │        │
│       └────────────┘   └────────────┘  └─────────────────┘        │
│                                                                   │
│  Data Redundancy:                                                 │
│    Hot:  3x replication in R2                                     │
│    Warm: RS(6,4) — 4 data + 2 parity shards in R2                │
│    Cold: RS(10,4) or XOR parity groups                            │
│                                                                   │
│  Read path:  Client → Worker → R2 (DO bypassed)                   │
│  Write path: Client → Worker → Coord DO → R2 → Manifest DO       │
└───────────────────────────────────────────────────────────────────┘
```

### 9.9 Conflict Resolution Summary

| Conflict | Resolution |
|----------|-----------|
| Section 1 recommended storing chunks in DOs | Overridden by Section 8's cost analysis — chunks go in R2 |
| Section 2 mapped DOs as RAID "disks" | Reinterpreted: R2 objects are the "disks," DOs are the controller |
| Section 3 recommended RS(6,4) across 6 DOs | Adapted: RS(6,4) across 6 R2 objects, one Manifest DO |
| Section 5 recommended 128 KB chunks; Section 1 recommended up to 16 MiB | Unified: 128 KB default (CDC), 1 MB for files >100 MB |
| Section 6 recommended WebSockets for reads | Overridden: reads bypass DOs entirely via R2 CDN caching |
| Section 6 recommended WebSockets for uploads | Retained for large uploads; HTTP/2 for small uploads |
| Section 4 placed chunks via HRW to DOs | Adapted: HRW maps to R2 placement groups and Manifest DOs |

### 9.10 Implementation Phases

**Phase 1 — MVP (Months 1-3)**

- R2 for chunk storage with content-addressed keys
- 3x replication (simple, no erasure coding yet)
- Single Manifest DO per repository (SQLite-backed)
- FastCDC chunking with 128 KB average in Edge Workers
- HTTP/2 parallel chunk fetches (4 connections)
- SHA-256 integrity verification

**Phase 2 — Optimization (Months 4-6)**

- Reed-Solomon RS(6,4) erasure coding for warm data
- Temperature-based tiering (hot → warm transition at 7 days)
- Rendezvous hashing for Manifest DO sharding
- AIMD adaptive concurrency controller
- Deduplication via Coordination DOs (bloom filters)
- WebSocket uploads for large files

**Phase 3 — Scale (Months 7-12)**

- Cold tier with RS(10,4) or higher efficiency coding
- Regional Manifest DO replicas with location hints
- Index DO sharding by hash prefix (16-256 shards)
- S3-compatible API layer
- Automated integrity scrubbing via DO alarms
- WASM-based FastCDC and RS encoding for performance

### 9.11 Key Metrics and Targets

| Metric | Target |
|--------|--------|
| Storage cost (at rest) | ~$0.025/GB/month (1.5x RS overhead on R2) |
| Read latency (edge-local) | < 50 ms TTFB |
| Read latency (cross-continent) | < 250 ms TTFB |
| Write throughput | 50+ MB/s sustained per client |
| Deduplication ratio (photo workload) | 75-85% |
| Fault tolerance | 2 simultaneous node failures |
| Chunk integrity verification | 100% (every read) |
| Cost per 1 TB stored | ~$25/month (hybrid architecture) |

---

*Research compiled: March 7, 2026*
*For the Mossaic project — distributed chunked storage on Cloudflare Durable Objects and R2*
