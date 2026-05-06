import { DurableObject } from "cloudflare:workers";
import type { Env } from "@shared/types";

export class ShardDO extends DurableObject<Env> {
  sql: SqlStorage;
  private initialized = false;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
  }

  private ensureInit(): void {
    if (this.initialized) return;
    this.initialized = true;

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS chunks (
        hash          TEXT PRIMARY KEY,
        data          BLOB NOT NULL,
        size          INTEGER NOT NULL,
        ref_count     INTEGER NOT NULL DEFAULT 1,
        created_at    INTEGER NOT NULL
      )
    `);

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS chunk_refs (
        chunk_hash    TEXT NOT NULL,
        file_id       TEXT NOT NULL,
        chunk_index   INTEGER NOT NULL,
        user_id       TEXT NOT NULL,
        PRIMARY KEY (chunk_hash, file_id, chunk_index)
      )
    `);

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS shard_meta (
        key           TEXT PRIMARY KEY,
        value         INTEGER NOT NULL
      )
    `);
  }

  async fetch(request: Request): Promise<Response> {
    this.ensureInit();

    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // Write a chunk: PUT /chunk
      if (path === "/chunk" && request.method === "PUT") {
        const chunkHash = request.headers.get("X-Chunk-Hash") || "";
        const fileId = request.headers.get("X-File-Id") || "";
        const chunkIndex = parseInt(
          request.headers.get("X-Chunk-Index") || "0"
        );
        const userId = request.headers.get("X-User-Id") || "";

        const data = await request.arrayBuffer();

        // Check for dedup
        const existing = this.sql
          .exec("SELECT hash, ref_count FROM chunks WHERE hash = ?", chunkHash)
          .toArray();

        if (existing.length > 0) {
          // Chunk exists — add reference only
          this.sql.exec(
            "UPDATE chunks SET ref_count = ref_count + 1 WHERE hash = ?",
            chunkHash
          );
          this.sql.exec(
            `INSERT OR IGNORE INTO chunk_refs (chunk_hash, file_id, chunk_index, user_id)
             VALUES (?, ?, ?, ?)`,
            chunkHash,
            fileId,
            chunkIndex,
            userId
          );
          return Response.json({
            status: "deduplicated",
            bytesStored: 0,
          });
        }

        // New chunk — store it with parameterized BLOB binding
        this.sql.exec(
          `INSERT INTO chunks (hash, data, size, ref_count, created_at)
           VALUES (?, ?, ?, 1, ?)`,
          chunkHash,
          data,
          data.byteLength,
          Date.now()
        );

        this.sql.exec(
          `INSERT INTO chunk_refs (chunk_hash, file_id, chunk_index, user_id)
           VALUES (?, ?, ?, ?)`,
          chunkHash,
          fileId,
          chunkIndex,
          userId
        );

        // Update capacity tracking
        this.updateCapacity(data.byteLength);

        return Response.json({
          status: "created",
          bytesStored: data.byteLength,
        });
      }

      // Read a chunk: GET /chunk/:hash
      if (path.startsWith("/chunk/") && request.method === "GET") {
        const hash = path.split("/")[2];
        const rows = this.sql
          .exec("SELECT data, size FROM chunks WHERE hash = ?", hash)
          .toArray();

        if (rows.length === 0) {
          return new Response("Chunk not found", { status: 404 });
        }

        const chunk = rows[0] as { data: ArrayBuffer; size: number };

        // Stream the chunk data
        return new Response(chunk.data, {
          headers: {
            "Content-Type": "application/octet-stream",
            "Content-Length": chunk.size.toString(),
            "Cache-Control": "public, max-age=31536000, immutable",
            ETag: `"${hash}"`,
          },
        });
      }

      // Shard stats: GET /stats
      if (path === "/stats" && request.method === "GET") {
        const stats = this.getStats();
        return Response.json(stats);
      }

      // Delete file refs: DELETE /refs/:fileId
      if (path.startsWith("/refs/") && request.method === "DELETE") {
        const fileId = path.split("/")[2];
        const freed = this.removeFileRefs(fileId);
        return Response.json({ freedBytes: freed });
      }

      return new Response("Not found", { status: 404 });
    } catch (err) {
      const message = err instanceof Error ? err.message : "Internal error";
      return Response.json({ error: message }, { status: 500 });
    }
  }

  private removeFileRefs(fileId: string): number {
    let freedBytes = 0;

    const refs = this.sql
      .exec("SELECT chunk_hash FROM chunk_refs WHERE file_id = ?", fileId)
      .toArray();

    for (const ref of refs) {
      const chunkHash = (ref as { chunk_hash: string }).chunk_hash;

      this.sql.exec(
        "DELETE FROM chunk_refs WHERE chunk_hash = ? AND file_id = ?",
        chunkHash,
        fileId
      );

      this.sql.exec(
        "UPDATE chunks SET ref_count = ref_count - 1 WHERE hash = ?",
        chunkHash
      );

      const chunk = this.sql
        .exec(
          "SELECT size FROM chunks WHERE hash = ? AND ref_count <= 0",
          chunkHash
        )
        .toArray();

      if (chunk.length > 0) {
        this.sql.exec("DELETE FROM chunks WHERE hash = ?", chunkHash);
        freedBytes += (chunk[0] as { size: number }).size;
      }
    }

    if (freedBytes > 0) {
      this.updateCapacity(-freedBytes);
    }

    return freedBytes;
  }

  private getStats(): {
    totalChunks: number;
    totalBytes: number;
    uniqueChunks: number;
    totalRefs: number;
    capacityUsed: number;
  } {
    // Total chunks (including dedup references counted by ref_count)
    const chunkAgg = this.sql
      .exec(
        "SELECT COUNT(*) as unique_chunks, COALESCE(SUM(size), 0) as total_bytes, COALESCE(SUM(ref_count), 0) as total_refs FROM chunks"
      )
      .toArray();

    const row = chunkAgg[0] as {
      unique_chunks: number;
      total_bytes: number;
      total_refs: number;
    };

    // Capacity from shard_meta
    const metaRows = this.sql
      .exec(
        "SELECT value FROM shard_meta WHERE key = 'capacity_used_bytes'"
      )
      .toArray();
    const capacityUsed =
      metaRows.length > 0 ? (metaRows[0] as { value: number }).value : 0;

    return {
      totalChunks: row.total_refs,
      totalBytes: row.total_bytes,
      uniqueChunks: row.unique_chunks,
      totalRefs: row.total_refs,
      capacityUsed,
    };
  }

  private updateCapacity(deltaBytes: number): void {
    const existing = this.sql
      .exec(
        "SELECT value FROM shard_meta WHERE key = 'capacity_used_bytes'"
      )
      .toArray();

    if (existing.length === 0) {
      this.sql.exec(
        "INSERT INTO shard_meta (key, value) VALUES ('capacity_used_bytes', ?)",
        Math.max(0, deltaBytes)
      );
    } else {
      this.sql.exec(
        "UPDATE shard_meta SET value = MAX(0, value + ?) WHERE key = 'capacity_used_bytes'",
        deltaBytes
      );
    }
  }
}
