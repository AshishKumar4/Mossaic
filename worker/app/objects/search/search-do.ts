import { DurableObject } from "cloudflare:workers";
import type { EnvApp as Env } from "@shared/types";
import type { VectorSpace } from "@shared/embedding-types";

/**
 * SearchDO — Durable Object for multi-modal vector storage.
 *
 * Stores embedding vectors as Float32Array blobs in SQLite with a `space` column
 * to separate different vector spaces (e.g. "clip" for image vectors, "text" for
 * document vectors). Implements brute-force cosine similarity search.
 *
 * One instance per user (keyed by "search:{userId}").
 */
export class SearchDO extends DurableObject<Env> {
  sql: SqlStorage;
  private initialized = false;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
  }

  private ensureInit(): void {
    if (this.initialized) return;
    this.initialized = true;

    // Main vectors table with space column for multi-modal support
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS vectors (
        id          TEXT NOT NULL,
        space       TEXT NOT NULL DEFAULT 'text',
        values_blob BLOB NOT NULL,
        dimensions  INTEGER NOT NULL,
        created_at  INTEGER NOT NULL,
        PRIMARY KEY (id, space)
      )
    `);

    // Metadata key-value pairs per vector
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS vector_metadata (
        vector_id   TEXT NOT NULL,
        space       TEXT NOT NULL DEFAULT 'text',
        key         TEXT NOT NULL,
        value       TEXT NOT NULL,
        PRIMARY KEY (vector_id, space, key)
      )
    `);

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS search_config (
        key         TEXT PRIMARY KEY,
        value       TEXT NOT NULL
      )
    `);

    // Migrate: add space column to existing tables if they don't have it.
    // SQLite's ALTER TABLE ADD COLUMN is safe to call — it's a no-op if the column exists.
    try {
      this.sql.exec("ALTER TABLE vectors ADD COLUMN space TEXT NOT NULL DEFAULT 'text'");
    } catch {
      // Column already exists or table was created with it
    }
    try {
      this.sql.exec("ALTER TABLE vector_metadata ADD COLUMN space TEXT NOT NULL DEFAULT 'text'");
    } catch {
      // Column already exists
    }

    // Create index for efficient space-scoped queries
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_vectors_space ON vectors(space)
    `);
  }

  async fetch(request: Request): Promise<Response> {
    this.ensureInit();

    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // Upsert vectors: POST /vectors/upsert
      if (path === "/vectors/upsert" && request.method === "POST") {
        const body = (await request.json()) as {
          vectors: { id: string; values: number[]; metadata?: Record<string, string> }[];
          space?: VectorSpace;
        };
        const space = body.space ?? "text";
        this.upsertVectors(body.vectors, space);
        return Response.json({ ok: true, count: body.vectors.length });
      }

      // Query vectors: POST /vectors/query
      if (path === "/vectors/query" && request.method === "POST") {
        const body = (await request.json()) as {
          vector: number[];
          topK?: number;
          filter?: Record<string, string>;
          space?: VectorSpace;
        };
        const space = body.space ?? "text";
        const results = this.queryVectors(body.vector, body.topK ?? 10, body.filter, space);
        return Response.json({ results });
      }

      // Delete vectors: POST /vectors/delete
      if (path === "/vectors/delete" && request.method === "POST") {
        const body = (await request.json()) as { ids: string[]; space?: VectorSpace };
        const space = body.space ?? "text";
        this.deleteVectors(body.ids, space);
        return Response.json({ ok: true, deleted: body.ids.length });
      }

      // Get config: GET /config
      if (path === "/config" && request.method === "GET") {
        const config = this.getConfig();
        return Response.json(config);
      }

      // Set config: POST /config
      if (path === "/config" && request.method === "POST") {
        const body = (await request.json()) as Record<string, string>;
        this.setConfig(body);
        return Response.json({ ok: true });
      }

      // Stats: GET /stats
      if (path === "/stats" && request.method === "GET") {
        const stats = this.getStats();
        return Response.json(stats);
      }

      // List all vector IDs: GET /vectors/ids
      if (path === "/vectors/ids" && request.method === "GET") {
        const space = url.searchParams.get("space") as VectorSpace | null;
        const ids = this.getAllVectorIds(space ?? undefined);
        return Response.json({ ids });
      }

      return new Response("Not found", { status: 404 });
    } catch (err) {
      const message = err instanceof Error ? err.message : "Internal error";
      return Response.json({ error: message }, { status: 500 });
    }
  }

  private upsertVectors(
    vectors: { id: string; values: number[]; metadata?: Record<string, string> }[],
    space: VectorSpace
  ): void {
    for (const vec of vectors) {
      // Convert number[] to Float32Array blob
      const float32 = new Float32Array(vec.values);
      const blob = float32.buffer;

      // Upsert vector (composite key: id + space)
      this.sql.exec(
        `INSERT INTO vectors (id, space, values_blob, dimensions, created_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(id, space) DO UPDATE SET
           values_blob = excluded.values_blob,
           dimensions = excluded.dimensions`,
        vec.id,
        space,
        blob,
        vec.values.length,
        Date.now()
      );

      // Delete old metadata for this vector+space
      this.sql.exec(
        "DELETE FROM vector_metadata WHERE vector_id = ? AND space = ?",
        vec.id,
        space
      );

      // Insert metadata
      if (vec.metadata) {
        for (const [key, value] of Object.entries(vec.metadata)) {
          this.sql.exec(
            "INSERT INTO vector_metadata (vector_id, space, key, value) VALUES (?, ?, ?, ?)",
            vec.id,
            space,
            key,
            value
          );
        }
      }
    }
  }

  private queryVectors(
    queryVector: number[],
    topK: number,
    filter?: Record<string, string>,
    space?: VectorSpace
  ): { id: string; score: number; metadata?: Record<string, string> }[] {
    // Get vectors from DB, optionally filtered by space
    const query = space
      ? "SELECT id, space, values_blob, dimensions FROM vectors WHERE space = ?"
      : "SELECT id, space, values_blob, dimensions FROM vectors";

    const rows = (space
      ? this.sql.exec(query, space)
      : this.sql.exec(query)
    ).toArray() as { id: string; space: string; values_blob: ArrayBuffer; dimensions: number }[];

    if (rows.length === 0) return [];

    // Pre-compute query vector norm
    let queryNorm = 0;
    for (const v of queryVector) queryNorm += v * v;
    queryNorm = Math.sqrt(queryNorm);

    if (queryNorm === 0) return [];

    // Compute cosine similarity for each stored vector
    const scored: { id: string; space: string; score: number }[] = [];

    for (const row of rows) {
      const stored = new Float32Array(row.values_blob);

      // Skip dimension mismatches (can happen if mixed spaces queried without filter)
      if (stored.length !== queryVector.length) continue;

      // Cosine similarity: dot(a,b) / (|a| * |b|)
      let dot = 0;
      let storedNorm = 0;
      for (let i = 0; i < stored.length; i++) {
        dot += queryVector[i] * stored[i];
        storedNorm += stored[i] * stored[i];
      }
      storedNorm = Math.sqrt(storedNorm);

      const score = storedNorm > 0 ? dot / (queryNorm * storedNorm) : 0;
      scored.push({ id: row.id, space: row.space, score });
    }

    // Sort by score descending
    scored.sort((a, b) => b.score - a.score);

    // Apply filter and collect top-K results
    const results: { id: string; score: number; metadata?: Record<string, string> }[] = [];

    for (const item of scored) {
      if (results.length >= topK) break;

      // Get metadata for this vector+space
      const metaRows = this.sql
        .exec(
          "SELECT key, value FROM vector_metadata WHERE vector_id = ? AND space = ?",
          item.id,
          item.space
        )
        .toArray() as { key: string; value: string }[];

      const metadata: Record<string, string> = {};
      for (const m of metaRows) {
        metadata[m.key] = m.value;
      }

      // Apply filter if provided
      if (filter) {
        let matches = true;
        for (const [key, value] of Object.entries(filter)) {
          if (metadata[key] !== value) {
            matches = false;
            break;
          }
        }
        if (!matches) continue;
      }

      results.push({
        id: item.id,
        score: item.score,
        metadata: Object.keys(metadata).length > 0 ? metadata : undefined,
      });
    }

    return results;
  }

  private deleteVectors(ids: string[], space: VectorSpace): void {
    for (const id of ids) {
      this.sql.exec(
        "DELETE FROM vector_metadata WHERE vector_id = ? AND space = ?",
        id,
        space
      );
      this.sql.exec("DELETE FROM vectors WHERE id = ? AND space = ?", id, space);
    }
  }

  private getConfig(): Record<string, string> {
    const rows = this.sql
      .exec("SELECT key, value FROM search_config")
      .toArray() as { key: string; value: string }[];

    const config: Record<string, string> = {};
    for (const row of rows) {
      config[row.key] = row.value;
    }
    return config;
  }

  private setConfig(entries: Record<string, string>): void {
    for (const [key, value] of Object.entries(entries)) {
      this.sql.exec(
        `INSERT INTO search_config (key, value)
         VALUES (?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
        key,
        value
      );
    }
  }

  private getStats(): {
    vectorCount: number;
    dimensions: number | null;
    spaces: { space: string; count: number; dimensions: number | null }[];
  } {
    const countRow = this.sql
      .exec("SELECT COUNT(*) as cnt FROM vectors")
      .toArray() as { cnt: number }[];

    const dimRow = this.sql
      .exec("SELECT dimensions FROM vectors LIMIT 1")
      .toArray() as { dimensions: number }[];

    // Per-space stats
    const spaceRows = this.sql
      .exec(
        "SELECT space, COUNT(*) as cnt, MIN(dimensions) as dims FROM vectors GROUP BY space"
      )
      .toArray() as { space: string; cnt: number; dims: number }[];

    return {
      vectorCount: countRow[0]?.cnt ?? 0,
      dimensions: dimRow[0]?.dimensions ?? null,
      spaces: spaceRows.map((r) => ({
        space: r.space,
        count: r.cnt,
        dimensions: r.dims ?? null,
      })),
    };
  }

  private getAllVectorIds(space?: VectorSpace): string[] {
    const query = space
      ? "SELECT id FROM vectors WHERE space = ?"
      : "SELECT id FROM vectors";

    const rows = (space
      ? this.sql.exec(query, space)
      : this.sql.exec(query)
    ).toArray() as { id: string }[];

    return rows.map((r) => r.id);
  }
}
