import { DurableObject } from "cloudflare:workers";
import type { EnvCore as Env } from "../../../../shared/types";
import {
  advanceSchemaMaintenance,
  applyMigrationOnce,
  ensureSchemaMaintenance,
  ensureMigrationsTable,
} from "../../lib/migrations";
import { verifyVFSMultipartToken } from "../../lib/auth";
import {
  MULTIPART_FENCE_GC_GRACE_MS,
  MULTIPART_MAX_TTL_MS,
  MULTIPART_STATUS_ENTRY_PAGE_SIZE,
} from "../../../../shared/multipart";

export const SHARD_CLEANUP_PAGE_SIZE = 256;
export const SHARD_CLEANUP_JOURNAL_TTL_MS =
  MULTIPART_MAX_TTL_MS + MULTIPART_FENCE_GC_GRACE_MS;
const LEGACY_CLEANUP_MAX_ROWS = SHARD_CLEANUP_PAGE_SIZE - 1;
const SHARD_SCHEMA_MAINTENANCE_PAGE_SIZE = 256;
const CLEANUP_JOURNAL_MAINTENANCE = "shard_cleanup_journal_lifecycle_v2";
const FENCE_EXPIRY_MAINTENANCE = "multipart_fence_expiry_v1";

export interface DeleteChunksPageResult {
  cursor: number;
  done: boolean;
  processed: number;
  marked: number;
}

export interface ClearMultipartStagingPageResult {
  cursor: number;
  done: boolean;
  dropped: number;
}

const ShardCleanupKind = Object.freeze({
  Refs: "refs",
  Staging: "staging",
} as const);
type ShardCleanupKind =
  (typeof ShardCleanupKind)[keyof typeof ShardCleanupKind];

export class ShardDO extends DurableObject<Env> {
  sql: SqlStorage;
  private initialized = false;

  protected recordRpc(): void {}

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
  }

  private ensureInit(): void {
    if (this.initialized) return;

    const maintenance = this.ctx.storage.transactionSync(() => {
      this.initializeSchema();
      return this.runSchemaMaintenancePage();
    });
    this.initialized = true;
    const nextAlarm = this.nextMaintenanceAlarm(maintenance);
    if (nextAlarm !== null) {
      this.ctx.waitUntil(this.armAlarmAt(nextAlarm));
    }
  }

  private initializeSchema(): void {
    ensureMigrationsTable(this.sql);

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

    // ── VFS GC bookkeeping (sdk-impl-plan §3.2, §8.3) ──────────────────────
    // deleted_at marks chunks pending hard-delete. Set when ref_count first
    // hits 0; the alarm sweeper hard-deletes after a grace
    // period. NULL = live.
    applyMigrationOnce(this.sql, "chunks_add_deleted_at", () =>
      this.sql.exec("ALTER TABLE chunks ADD COLUMN deleted_at INTEGER")
    );
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_chunks_deleted
        ON chunks(deleted_at)
        WHERE deleted_at IS NOT NULL
    `);
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_chunk_refs_file
        ON chunk_refs(file_id)
    `);
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS shard_cleanup_pages (
        cleanup_kind       TEXT NOT NULL,
        ref_id             TEXT NOT NULL,
        cleanup_generation TEXT NOT NULL,
        request_cursor     INTEGER NOT NULL,
        next_cursor        INTEGER NOT NULL,
        processed          INTEGER NOT NULL,
        marked             INTEGER NOT NULL,
        done               INTEGER NOT NULL,
        created_at         INTEGER NOT NULL,
        PRIMARY KEY (
          cleanup_kind, ref_id, cleanup_generation, request_cursor
        )
      )
    `);
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS shard_cleanup_progress (
        cleanup_kind       TEXT NOT NULL,
        ref_id             TEXT NOT NULL,
        cleanup_generation TEXT NOT NULL,
        next_cursor        INTEGER NOT NULL,
        done               INTEGER NOT NULL,
        updated_at         INTEGER NOT NULL,
        PRIMARY KEY (cleanup_kind, ref_id, cleanup_generation)
      )
    `);
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS shard_cleanup_page_expirations (
        expires_at         INTEGER NOT NULL,
        cleanup_kind       TEXT NOT NULL,
        ref_id             TEXT NOT NULL,
        cleanup_generation TEXT NOT NULL,
        request_cursor     INTEGER NOT NULL,
        PRIMARY KEY (
          expires_at, cleanup_kind, ref_id, cleanup_generation, request_cursor
        ),
        UNIQUE (cleanup_kind, ref_id, cleanup_generation, request_cursor)
      ) WITHOUT ROWID
    `);

    // ── multipart staging table ───────────────────────────────
    //
    // Records `(upload_id, chunk_index)` → `chunk_hash` for each chunk
    // landed during a multipart upload. The chunk bytes themselves
    // live in `chunks` and are referenced through `chunk_refs` exactly
    // as for a non-multipart write — the staging table is metadata
    // only, used by UserDO's finalize to verify that every chunk in
    // the client's hash list actually landed and matches.
    //
    // Written in the same DO turn as `chunk_refs` by `putChunkMultipart`,
    // so each per-chunk PUT costs zero extra subrequests.
    //
    // PRIMARY KEY (upload_id, chunk_index) makes re-PUT idempotent —
    // a retry under the same hash is `INSERT OR REPLACE` no-op; a
    // retry with different bytes overwrites and `putChunkMultipart`
    // takes the supersession branch (drops old ref, registers new
    // chunk, replaces this row).
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS upload_chunks (
        upload_id    TEXT NOT NULL,
        chunk_index  INTEGER NOT NULL,
        chunk_hash   TEXT NOT NULL,
        chunk_size   INTEGER NOT NULL,
        user_id      TEXT NOT NULL,
        created_at   INTEGER NOT NULL,
        PRIMARY KEY (upload_id, chunk_index)
      )
    `);
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_upload_chunks_user
        ON upload_chunks(user_id, upload_id)
    `);
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS multipart_fences (
        upload_id  TEXT PRIMARY KEY,
        fence_id   TEXT NOT NULL,
        state      TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      )
    `);
    applyMigrationOnce(this.sql, "multipart_fences_add_expires_at", () =>
      this.sql.exec("ALTER TABLE multipart_fences ADD COLUMN expires_at INTEGER")
    );
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS multipart_fence_expirations (
        expires_at INTEGER NOT NULL,
        upload_id  TEXT NOT NULL,
        PRIMARY KEY (expires_at, upload_id),
        UNIQUE (upload_id)
      ) WITHOUT ROWID
    `);
    this.sql.exec(`
      CREATE TRIGGER IF NOT EXISTS multipart_fence_expiry_insert
      AFTER INSERT ON multipart_fences WHEN NEW.expires_at IS NOT NULL BEGIN
        DELETE FROM multipart_fence_expirations WHERE upload_id = NEW.upload_id;
        INSERT INTO multipart_fence_expirations (expires_at, upload_id)
        VALUES (NEW.expires_at, NEW.upload_id);
      END
    `);
    this.sql.exec(`
      CREATE TRIGGER IF NOT EXISTS multipart_fence_expiry_update
      AFTER UPDATE OF expires_at ON multipart_fences
      WHEN NEW.expires_at IS NOT NULL BEGIN
        DELETE FROM multipart_fence_expirations WHERE upload_id = NEW.upload_id;
        INSERT INTO multipart_fence_expirations (expires_at, upload_id)
        VALUES (NEW.expires_at, NEW.upload_id);
      END
    `);
    this.sql.exec(`
      CREATE TRIGGER IF NOT EXISTS multipart_fence_expiry_delete
      AFTER DELETE ON multipart_fences BEGIN
        DELETE FROM multipart_fence_expirations WHERE upload_id = OLD.upload_id;
      END
    `);
  }

  private maintainRows(
    name: string,
    page: (cursor: number) => number[]
  ): boolean {
    const maintenance = ensureSchemaMaintenance(this.sql, name);
    if (maintenance.state === "ready") return false;
    const cursor = Number(maintenance.cursor || "0");
    const rows = page(cursor);
    const nextCursor = rows.at(-1) ?? cursor;
    const done = rows.length < SHARD_SCHEMA_MAINTENANCE_PAGE_SIZE;
    advanceSchemaMaintenance(this.sql, name, String(nextCursor), done);
    return !done;
  }

  private runSchemaMaintenancePage(): boolean {
    const journalPending = this.maintainRows(
      CLEANUP_JOURNAL_MAINTENANCE,
      (cursor) => {
        const rows = this.sql
          .exec<{
            rowid: number;
            cleanup_kind: string;
            ref_id: string;
            cleanup_generation: string;
            request_cursor: number;
            next_cursor: number;
            done: number;
            created_at: number;
          } & Record<string, SqlStorageValue>>(
            `SELECT rowid, cleanup_kind, ref_id, cleanup_generation,
                    request_cursor, next_cursor, done, created_at
               FROM shard_cleanup_pages
              WHERE rowid > ? ORDER BY rowid LIMIT ?`,
            cursor,
            SHARD_SCHEMA_MAINTENANCE_PAGE_SIZE
          )
          .toArray();
        for (const row of rows) {
          this.sql.exec(
            `INSERT INTO shard_cleanup_progress
               (cleanup_kind, ref_id, cleanup_generation, next_cursor, done, updated_at)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(cleanup_kind, ref_id, cleanup_generation) DO UPDATE SET
               next_cursor = excluded.next_cursor,
               done = excluded.done,
               updated_at = excluded.updated_at
             WHERE excluded.next_cursor > shard_cleanup_progress.next_cursor
                OR (excluded.next_cursor = shard_cleanup_progress.next_cursor
                    AND excluded.done > shard_cleanup_progress.done)`,
            row.cleanup_kind,
            row.ref_id,
            row.cleanup_generation,
            row.next_cursor,
            row.done,
            row.created_at
          );
          this.sql.exec(
            `INSERT OR IGNORE INTO shard_cleanup_page_expirations
               (expires_at, cleanup_kind, ref_id, cleanup_generation, request_cursor)
             VALUES (?, ?, ?, ?, ?)`,
            row.created_at + SHARD_CLEANUP_JOURNAL_TTL_MS,
            row.cleanup_kind,
            row.ref_id,
            row.cleanup_generation,
            row.request_cursor
          );
        }
        return rows.map((row) => row.rowid);
      }
    );
    const fencePending = this.maintainRows(
      FENCE_EXPIRY_MAINTENANCE,
      (cursor) => {
        const rows = this.sql
          .exec<{
            rowid: number;
            upload_id: string;
            updated_at: number;
            expires_at: number | null;
          } & Record<string, SqlStorageValue>>(
            `SELECT rowid, upload_id, updated_at, expires_at
               FROM multipart_fences
              WHERE rowid > ? ORDER BY rowid LIMIT ?`,
            cursor,
            SHARD_SCHEMA_MAINTENANCE_PAGE_SIZE
          )
          .toArray();
        for (const row of rows) {
          const expiresAt = row.expires_at ?? row.updated_at + MULTIPART_MAX_TTL_MS;
          if (row.expires_at === null) {
            this.sql.exec(
              "UPDATE multipart_fences SET expires_at = ? WHERE rowid = ? AND expires_at IS NULL",
              expiresAt,
              row.rowid
            );
          } else {
            this.recordMultipartFenceExpiry(row.upload_id, expiresAt);
          }
        }
        return rows.map((row) => row.rowid);
      }
    );
    return journalPending || fencePending;
  }

  private recordMultipartFenceExpiry(uploadId: string, expiresAt: number): void {
    this.sql.exec(
      "DELETE FROM multipart_fence_expirations WHERE upload_id = ?",
      uploadId
    );
    this.sql.exec(
      `INSERT INTO multipart_fence_expirations (expires_at, upload_id)
       VALUES (?, ?)`,
      expiresAt,
      uploadId
    );
  }

  private nextMaintenanceAlarm(maintenancePending: boolean): number | null {
    if (maintenancePending) return Date.now() + 1_000;
    const journal = this.sql
      .exec<{ expires_at: number } & Record<string, SqlStorageValue>>(
        `SELECT expiration.expires_at
           FROM shard_cleanup_page_expirations AS expiration
           JOIN shard_cleanup_progress AS progress
             ON progress.cleanup_kind = expiration.cleanup_kind
            AND progress.ref_id = expiration.ref_id
            AND progress.cleanup_generation = expiration.cleanup_generation
          WHERE progress.done != 0
          ORDER BY expiration.expires_at LIMIT 1`
      )
      .toArray()[0]?.expires_at;
    const fence = this.sql
      .exec<{ expires_at: number } & Record<string, SqlStorageValue>>(
        `SELECT expires_at FROM multipart_fence_expirations
          ORDER BY expires_at LIMIT 1`
      )
      .toArray()[0]?.expires_at;
    if (journal === undefined && fence === undefined) return null;
    return Math.max(
      Date.now() + 1_000,
      Math.min(
        journal ?? Number.POSITIVE_INFINITY,
        fence === undefined
          ? Number.POSITIVE_INFINITY
          : fence + MULTIPART_FENCE_GC_GRACE_MS
      )
    );
  }

  private async armAlarmAt(target: number): Promise<void> {
    const current = await this.ctx.storage.getAlarm();
    if (current === null || current > target) {
      await this.ctx.storage.setAlarm(target);
    }
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
        const result = this.writeChunkInternal(
          chunkHash,
          new Uint8Array(data),
          fileId,
          chunkIndex,
          userId
        );
        return Response.json(result);
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
      // Legacy HTTP shape — kept for back-compat. The body now reflects
      // soft-mark semantics: bytes are not freed synchronously, they are
      // marked for the alarm sweeper. No current route actually invokes
      // this endpoint (verified); the public DO RPC is preferred.
      if (path.startsWith("/refs/") && request.method === "DELETE") {
        const fileId = path.split("/")[2];
        const result = await this.deleteChunks(fileId);
        return Response.json({
          freedBytes: 0,
          markedChunks: result.marked,
        });
      }

      // ── multipart staging endpoints ─────────────────────────
      //
      // Internal HTTP shapes used by UserDO finalize/abort to query
      // the staging table this shard accumulated during the upload.
      // All three are read-only or staging-only (they never touch
      // `chunk_refs` or `chunks`); committing real refs goes through
      // the typed `putChunkMultipart` RPC.
      if (path === "/multipart/manifest" && request.method === "GET") {
        const uploadId = url.searchParams.get("upload_id") ?? "";
        if (uploadId.length === 0) {
          return Response.json({ error: "upload_id required" }, { status: 400 });
        }
        const afterIndex = Number(url.searchParams.get("after_index") ?? -1);
        const limit = Number(
          url.searchParams.get("limit") ?? MULTIPART_STATUS_ENTRY_PAGE_SIZE
        );
        return Response.json(
          await this.getMultipartManifest(uploadId, afterIndex, limit)
        );
      }

      if (path === "/multipart/landed" && request.method === "GET") {
        const uploadId = url.searchParams.get("upload_id") ?? "";
        if (uploadId.length === 0) {
          return Response.json({ error: "upload_id required" }, { status: 400 });
        }
        const afterIndex = Number(url.searchParams.get("after_index") ?? -1);
        const limit = Number(
          url.searchParams.get("limit") ?? MULTIPART_STATUS_ENTRY_PAGE_SIZE
        );
        return Response.json(
          await this.getMultipartLanded(uploadId, afterIndex, limit)
        );
      }

      if (path === "/multipart/clear" && request.method === "DELETE") {
        const uploadId = url.searchParams.get("upload_id") ?? "";
        if (uploadId.length === 0) {
          return Response.json({ error: "upload_id required" }, { status: 400 });
        }
        return Response.json(await this.clearMultipartStaging(uploadId));
      }

      return new Response("Not found", { status: 404 });
    } catch (err) {
      const message = err instanceof Error ? err.message : "Internal error";
      return Response.json({ error: message }, { status: 500 });
    }
  }

  // ── VFS write RPC surface (sdk-impl-plan §7) ───────────────────────────
  //
  // putChunk is the typed RPC counterpart to the legacy
  // PUT /chunk HTTP route. UserDO's vfsWriteFile path calls this
  // directly via the typed DO stub: no JSON marshalling, no headers,
  // typed return.
  //
  // Refcount semantics are identical to the HTTP route — they share
  // writeChunkInternal so the dedup-drift fix applies to
  // both paths.
  async putChunk(
    chunkHash: string,
    data: Uint8Array,
    fileId: string,
    chunkIndex: number,
    userId: string
  ): Promise<{ status: "created" | "deduplicated"; bytesStored: number }> {
    this.ensureInit();
    return this.writeChunkInternal(
      chunkHash,
      data,
      fileId,
      chunkIndex,
      userId
    );
  }

  /**
   * Atomic chunk-ref restoration (P1-1 fix).
   *
   * Pre-fix `restoreVersion` split a `chunksAlive` pre-flight + a
   * `putChunk(empty)` per chunk into two RPCs. Between the two,
   * a concurrent `dropVersions` of an unrelated version could
   * decrement chunk_refs on a shared chunk to zero, soft-mark it,
   * and let the alarm sweeper reap it during the grace window —
   * leaving `restoreVersion`'s subsequent `putChunk(empty)` to
   * either (a) hit the 0-byte cold-path defense and throw partway,
   * leaking already-bumped chunk_refs under `newRefId`, or (b)
   * succeed under a swept-then-resurrected chunk row (the
   * resurrection-aware logic at writeChunkInternal:484-487 saves
   * data correctness, but the partial-state cleanup is messy).
   *
   * `restoreChunkRef` collapses both steps into a single ShardDO
   * RPC, atomic per DO turn (synchronous SQL only, no awaits):
   *
   *   1. Verify chunk row exists AND is alive (`deleted_at IS NULL`)
   *      AND `ref_count >= 1`. If any condition fails, throw an
   *      explicit ENOENT — the caller maps to a clean
   *      `restoreVersion: source chunks swept` error.
   *   2. INSERT OR IGNORE a fresh chunk_refs row keyed by
   *      `(chunkHash, newRefId, chunkIndex)`. If the INSERT was
   *      a no-op (already-exists; idempotent re-restore), do NOT
   *      bump ref_count — same shape as writeChunkInternal's dedup
   *      path uses `changes()` to gate the bump.
   *   3. Bump `ref_count` only on actual INSERT.
   *
   * No await between steps 1, 2, 3 → atomic per DO turn → no
   * concurrent `dropVersions` can sweep a chunk between our check
   * and our ref bump. The audit C2 race (chunksAlive + putChunk
   * split) is structurally impossible by construction here.
   *
   * Returns `{ status }` — `"restored"` on a fresh ref bump,
   * `"already_referenced"` on idempotent re-restore.
   *
   * @lean-invariant Mossaic.Vfs.Refcount.restoreChunkRef_atomic
   *   The abstract theorem states that modeled refs and counts update in
   *   one transition or the state is unchanged. It does not refine this
   *   SQL/DO implementation.
   */
  async restoreChunkRef(
    chunkHash: string,
    newRefId: string,
    chunkIndex: number,
    userId: string
  ): Promise<{ status: "restored" | "already_referenced" }> {
    this.ensureInit();
    // Step 1 — verify chunk is alive. The composite condition
    // mirrors `chunksAlive`: present, not soft-marked, ref_count
    // ≥ 1. A swept chunk (deleted_at NOT NULL) is unsafe to
    // restore against — even if the alarm hasn't reaped yet, a
    // concurrent sweep within the same turn could.
    const live = this.sql
      .exec(
        `SELECT 1 FROM chunks
          WHERE hash = ? AND deleted_at IS NULL AND ref_count >= 1
          LIMIT 1`,
        chunkHash
      )
      .toArray();
    if (live.length === 0) {
      throw new Error(
        `ENOENT: restoreChunkRef: chunk ${chunkHash} is not alive on this shard`
      );
    }

    // Step 2 — idempotent INSERT. INSERT OR IGNORE sets `changes()`
    // to 0 if the row already exists (re-restore of the same
    // version_id), 1 on fresh insert.
    this.sql.exec(
      `INSERT OR IGNORE INTO chunk_refs (chunk_hash, file_id, chunk_index, user_id)
       VALUES (?, ?, ?, ?)`,
      chunkHash,
      newRefId,
      chunkIndex,
      userId
    );
    const inserted =
      (
        this.sql.exec("SELECT changes() AS n").toArray()[0] as {
          n: number;
        }
      ).n > 0;

    // Step 3 — bump ref_count only on fresh insert. Mirrors
    // writeChunkInternal's dedup branch.
    if (inserted) {
      this.sql.exec(
        "UPDATE chunks SET ref_count = ref_count + 1 WHERE hash = ?",
        chunkHash
      );
      return { status: "restored" };
    }
    return { status: "already_referenced" };
  }

  // ── multipart staging-aware put ────────────────────────────
  //
  // Same semantics as `putChunk` PLUS:
  //   - records `(upload_id, chunk_index)` → hash in `upload_chunks` so
  //     UserDO finalize can verify completeness without per-chunk
  //     UserDO touches.
  //   - on re-PUT with a *different* hash for the same `(upload_id,
  //     chunk_index)`, drops the prior `chunk_refs` row, decrements the
  //     prior chunk's ref_count (soft-mark if it hits 0), and proceeds
  //     to register the new chunk. Both the old and new operations
  //     happen in a single DO turn → atomic relative to refcount
  //     observers.
  //
  // `uploadId` is the same value as the tmp `files.file_id` minted by
  // `vfsBeginMultipart`. This means after `commitRename` (which is a
  // file_name UPDATE; file_id is preserved) the chunk_refs rows are
  // already keyed correctly to the post-rename file id — no refcount
  // transfer needed at finalize.
  //
  // Returns one of "created" / "deduplicated" / "superseded" so the
  // caller (and tests) can observe which branch fired.
  //
  // @lean-invariant Mossaic.Vfs.Multipart.putChunkMultipart_idempotent
  //   Repeating the same abstract chunk/ref transition produces exactly
  //   the same modeled ShardState. This does not refine the SQL or staging
  //   implementation; see the formal-verification boundary document.
  async putChunkMultipart(
    chunkHash: string,
    data: Uint8Array,
    uploadId: string,
    chunkIndex: number,
    userId: string,
    sessionToken: string
  ): Promise<{
    status: "created" | "deduplicated" | "superseded";
    bytesStored: number;
  }> {
    this.recordRpc();
    this.ensureInit();

    const payload = await verifyVFSMultipartToken(this.env, sessionToken);
    if (
      payload === null ||
      payload.uploadId !== uploadId ||
      payload.userId !== userId ||
      payload.fenceId === undefined ||
      chunkIndex < 0 ||
      chunkIndex >= payload.totalChunks
    ) {
      throw new Error("EACCES: invalid multipart session capability");
    }
    const fenceId = payload.fenceId;

    const priorBeforeFence = this.sql
      .exec(
        "SELECT chunk_hash FROM upload_chunks WHERE upload_id = ? AND chunk_index = ?",
        uploadId,
        chunkIndex
      )
      .toArray()[0] as { chunk_hash: string } | undefined;
    if (priorBeforeFence && priorBeforeFence.chunk_hash !== chunkHash) {
      await this.scheduleSweep();
    }

    return this.ctx.storage.transactionSync(() => {
      // This is the final fence check. There are no awaits after it, so a
      // terminal fence cannot interleave before the staging/ref mutation.
      this.assertMultipartFenceOpen(uploadId, fenceId, payload.exp * 1000);
      return this.putChunkMultipartInternal(
        chunkHash,
        data,
        uploadId,
        chunkIndex,
        userId
      );
    });
  }

  private putChunkMultipartInternal(
    chunkHash: string,
    data: Uint8Array,
    uploadId: string,
    chunkIndex: number,
    userId: string
  ): {
    status: "created" | "deduplicated" | "superseded";
    bytesStored: number;
  } {
    const prior = this.sql
      .exec(
        "SELECT chunk_hash FROM upload_chunks WHERE upload_id = ? AND chunk_index = ?",
        uploadId,
        chunkIndex
      )
      .toArray()[0] as { chunk_hash: string } | undefined;

    let supersededOldRef = false;
    if (prior && prior.chunk_hash !== chunkHash) {
      this.sql.exec(
        "DELETE FROM chunk_refs WHERE chunk_hash = ? AND file_id = ? AND chunk_index = ?",
        prior.chunk_hash,
        uploadId,
        chunkIndex
      );
      const decremented =
        (
          this.sql.exec("SELECT changes() AS n").toArray()[0] as {
            n: number;
          }
        ).n > 0;
      if (decremented) {
        this.sql.exec(
          "UPDATE chunks SET ref_count = MAX(0, ref_count - 1) WHERE hash = ?",
          prior.chunk_hash
        );
        const r = this.sql
          .exec("SELECT ref_count FROM chunks WHERE hash = ?", prior.chunk_hash)
          .toArray()[0] as { ref_count: number } | undefined;
        if (r && r.ref_count === 0) {
          this.sql.exec(
            "UPDATE chunks SET deleted_at = ? WHERE hash = ? AND deleted_at IS NULL",
            Date.now(),
            prior.chunk_hash
          );
        }
      }
      supersededOldRef = true;
    }

    const writeResult = this.writeChunkInternal(
      chunkHash,
      data,
      uploadId,
      chunkIndex,
      userId
    );

    this.sql.exec(
      `INSERT OR REPLACE INTO upload_chunks
         (upload_id, chunk_index, chunk_hash, chunk_size, user_id, created_at)
       VALUES (?, ?, ?, ?, ?, ?)`,
      uploadId,
      chunkIndex,
      chunkHash,
      data.byteLength,
      userId,
      Date.now()
    );

    const status: "created" | "deduplicated" | "superseded" = supersededOldRef
      ? "superseded"
      : writeResult.status;
    return { status, bytesStored: writeResult.bytesStored };
  }

  async fenceMultipart(
    uploadId: string,
    fenceId: string,
    state: "finalizing" | "aborting",
    expiresAt: number
  ): Promise<void> {
    this.recordRpc();
    this.ensureInit();
    this.ctx.storage.transactionSync(() => {
      const current = this.sql
        .exec(
          "SELECT fence_id, state, expires_at FROM multipart_fences WHERE upload_id = ?",
          uploadId
        )
        .toArray()[0] as
        | { fence_id: string; state: string; expires_at: number | null }
        | undefined;
      if (current && current.fence_id !== fenceId) {
        throw new Error("EACCES: multipart fence capability mismatch");
      }
      if (
        current &&
        current.state !== "open" &&
        current.state !== state &&
        !(current.state === "finalizing" && state === "aborting")
      ) {
        throw new Error(
          `EBUSY: multipart upload already fenced as ${current.state}`
        );
      }
      this.sql.exec(
        `INSERT INTO multipart_fences (upload_id, fence_id, state, updated_at, expires_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(upload_id) DO UPDATE SET
           state = excluded.state,
           updated_at = excluded.updated_at,
           expires_at = MAX(COALESCE(multipart_fences.expires_at, 0), excluded.expires_at)`,
        uploadId,
        fenceId,
        state,
        Date.now(),
        expiresAt
      );
    });
    await this.armAlarmAt(
      Math.max(Date.now() + 1_000, expiresAt + MULTIPART_FENCE_GC_GRACE_MS)
    );
  }

  private assertMultipartFenceOpen(
    uploadId: string,
    fenceId: string | undefined,
    expiresAt: number
  ): void {
    const current = this.sql
      .exec(
        "SELECT fence_id, state, expires_at FROM multipart_fences WHERE upload_id = ?",
        uploadId
      )
      .toArray()[0] as
      | { fence_id: string; state: string; expires_at: number | null }
      | undefined;
    if (current) {
      if (current.state !== "open") {
        throw new Error(`EBUSY: multipart upload is ${current.state}`);
      }
      if (fenceId === undefined || current.fence_id !== fenceId) {
        throw new Error("EACCES: multipart fence capability mismatch");
      }
      if ((current.expires_at ?? 0) < expiresAt) {
        this.sql.exec(
          `UPDATE multipart_fences SET expires_at = ?, updated_at = ?
            WHERE upload_id = ?`,
          expiresAt,
          Date.now(),
          uploadId
        );
      }
      return;
    }
    if (fenceId !== undefined) {
      this.sql.exec(
        `INSERT INTO multipart_fences (upload_id, fence_id, state, updated_at, expires_at)
         VALUES (?, ?, 'open', ?, ?)`,
        uploadId,
        fenceId,
        Date.now(),
        expiresAt
      );
    }
  }

  /**
   * read the staging manifest for a given upload_id. Used by
   * UserDO finalize to verify chunk completeness across all touched
   * shards. Read-only; never mutates state.
   */
  async getMultipartManifest(
    uploadId: string,
    afterIndex = -1,
    limit = MULTIPART_STATUS_ENTRY_PAGE_SIZE
  ): Promise<{ rows: Array<{ idx: number; hash: string; size: number }> }> {
    this.ensureInit();
    this.validateMultipartReadPage(afterIndex, limit);
    const rows = this.sql
      .exec(
        `SELECT chunk_index AS idx, chunk_hash AS hash, chunk_size AS size
           FROM upload_chunks
          WHERE upload_id = ? AND chunk_index > ?
          ORDER BY chunk_index
          LIMIT ?`,
        uploadId,
        afterIndex,
        limit
      )
      .toArray() as Array<{ idx: number; hash: string; size: number }>;
    return { rows };
  }

  async getMultipartManifestRange(
    uploadId: string,
    startIndex: number,
    endIndex: number
  ): Promise<{ rows: Array<{ idx: number; hash: string; size: number }> }> {
    this.recordRpc();
    this.ensureInit();
    if (
      !Number.isInteger(startIndex) ||
      !Number.isInteger(endIndex) ||
      startIndex < 0 ||
      endIndex < startIndex ||
      endIndex - startIndex > 256
    ) {
      throw new Error("EINVAL: multipart manifest range must cover <=256 chunks");
    }
    const rows = this.sql
      .exec(
        `SELECT chunk_index AS idx, chunk_hash AS hash, chunk_size AS size
           FROM upload_chunks
          WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?
          ORDER BY chunk_index`,
        uploadId,
        startIndex,
        endIndex
      )
      .toArray() as Array<{ idx: number; hash: string; size: number }>;
    return { rows };
  }

  /**
   * read just the landed-chunk indices. Cheaper than the
   * full manifest — used by status / resume probe.
   */
  async getMultipartLanded(
    uploadId: string,
    afterIndex = -1,
    limit = MULTIPART_STATUS_ENTRY_PAGE_SIZE
  ): Promise<{ idx: number[]; sizes: number[] }> {
    this.recordRpc();
    this.ensureInit();
    this.validateMultipartReadPage(afterIndex, limit);
    const rows = this.sql
      .exec(
        `SELECT chunk_index, chunk_size
           FROM upload_chunks
          WHERE upload_id = ? AND chunk_index > ?
          ORDER BY chunk_index
          LIMIT ?`,
        uploadId,
        afterIndex,
        limit
      )
      .toArray() as Array<{ chunk_index: number; chunk_size: number }>;
    return {
      idx: rows.map((row) => row.chunk_index),
      sizes: rows.map((row) => row.chunk_size),
    };
  }

  private validateMultipartReadPage(afterIndex: number, limit: number): void {
    if (
      !Number.isSafeInteger(afterIndex) ||
      afterIndex < -1 ||
      !Number.isSafeInteger(limit) ||
      limit < 1 ||
      limit > MULTIPART_STATUS_ENTRY_PAGE_SIZE
    ) {
      throw new Error(
        `EINVAL: multipart read page requires afterIndex >= -1 and limit 1..${MULTIPART_STATUS_ENTRY_PAGE_SIZE}`
      );
    }
  }

  /**
   * Legacy staging cleanup for small uploads. Paged outbox cleanup uses
   * `clearMultipartStagingPage`.
   */
  async clearMultipartStaging(uploadId: string): Promise<{ dropped: number }> {
    this.ensureInit();
    this.assertLegacyCleanupBounded(
      "upload_chunks",
      "upload_id",
      uploadId,
      "clearMultipartStaging"
    );
    const before = (
      this.sql
        .exec(
          "SELECT COUNT(*) AS n FROM upload_chunks WHERE upload_id = ?",
          uploadId
        )
        .toArray()[0] as { n: number }
    ).n;
    this.sql.exec(
      "DELETE FROM upload_chunks WHERE upload_id = ?",
      uploadId
    );
    return { dropped: before };
  }

  async clearMultipartStagingPage(
    uploadId: string,
    cursor: number,
    generation: string | number
  ): Promise<ClearMultipartStagingPageResult> {
    this.recordRpc();
    this.ensureInit();
    const result = this.runCleanupPage(
      ShardCleanupKind.Staging,
      uploadId,
      cursor,
      generation
    );
    return {
      cursor: result.cursor,
      done: result.done,
      dropped: result.processed,
    };
  }

  /**
   * Shared write path used by both the legacy HTTP PUT /chunk route and
   * the new putChunk RPC. Implements:
   *   - dedup: existing hash → INSERT OR IGNORE chunk_refs, conditional
   *    ref_count++ via SELECT changes() (fix), clear deleted_at
   *     on resurrection
   *   - cold path: INSERT INTO chunks + chunk_refs, update capacity
   *
   * Returns the same shape as the HTTP route's JSON body.
   *
   * @lean-invariant Mossaic.Generated.ShardDO.chunk_invariant_preserved
   *   The Lean state-machine proves these properties for its abstract
   *   putChunk transition. No refinement from this SQL implementation to
   *   that transition is currently proved.
   */
  private writeChunkInternal(
    chunkHash: string,
    data: Uint8Array,
    fileId: string,
    chunkIndex: number,
    userId: string
  ): { status: "created" | "deduplicated"; bytesStored: number } {
    const existing = this.sql
      .exec("SELECT hash FROM chunks WHERE hash = ?", chunkHash)
      .toArray();

    if (existing.length > 0) {
      this.sql.exec(
        `INSERT OR IGNORE INTO chunk_refs (chunk_hash, file_id, chunk_index, user_id)
         VALUES (?, ?, ?, ?)`,
        chunkHash,
        fileId,
        chunkIndex,
        userId
      );
      const inserted =
        (
          this.sql.exec("SELECT changes() AS n").toArray()[0] as {
            n: number;
          }
        ).n > 0;
      if (inserted) {
        this.sql.exec(
          "UPDATE chunks SET ref_count = ref_count + 1 WHERE hash = ?",
          chunkHash
        );
      }
      // Resurrection: a previous unlink may have soft-marked this chunk;
      // a fresh write/dedup cancels the GC.
      this.sql.exec(
        "UPDATE chunks SET deleted_at = NULL WHERE hash = ? AND deleted_at IS NOT NULL",
        chunkHash
      );
      return { status: "deduplicated", bytesStored: 0 };
    }

    // Audit C2 defense-in-depth: a 0-byte cold-path INSERT under any
    // hash other than the well-known SHA-256 of the empty string is a
    // strong signal that the caller passed a placeholder buffer
    // expecting the dedup branch to short-circuit (e.g. restoreVersion
    // when its source chunk has been swept). Rejecting the write here
    // turns silent corruption into a loud failure that the caller can
    // map to ENOENT.
    //
    // SHA-256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    if (
      data.byteLength === 0 &&
      chunkHash !==
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    ) {
      throw new Error(
        `EINVAL: putChunk cold-path with empty buffer under hash ${chunkHash}; the chunk was expected to exist but has been swept`
      );
    }

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
    this.updateCapacity(data.byteLength);
    return { status: "created", bytesStored: data.byteLength };
  }

  // ── VFS GC RPC surface (sdk-impl-plan §8.2) ────────────────────────────
  //
  // The legacy RPC remains for callers whose cleanup is provably small.
  // Durable UserDO cleanup uses deleteChunksPage. Both decrement
  // each chunk's `ref_count` by its grouped ref count, and soft-marks any
  // chunk that hits ref_count=0 by setting `deleted_at`. The alarm
  // sweeper (alarm() handler) hard-deletes after a 30s grace window,
  // re-checking ref_count to absorb resurrection races where a
  // concurrent dedup PUT re-references the chunk.
  //
  // Returns `{ marked }` — number of chunks newly marked deleted_at on
  // this call. Callers don't need this for correctness; it's
  // observability for tests + future quota reconciliation.
  async deleteChunks(fileId: string): Promise<{ marked: number }> {
    this.ensureInit();
    this.assertLegacyCleanupBounded(
      "chunk_refs",
      "file_id",
      fileId,
      "deleteChunks"
    );
    return this.removeFileRefs(fileId);
  }

  async deleteChunksPage(
    fileId: string,
    cursor: number,
    generation: string | number
  ): Promise<DeleteChunksPageResult> {
    this.recordRpc();
    this.ensureInit();
    await this.scheduleSweep();
    const result = this.runCleanupPage(
      ShardCleanupKind.Refs,
      fileId,
      cursor,
      generation
    );
    return {
      cursor: result.cursor,
      done: result.done,
      processed: result.processed,
      marked: result.marked,
    };
  }

  /**
   * Legacy batched chunk-ref drop, bounded across the complete input.
   *
   * Idempotent per fileId: a file_id with no chunk_refs produces no
   * refcount change or new soft mark. Total \`marked\` is the sum across
   * input file_ids. One alarm ensure covers the bounded synchronous
   * sequence of per-file transactions.
   *
   * Inputs that could touch 256 refs are rejected before mutation.
   */
  async deleteManyChunks(
    fileIds: readonly string[]
  ): Promise<{ marked: number }> {
    this.ensureInit();
    if (fileIds.length === 0) return { marked: 0 };
    if (fileIds.length > SHARD_CLEANUP_PAGE_SIZE) {
      throw new Error(
        `E2BIG: deleteManyChunks accepts at most ${SHARD_CLEANUP_PAGE_SIZE} file ids`
      );
    }
    const fileIdsJson = JSON.stringify([...new Set(fileIds)]);
    const overflow = this.sql
      .exec(
        `SELECT 1 FROM chunk_refs
          WHERE file_id IN (SELECT CAST(value AS TEXT) FROM json_each(?))
          LIMIT 1 OFFSET ?`,
        fileIdsJson,
        LEGACY_CLEANUP_MAX_ROWS
      )
      .toArray();
    if (overflow.length > 0) {
      throw new Error(
        `E2BIG: deleteManyChunks cleanup exceeds the bounded legacy limit of ${LEGACY_CLEANUP_MAX_ROWS} refs`
      );
    }

    await this.scheduleSweep();
    let totalMarked = 0;
    for (const fileId of fileIds) {
      const r = this.removeFileRefsTransaction(fileId);
      totalMarked += r.marked;
    }
    return { marked: totalMarked };
  }

  private runCleanupPage(
    cleanupKind: ShardCleanupKind,
    refId: string,
    cursor: number,
    generation: string | number
  ): {
    cursor: number;
    done: boolean;
    processed: number;
    marked: number;
  } {
    if (!Number.isSafeInteger(cursor) || cursor < 0) {
      throw new Error("EINVAL: cleanup cursor must be a non-negative integer");
    }
    const normalizedGeneration = String(generation) || "legacy";
    if (normalizedGeneration.length > 256) {
      throw new Error("EINVAL: cleanup generation must not exceed 256 characters");
    }

    let completedJournalExpiresAt: number | undefined;
    const result = this.ctx.storage.transactionSync(() => {
      const replay = this.sql
        .exec(
          `SELECT next_cursor, processed, marked, done
             FROM shard_cleanup_pages
            WHERE cleanup_kind = ? AND ref_id = ?
              AND cleanup_generation = ? AND request_cursor = ?`,
          cleanupKind,
          refId,
          normalizedGeneration,
          cursor
        )
        .toArray()[0] as
        | {
            next_cursor: number;
            processed: number;
            marked: number;
            done: number;
          }
        | undefined;
      if (replay) {
        return {
          cursor: replay.next_cursor,
          done: replay.done !== 0,
          processed: replay.processed,
          marked: replay.marked,
        };
      }

      let progress = this.sql
        .exec(
          `SELECT next_cursor, done FROM shard_cleanup_progress
            WHERE cleanup_kind = ? AND ref_id = ? AND cleanup_generation = ?`,
          cleanupKind,
          refId,
          normalizedGeneration
        )
        .toArray()[0] as
        | { next_cursor: number; done: number }
        | undefined;
      if (progress === undefined) {
        progress = this.sql
          .exec(
            `SELECT next_cursor, done FROM shard_cleanup_pages
              WHERE cleanup_kind = ? AND ref_id = ? AND cleanup_generation = ?
              ORDER BY request_cursor DESC LIMIT 1`,
            cleanupKind,
            refId,
            normalizedGeneration
          )
          .toArray()[0] as
          | { next_cursor: number; done: number }
          | undefined;
        if (progress !== undefined) {
          this.sql.exec(
            `INSERT INTO shard_cleanup_progress
               (cleanup_kind, ref_id, cleanup_generation, next_cursor, done, updated_at)
             VALUES (?, ?, ?, ?, ?, ?)`,
            cleanupKind,
            refId,
            normalizedGeneration,
            progress.next_cursor,
            progress.done,
            Date.now()
          );
        }
      }
      const expectedCursor = progress?.next_cursor ?? 0;
      if (
        cursor !== expectedCursor ||
        (progress !== undefined && progress.done !== 0)
      ) {
        throw new Error(
          `EINVAL: cleanup cursor ${cursor} does not match expected cursor ${expectedCursor}`
        );
      }

      const rows =
        cleanupKind === ShardCleanupKind.Refs
          ? (this.sql
              .exec(
                `SELECT rowid AS source_rowid, chunk_hash
                   FROM chunk_refs WHERE file_id = ? ORDER BY rowid LIMIT ?`,
                refId,
                SHARD_CLEANUP_PAGE_SIZE
              )
              .toArray() as Array<{
              source_rowid: number;
              chunk_hash: string;
            }>)
          : (this.sql
              .exec(
                `SELECT chunk_index AS source_rowid, chunk_hash
                   FROM upload_chunks
                  WHERE upload_id = ? ORDER BY chunk_index LIMIT ?`,
                refId,
                SHARD_CLEANUP_PAGE_SIZE
              )
              .toArray() as Array<{
              source_rowid: number;
              chunk_hash: string;
            }>);
      const rowIds = JSON.stringify(rows.map((row) => row.source_rowid));
      let marked = 0;

      if (cleanupKind === ShardCleanupKind.Refs && rows.length > 0) {
        const decrements = new Map<string, number>();
        for (const row of rows) {
          decrements.set(
            row.chunk_hash,
            (decrements.get(row.chunk_hash) ?? 0) + 1
          );
        }
        for (const [hash, count] of decrements) {
          this.sql.exec(
            "UPDATE chunks SET ref_count = MAX(0, ref_count - ?) WHERE hash = ?",
            count,
            hash
          );
        }
        const hashes = JSON.stringify([...decrements.keys()]);
        this.sql.exec(
          `UPDATE chunks SET deleted_at = ?
            WHERE ref_count = 0 AND deleted_at IS NULL
              AND hash IN (SELECT CAST(value AS TEXT) FROM json_each(?))`,
          Date.now(),
          hashes
        );
        marked = (
          this.sql.exec("SELECT changes() AS n").toArray()[0] as { n: number }
        ).n;
        this.sql.exec(
          `DELETE FROM chunk_refs
            WHERE rowid IN (SELECT CAST(value AS INTEGER) FROM json_each(?))`,
          rowIds
        );
      } else if (rows.length > 0) {
        this.sql.exec(
          `DELETE FROM upload_chunks
            WHERE upload_id = ?
              AND chunk_index IN (
                SELECT CAST(value AS INTEGER) FROM json_each(?)
              )`,
          refId,
          rowIds
        );
      }

      const nextCursor = cursor + rows.length;
      const done = rows.length < SHARD_CLEANUP_PAGE_SIZE;
      this.sql.exec(
        `INSERT INTO shard_cleanup_pages
           (cleanup_kind, ref_id, cleanup_generation, request_cursor,
            next_cursor, processed, marked, done, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        cleanupKind,
        refId,
        normalizedGeneration,
        cursor,
        nextCursor,
        rows.length,
        marked,
        done ? 1 : 0,
        Date.now()
      );
      const createdAt = Date.now();
      this.sql.exec(
        `INSERT INTO shard_cleanup_progress
           (cleanup_kind, ref_id, cleanup_generation, next_cursor, done, updated_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(cleanup_kind, ref_id, cleanup_generation) DO UPDATE SET
           next_cursor = excluded.next_cursor,
           done = excluded.done,
           updated_at = excluded.updated_at`,
        cleanupKind,
        refId,
        normalizedGeneration,
        nextCursor,
        done ? 1 : 0,
        createdAt
      );
      this.sql.exec(
        `INSERT OR REPLACE INTO shard_cleanup_page_expirations
           (expires_at, cleanup_kind, ref_id, cleanup_generation, request_cursor)
         VALUES (?, ?, ?, ?, ?)`,
        createdAt + SHARD_CLEANUP_JOURNAL_TTL_MS,
        cleanupKind,
        refId,
        normalizedGeneration,
        cursor
      );
      if (done) {
        completedJournalExpiresAt = createdAt + SHARD_CLEANUP_JOURNAL_TTL_MS;
      }
      return {
        cursor: nextCursor,
        done,
        processed: rows.length,
        marked,
      };
    });
    if (completedJournalExpiresAt !== undefined) {
      this.ctx.waitUntil(this.armAlarmAt(completedJournalExpiresAt));
    }
    return result;
  }

  private assertLegacyCleanupBounded(
    table: "chunk_refs" | "upload_chunks",
    column: "file_id" | "upload_id",
    refId: string,
    operation: "deleteChunks" | "clearMultipartStaging"
  ): void {
    const overflow = this.sql
      .exec(
        `SELECT 1 FROM ${table} WHERE ${column} = ? LIMIT 1 OFFSET ?`,
        refId,
        LEGACY_CLEANUP_MAX_ROWS
      )
      .toArray();
    if (overflow.length > 0) {
      throw new Error(
        `E2BIG: ${operation} cleanup exceeds the bounded legacy limit of ${LEGACY_CLEANUP_MAX_ROWS} rows; use the paged RPC`
      );
    }
  }

  /**
   * Existence + liveness probe for a set of chunk hashes (audit C2).
   *
   * Returns the subset of `hashes` that are present on this shard
   * AND not soft-marked for deletion (`deleted_at IS NULL`) AND have
   * `ref_count >= 1`. A "missing" chunk for our purposes is one where
   * any of those conditions fail — the alarm sweeper might race the
   * caller and hard-delete it before the caller's logic completes,
   * but a soft-marked chunk is also unsafe to reuse (the grace window
   * could elapse mid-operation).
   *
   * Used by `restoreVersion` to refuse to commit a manifest whose
   * source chunks have been GC'd. Without this check the cold-path
   * INSERT in `writeChunkInternal` would silently store a 0-byte chunk
   * under the original hash and corrupt every subsequent read.
   */
  async chunksAlive(hashes: string[]): Promise<{ alive: string[] }> {
    this.ensureInit();
    if (hashes.length === 0) return { alive: [] };
    // SQLite parameter list: bind one ? per hash. The list size is
    // bounded by the caller (one shard's contribution to a manifest);
    // typical manifests have dozens, not thousands, of chunks per shard.
    const placeholders = hashes.map(() => "?").join(",");
    const rows = this.sql
      .exec(
        `SELECT hash FROM chunks
          WHERE hash IN (${placeholders})
            AND deleted_at IS NULL
            AND ref_count >= 1`,
        ...hashes
      )
      .toArray() as { hash: string }[];
    return { alive: rows.map((r) => r.hash) };
  }

  /**
   * Typed single-chunk read RPC.
   *
   * The HTTP-style `GET /chunk/:hash` route remains for the legacy
   * upload/download surface; this RPC is what UserDO's read paths
   * call for typed in-process invocation. Compared to
   * `stub.fetch(new Request("http://internal/chunk/<hash>"))`, the
   * typed RPC:
   *   - skips Request/Response construction and JSON header parsing
   *   - uses workerd's RPC arg/return marshalling (one IPC hop) vs
   *     two awaits on the HTTP path (`fetch` + `arrayBuffer()`)
   *   - benefits from CF Workers RPC promise pipelining: callers
   *     that issue multiple `stub.getChunkBytes(...)` calls without
   *     awaiting each receive Promise stubs back and only pay one
   *     round trip if their consumption pattern allows.
   *
   * Returns `null` when the chunk is missing — caller maps to
   * VFSError("ENOENT") with their preferred phrasing. Returning
   * `null` (vs throwing) keeps the typed return shape predictable
   * for parallel callers; thrown errors abort `Promise.all` and
   * cancel sibling RPCs which is undesirable for partial-success
   * patterns.
   */
  async getChunkBytes(hash: string): Promise<Uint8Array | null> {
    this.ensureInit();
    const rows = this.sql
      .exec("SELECT data FROM chunks WHERE hash = ?", hash)
      .toArray() as { data: ArrayBuffer }[];
    if (rows.length === 0) return null;
    return new Uint8Array(rows[0].data);
  }

  /**
   * Batched chunk read RPC.
   *
   * Single round-trip retrieval of N chunks on this shard. Replaces
   * the previous "loop with `stub.fetch` per chunk" which paid one
   * intra-DO RPC per chunk; for a 100-chunk file landing across
   * 32 shards, fan-out drops from 100 round-trips to 32.
   *
   * Returns a parallel array `bytes[i]` for `hashes[i]`; missing
   * chunks come back as `null` so the caller can map exactly which
   * hash failed (the order is preserved by index, not by SQLite
   * row order). Order matters: callers fix per-chunk destination
   * offsets up-front against `chunk_index` and feed those offsets
   * with the returned bytes.
   *
   * Empty input → empty output (`{ bytes: [] }`); zero allocations.
   *
   * Memory bound: caller must already enforce READFILE_MAX (server-
   * side cap, default 100 MB) BEFORE calling — we hold
   * the response buffer in memory. The list of hashes is also
   * bounded by the caller (one shard's contribution to a manifest);
   * typical manifests have dozens of chunks per shard, not
   * thousands.
   */
  async getChunksBatch(
    hashes: string[]
  ): Promise<{ bytes: (Uint8Array | null)[] }> {
    this.ensureInit();
    if (hashes.length === 0) return { bytes: [] };
    // Single SQL with `WHERE hash IN (?, ?, ...)` — SQLite parses N
    // bound parameters in O(N) and the IN scan is one indexed range
    // per hash. We then re-order the result to match the input
    // order so the caller's offset map lines up.
    const placeholders = hashes.map(() => "?").join(",");
    const rows = this.sql
      .exec(
        `SELECT hash, data FROM chunks WHERE hash IN (${placeholders})`,
        ...hashes
      )
      .toArray() as { hash: string; data: ArrayBuffer }[];
    // Build a hash → bytes map so duplicate hashes in the input
    // (which can legally happen if a manifest references the same
    // dedup'd chunk at multiple indices) all resolve to the same
    // byte array.
    const map = new Map<string, Uint8Array>();
    for (const row of rows) {
      map.set(row.hash, new Uint8Array(row.data));
    }
    const out: (Uint8Array | null)[] = new Array(hashes.length);
    for (let i = 0; i < hashes.length; i++) {
      out[i] = map.get(hashes[i]) ?? null;
    }
    return { bytes: out };
  }

  /**
   * Telemetry RPC.
   *
   * Returns the bytes currently stored on this shard, the count of
   * unique chunks (post-dedup), and a soft-cap value for monitoring.
   * **This is observability only — there is no enforcement here.**
   * The architecture intentionally lets the SQLite ceiling (~10 GB
   * per DO) be the hard backstop and relies on `recordWriteUsage`
   * (`worker/core/objects/user/vfs/helpers.ts:376`) growing the
   * tenant's pool BEFORE shards fill, so chunks fan out across more
   * shards rather than concentrating on the original 32.
   *
   * Operators can poll this RPC across all shards in a tenant to:
   *   - Detect skewed chunk distribution (one shard at 90%, others
   *     at 5%) — a sign that pool growth lagged a burst write.
   *   - Project capacity ahead of the SQLite ceiling and provision
   *     the tenant a higher `quota.storage_limit` if needed.
   *   - Feed dashboards / alerting (Cloudflare Workers Analytics
   *     Engine, custom KV-backed metrics, etc.).
   *
   * Read-only; safe to call concurrently with writes.
   */
  async getStorageBytes(): Promise<{
    bytesStored: number;
    uniqueChunks: number;
    softCapBytes: number;
  }> {
    this.ensureInit();
    const row = this.sql
      .exec(
        "SELECT COUNT(*) AS unique_chunks, COALESCE(SUM(size), 0) AS bytes FROM chunks WHERE deleted_at IS NULL"
      )
      .toArray()[0] as { unique_chunks: number; bytes: number } | undefined;
    // Soft cap surfaced for monitoring. The hard cap is workerd's
    // SQLite limit (~10 GB) — we publish 9 GB so dashboards can flag
    // approach-to-limit before the runtime starts refusing writes.
    const SOFT_CAP_BYTES = 9 * 1024 * 1024 * 1024;
    return {
      bytesStored: row ? row.bytes : 0,
      uniqueChunks: row ? row.unique_chunks : 0,
      softCapBytes: SOFT_CAP_BYTES,
    };
  }

  /**
   * Drop all refs for a fileId in one explicit synchronous transaction.
   * Refcounts are decremented by the grouped number of matching indices,
   * chunks that first reach zero are soft-marked, and the source refs are
   * then deleted. The alarm handler does the hard-delete after a grace period.
   *
   * Per-call observable side-effects:
   *   - chunk_refs rows for this fileId removed
   *   - chunks.ref_count decremented
   *   - chunks.deleted_at set on rows that hit 0
   *   - a future alarm is ensured before any ref deletion commits
   *
   * @lean-invariant Mossaic.Generated.ShardDO.chunk_invariant_preserved
   *   The Lean state-machine proves preservation for its abstract
   *   deleteChunks transition, not this SQL implementation.
   */
  private async removeFileRefs(fileId: string): Promise<{ marked: number }> {
    await this.scheduleSweep();
    return this.removeFileRefsTransaction(fileId);
  }

  private removeFileRefsTransaction(fileId: string): { marked: number } {
    const deletedAt = Date.now();
    return this.ctx.storage.transactionSync(() => {
      this.sql.exec(
        `UPDATE chunks
            SET ref_count = MAX(0, ref_count - (
              SELECT COUNT(*)
                FROM chunk_refs
               WHERE file_id = ? AND chunk_hash = chunks.hash
            ))
          WHERE hash IN (
            SELECT chunk_hash FROM chunk_refs WHERE file_id = ?
          )`,
        fileId,
        fileId
      );

      this.sql.exec(
        `UPDATE chunks
            SET deleted_at = ?
          WHERE ref_count = 0
            AND deleted_at IS NULL
            AND hash IN (
              SELECT chunk_hash FROM chunk_refs WHERE file_id = ?
            )`,
        deletedAt,
        fileId
      );
      const marked = (
        this.sql.exec("SELECT changes() AS n").toArray()[0] as { n: number }
      ).n;

      this.sql.exec("DELETE FROM chunk_refs WHERE file_id = ?", fileId);
      return { marked };
    });
  }

  /**
   * Ensure an alarm is scheduled for the next sweep. If one is already
   * set sooner than our target, leave it alone — single alarm per DO.
   */
  protected async scheduleSweep(): Promise<void> {
    const cur = await this.ctx.storage.getAlarm();
    const next = Date.now() + 5 * 60 * 1000; // 5 min default cadence
    if (cur === null || cur > next) {
      await this.ctx.storage.setAlarm(next);
    }
  }

  /**
   * Alarm handler — hard-deletes chunks soft-marked for >= 30s and
   * still at ref_count=0. Resurrected chunks (ref_count went back up
   * via a concurrent dedup PUT, with the deleted_at clear handled in
   * the PUT path) need the un-mark belt-and-suspenders here too.
   *
   * Reschedules itself if more rows remain past this batch's LIMIT 500.
   * Cloudflare alarms have at-least-once semantics with exponential
   * backoff retry on throw, so this handler is idempotent — re-running
   * the same batch is a no-op (the rows are already deleted).
   *
   * @lean-invariant Mossaic.Generated.ShardDO.alarm_safe
   *   The abstract alarm transition preserves the modeled validState.
   *   Cloudflare alarms, SQL, retries, and this implementation are not
   *   refined by that theorem.
   *
   * @lean-invariant Mossaic.Generated.ShardDO.alarm_only_deletes_zero
   *   The abstract transition only deletes modeled chunks satisfying its
   *   zero-refcount and grace predicates; no implementation refinement is
   *   claimed.
   */
  async alarm(): Promise<void> {
    this.recordRpc();
    this.ensureInit();
    const maintenancePending = this.ctx.storage.transactionSync(() =>
      this.runSchemaMaintenancePage()
    );
    const now = Date.now();
    const cutoff = now - 30_000; // 30s grace
    const rows = this.sql
      .exec(
        "SELECT hash, size FROM chunks WHERE deleted_at IS NOT NULL AND deleted_at < ? LIMIT 500",
        cutoff
      )
      .toArray() as { hash: string; size: number }[];

    let freed = 0;
    for (const { hash, size } of rows) {
      // Re-check ref_count under the same single-threaded fetch to
      // catch a resurrection-by-PUT that happened after the
      // chunk_refs DELETE but before the sweeper saw it.
      const live = this.sql
        .exec("SELECT ref_count FROM chunks WHERE hash = ?", hash)
        .toArray()[0] as { ref_count: number } | undefined;

      if (!live || live.ref_count > 0) {
        // Resurrected; un-mark and skip the delete.
        this.sql.exec(
          "UPDATE chunks SET deleted_at = NULL WHERE hash = ?",
          hash
        );
        continue;
      }

      this.sql.exec("DELETE FROM chunks WHERE hash = ?", hash);
      freed += size;
    }
    if (freed > 0) this.updateCapacity(-freed);

    const expiredJournal = this.sql
      .exec<{
        cleanup_kind: string;
        ref_id: string;
        cleanup_generation: string;
        request_cursor: number;
      } & Record<string, SqlStorageValue>>(
        `SELECT expiration.cleanup_kind, expiration.ref_id,
                expiration.cleanup_generation, expiration.request_cursor
           FROM shard_cleanup_page_expirations AS expiration
           JOIN shard_cleanup_progress AS progress
             ON progress.cleanup_kind = expiration.cleanup_kind
            AND progress.ref_id = expiration.ref_id
            AND progress.cleanup_generation = expiration.cleanup_generation
          WHERE expiration.expires_at <= ? AND progress.done != 0
          ORDER BY expiration.expires_at LIMIT ?`,
        now,
        SHARD_CLEANUP_PAGE_SIZE
      )
      .toArray();
    this.ctx.storage.transactionSync(() => {
      for (const page of expiredJournal) {
        this.sql.exec(
          `DELETE FROM shard_cleanup_pages
            WHERE cleanup_kind = ? AND ref_id = ?
              AND cleanup_generation = ? AND request_cursor = ?`,
          page.cleanup_kind,
          page.ref_id,
          page.cleanup_generation,
          page.request_cursor
        );
        this.sql.exec(
          `DELETE FROM shard_cleanup_page_expirations
            WHERE cleanup_kind = ? AND ref_id = ?
              AND cleanup_generation = ? AND request_cursor = ?`,
          page.cleanup_kind,
          page.ref_id,
          page.cleanup_generation,
          page.request_cursor
        );
        this.sql.exec(
          `DELETE FROM shard_cleanup_progress
            WHERE cleanup_kind = ? AND ref_id = ? AND cleanup_generation = ?
              AND done != 0
              AND NOT EXISTS (
                SELECT 1 FROM shard_cleanup_pages
                 WHERE cleanup_kind = ? AND ref_id = ? AND cleanup_generation = ?
              )`,
          page.cleanup_kind,
          page.ref_id,
          page.cleanup_generation,
          page.cleanup_kind,
          page.ref_id,
          page.cleanup_generation
        );
      }
    });

    const expiredFences = this.sql
      .exec<{ upload_id: string } & Record<string, SqlStorageValue>>(
        `SELECT upload_id FROM multipart_fence_expirations
          WHERE expires_at < ? ORDER BY expires_at LIMIT ?`,
        now - MULTIPART_FENCE_GC_GRACE_MS,
        SHARD_SCHEMA_MAINTENANCE_PAGE_SIZE
      )
      .toArray();
    this.ctx.storage.transactionSync(() => {
      for (const fence of expiredFences) {
        this.sql.exec("DELETE FROM multipart_fences WHERE upload_id = ?", fence.upload_id);
        this.sql.exec(
          "DELETE FROM multipart_fence_expirations WHERE upload_id = ?",
          fence.upload_id
        );
      }
    });

    // Reschedule if the sweep was capped at LIMIT 500.
    const more = this.sql
      .exec("SELECT 1 FROM chunks WHERE deleted_at IS NOT NULL LIMIT 1")
      .toArray();
    if (more.length > 0) {
      await this.ctx.storage.setAlarm(Date.now() + 60_000);
    } else {
      const nextAlarm = this.nextMaintenanceAlarm(
        maintenancePending ||
          expiredJournal.length === SHARD_CLEANUP_PAGE_SIZE ||
          expiredFences.length === SHARD_SCHEMA_MAINTENANCE_PAGE_SIZE
      );
      if (nextAlarm !== null) await this.ctx.storage.setAlarm(nextAlarm);
    }
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
