import { DurableObject } from "cloudflare:workers";
import type {
  Env,
  UserStats,
  UserFilesByStatus,
  MimeDistribution,
  RecentUpload,
  ShardDistribution,
} from "@shared/types";
import { handleSignup, handleLogin } from "./auth";
import {
  createFile,
  recordChunk,
  completeFile,
  getFileManifest,
  listFiles,
  deleteFile,
  getFile,
} from "./files";
import { createFolder, listFolders, getFolder, getFolderPath } from "./folders";
import { getQuota, checkQuota, updateUsage } from "./quota";
import {
  vfsAbortWriteStream,
  vfsAppendWriteStream,
  vfsBeginWriteStream,
  vfsChmod,
  vfsCommitWriteStream,
  vfsCreateReadStream,
  vfsCreateWriteStream,
  vfsExists,
  vfsLstat,
  vfsMkdir,
  vfsOpenManifest,
  vfsOpenReadStream,
  vfsPullReadStream,
  vfsReadChunk,
  vfsReadFile,
  vfsReadlink,
  vfsReadManyStat,
  vfsReaddir,
  vfsRemoveRecursive,
  vfsRename,
  vfsRmdir,
  vfsStat,
  vfsSymlink,
  vfsUnlink,
  vfsWriteFile,
  type VFSReadHandle,
  type VFSWriteHandle,
} from "./vfs-ops";
import type {
  OpenManifestResult,
  VFSScope,
  VFSStatRaw,
} from "@shared/vfs-types";

export class UserDO extends DurableObject<Env> {
  sql: SqlStorage;
  /**
   * Public alias for the protected `env` from the DurableObject base
   * class. Phase 2 vfs-ops needs to dispatch ShardDO subrequests by
   * binding name; without this alias TS rejects external access. The
   * base class's `env` remains protected; we shadow it.
   */
  envPublic: Env;
  private initialized = false;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.envPublic = env;
  }

  private ensureInit(): void {
    if (this.initialized) return;
    this.initialized = true;

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS auth (
        user_id       TEXT PRIMARY KEY,
        email         TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at    INTEGER NOT NULL,
        updated_at    INTEGER NOT NULL
      )
    `);

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS files (
        file_id       TEXT PRIMARY KEY,
        user_id       TEXT NOT NULL,
        parent_id     TEXT,
        file_name     TEXT NOT NULL,
        file_size     INTEGER NOT NULL,
        file_hash     TEXT NOT NULL,
        mime_type     TEXT NOT NULL,
        chunk_size    INTEGER NOT NULL,
        chunk_count   INTEGER NOT NULL,
        pool_size     INTEGER NOT NULL,
        status        TEXT NOT NULL DEFAULT 'uploading',
        created_at    INTEGER NOT NULL,
        updated_at    INTEGER NOT NULL,
        deleted_at    INTEGER
      )
    `);

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS file_chunks (
        file_id       TEXT NOT NULL,
        chunk_index   INTEGER NOT NULL,
        chunk_hash    TEXT NOT NULL,
        chunk_size    INTEGER NOT NULL,
        shard_index   INTEGER NOT NULL,
        PRIMARY KEY (file_id, chunk_index)
      )
    `);

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS folders (
        folder_id     TEXT PRIMARY KEY,
        user_id       TEXT NOT NULL,
        parent_id     TEXT,
        name          TEXT NOT NULL,
        created_at    INTEGER NOT NULL,
        updated_at    INTEGER NOT NULL
      )
    `);

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS quota (
        user_id       TEXT PRIMARY KEY,
        storage_used  INTEGER NOT NULL DEFAULT 0,
        storage_limit INTEGER NOT NULL DEFAULT 107374182400,
        file_count    INTEGER NOT NULL DEFAULT 0,
        pool_size     INTEGER NOT NULL DEFAULT 32
      )
    `);

    // ── VFS schema migrations (sdk-impl-plan §3.1) ─────────────────────────
    // Each ALTER is idempotent via try/catch: SQLite throws "duplicate
    // column name" if the column already exists. The pattern matches
    // search-do.ts:59-68. CREATE TABLE/INDEX IF NOT EXISTS is naturally
    // idempotent.
    //
    // Backward compatibility: existing rows get default mode, NULL inline
    // data, node_kind='file'. The legacy app's reads keep working because
    // (a) new columns have defaults, (b) the manifest reader (files.ts)
    // continues to fall through to file_chunks when inline_data IS NULL.

    // file mode (POSIX), inline tier, symlink kind
    try {
      this.sql.exec(
        "ALTER TABLE files ADD COLUMN mode INTEGER NOT NULL DEFAULT 420"
      ); // 0o644
    } catch {
      // column already exists
    }
    try {
      this.sql.exec("ALTER TABLE files ADD COLUMN inline_data BLOB");
    } catch {
      // column already exists
    }
    try {
      this.sql.exec("ALTER TABLE files ADD COLUMN symlink_target TEXT");
    } catch {
      // column already exists
    }
    try {
      this.sql.exec(
        "ALTER TABLE files ADD COLUMN node_kind TEXT NOT NULL DEFAULT 'file'"
      );
    } catch {
      // column already exists
    }
    try {
      this.sql.exec(
        "ALTER TABLE folders ADD COLUMN mode INTEGER NOT NULL DEFAULT 493"
      ); // 0o755
    } catch {
      // column already exists
    }

    // POSIX uniqueness via partial indexes (SQLite cannot ALTER TABLE ADD
    // UNIQUE on existing tables). Scoped to non-deleted rows so prior
    // soft-deleted duplicates don't block migration.
    //
    // If existing data has live duplicates, this CREATE throws and is
    // swallowed; the admin dedupe route (Phase 6) resolves them later.
    try {
      this.sql.exec(`
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_files_parent_name
          ON files(user_id, IFNULL(parent_id, ''), file_name)
          WHERE status != 'deleted'
      `);
    } catch {
      // dupe live rows exist; admin dedupe is required before re-running
    }
    try {
      this.sql.exec(`
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_folders_parent_name
          ON folders(user_id, IFNULL(parent_id, ''), name)
      `);
    } catch {
      // dupe folder rows exist; admin dedupe is required before re-running
    }

    // Lookup indexes (overdue per study §4)
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_files_parent
        ON files(user_id, parent_id, status)
    `);
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_folders_parent
        ON folders(user_id, parent_id)
    `);
  }

  async fetch(request: Request): Promise<Response> {
    this.ensureInit();

    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    try {
      // Auth routes
      if (path === "/signup" && method === "POST") {
        const { email, password } = (await request.json()) as {
          email: string;
          password: string;
        };
        const result = await handleSignup(this, email, password);
        return Response.json(result);
      }

      if (path === "/login" && method === "POST") {
        const { email, password } = (await request.json()) as {
          email: string;
          password: string;
        };
        const result = await handleLogin(this, email, password);
        return Response.json(result);
      }

      // File routes
      if (path === "/files/create" && method === "POST") {
        const body = (await request.json()) as {
          userId: string;
          fileName: string;
          fileSize: number;
          mimeType: string;
          parentId?: string | null;
        };
        if (!checkQuota(this, body.userId, body.fileSize)) {
          return Response.json({ error: "Quota exceeded" }, { status: 403 });
        }
        const result = createFile(
          this,
          body.userId,
          body.fileName,
          body.fileSize,
          body.mimeType,
          body.parentId ?? null
        );
        return Response.json(result);
      }

      if (path === "/files/chunk" && method === "POST") {
        const body = (await request.json()) as {
          fileId: string;
          chunkIndex: number;
          chunkHash: string;
          chunkSize: number;
          shardIndex: number;
        };
        recordChunk(
          this,
          body.fileId,
          body.chunkIndex,
          body.chunkHash,
          body.chunkSize,
          body.shardIndex
        );
        return Response.json({ ok: true });
      }

      if (path === "/files/complete" && method === "POST") {
        const body = (await request.json()) as {
          fileId: string;
          fileHash: string;
          userId: string;
          fileSize: number;
        };
        completeFile(this, body.fileId, body.fileHash);
        updateUsage(this, body.userId, body.fileSize, 1);
        return Response.json({ ok: true });
      }

      if (path.startsWith("/files/manifest/")) {
        const fileId = path.split("/")[3];
        const manifest = getFileManifest(this, fileId);
        if (!manifest) {
          return Response.json({ error: "File not found" }, { status: 404 });
        }
        return Response.json(manifest);
      }

      if (path === "/files/list" && method === "POST") {
        const body = (await request.json()) as {
          userId: string;
          parentId?: string | null;
        };
        const files = listFiles(this, body.userId, body.parentId ?? null);
        const folders = listFolders(this, body.userId, body.parentId ?? null);
        return Response.json({ files, folders });
      }

      if (path.startsWith("/files/delete/") && method === "DELETE") {
        const fileId = path.split("/")[3];
        const file = getFile(this, fileId);
        if (!file) {
          return Response.json({ error: "File not found" }, { status: 404 });
        }
        deleteFile(this, fileId);
        const userId = request.headers.get("X-User-Id") || "";
        updateUsage(this, userId, -(file.file_size as number), -1);
        return Response.json({ ok: true });
      }

      if (path.startsWith("/files/get/")) {
        const fileId = path.split("/")[3];
        const file = getFile(this, fileId);
        if (!file) {
          return Response.json({ error: "File not found" }, { status: 404 });
        }
        return Response.json(file);
      }

      // Folder routes
      if (path === "/folders/create" && method === "POST") {
        const body = (await request.json()) as {
          userId: string;
          name: string;
          parentId?: string | null;
        };
        const folder = createFolder(
          this,
          body.userId,
          body.name,
          body.parentId ?? null
        );
        return Response.json(folder);
      }

      if (path.startsWith("/folders/list") && method === "POST") {
        const body = (await request.json()) as {
          userId: string;
          parentId?: string | null;
        };
        const folders = listFolders(this, body.userId, body.parentId ?? null);
        return Response.json(folders);
      }

      if (path.startsWith("/folders/path/")) {
        const folderId = path.split("/")[3] || null;
        const folderPath = getFolderPath(this, folderId);
        return Response.json(folderPath);
      }

      // Gallery route — list all images across all folders
      if (path === "/gallery/photos" && method === "POST") {
        const body = (await request.json()) as { userId: string };
        const rows = this.sql
          .exec(
            "SELECT file_id, file_name, file_size, mime_type, parent_id, created_at, updated_at FROM files WHERE user_id = ? AND status = 'complete' AND mime_type LIKE 'image/%' ORDER BY created_at DESC",
            body.userId
          )
          .toArray();

        const photos = rows.map((r: Record<string, unknown>) => ({
          fileId: r.file_id as string,
          fileName: r.file_name as string,
          fileSize: r.file_size as number,
          mimeType: r.mime_type as string,
          parentId: (r.parent_id as string) || null,
          createdAt: r.created_at as number,
          updatedAt: r.updated_at as number,
        }));

        return Response.json({ photos });
      }

      // Stats route
      if (path === "/stats" && method === "POST") {
        const body = (await request.json()) as { userId: string };
        const stats = this.getUserStats(body.userId);
        return Response.json(stats);
      }

      // Quota routes
      if (path === "/quota" && method === "POST") {
        const body = (await request.json()) as { userId: string };
        const quota = getQuota(this, body.userId);
        return Response.json(quota);
      }

      return Response.json({ error: "Not found" }, { status: 404 });
    } catch (err) {
      const message = err instanceof Error ? err.message : "Internal error";
      return Response.json({ error: message }, { status: 500 });
    }
  }

  private getUserStats(userId: string): UserStats {
    // Total files and storage
    const quotaRows = this.sql
      .exec("SELECT * FROM quota WHERE user_id = ?", userId)
      .toArray();

    const quota =
      quotaRows.length > 0
        ? (quotaRows[0] as {
            storage_used: number;
            storage_limit: number;
            file_count: number;
          })
        : { storage_used: 0, storage_limit: 107374182400, file_count: 0 };

    // Files by status
    const statusRows = this.sql
      .exec(
        "SELECT status, COUNT(*) as count FROM files WHERE user_id = ? GROUP BY status",
        userId
      )
      .toArray() as { status: string; count: number }[];

    const filesByStatus: UserFilesByStatus = {
      uploading: 0,
      complete: 0,
      failed: 0,
      deleted: 0,
    };
    for (const row of statusRows) {
      if (row.status in filesByStatus) {
        filesByStatus[row.status as keyof UserFilesByStatus] = row.count;
      }
    }

    // Derive totalFiles from actual files table for consistency —
    // the quota row may be stale or missing, so count non-deleted files directly
    const actualFileCount =
      filesByStatus.complete + filesByStatus.uploading + filesByStatus.failed;

    // Mime type distribution
    const mimeRows = this.sql
      .exec(
        "SELECT mime_type, COUNT(*) as count, COALESCE(SUM(file_size), 0) as total_size FROM files WHERE user_id = ? AND status != 'deleted' GROUP BY mime_type ORDER BY count DESC",
        userId
      )
      .toArray() as { mime_type: string; count: number; total_size: number }[];

    const mimeDistribution: MimeDistribution[] = mimeRows.map((r) => ({
      mimeType: r.mime_type,
      count: r.count,
      totalSize: r.total_size,
    }));

    // Recent uploads (last 10)
    const recentRows = this.sql
      .exec(
        "SELECT file_id, file_name, file_size, mime_type, status, created_at FROM files WHERE user_id = ? ORDER BY created_at DESC LIMIT 10",
        userId
      )
      .toArray() as {
      file_id: string;
      file_name: string;
      file_size: number;
      mime_type: string;
      status: string;
      created_at: number;
    }[];

    const recentUploads: RecentUpload[] = recentRows.map((r) => ({
      fileId: r.file_id,
      fileName: r.file_name,
      fileSize: r.file_size,
      mimeType: r.mime_type,
      status: r.status,
      createdAt: r.created_at,
    }));

    // Shard distribution
    const shardRows = this.sql
      .exec(
        "SELECT fc.shard_index, COUNT(*) as chunk_count FROM file_chunks fc JOIN files f ON fc.file_id = f.file_id WHERE f.user_id = ? GROUP BY fc.shard_index ORDER BY fc.shard_index",
        userId
      )
      .toArray() as { shard_index: number; chunk_count: number }[];

    const shardDistribution: ShardDistribution[] = shardRows.map((r) => ({
      shardIndex: r.shard_index,
      chunkCount: r.chunk_count,
    }));

    return {
      totalFiles: Math.max(actualFileCount, quota.file_count),
      totalStorageUsed: quota.storage_used,
      quotaLimit: quota.storage_limit,
      filesByStatus,
      mimeDistribution,
      recentUploads,
      shardDistribution,
    };
  }

  // ── VFS RPC surface (Phase 2: read-side) ───────────────────────────────
  //
  // Cloudflare DO RPC: any public async method on the DO class is callable
  // from a holder of the stub via `stub.methodName(args)`. The consumer
  // pays exactly one subrequest per call regardless of internal fan-out.
  // See sdk-impl-plan §5.3 for the full contract; these are the read-side
  // methods that land in Phase 2. Write-side and streaming methods come
  // in Phases 3 and 4.
  //
  // Each method calls ensureInit() so the schema migrations (Phase 1)
  // run before any VFS access on a DO that hasn't seen any legacy
  // /fetch traffic yet.

  /** stat() — follows trailing symlinks. Throws ENOENT/ELOOP/ENOTDIR. */
  async vfsStat(scope: VFSScope, path: string): Promise<VFSStatRaw> {
    this.ensureInit();
    return vfsStat(this, scope, path);
  }

  /** lstat() — does NOT follow trailing symlinks. */
  async vfsLstat(scope: VFSScope, path: string): Promise<VFSStatRaw> {
    this.ensureInit();
    return vfsLstat(this, scope, path);
  }

  /** exists() — returns true iff the path resolves to a file/dir/symlink. */
  async vfsExists(scope: VFSScope, path: string): Promise<boolean> {
    this.ensureInit();
    return vfsExists(this, scope, path);
  }

  /** readlink() — returns the symlink target string. EINVAL if not a symlink. */
  async vfsReadlink(scope: VFSScope, path: string): Promise<string> {
    this.ensureInit();
    return vfsReadlink(this, scope, path);
  }

  /** readdir() — entry names under a directory. ENOTDIR/ENOENT if applicable. */
  async vfsReaddir(scope: VFSScope, path: string): Promise<string[]> {
    this.ensureInit();
    return vfsReaddir(this, scope, path);
  }

  /** readManyStat() — batched lstat for git-style workloads. */
  async vfsReadManyStat(
    scope: VFSScope,
    paths: string[]
  ): Promise<(VFSStatRaw | null)[]> {
    this.ensureInit();
    return vfsReadManyStat(this, scope, paths);
  }

  /** readFile() — returns Uint8Array bytes. EISDIR/EFBIG/ENOENT/ELOOP. */
  async vfsReadFile(scope: VFSScope, path: string): Promise<Uint8Array> {
    this.ensureInit();
    return vfsReadFile(this, scope, path);
  }

  /** openManifest() — public, shard-index-stripped manifest for caller-orchestrated reads. */
  async vfsOpenManifest(
    scope: VFSScope,
    path: string
  ): Promise<OpenManifestResult> {
    this.ensureInit();
    return vfsOpenManifest(this, scope, path);
  }

  /** readChunk() — fetch one chunk by (path, chunkIndex). */
  async vfsReadChunk(
    scope: VFSScope,
    path: string,
    chunkIndex: number
  ): Promise<Uint8Array> {
    this.ensureInit();
    return vfsReadChunk(this, scope, path, chunkIndex);
  }

  // ── VFS RPC surface (Phase 3: write-side) ──────────────────────────────
  //
  // Atomic writes (temp-id-then-rename), hard delete with chunk GC fan-out,
  // and the supporting mutating ops. Each method runs inside a single
  // single-threaded DO invocation, so the supersede + rename sequence in
  // commitRename is atomic against concurrent reads/writes (sdk-impl-plan
  // §7). Chunks are reaped via ShardDO.deleteChunks RPC, which soft-marks
  // and lets the alarm-driven sweeper hard-delete after a 30s grace
  // (sdk-impl-plan §8.3).
  //
  // Inline tier (≤ INLINE_LIMIT) writes never touch ShardDO — the data
  // lives in files.inline_data and the entire write is one INSERT.

  /** writeFile() — atomic, last-writer-wins. Inline tier ≤16KB; chunked otherwise. */
  async vfsWriteFile(
    scope: VFSScope,
    path: string,
    data: Uint8Array,
    opts?: { mode?: number; mimeType?: string }
  ): Promise<void> {
    this.ensureInit();
    return vfsWriteFile(this, scope, path, data, opts);
  }

  /** unlink() — hard-delete file/symlink + dispatch chunk GC. EISDIR for dirs. */
  async vfsUnlink(scope: VFSScope, path: string): Promise<void> {
    this.ensureInit();
    return vfsUnlink(this, scope, path);
  }

  /** mkdir() — create folder; recursive flag walks intermediates. */
  async vfsMkdir(
    scope: VFSScope,
    path: string,
    opts?: { recursive?: boolean; mode?: number }
  ): Promise<void> {
    this.ensureInit();
    vfsMkdir(this, scope, path, opts);
  }

  /** rmdir() — remove empty directory. ENOTEMPTY/ENOTDIR/ENOENT. */
  async vfsRmdir(scope: VFSScope, path: string): Promise<void> {
    this.ensureInit();
    vfsRmdir(this, scope, path);
  }

  /** rename() — atomic move/rename. Replace semantics for files, EEXIST for dirs. */
  async vfsRename(
    scope: VFSScope,
    src: string,
    dst: string
  ): Promise<void> {
    this.ensureInit();
    return vfsRename(this, scope, src, dst);
  }

  /** chmod() — update mode bits on a file/symlink/dir. */
  async vfsChmod(
    scope: VFSScope,
    path: string,
    mode: number
  ): Promise<void> {
    this.ensureInit();
    vfsChmod(this, scope, path, mode);
  }

  /** symlink() — create a symlink at linkPath pointing to target. */
  async vfsSymlink(
    scope: VFSScope,
    target: string,
    linkPath: string
  ): Promise<void> {
    this.ensureInit();
    vfsSymlink(this, scope, target, linkPath);
  }

  /** removeRecursive() — paginated rm -rf on a directory subtree. */
  async vfsRemoveRecursive(
    scope: VFSScope,
    path: string,
    cursor?: string
  ): Promise<{ done: boolean; cursor?: string }> {
    this.ensureInit();
    return vfsRemoveRecursive(this, scope, path, cursor);
  }

  // ── Phase 4: streaming + handle-based stream primitives ───────────────
  //
  // Two shapes per stream direction:
  //
  //   Read:  vfsOpenReadStream + vfsPullReadStream (handle-based, works
  //          across separate consumer invocations — the escape hatch
  //          for files larger than one Worker invocation can fan out)
  //          and vfsCreateReadStream (returns a ReadableStream over RPC
  //          for in-the-same-invocation use cases).
  //
  //   Write: vfsBeginWriteStream + vfsAppendWriteStream +
  //          vfsCommitWriteStream / vfsAbortWriteStream (handle-based,
  //          chunk-by-chunk, resumable across consumer invocations)
  //          and vfsCreateWriteStream (returns a WritableStream that
  //          drives the same primitives internally).
  //
  // The handle-based primitives are the load-bearing surface — the
  // stream wrappers are convenience built on top. Both share the
  // commit-rename atomicity protocol from Phase 3.

  /** openReadStream — open a read handle. Caller pumps via vfsPullReadStream. */
  async vfsOpenReadStream(
    scope: VFSScope,
    path: string
  ): Promise<VFSReadHandle> {
    this.ensureInit();
    return vfsOpenReadStream(this, scope, path);
  }

  /** pullReadStream — fetch one chunk from an open read handle. Optional byte range within the chunk. */
  async vfsPullReadStream(
    scope: VFSScope,
    handle: VFSReadHandle,
    chunkIndex: number,
    range?: { start?: number; end?: number }
  ): Promise<Uint8Array> {
    this.ensureInit();
    return vfsPullReadStream(this, scope, handle, chunkIndex, range);
  }

  /** createReadStream — return a ReadableStream pulling chunks lazily. Optional byte-range over the file. */
  async vfsCreateReadStream(
    scope: VFSScope,
    path: string,
    range?: { start?: number; end?: number }
  ): Promise<ReadableStream<Uint8Array>> {
    this.ensureInit();
    return vfsCreateReadStream(this, scope, path, range);
  }

  /** beginWriteStream — open a write handle. Caller pumps via vfsAppendWriteStream then commits. */
  async vfsBeginWriteStream(
    scope: VFSScope,
    path: string,
    opts?: { mode?: number; mimeType?: string }
  ): Promise<VFSWriteHandle> {
    this.ensureInit();
    return vfsBeginWriteStream(this, scope, path, opts);
  }

  /** appendWriteStream — push one chunk. chunkIndex must be sequential. Returns cumulative bytes. */
  async vfsAppendWriteStream(
    scope: VFSScope,
    handle: VFSWriteHandle,
    chunkIndex: number,
    data: Uint8Array
  ): Promise<{ bytesWritten: number }> {
    this.ensureInit();
    return vfsAppendWriteStream(this, scope, handle, chunkIndex, data);
  }

  /** commitWriteStream — atomic supersede + rename (Phase 3 protocol). */
  async vfsCommitWriteStream(
    scope: VFSScope,
    handle: VFSWriteHandle
  ): Promise<void> {
    this.ensureInit();
    return vfsCommitWriteStream(this, scope, handle);
  }

  /** abortWriteStream — drop the tmp row + queue chunk GC. Idempotent. */
  async vfsAbortWriteStream(
    scope: VFSScope,
    handle: VFSWriteHandle
  ): Promise<void> {
    this.ensureInit();
    return vfsAbortWriteStream(this, scope, handle);
  }

  /**
   * createWriteStream — return a WritableStream backed by the handle
   * primitives. Returns the wrapper { stream, handle } so callers that
   * need to surface the handle (for resumability or progress tracking)
   * can grab it.
   */
  async vfsCreateWriteStream(
    scope: VFSScope,
    path: string,
    opts?: { mode?: number; mimeType?: string }
  ): Promise<{ stream: WritableStream<Uint8Array>; handle: VFSWriteHandle }> {
    this.ensureInit();
    return vfsCreateWriteStream(this, scope, path, opts);
  }
}
