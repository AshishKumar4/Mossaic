/**
 * Phase 17 — typed-RPC test helpers.
 *
 * Replaces the pre-Phase-17 pattern of hand-rolled JSON-router
 * fetches like `stub.fetch("http://internal/signup", ...)` with the
 * typed RPC methods on `UserDO` (`appHandleSignup`, `appCreateFile`,
 * etc.).
 *
 * The legacy fetch handler was deleted in Phase 17 because every
 * production caller migrated to typed RPCs; these helpers keep the
 * existing tests focused on what they're testing (VFS write/read,
 * versioning, refcount accounting) without duplicating the typed-RPC
 * boilerplate at every call site.
 */

import type { UserDO } from "../../../worker/app/objects/user/user-do";

type UserStub = DurableObjectStub<UserDO>;

/** Materialize a fresh user via the typed `appHandleSignup` RPC. */
export async function rpcSignup(
  stub: UserStub,
  email: string,
  password = "abcd1234"
): Promise<string> {
  const { userId } = await stub.appHandleSignup(email, password);
  return userId;
}

/** Create a tmp/uploading file row. Returns fileId + chunk spec. */
export async function rpcCreateFile(
  stub: UserStub,
  args: {
    userId: string;
    fileName: string;
    fileSize: number;
    mimeType?: string;
    parentId?: string | null;
  }
): Promise<{
  fileId: string;
  chunkSize: number;
  chunkCount: number;
  poolSize: number;
}> {
  return stub.appCreateFile(
    args.userId,
    args.fileName,
    args.fileSize,
    args.mimeType ?? "application/octet-stream",
    args.parentId ?? null
  );
}

/** Record a successfully-uploaded chunk in `file_chunks`. */
export async function rpcRecordChunk(
  stub: UserStub,
  args: {
    fileId: string;
    chunkIndex: number;
    chunkHash: string;
    chunkSize: number;
    shardIndex: number;
  }
): Promise<void> {
  await stub.appRecordChunk(
    args.fileId,
    args.chunkIndex,
    args.chunkHash,
    args.chunkSize,
    args.shardIndex
  );
}

/** Flip the file row to `complete` and bump quota. */
export async function rpcCompleteFile(
  stub: UserStub,
  args: {
    fileId: string;
    fileHash: string;
    userId: string;
    fileSize: number;
  }
): Promise<void> {
  await stub.appCompleteFile(
    args.fileId,
    args.fileHash,
    args.userId,
    args.fileSize
  );
}

/** Read the file manifest (file row + chunks). */
export async function rpcGetManifest(
  stub: UserStub,
  fileId: string
): Promise<ReturnType<UserDO["appGetFileManifest"]>> {
  return stub.appGetFileManifest(fileId);
}

/** Insert a folder row. */
export async function rpcCreateFolder(
  stub: UserStub,
  args: { userId: string; name: string; parentId?: string | null }
): Promise<ReturnType<UserDO["appCreateFolder"]>> {
  return stub.appCreateFolder(args.userId, args.name, args.parentId ?? null);
}

/** Read user analytics (legacy `/stats`). */
export async function rpcGetStats(
  stub: UserStub,
  userId: string
): Promise<ReturnType<UserDO["appGetUserStats"]>> {
  return stub.appGetUserStats(userId);
}

/** Read user quota (legacy `/quota`). */
export async function rpcGetQuota(
  stub: UserStub,
  userId: string
): Promise<ReturnType<UserDO["appGetQuota"]>> {
  return stub.appGetQuota(userId);
}

/** List files+folders at a parent. */
export async function rpcListFiles(
  stub: UserStub,
  args: { userId: string; parentId?: string | null }
): Promise<ReturnType<UserDO["appListFiles"]>> {
  return stub.appListFiles(args.userId, args.parentId ?? null);
}
