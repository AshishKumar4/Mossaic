import type { UserDO } from "./user-do";
import type { Folder } from "@shared/types";
import { generateId } from "../../lib/utils";

/**
 * Create a new folder.
 */
export function createFolder(
  durableObject: UserDO,
  userId: string,
  name: string,
  parentId: string | null
): Folder {
  const folderId = generateId();
  const now = Date.now();

  durableObject.sql.exec(
    `INSERT INTO folders (folder_id, user_id, parent_id, name, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?)`,
    folderId,
    userId,
    parentId,
    name,
    now,
    now
  );

  return { folderId, name, parentId, createdAt: now, updatedAt: now };
}

/**
 * List folders in a parent folder.
 */
export function listFolders(
  durableObject: UserDO,
  userId: string,
  parentId: string | null
): Folder[] {
  const rows = parentId
    ? durableObject.sql
        .exec(
          "SELECT * FROM folders WHERE user_id = ? AND parent_id = ? ORDER BY name",
          userId,
          parentId
        )
        .toArray()
    : durableObject.sql
        .exec(
          "SELECT * FROM folders WHERE user_id = ? AND parent_id IS NULL ORDER BY name",
          userId
        )
        .toArray();

  return rows.map((r: Record<string, unknown>) => ({
    folderId: r.folder_id as string,
    name: r.name as string,
    parentId: (r.parent_id as string) || null,
    createdAt: r.created_at as number,
    updatedAt: r.updated_at as number,
  }));
}

/**
 * Get a folder by ID.
 */
export function getFolder(
  durableObject: UserDO,
  folderId: string
): Folder | null {
  const rows = durableObject.sql
    .exec("SELECT * FROM folders WHERE folder_id = ?", folderId)
    .toArray();

  if (rows.length === 0) return null;
  const r = rows[0] as Record<string, unknown>;
  return {
    folderId: r.folder_id as string,
    name: r.name as string,
    parentId: (r.parent_id as string) || null,
    createdAt: r.created_at as number,
    updatedAt: r.updated_at as number,
  };
}

/**
 * Get breadcrumb path for a folder.
 */
export function getFolderPath(
  durableObject: UserDO,
  folderId: string | null
): Folder[] {
  const path: Folder[] = [];
  let currentId = folderId;

  while (currentId) {
    const folder = getFolder(durableObject, currentId);
    if (!folder) break;
    path.unshift(folder);
    currentId = folder.parentId;
  }

  return path;
}
