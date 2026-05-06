import { useState, useCallback, useEffect } from "react";
import { api, ApiError } from "@/lib/api";
import type { UserFile, Folder, FileListResponse } from "@shared/types";

interface FilesState {
  files: UserFile[];
  folders: Folder[];
  path: Folder[];
  loading: boolean;
  error: string | null;
}

export function useFiles(parentId: string | null) {
  const [state, setState] = useState<FilesState>({
    files: [],
    folders: [],
    path: [],
    loading: true,
    error: null,
  });

  const refresh = useCallback(async () => {
    setState((s) => ({ ...s, loading: true, error: null }));
    try {
      let result: FileListResponse & { path?: Folder[] };
      if (parentId) {
        result = await api.getFolder(parentId);
      } else {
        result = await api.listFiles(null);
      }
      setState({
        files: result.files,
        folders: result.folders,
        path: (result as { path?: Folder[] }).path || [],
        loading: false,
        error: null,
      });
    } catch (err) {
      const message =
        err instanceof ApiError ? err.message : "Failed to load files";
      setState((s) => ({ ...s, loading: false, error: message }));
    }
  }, [parentId]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const deleteFile = useCallback(
    async (fileId: string) => {
      await api.deleteFile(fileId);
      await refresh();
    },
    [refresh]
  );

  const createFolder = useCallback(
    async (name: string) => {
      await api.createFolder({ name, parentId });
      await refresh();
    },
    [parentId, refresh]
  );

  return { ...state, refresh, deleteFile, createFolder };
}
