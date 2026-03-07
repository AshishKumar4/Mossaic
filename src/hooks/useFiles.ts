import { useState, useCallback, useEffect } from "react";
import type {
  UserFile,
  Folder,
  TransferProgress,
} from "@shared/types";
import { api } from "../lib/api";
import { uploadFile } from "../lib/upload-engine";
import { downloadFile, saveBlobAs } from "../lib/download-engine";
import { useAuth } from "./useAuth";

export function useFiles(parentId: string | null = null) {
  const { userId } = useAuth();
  const [files, setFiles] = useState<UserFile[]>([]);
  const [folders, setFolders] = useState<Folder[]>([]);
  const [folderPath, setFolderPath] = useState<Folder[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [uploads, setUploads] = useState<Map<string, TransferProgress>>(
    new Map()
  );
  const [downloads, setDownloads] = useState<Map<string, TransferProgress>>(
    new Map()
  );

  const refresh = useCallback(async () => {
    try {
      setIsLoading(true);
      setError(null);

      if (parentId) {
        const result = await api.getFolderContents(parentId);
        setFiles(result.files);
        setFolders(result.folders);
        setFolderPath((result as { path?: Folder[] }).path || []);
      } else {
        const result = await api.listFiles(null);
        setFiles(result.files);
        setFolders(result.folders);
        setFolderPath([]);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load files");
    } finally {
      setIsLoading(false);
    }
  }, [parentId]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const upload = useCallback(
    async (fileList: FileList | File[]) => {
      if (!userId) return;

      const filesToUpload = Array.from(fileList);
      for (const file of filesToUpload) {
        uploadFile(file, parentId, userId, {
          onProgress: (progress) => {
            setUploads((prev) => {
              const next = new Map(prev);
              next.set(progress.fileId, progress);
              return next;
            });
          },
          onComplete: (fileId) => {
            setUploads((prev) => {
              const next = new Map(prev);
              next.delete(fileId);
              return next;
            });
            refresh();
          },
          onError: (err) => {
            setError(err.message);
          },
        });
      }
    },
    [userId, parentId, refresh]
  );

  const download = useCallback(async (fileId: string) => {
    downloadFile(fileId, {
      onProgress: (progress) => {
        setDownloads((prev) => {
          const next = new Map(prev);
          next.set(progress.fileId, progress);
          return next;
        });
      },
      onComplete: (blob, fileName) => {
        saveBlobAs(blob, fileName);
        setDownloads((prev) => {
          const next = new Map(prev);
          next.delete(fileId);
          return next;
        });
      },
      onError: (err) => {
        setError(err.message);
        setDownloads((prev) => {
          const next = new Map(prev);
          next.delete(fileId);
          return next;
        });
      },
    });
  }, []);

  const deleteFile = useCallback(
    async (fileId: string) => {
      try {
        await api.deleteFile(fileId);
        await refresh();
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to delete file");
      }
    },
    [refresh]
  );

  const createFolder = useCallback(
    async (name: string) => {
      try {
        await api.createFolder({ name, parentId });
        await refresh();
      } catch (err) {
        setError(
          err instanceof Error ? err.message : "Failed to create folder"
        );
      }
    },
    [parentId, refresh]
  );

  return {
    files,
    folders,
    folderPath,
    isLoading,
    error,
    uploads,
    downloads,
    refresh,
    upload,
    download,
    deleteFile,
    createFolder,
  };
}
