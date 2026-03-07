import { useState, useEffect, useMemo, useCallback } from "react";
import type { UserFile, Folder } from "@shared/types";
import { api } from "../lib/api";

export interface Album {
  folderId: string | null;
  name: string;
  imageCount: number;
}

export function useGallery() {
  const [allFiles, setAllFiles] = useState<UserFile[]>([]);
  const [folders, setFolders] = useState<Folder[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedImage, setSelectedImage] = useState<UserFile | null>(null);
  const [selectedAlbum, setSelectedAlbum] = useState<string | null>(null); // null = "All"

  // Load all files including from folders
  useEffect(() => {
    async function loadImages() {
      try {
        setIsLoading(true);

        // Fetch root files and folders
        const rootResult = await api.listFiles(null);
        let files = [...rootResult.files];
        const rootFolders = rootResult.folders || [];

        // Fetch files within each folder in parallel
        const folderPromises = rootFolders.map(async (folder) => {
          try {
            const contents = await api.getFolderContents(folder.folderId);
            return contents.files.map((f) => ({
              ...f,
              // Preserve parentId to know which folder it belongs to
            }));
          } catch {
            return [];
          }
        });

        const folderFiles = await Promise.all(folderPromises);
        for (const ff of folderFiles) {
          files = files.concat(ff);
        }

        setAllFiles(files);
        setFolders(rootFolders);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load gallery");
      } finally {
        setIsLoading(false);
      }
    }
    loadImages();
  }, []);

  // Filter to only images
  const images = useMemo(
    () =>
      allFiles.filter(
        (f) => f.mimeType.startsWith("image/") && f.status === "complete"
      ),
    [allFiles]
  );

  // Build album list from folders that contain images
  const albums = useMemo<Album[]>(() => {
    const albumMap = new Map<string | null, number>();

    // Count images per folder
    for (const img of images) {
      const parent = img.parentId || null;
      albumMap.set(parent, (albumMap.get(parent) || 0) + 1);
    }

    const result: Album[] = [];

    // Root "album"
    const rootCount = albumMap.get(null) || 0;
    if (rootCount > 0) {
      result.push({ folderId: null, name: "Unsorted", imageCount: rootCount });
    }

    // Folder-based albums
    for (const folder of folders) {
      const count = albumMap.get(folder.folderId) || 0;
      if (count > 0) {
        result.push({
          folderId: folder.folderId,
          name: folder.name,
          imageCount: count,
        });
      }
    }

    return result;
  }, [images, folders]);

  // Filtered images based on selected album
  const filteredImages = useMemo(() => {
    if (selectedAlbum === null) return images; // Show all
    if (selectedAlbum === "__unsorted__") {
      return images.filter((f) => !f.parentId);
    }
    return images.filter((f) => f.parentId === selectedAlbum);
  }, [images, selectedAlbum]);

  // Navigation: select next/prev image
  const selectNext = useCallback(() => {
    if (!selectedImage) return;
    const idx = filteredImages.findIndex(
      (f) => f.fileId === selectedImage.fileId
    );
    if (idx >= 0 && idx < filteredImages.length - 1) {
      setSelectedImage(filteredImages[idx + 1]);
    }
  }, [selectedImage, filteredImages]);

  const selectPrev = useCallback(() => {
    if (!selectedImage) return;
    const idx = filteredImages.findIndex(
      (f) => f.fileId === selectedImage.fileId
    );
    if (idx > 0) {
      setSelectedImage(filteredImages[idx - 1]);
    }
  }, [selectedImage, filteredImages]);

  const currentIndex = selectedImage
    ? filteredImages.findIndex((f) => f.fileId === selectedImage.fileId)
    : -1;

  return {
    images: filteredImages,
    allImageCount: images.length,
    albums,
    selectedAlbum,
    setSelectedAlbum,
    isLoading,
    error,
    selectedImage,
    setSelectedImage,
    selectNext,
    selectPrev,
    currentIndex,
    totalImages: filteredImages.length,
  };
}
