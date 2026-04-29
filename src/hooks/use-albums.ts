import { useState, useCallback, useEffect } from "react";
import { useAuth } from "@/lib/auth";
import type { Album, SharedAlbum } from "@app/types";

const ALBUMS_KEY = "mossaic_albums";
const SHARES_KEY = "mossaic_shares";

function generateId(): string {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
}

function loadAlbums(): Album[] {
  try {
    const raw = localStorage.getItem(ALBUMS_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

function saveAlbums(albums: Album[]) {
  localStorage.setItem(ALBUMS_KEY, JSON.stringify(albums));
}

function loadShares(): SharedAlbum[] {
  try {
    const raw = localStorage.getItem(SHARES_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

function saveShares(shares: SharedAlbum[]) {
  localStorage.setItem(SHARES_KEY, JSON.stringify(shares));
}

export function useAlbums() {
  const { userId } = useAuth();
  const [albums, setAlbums] = useState<Album[]>(loadAlbums);
  const [shares, setShares] = useState<SharedAlbum[]>(loadShares);

  useEffect(() => {
    saveAlbums(albums);
  }, [albums]);

  useEffect(() => {
    saveShares(shares);
  }, [shares]);

  const createAlbum = useCallback((name: string, photoIds: string[]) => {
    const album: Album = {
      id: generateId(),
      name,
      photoIds,
      coverPhotoId: photoIds[0] || null,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    };
    setAlbums((prev) => [album, ...prev]);
    return album;
  }, []);

  const updateAlbum = useCallback(
    (albumId: string, updates: Partial<Pick<Album, "name" | "photoIds" | "coverPhotoId">>) => {
      setAlbums((prev) =>
        prev.map((a) =>
          a.id === albumId
            ? { ...a, ...updates, updatedAt: Date.now() }
            : a
        )
      );
    },
    []
  );

  const deleteAlbum = useCallback((albumId: string) => {
    setAlbums((prev) => prev.filter((a) => a.id !== albumId));
    setShares((prev) => prev.filter((s) => s.albumId !== albumId));
  }, []);

  const addPhotosToAlbum = useCallback(
    (albumId: string, photoIds: string[]) => {
      setAlbums((prev) =>
        prev.map((a) => {
          if (a.id !== albumId) return a;
          const newIds = [...new Set([...a.photoIds, ...photoIds])];
          return {
            ...a,
            photoIds: newIds,
            coverPhotoId: a.coverPhotoId || newIds[0] || null,
            updatedAt: Date.now(),
          };
        })
      );
    },
    []
  );

  const removePhotoFromAlbum = useCallback(
    (albumId: string, photoId: string) => {
      setAlbums((prev) =>
        prev.map((a) => {
          if (a.id !== albumId) return a;
          const newIds = a.photoIds.filter((id) => id !== photoId);
          return {
            ...a,
            photoIds: newIds,
            coverPhotoId:
              a.coverPhotoId === photoId ? newIds[0] || null : a.coverPhotoId,
            updatedAt: Date.now(),
          };
        })
      );
    },
    []
  );

  const shareAlbum = useCallback(
    (albumId: string): string => {
      const album = albums.find((a) => a.id === albumId);
      if (!album || !userId) return "";

      // Check if already shared
      const existing = shares.find((s) => s.albumId === albumId);
      if (existing) return existing.token;

      // Create share token: base64 encoded JSON with userId + fileIds
      const tokenData = JSON.stringify({
        userId,
        fileIds: album.photoIds,
        albumName: album.name,
      });
      const token = btoa(tokenData);

      const share: SharedAlbum = {
        token,
        albumId,
        createdAt: Date.now(),
      };

      setShares((prev) => [...prev, share]);
      return token;
    },
    [albums, shares, userId]
  );

  const unshareAlbum = useCallback((albumId: string) => {
    setShares((prev) => prev.filter((s) => s.albumId !== albumId));
  }, []);

  const getShareToken = useCallback(
    (albumId: string): string | null => {
      const share = shares.find((s) => s.albumId === albumId);
      return share?.token || null;
    },
    [shares]
  );

  const getAlbum = useCallback(
    (albumId: string): Album | undefined => {
      return albums.find((a) => a.id === albumId);
    },
    [albums]
  );

  return {
    albums,
    shares,
    createAlbum,
    updateAlbum,
    deleteAlbum,
    addPhotosToAlbum,
    removePhotoFromAlbum,
    shareAlbum,
    unshareAlbum,
    getShareToken,
    getAlbum,
  };
}
