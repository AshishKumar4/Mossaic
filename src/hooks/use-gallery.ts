import { useState, useCallback, useEffect, useMemo } from "react";
import { api, ApiError } from "@/lib/api";
import type { GalleryPhoto } from "@app/types";

interface DateGroup {
  date: string;
  label: string;
  photos: GalleryPhoto[];
}

interface GalleryState {
  photos: GalleryPhoto[];
  loading: boolean;
  error: string | null;
}

function formatGroupDate(timestamp: number): { date: string; label: string } {
  const d = new Date(timestamp);
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const photoDate = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  const diffDays = Math.floor(
    (today.getTime() - photoDate.getTime()) / (1000 * 60 * 60 * 24)
  );

  const dateStr = d.toISOString().split("T")[0];

  if (diffDays === 0) return { date: dateStr, label: "Today" };
  if (diffDays === 1) return { date: dateStr, label: "Yesterday" };
  if (diffDays < 7)
    return {
      date: dateStr,
      label: d.toLocaleDateString("en-US", { weekday: "long" }),
    };
  if (d.getFullYear() === now.getFullYear()) {
    return {
      date: dateStr,
      label: d.toLocaleDateString("en-US", {
        month: "long",
        day: "numeric",
      }),
    };
  }
  return {
    date: dateStr,
    label: d.toLocaleDateString("en-US", {
      month: "long",
      day: "numeric",
      year: "numeric",
    }),
  };
}

export function useGallery() {
  const [state, setState] = useState<GalleryState>({
    photos: [],
    loading: true,
    error: null,
  });

  const refresh = useCallback(async () => {
    setState((s) => ({ ...s, loading: true, error: null }));
    try {
      const result = await api.getGalleryPhotos();
      setState({
        photos: result.photos,
        loading: false,
        error: null,
      });
    } catch (err) {
      const message =
        err instanceof ApiError ? err.message : "Failed to load photos";
      setState((s) => ({ ...s, loading: false, error: message }));
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const dateGroups = useMemo<DateGroup[]>(() => {
    const groups = new Map<string, { label: string; photos: GalleryPhoto[] }>();
    for (const photo of state.photos) {
      const { date, label } = formatGroupDate(photo.createdAt);
      if (!groups.has(date)) {
        groups.set(date, { label, photos: [] });
      }
      groups.get(date)!.photos.push(photo);
    }
    return Array.from(groups.entries())
      .sort((a, b) => b[0].localeCompare(a[0]))
      .map(([date, { label, photos }]) => ({ date, label, photos }));
  }, [state.photos]);

  const deletePhoto = useCallback(
    async (fileId: string) => {
      await api.deleteFile(fileId);
      setState((s) => ({
        ...s,
        photos: s.photos.filter((p) => p.fileId !== fileId),
      }));
    },
    []
  );

  return { ...state, dateGroups, refresh, deletePhoto };
}
