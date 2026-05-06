import { useState, useEffect, useRef } from "react";
import { api } from "@/lib/api";

interface ImageState {
  src: string | null;
  loaded: boolean;
  error: boolean;
}

/**
 * Hook to load images with auth headers and blur-up placeholder effect.
 * Returns object URL for the image and loading state.
 */
export function useImageLoader(
  fileId: string,
  type: "thumbnail" | "full" = "thumbnail"
) {
  const [state, setState] = useState<ImageState>({
    src: null,
    loaded: false,
    error: false,
  });
  const urlRef = useRef<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    const loadImage = async () => {
      try {
        const url =
          type === "thumbnail"
            ? api.getThumbnailUrl(fileId)
            : api.getImageUrl(fileId);

        const res = await fetch(url, {
          headers: api.getImageHeaders(),
        });

        if (!res.ok) throw new Error("Failed to load image");
        if (cancelled) return;

        const blob = await res.blob();
        if (cancelled) return;

        const objectUrl = URL.createObjectURL(blob);
        urlRef.current = objectUrl;
        setState({ src: objectUrl, loaded: true, error: false });
      } catch {
        if (!cancelled) {
          setState({ src: null, loaded: false, error: true });
        }
      }
    };

    loadImage();

    return () => {
      cancelled = true;
      if (urlRef.current) {
        URL.revokeObjectURL(urlRef.current);
        urlRef.current = null;
      }
    };
  }, [fileId, type]);

  return state;
}

/**
 * Hook to load shared album images (no auth needed).
 */
export function useSharedImageLoader(
  token: string,
  fileId: string
) {
  const [state, setState] = useState<ImageState>({
    src: null,
    loaded: false,
    error: false,
  });
  const urlRef = useRef<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    const loadImage = async () => {
      try {
        const url = api.getSharedImageUrl(token, fileId);
        const res = await fetch(url);

        if (!res.ok) throw new Error("Failed to load image");
        if (cancelled) return;

        const blob = await res.blob();
        if (cancelled) return;

        const objectUrl = URL.createObjectURL(blob);
        urlRef.current = objectUrl;
        setState({ src: objectUrl, loaded: true, error: false });
      } catch {
        if (!cancelled) {
          setState({ src: null, loaded: false, error: true });
        }
      }
    };

    loadImage();

    return () => {
      cancelled = true;
      if (urlRef.current) {
        URL.revokeObjectURL(urlRef.current);
        urlRef.current = null;
      }
    };
  }, [token, fileId]);

  return state;
}
