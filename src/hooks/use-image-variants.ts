import { useEffect, useRef, useState } from "react";
import { api } from "@/lib/api";

/**
 * Progressive image loader for the gallery.
 *
 * Loads a low-quality preview (the legacy thumbnail endpoint), and
 * once the consumer signals viewport entry, optionally upgrades to
 * a higher-fidelity render. The hook is shaped so a future
 * migration to `vfs.readPreview()` is a one-line swap inside the
 * fetch path — the consumer surface (`{ src, loaded, error,
 * upgrade(quality) }`) stays stable.
 *
 * Why two stages: galleries with hundreds of photos hit the
 * thumbnail endpoint for every visible card. Progressive upgrade
 * keeps the initial paint fast while letting hovered / clicked
 * cards swap to a sharper render without re-loading the entire
 * grid.
 *
 * Migration to VFS preview pipeline (follow-up work): replace the
 * `api.getThumbnailUrl(fileId)` call with a call to the new
 * `/api/gallery/preview/:fileId?variant=thumb` route that
 * delegates to `vfsReadPreview`. The hook contract is preserved.
 */

type Quality = "thumb" | "medium" | "lightbox";

interface ImageVariantsState {
  src: string | null;
  loaded: boolean;
  error: boolean;
  /** The quality currently rendered. */
  quality: Quality | null;
}

interface UseImageVariantsResult extends ImageVariantsState {
  /**
   * Imperatively upgrade to a higher-quality variant. The hook
   * fetches the new bytes and swaps `src` on success; existing
   * lower-quality `src` stays visible until the upgrade resolves
   * to avoid flashing.
   */
  upgrade: (q: Quality) => void;
}

/**
 * Map a logical quality to a concrete URL. The thumbnail and
 * full-image endpoints already exist in the legacy gallery routes;
 * `medium` reuses the full-image bytes (the browser scales via
 * object-fit: cover) until the dedicated `/api/gallery/preview`
 * route is wired in.
 */
function urlForQuality(fileId: string, q: Quality): string {
  if (q === "thumb") return api.getThumbnailUrl(fileId);
  return api.getImageUrl(fileId);
}

export function useImageVariants(
  fileId: string,
  initialQuality: Quality = "thumb"
): UseImageVariantsResult {
  const [state, setState] = useState<ImageVariantsState>({
    src: null,
    loaded: false,
    error: false,
    quality: null,
  });
  const [requestedQuality, setRequestedQuality] = useState<Quality>(initialQuality);
  const objectUrlRef = useRef<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const url = urlForQuality(fileId, requestedQuality);
    fetch(url, { headers: api.getImageHeaders() })
      .then(async (res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const blob = await res.blob();
        if (cancelled) return;
        const next = URL.createObjectURL(blob);
        if (objectUrlRef.current !== null) {
          URL.revokeObjectURL(objectUrlRef.current);
        }
        objectUrlRef.current = next;
        setState({
          src: next,
          loaded: true,
          error: false,
          quality: requestedQuality,
        });
      })
      .catch(() => {
        if (cancelled) return;
        setState((prev) => ({ ...prev, error: true }));
      });
    return () => {
      cancelled = true;
    };
  }, [fileId, requestedQuality]);

  // Cleanup the last object URL on unmount.
  useEffect(() => {
    return () => {
      if (objectUrlRef.current !== null) {
        URL.revokeObjectURL(objectUrlRef.current);
        objectUrlRef.current = null;
      }
    };
  }, []);

  return {
    ...state,
    upgrade: (q: Quality) => {
      // Only upgrade upward (thumb → medium → lightbox); never
      // downgrade. The order is enforced by the Quality enum
      // ordering at the type level — runtime guard for safety.
      const order: Quality[] = ["thumb", "medium", "lightbox"];
      if (order.indexOf(q) > order.indexOf(requestedQuality)) {
        setRequestedQuality(q);
      }
    },
  };
}
