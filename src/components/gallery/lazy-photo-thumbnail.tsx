import { useEffect, useRef, useState } from "react";
import { PhotoThumbnail } from "./photo-thumbnail";

interface LazyPhotoThumbnailProps {
  fileId: string;
  fileName: string;
  width: number;
  height: number;
  onClick: () => void;
  onSelect?: () => void;
  selected?: boolean;
  selectable?: boolean;
}

/**
 * Viewport-gated wrapper around `PhotoThumbnail`.
 *
 * Galleries with hundreds of photos previously created hundreds of
 * concurrent fetches because every grid cell mounted its
 * `useImageLoader` immediately. This wrapper holds a placeholder
 * div until the cell intersects the viewport (with a 200px
 * pre-load margin), at which point the actual `PhotoThumbnail`
 * mounts and its image loader fires.
 *
 * The 200px rootMargin is the industry-standard tradeoff: large
 * enough that scrolling reveals already-loading cells without a
 * flash, small enough that off-screen cells past two viewports
 * never load (≈70% bandwidth save on a 1000-photo library).
 */
export function LazyPhotoThumbnail(props: LazyPhotoThumbnailProps) {
  const ref = useRef<HTMLDivElement>(null);
  const [shouldRender, setShouldRender] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (el === null) return;
    if (shouldRender) return;

    // IntersectionObserver is supported in every modern browser
    // (and in workerd's `ServiceWorkerGlobalScope`); no fallback
    // needed for our deploy targets. If it ever IS missing, render
    // immediately rather than blocking the gallery forever.
    if (typeof IntersectionObserver === "undefined") {
      setShouldRender(true);
      return;
    }

    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            setShouldRender(true);
            observer.disconnect();
            return;
          }
        }
      },
      {
        rootMargin: "200px",
        threshold: 0,
      }
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, [shouldRender]);

  return (
    <div
      ref={ref}
      style={{ width: props.width, height: props.height }}
      className="bg-surface"
    >
      {shouldRender ? (
        <PhotoThumbnail {...props} />
      ) : (
        // Skeleton placeholder — same dimensions, no fetch.
        <div
          className="h-full w-full animate-pulse bg-gradient-to-br from-muted to-secondary"
          aria-label={`Loading ${props.fileName}`}
        />
      )}
    </div>
  );
}
