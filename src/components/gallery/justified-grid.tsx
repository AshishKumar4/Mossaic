import { useMemo, useRef, useEffect, useState } from "react";
import type { GalleryPhoto } from "@app/types";
import { LazyPhotoThumbnail } from "./lazy-photo-thumbnail";

interface JustifiedGridProps {
  photos: GalleryPhoto[];
  targetRowHeight?: number;
  gap?: number;
  onPhotoClick: (photo: GalleryPhoto, index: number) => void;
  onPhotoSelect?: (photo: GalleryPhoto) => void;
  selectedIds?: Set<string>;
  selectable?: boolean;
}

interface LayoutItem {
  photo: GalleryPhoto;
  index: number;
  width: number;
  height: number;
  x: number;
  y: number;
}

/**
 * Google Photos-style justified grid layout.
 * Fills each row with photos at varying widths to maintain consistent row heights.
 * Uses an assumed aspect ratio (4:3 default) since we don't have image dimensions.
 */
function computeLayout(
  photos: GalleryPhoto[],
  containerWidth: number,
  targetRowHeight: number,
  gap: number
): { items: LayoutItem[]; totalHeight: number } {
  if (containerWidth <= 0 || photos.length === 0) {
    return { items: [], totalHeight: 0 };
  }

  const items: LayoutItem[] = [];
  let currentRow: { photo: GalleryPhoto; index: number; aspect: number }[] = [];
  let y = 0;

  // Assign aspect ratios based on filename heuristics or default
  const getAspectRatio = (photo: GalleryPhoto): number => {
    // Use a pseudo-random but deterministic aspect ratio based on fileId
    // This gives visual variety like real photos
    const hash = photo.fileId.charCodeAt(0) + photo.fileId.charCodeAt(1);
    const ratios = [4 / 3, 3 / 2, 16 / 9, 1, 3 / 4, 5 / 4];
    return ratios[hash % ratios.length];
  };

  const flushRow = (row: typeof currentRow, isLast: boolean) => {
    if (row.length === 0) return;

    const totalAspect = row.reduce((sum, item) => sum + item.aspect, 0);
    const totalGaps = (row.length - 1) * gap;
    const availableWidth = containerWidth - totalGaps;

    // Calculate row height to fill the width
    let rowHeight = availableWidth / totalAspect;

    // Clamp height — don't let rows get too tall or short
    if (isLast && row.length < 3 && rowHeight > targetRowHeight * 1.5) {
      rowHeight = targetRowHeight;
    }
    rowHeight = Math.min(rowHeight, targetRowHeight * 1.8);

    let x = 0;
    row.forEach((item, i) => {
      const width =
        i === row.length - 1
          ? containerWidth - x // Last item takes remaining space
          : Math.round(item.aspect * rowHeight);

      items.push({
        photo: item.photo,
        index: item.index,
        width,
        height: Math.round(rowHeight),
        x,
        y,
      });

      x += width + gap;
    });

    y += Math.round(rowHeight) + gap;
  };

  photos.forEach((photo, index) => {
    const aspect = getAspectRatio(photo);
    currentRow.push({ photo, index, aspect });

    // Check if this row is full enough
    const totalAspect = currentRow.reduce((sum, item) => sum + item.aspect, 0);
    const totalGaps = (currentRow.length - 1) * gap;
    const rowHeight = (containerWidth - totalGaps) / totalAspect;

    if (rowHeight <= targetRowHeight) {
      flushRow(currentRow, false);
      currentRow = [];
    }
  });

  // Flush remaining items
  if (currentRow.length > 0) {
    flushRow(currentRow, true);
  }

  return { items, totalHeight: y };
}

export function JustifiedGrid({
  photos,
  targetRowHeight = 220,
  gap = 4,
  onPhotoClick,
  onPhotoSelect,
  selectedIds,
  selectable = false,
}: JustifiedGridProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [containerWidth, setContainerWidth] = useState(0);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        setContainerWidth(entry.contentRect.width);
      }
    });

    observer.observe(el);
    setContainerWidth(el.clientWidth);

    return () => observer.disconnect();
  }, []);

  const { items, totalHeight } = useMemo(
    () => computeLayout(photos, containerWidth, targetRowHeight, gap),
    [photos, containerWidth, targetRowHeight, gap]
  );

  return (
    <div ref={containerRef} className="relative w-full" style={{ height: totalHeight }}>
      {items.map((item) => (
        <div
          key={item.photo.fileId}
          className="absolute"
          style={{
            left: item.x,
            top: item.y,
            width: item.width,
            height: item.height,
          }}
        >
          <LazyPhotoThumbnail
            fileId={item.photo.fileId}
            fileName={item.photo.fileName}
            width={item.width}
            height={item.height}
            onClick={() => onPhotoClick(item.photo, item.index)}
            onSelect={() => onPhotoSelect?.(item.photo)}
            selected={selectedIds?.has(item.photo.fileId)}
            selectable={selectable}
          />
        </div>
      ))}
    </div>
  );
}
