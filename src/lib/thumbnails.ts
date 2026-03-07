/**
 * Client-side image thumbnail generation.
 * Creates canvas-based thumbnails for preview without server-side processing.
 */

const THUMB_SIZE = 256;
const thumbCache = new Map<string, string>();

/**
 * Generate a thumbnail data URL from a File object.
 */
export async function generateThumbnail(file: File): Promise<string | null> {
  if (!file.type.startsWith("image/")) return null;

  const cached = thumbCache.get(`${file.name}-${file.size}-${file.lastModified}`);
  if (cached) return cached;

  return new Promise((resolve) => {
    const img = new Image();
    const url = URL.createObjectURL(file);

    img.onload = () => {
      const canvas = document.createElement("canvas");
      const ctx = canvas.getContext("2d");
      if (!ctx) {
        URL.revokeObjectURL(url);
        resolve(null);
        return;
      }

      // Calculate scaling
      let w = img.width;
      let h = img.height;
      if (w > h) {
        if (w > THUMB_SIZE) {
          h = Math.round((h * THUMB_SIZE) / w);
          w = THUMB_SIZE;
        }
      } else {
        if (h > THUMB_SIZE) {
          w = Math.round((w * THUMB_SIZE) / h);
          h = THUMB_SIZE;
        }
      }

      canvas.width = w;
      canvas.height = h;
      ctx.drawImage(img, 0, 0, w, h);

      const dataUrl = canvas.toDataURL("image/webp", 0.7);
      const key = `${file.name}-${file.size}-${file.lastModified}`;
      thumbCache.set(key, dataUrl);
      URL.revokeObjectURL(url);
      resolve(dataUrl);
    };

    img.onerror = () => {
      URL.revokeObjectURL(url);
      resolve(null);
    };

    img.src = url;
  });
}

/**
 * Generate a thumbnail from a Blob (for downloaded files).
 */
export async function generateThumbnailFromBlob(
  blob: Blob,
  fileId: string
): Promise<string | null> {
  if (!blob.type.startsWith("image/")) return null;

  const cached = thumbCache.get(fileId);
  if (cached) return cached;

  return new Promise((resolve) => {
    const img = new Image();
    const url = URL.createObjectURL(blob);

    img.onload = () => {
      const canvas = document.createElement("canvas");
      const ctx = canvas.getContext("2d");
      if (!ctx) {
        URL.revokeObjectURL(url);
        resolve(null);
        return;
      }

      let w = img.width;
      let h = img.height;
      const maxDim = THUMB_SIZE;
      if (w > h) {
        if (w > maxDim) {
          h = Math.round((h * maxDim) / w);
          w = maxDim;
        }
      } else {
        if (h > maxDim) {
          w = Math.round((w * maxDim) / h);
          h = maxDim;
        }
      }

      canvas.width = w;
      canvas.height = h;
      ctx.drawImage(img, 0, 0, w, h);

      const dataUrl = canvas.toDataURL("image/webp", 0.7);
      thumbCache.set(fileId, dataUrl);
      URL.revokeObjectURL(url);
      resolve(dataUrl);
    };

    img.onerror = () => {
      URL.revokeObjectURL(url);
      resolve(null);
    };

    img.src = url;
  });
}

/**
 * Get a cached thumbnail.
 */
export function getCachedThumbnail(key: string): string | undefined {
  return thumbCache.get(key);
}

/**
 * Clear the thumbnail cache.
 */
export function clearThumbnailCache(): void {
  thumbCache.clear();
}
