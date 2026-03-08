import { useState, useCallback, useEffect } from "react";
import { useParams } from "react-router-dom";
import { motion, AnimatePresence } from "framer-motion";
import { BookImage, ImageOff } from "lucide-react";
import { api } from "@/lib/api";
import { useSharedImageLoader } from "@/hooks/use-image-loader";
import { Spinner } from "@/components/ui/spinner";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import type { SharedAlbumPhotosResponse } from "@shared/types";

interface SharedPhoto {
  fileId: string;
  fileName: string;
  fileSize: number;
  mimeType: string;
  createdAt: number;
}

function SharedThumbnail({
  token,
  fileId,
  onClick,
}: {
  token: string;
  fileId: string;
  onClick: () => void;
}) {
  const { src, loaded } = useSharedImageLoader(token, fileId);

  return (
    <motion.div
      className="relative cursor-pointer overflow-hidden rounded-lg bg-surface aspect-square"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      whileHover={{ scale: 1.02 }}
      transition={{ duration: 0.2 }}
      onClick={onClick}
    >
      {!loaded && (
        <div className="absolute inset-0 animate-pulse bg-gradient-to-br from-muted to-secondary" />
      )}
      {src && (
        <img
          src={src}
          className={cn(
            "h-full w-full object-cover transition-opacity duration-300",
            loaded ? "opacity-100" : "opacity-0"
          )}
          draggable={false}
        />
      )}
    </motion.div>
  );
}

function SharedLightbox({
  token,
  photos,
  initialIndex,
  onClose,
}: {
  token: string;
  photos: SharedPhoto[];
  initialIndex: number;
  onClose: () => void;
}) {
  const [idx, setIdx] = useState(initialIndex);
  const photo = photos[idx];
  const { src, loaded } = useSharedImageLoader(token, photo?.fileId || "");

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
      if (e.key === "ArrowRight") setIdx((i) => Math.min(i + 1, photos.length - 1));
      if (e.key === "ArrowLeft") setIdx((i) => Math.max(i - 1, 0));
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose, photos.length]);

  if (!photo) return null;

  return (
    <motion.div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/90"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      onClick={onClose}
    >
      <div className="absolute top-4 left-4 text-sm text-white/60">
        {idx + 1} / {photos.length}
      </div>
      <div
        className="relative max-h-[90vh] max-w-[90vw]"
        onClick={(e) => e.stopPropagation()}
      >
        {!loaded && (
          <div className="flex h-64 w-64 items-center justify-center">
            <div className="h-8 w-8 animate-spin rounded-full border-2 border-white/30 border-t-white" />
          </div>
        )}
        {src && (
          <img
            src={src}
            alt={photo.fileName}
            className="max-h-[90vh] max-w-[90vw] object-contain"
          />
        )}
      </div>
      {idx > 0 && (
        <button
          className="absolute left-4 top-1/2 -translate-y-1/2 rounded-full bg-white/10 p-3 text-white hover:bg-white/20"
          onClick={(e) => {
            e.stopPropagation();
            setIdx((i) => i - 1);
          }}
        >
          ‹
        </button>
      )}
      {idx < photos.length - 1 && (
        <button
          className="absolute right-4 top-1/2 -translate-y-1/2 rounded-full bg-white/10 p-3 text-white hover:bg-white/20"
          onClick={(e) => {
            e.stopPropagation();
            setIdx((i) => i + 1);
          }}
        >
          ›
        </button>
      )}
    </motion.div>
  );
}

export function SharedAlbumPage() {
  const { token } = useParams<{ token: string }>();
  const [data, setData] = useState<SharedAlbumPhotosResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lightboxIdx, setLightboxIdx] = useState<number | null>(null);

  useEffect(() => {
    if (!token) return;
    let cancelled = false;

    const load = async () => {
      try {
        const result = await api.getSharedAlbumPhotos(token);
        if (!cancelled) {
          setData(result);
          setLoading(false);
        }
      } catch {
        if (!cancelled) {
          setError("This shared album doesn't exist or has expired.");
          setLoading(false);
        }
      }
    };

    load();
    return () => { cancelled = true; };
  }, [token]);

  if (loading) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background">
        <div className="text-center">
          <Spinner className="h-8 w-8 text-primary mx-auto" />
          <p className="mt-3 text-sm text-muted-foreground">Loading album...</p>
        </div>
      </div>
    );
  }

  if (error || !data || !token) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background">
        <div className="text-center">
          <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-2xl bg-destructive/10">
            <ImageOff className="h-6 w-6 text-destructive" />
          </div>
          <p className="mt-4 text-sm font-medium text-foreground">
            Album not found
          </p>
          <p className="mt-1 text-xs text-muted-foreground">
            {error || "Invalid link"}
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <div className="border-b border-white/[0.06] px-6 py-6">
        <div className="mx-auto max-w-5xl">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-primary/10">
              <BookImage className="h-5 w-5 text-primary" />
            </div>
            <div>
              <h1 className="text-lg font-semibold text-heading">
                {data.albumName}
              </h1>
              <p className="text-xs text-muted-foreground">
                {data.photos.length} photo{data.photos.length !== 1 ? "s" : ""} &middot; Shared album
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Grid */}
      <div className="mx-auto max-w-5xl px-6 py-6">
        {data.photos.length === 0 ? (
          <div className="py-20 text-center">
            <p className="text-sm text-muted-foreground">This album is empty</p>
          </div>
        ) : (
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 md:grid-cols-4">
            {data.photos.map((photo, idx) => (
              <SharedThumbnail
                key={photo.fileId}
                token={token}
                fileId={photo.fileId}
                onClick={() => setLightboxIdx(idx)}
              />
            ))}
          </div>
        )}
      </div>

      {/* Lightbox */}
      <AnimatePresence>
        {lightboxIdx !== null && (
          <SharedLightbox
            token={token}
            photos={data.photos}
            initialIndex={lightboxIdx}
            onClose={() => setLightboxIdx(null)}
          />
        )}
      </AnimatePresence>
    </div>
  );
}
