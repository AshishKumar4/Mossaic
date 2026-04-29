import { useState, useEffect, useCallback, useRef } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  X,
  ChevronLeft,
  ChevronRight,
  Download,
  Trash2,
  Info,
  ZoomIn,
  ZoomOut,
  RotateCcw,
} from "lucide-react";
import { cn, formatBytes, formatDate } from "@/lib/utils";
import { useImageLoader } from "@/hooks/use-image-loader";
import { useDownload } from "@/hooks/use-download";
import { Button } from "@/components/ui/button";
import type { GalleryPhoto } from "@app/types";

interface LightboxProps {
  photos: GalleryPhoto[];
  initialIndex: number;
  onClose: () => void;
  onDelete?: (fileId: string) => void;
}

function LightboxImage({ fileId, fileName }: { fileId: string; fileName: string }) {
  const { src, loaded } = useImageLoader(fileId, "full");

  return (
    <div className="relative flex h-full w-full items-center justify-center">
      {!loaded && (
        <div className="absolute inset-0 flex items-center justify-center">
          <div className="h-8 w-8 animate-spin rounded-full border-2 border-muted-foreground border-t-primary" />
        </div>
      )}
      {src && (
        <motion.img
          src={src}
          alt={fileName}
          className="max-h-full max-w-full object-contain select-none"
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: loaded ? 1 : 0, scale: loaded ? 1 : 0.95 }}
          transition={{ duration: 0.3 }}
          draggable={false}
        />
      )}
    </div>
  );
}

export function Lightbox({
  photos,
  initialIndex,
  onClose,
  onDelete,
}: LightboxProps) {
  const [currentIndex, setCurrentIndex] = useState(initialIndex);
  const [showInfo, setShowInfo] = useState(false);
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const isDragging = useRef(false);
  const dragStart = useRef({ x: 0, y: 0 });
  const panStart = useRef({ x: 0, y: 0 });
  const { downloadFile } = useDownload();

  const photo = photos[currentIndex];
  if (!photo) return null;

  const goNext = useCallback(() => {
    setCurrentIndex((i) => Math.min(i + 1, photos.length - 1));
    setZoom(1);
    setPan({ x: 0, y: 0 });
  }, [photos.length]);

  const goPrev = useCallback(() => {
    setCurrentIndex((i) => Math.max(i - 1, 0));
    setZoom(1);
    setPan({ x: 0, y: 0 });
  }, []);

  const handleZoomIn = useCallback(() => {
    setZoom((z) => Math.min(z * 1.5, 5));
  }, []);

  const handleZoomOut = useCallback(() => {
    setZoom((z) => {
      const newZoom = Math.max(z / 1.5, 1);
      if (newZoom === 1) setPan({ x: 0, y: 0 });
      return newZoom;
    });
  }, []);

  const handleResetZoom = useCallback(() => {
    setZoom(1);
    setPan({ x: 0, y: 0 });
  }, []);

  // Keyboard navigation
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      switch (e.key) {
        case "Escape":
          onClose();
          break;
        case "ArrowRight":
          goNext();
          break;
        case "ArrowLeft":
          goPrev();
          break;
        case "i":
          setShowInfo((s) => !s);
          break;
        case "+":
        case "=":
          handleZoomIn();
          break;
        case "-":
          handleZoomOut();
          break;
        case "0":
          handleResetZoom();
          break;
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose, goNext, goPrev, handleZoomIn, handleZoomOut, handleResetZoom]);

  // Mouse wheel zoom
  useEffect(() => {
    const handleWheel = (e: WheelEvent) => {
      e.preventDefault();
      if (e.deltaY < 0) {
        setZoom((z) => Math.min(z * 1.1, 5));
      } else {
        setZoom((z) => {
          const newZoom = Math.max(z / 1.1, 1);
          if (newZoom === 1) setPan({ x: 0, y: 0 });
          return newZoom;
        });
      }
    };

    window.addEventListener("wheel", handleWheel, { passive: false });
    return () => window.removeEventListener("wheel", handleWheel);
  }, []);

  // Pan handling
  const handleMouseDown = useCallback(
    (e: React.MouseEvent) => {
      if (zoom > 1) {
        isDragging.current = true;
        dragStart.current = { x: e.clientX, y: e.clientY };
        panStart.current = { ...pan };
      }
    },
    [zoom, pan]
  );

  const handleMouseMove = useCallback(
    (e: React.MouseEvent) => {
      if (isDragging.current && zoom > 1) {
        setPan({
          x: panStart.current.x + (e.clientX - dragStart.current.x),
          y: panStart.current.y + (e.clientY - dragStart.current.y),
        });
      }
    },
    [zoom]
  );

  const handleMouseUp = useCallback(() => {
    isDragging.current = false;
  }, []);

  // Touch swipe for navigation
  const touchStart = useRef<number | null>(null);
  const handleTouchStart = useCallback((e: React.TouchEvent) => {
    if (e.touches.length === 1) {
      touchStart.current = e.touches[0].clientX;
    }
  }, []);

  const handleTouchEnd = useCallback(
    (e: React.TouchEvent) => {
      if (touchStart.current !== null && e.changedTouches.length === 1) {
        const diff = e.changedTouches[0].clientX - touchStart.current;
        if (Math.abs(diff) > 60) {
          if (diff > 0) goPrev();
          else goNext();
        }
        touchStart.current = null;
      }
    },
    [goNext, goPrev]
  );

  const handleDownload = useCallback(() => {
    downloadFile(photo.fileId, photo.fileName);
  }, [downloadFile, photo]);

  const handleDelete = useCallback(() => {
    if (onDelete && confirm("Delete this photo permanently?")) {
      onDelete(photo.fileId);
      if (photos.length <= 1) {
        onClose();
      } else if (currentIndex >= photos.length - 1) {
        setCurrentIndex(currentIndex - 1);
      }
    }
  }, [onDelete, photo, photos.length, currentIndex, onClose]);

  const formattedDate = new Date(photo.createdAt).toLocaleDateString("en-US", {
    weekday: "long",
    year: "numeric",
    month: "long",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });

  return (
    <motion.div
      className="fixed inset-0 z-50 flex flex-col bg-surface"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.2 }}
    >
      {/* Top bar */}
      <div className="relative z-10 flex items-center justify-between px-4 py-3">
        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="icon"
            className="text-foreground hover:bg-white/[0.08]"
            onClick={onClose}
          >
            <X className="h-5 w-5" />
          </Button>
          <span className="text-sm text-muted-foreground">
            {currentIndex + 1} / {photos.length}
          </span>
        </div>

        <div className="flex items-center gap-1">
          <Button
            variant="ghost"
            size="icon"
            className="text-foreground hover:bg-white/[0.08]"
            onClick={handleZoomOut}
            disabled={zoom <= 1}
          >
            <ZoomOut className="h-4.5 w-4.5" />
          </Button>
          {zoom > 1 && (
            <span className="min-w-[3rem] text-center text-xs text-muted-foreground">
              {Math.round(zoom * 100)}%
            </span>
          )}
          <Button
            variant="ghost"
            size="icon"
            className="text-foreground hover:bg-white/[0.08]"
            onClick={handleZoomIn}
            disabled={zoom >= 5}
          >
            <ZoomIn className="h-4.5 w-4.5" />
          </Button>
          {zoom > 1 && (
            <Button
              variant="ghost"
              size="icon"
              className="text-foreground hover:bg-white/[0.08]"
              onClick={handleResetZoom}
            >
              <RotateCcw className="h-4 w-4" />
            </Button>
          )}
          <div className="mx-2 h-5 w-px bg-border" />
          <Button
            variant="ghost"
            size="icon"
            className="text-foreground hover:bg-white/[0.08]"
            onClick={() => setShowInfo(!showInfo)}
          >
            <Info className={cn("h-4.5 w-4.5", showInfo && "text-primary")} />
          </Button>
          <Button
            variant="ghost"
            size="icon"
            className="text-foreground hover:bg-white/[0.08]"
            onClick={handleDownload}
          >
            <Download className="h-4.5 w-4.5" />
          </Button>
          {onDelete && (
            <Button
              variant="ghost"
              size="icon"
              className="text-foreground hover:bg-white/[0.08] hover:text-destructive"
              onClick={handleDelete}
            >
              <Trash2 className="h-4.5 w-4.5" />
            </Button>
          )}
        </div>
      </div>

      {/* Main image area */}
      <div className="relative flex flex-1 overflow-hidden">
        {/* Navigation arrows */}
        {currentIndex > 0 && (
          <button
            className="absolute left-2 top-1/2 z-10 flex h-10 w-10 -translate-y-1/2 items-center justify-center rounded-full bg-surface/80 text-foreground transition-colors hover:bg-white/[0.08]"
            onClick={goPrev}
          >
            <ChevronLeft className="h-6 w-6" />
          </button>
        )}
        {currentIndex < photos.length - 1 && (
          <button
            className="absolute right-2 top-1/2 z-10 flex h-10 w-10 -translate-y-1/2 items-center justify-center rounded-full bg-surface/80 text-foreground transition-colors hover:bg-white/[0.08]"
            onClick={goNext}
          >
            <ChevronRight className="h-6 w-6" />
          </button>
        )}

        {/* Image container with zoom/pan */}
        <div
          className={cn(
            "flex-1 overflow-hidden",
            zoom > 1 ? "cursor-grab active:cursor-grabbing" : "cursor-default"
          )}
          onMouseDown={handleMouseDown}
          onMouseMove={handleMouseMove}
          onMouseUp={handleMouseUp}
          onMouseLeave={handleMouseUp}
          onTouchStart={handleTouchStart}
          onTouchEnd={handleTouchEnd}
        >
          <div
            className="flex h-full w-full items-center justify-center p-4"
            style={{
              transform: `scale(${zoom}) translate(${pan.x / zoom}px, ${pan.y / zoom}px)`,
              transition: isDragging.current ? "none" : "transform 0.2s ease-out",
            }}
          >
            <AnimatePresence mode="wait">
              <motion.div
                key={photo.fileId}
                className="flex h-full w-full items-center justify-center"
                initial={{ opacity: 0, x: 50 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: -50 }}
                transition={{ duration: 0.2 }}
              >
                <LightboxImage fileId={photo.fileId} fileName={photo.fileName} />
              </motion.div>
            </AnimatePresence>
          </div>
        </div>

        {/* Info panel */}
        <AnimatePresence>
          {showInfo && (
            <motion.div
              className="w-80 border-l border-white/[0.06] bg-sidebar p-6 overflow-y-auto"
              initial={{ width: 0, opacity: 0 }}
              animate={{ width: 320, opacity: 1 }}
              exit={{ width: 0, opacity: 0 }}
              transition={{ duration: 0.2, ease: "easeInOut" }}
            >
              <h3 className="text-sm font-semibold text-foreground mb-6">Details</h3>

              <div className="space-y-5">
                <InfoRow label="Name" value={photo.fileName} />
                <InfoRow label="Date" value={formattedDate} />
                <InfoRow label="Size" value={formatBytes(photo.fileSize)} />
                <InfoRow label="Type" value={photo.mimeType} />
                {photo.parentId && (
                  <InfoRow label="Folder" value={photo.parentId} />
                )}
              </div>

              <div className="mt-8 pt-6 border-t border-white/[0.06]">
                <h4 className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-3">
                  File Info
                </h4>
                <div className="space-y-3">
                  <InfoRow label="File ID" value={photo.fileId} mono />
                  <InfoRow
                    label="Uploaded"
                    value={formatDate(photo.createdAt)}
                  />
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>

      {/* Bottom filmstrip */}
      {photos.length > 1 && (
        <div className="flex items-center justify-center gap-1 px-4 py-3 overflow-x-auto">
          {photos.slice(
            Math.max(0, currentIndex - 5),
            Math.min(photos.length, currentIndex + 6)
          ).map((p, i) => {
            const actualIndex = Math.max(0, currentIndex - 5) + i;
            return (
              <button
                key={p.fileId}
                className={cn(
                  "h-12 w-12 flex-shrink-0 overflow-hidden rounded transition-all",
                  actualIndex === currentIndex
                    ? "ring-2 ring-primary scale-110"
                    : "opacity-50 hover:opacity-80"
                )}
                onClick={() => {
                  setCurrentIndex(actualIndex);
                  setZoom(1);
                  setPan({ x: 0, y: 0 });
                }}
              >
                <FilmstripThumb fileId={p.fileId} />
              </button>
            );
          })}
        </div>
      )}
    </motion.div>
  );
}

function FilmstripThumb({ fileId }: { fileId: string }) {
  const { src, loaded } = useImageLoader(fileId, "thumbnail");
  return src ? (
    <img
      src={src}
      className={cn(
        "h-full w-full object-cover transition-opacity",
        loaded ? "opacity-100" : "opacity-0"
      )}
      draggable={false}
    />
  ) : (
    <div className="h-full w-full bg-surface animate-pulse" />
  );
}

function InfoRow({
  label,
  value,
  mono,
}: {
  label: string;
  value: string;
  mono?: boolean;
}) {
  return (
    <div>
      <div className="text-xs text-muted-foreground mb-0.5">{label}</div>
      <div
        className={cn(
          "text-sm text-foreground break-all",
          mono && "font-mono text-xs"
        )}
      >
        {value}
      </div>
    </div>
  );
}
