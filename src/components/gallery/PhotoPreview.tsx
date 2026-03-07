import { useState, useEffect, useCallback } from "react";
import {
  X,
  DownloadSimple,
  CaretLeft,
  CaretRight,
} from "@phosphor-icons/react";
import { Button, Loader, Text } from "@cloudflare/kumo";
import type { UserFile } from "@shared/types";
import { downloadFile, saveBlobAs } from "../../lib/download-engine";
import { formatBytes } from "../../lib/utils";

interface PhotoPreviewProps {
  file: UserFile;
  onClose: () => void;
  onNext?: () => void;
  onPrev?: () => void;
  currentIndex?: number;
  totalCount?: number;
}

export function PhotoPreview({
  file,
  onClose,
  onNext,
  onPrev,
  currentIndex,
  totalCount,
}: PhotoPreviewProps) {
  const [imageUrl, setImageUrl] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [blobRef, setBlobRef] = useState<Blob | null>(null);

  // Reset state when file changes
  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    setError(null);
    setImageUrl(null);

    downloadFile(file.fileId, {
      onProgress: () => {},
      onComplete: (blob) => {
        if (!cancelled) {
          const url = URL.createObjectURL(blob);
          setImageUrl(url);
          setBlobRef(blob);
          setIsLoading(false);
        }
      },
      onError: (err) => {
        if (!cancelled) {
          setError(err.message);
          setIsLoading(false);
        }
      },
    });

    return () => {
      cancelled = true;
    };
  }, [file.fileId]);

  // Cleanup URL on unmount or when URL changes
  useEffect(() => {
    return () => {
      if (imageUrl) URL.revokeObjectURL(imageUrl);
    };
  }, [imageUrl]);

  // Keyboard navigation
  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
      if (e.key === "ArrowRight" && onNext) onNext();
      if (e.key === "ArrowLeft" && onPrev) onPrev();
    },
    [onClose, onNext, onPrev]
  );

  useEffect(() => {
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [handleKeyDown]);

  const handleDownload = () => {
    if (blobRef) {
      saveBlobAs(blobRef, file.fileName);
    }
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/90 backdrop-blur-sm"
      onClick={onClose}
    >
      {/* Top controls */}
      <div className="absolute top-4 right-4 z-10 flex items-center gap-2">
        {currentIndex !== undefined && totalCount !== undefined && (
          <div className="mr-2 rounded-full bg-black/50 px-3 py-1">
            <Text size="xs" className="text-white/80">
              {currentIndex + 1} / {totalCount}
            </Text>
          </div>
        )}
        <Button
          variant="secondary"
          shape="square"
          size="sm"
          onClick={(e) => {
            e.stopPropagation();
            handleDownload();
          }}
          aria-label="Download"
          icon={<DownloadSimple size={18} />}
        />
        <Button
          variant="secondary"
          shape="square"
          size="sm"
          onClick={(e) => {
            e.stopPropagation();
            onClose();
          }}
          aria-label="Close"
          icon={<X size={18} />}
        />
      </div>

      {/* Previous button */}
      {onPrev && (
        <button
          onClick={(e) => {
            e.stopPropagation();
            onPrev();
          }}
          className="absolute left-4 z-10 flex h-10 w-10 items-center justify-center rounded-full bg-black/50 text-white/80 transition-colors hover:bg-black/70 hover:text-white"
          aria-label="Previous image"
        >
          <CaretLeft size={24} weight="bold" />
        </button>
      )}

      {/* Next button */}
      {onNext && (
        <button
          onClick={(e) => {
            e.stopPropagation();
            onNext();
          }}
          className="absolute right-4 z-10 flex h-10 w-10 items-center justify-center rounded-full bg-black/50 text-white/80 transition-colors hover:bg-black/70 hover:text-white"
          aria-label="Next image"
        >
          <CaretRight size={24} weight="bold" />
        </button>
      )}

      {/* Image info */}
      <div className="absolute bottom-4 left-4 z-10">
        <p className="text-sm font-medium text-white">{file.fileName}</p>
        <p className="text-xs text-white/60">
          {formatBytes(file.fileSize)} &middot; {file.mimeType}
        </p>
      </div>

      {/* Content */}
      <div onClick={(e) => e.stopPropagation()} className="max-h-[90vh] max-w-[90vw]">
        {isLoading && (
          <div className="flex flex-col items-center gap-3">
            <Loader size="lg" />
            <Text variant="secondary">Loading image...</Text>
          </div>
        )}

        {error && (
          <Text className="text-rose-400">Failed to load: {error}</Text>
        )}

        {imageUrl && (
          <img
            src={imageUrl}
            alt={file.fileName}
            className="max-h-[90vh] max-w-[90vw] rounded-lg object-contain shadow-2xl"
          />
        )}
      </div>
    </div>
  );
}
