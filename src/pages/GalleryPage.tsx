import { useState, useEffect, useCallback } from "react";
import {
  Images,
  WarningCircle,
  FolderSimple,
  GridFour,
} from "@phosphor-icons/react";
import { Empty, Loader, Banner, Text, Badge } from "@cloudflare/kumo";
import { useGallery } from "../hooks/useGallery";
import { PhotoPreview } from "../components/gallery/PhotoPreview";
import { formatBytes, cn } from "../lib/utils";
import { downloadFile } from "../lib/download-engine";
import { generateThumbnailFromBlob } from "../lib/thumbnails";

// Thumbnail loader component with lazy download
function GalleryThumbnail({ fileId, mimeType }: { fileId: string; mimeType: string }) {
  const [thumbUrl, setThumbUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);

    downloadFile(fileId, {
      onProgress: () => {},
      onComplete: async (blob) => {
        if (cancelled) return;
        try {
          const url = await generateThumbnailFromBlob(blob, fileId);
          if (!cancelled) {
            setThumbUrl(url);
            setLoading(false);
          }
        } catch {
          if (!cancelled) {
            // Fallback: use blob directly
            const url = URL.createObjectURL(blob);
            setThumbUrl(url);
            setLoading(false);
          }
        }
      },
      onError: () => {
        if (!cancelled) {
          setFailed(true);
          setLoading(false);
        }
      },
    });

    return () => {
      cancelled = true;
    };
  }, [fileId, mimeType]);

  if (loading) {
    return (
      <div className="flex h-full w-full items-center justify-center">
        <Loader size="sm" />
      </div>
    );
  }

  if (failed || !thumbUrl) {
    return (
      <div className="flex h-full w-full items-center justify-center">
        <Images size={28} weight="duotone" className="text-kumo-subtle" />
      </div>
    );
  }

  return (
    <img
      src={thumbUrl}
      alt=""
      className="h-full w-full object-cover transition-transform duration-300 group-hover:scale-105"
      loading="lazy"
    />
  );
}

export function GalleryPage() {
  const {
    images,
    allImageCount,
    albums,
    selectedAlbum,
    setSelectedAlbum,
    isLoading,
    error,
    selectedImage,
    setSelectedImage,
    selectNext,
    selectPrev,
    currentIndex,
    totalImages,
  } = useGallery();

  if (isLoading) {
    return (
      <div className="flex flex-col items-center justify-center gap-3 py-20">
        <Loader size="lg" />
        <Text variant="secondary">Loading gallery...</Text>
      </div>
    );
  }

  if (error) {
    return (
      <Banner
        variant="error"
        icon={<WarningCircle weight="fill" />}
        title={error}
      />
    );
  }

  if (allImageCount === 0) {
    return (
      <Empty
        icon={<Images size={48} className="text-kumo-subtle" />}
        title="No photos yet"
        description="Upload some images from the Files tab to see them here in the gallery."
      />
    );
  }

  return (
    <div className="space-y-5">
      {/* Album selector bar */}
      {albums.length > 0 && (
        <div className="flex items-center gap-2 overflow-x-auto pb-1">
          <button
            onClick={() => setSelectedAlbum(null)}
            className={cn(
              "flex shrink-0 items-center gap-2 rounded-lg border px-3 py-1.5 text-sm font-medium transition-colors",
              selectedAlbum === null
                ? "border-kumo-fill bg-kumo-overlay text-kumo-default"
                : "border-kumo-line bg-kumo-base text-kumo-strong hover:bg-kumo-overlay/50"
            )}
          >
            <GridFour size={14} weight="duotone" />
            All Photos
            <Badge variant="secondary">{allImageCount}</Badge>
          </button>

          {albums.map((album) => (
            <button
              key={album.folderId ?? "__unsorted__"}
              onClick={() =>
                setSelectedAlbum(album.folderId ?? "__unsorted__")
              }
              className={cn(
                "flex shrink-0 items-center gap-2 rounded-lg border px-3 py-1.5 text-sm font-medium transition-colors",
                selectedAlbum ===
                  (album.folderId ?? "__unsorted__")
                  ? "border-kumo-fill bg-kumo-overlay text-kumo-default"
                  : "border-kumo-line bg-kumo-base text-kumo-strong hover:bg-kumo-overlay/50"
              )}
            >
              <FolderSimple size={14} weight="duotone" />
              {album.name}
              <Badge variant="secondary">{album.imageCount}</Badge>
            </button>
          ))}
        </div>
      )}

      {/* Photo grid */}
      {images.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-12">
          <FolderSimple
            size={48}
            weight="duotone"
            className="text-kumo-subtle"
          />
          <Text variant="secondary" className="mt-3">
            No photos in this album
          </Text>
        </div>
      ) : (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6">
          {images.map((img) => (
            <button
              key={img.fileId}
              onClick={() => setSelectedImage(img)}
              className="group relative aspect-square overflow-hidden rounded-xl border border-kumo-line bg-kumo-elevated transition-all hover:border-kumo-fill hover:shadow-lg focus:outline-none focus-visible:ring-2 focus-visible:ring-kumo-ring"
            >
              <GalleryThumbnail
                fileId={img.fileId}
                mimeType={img.mimeType}
              />

              {/* Overlay on hover */}
              <div className="absolute inset-0 flex flex-col justify-end bg-gradient-to-t from-black/60 via-transparent to-transparent p-3 opacity-0 transition-opacity group-hover:opacity-100">
                <Text size="xs" bold className="truncate text-white">
                  {img.fileName}
                </Text>
                <Text size="xs" className="text-white/70">
                  {formatBytes(img.fileSize)}
                </Text>
              </div>
            </button>
          ))}
        </div>
      )}

      {/* Lightbox */}
      {selectedImage && (
        <PhotoPreview
          file={selectedImage}
          onClose={() => setSelectedImage(null)}
          onNext={
            currentIndex < totalImages - 1 ? selectNext : undefined
          }
          onPrev={currentIndex > 0 ? selectPrev : undefined}
          currentIndex={currentIndex}
          totalCount={totalImages}
        />
      )}
    </div>
  );
}
