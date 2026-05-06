import { useState, useCallback, useMemo } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  Image,
  Film,
  SlidersHorizontal,
  ArrowUpDown,
  Trash2,
  RefreshCw,
  CheckSquare,
  X,
  ImageOff,
  Camera,
} from "lucide-react";
import { useGallery } from "@/hooks/use-gallery";
import { useUpload } from "@/hooks/use-upload";
import { useDropZone } from "@/hooks/use-drop-zone";
import { JustifiedGrid } from "@/components/gallery/justified-grid";
import { Lightbox } from "@/components/gallery/lightbox";
import { DropZoneOverlay } from "@/components/upload/drop-zone-overlay";
import { TransferPanel } from "@/components/upload/transfer-panel";
import { Button } from "@/components/ui/button";
import { Spinner } from "@/components/ui/spinner";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  DropdownMenuSeparator,
} from "@/components/ui/dropdown-menu";
import type { GalleryPhoto } from "@app/types";

type FilterType = "all" | "photos" | "videos";
type SortOrder = "newest" | "oldest";

export function GalleryPage() {
  const { photos, dateGroups, loading, error, refresh, deletePhoto } =
    useGallery();

  const {
    transfers: uploadTransfers,
    uploadFile,
    clearTransfer: clearUpload,
  } = useUpload(refresh);

  const handleFileDrop = useCallback(
    (files: File[]) => {
      for (const file of files) {
        uploadFile(file, null);
      }
    },
    [uploadFile]
  );

  const { isDragOver, dropZoneProps } = useDropZone({
    onDrop: handleFileDrop,
  });

  const [lightboxIndex, setLightboxIndex] = useState<number | null>(null);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [selectable, setSelectable] = useState(false);
  const [filter, setFilter] = useState<FilterType>("all");
  const [sort, setSort] = useState<SortOrder>("newest");

  // Filter photos
  const filteredPhotos = useMemo(() => {
    let result = photos;
    if (filter === "photos") {
      result = result.filter(
        (p) =>
          p.mimeType.startsWith("image/") && !p.mimeType.includes("gif")
      );
    } else if (filter === "videos") {
      result = result.filter(
        (p) =>
          p.mimeType.startsWith("video/") || p.mimeType === "image/gif"
      );
    }
    if (sort === "oldest") {
      result = [...result].reverse();
    }
    return result;
  }, [photos, filter, sort]);

  // Group filtered photos by date
  const filteredGroups = useMemo(() => {
    if (filter === "all" && sort === "newest") return dateGroups;

    const groups = new Map<
      string,
      { label: string; photos: GalleryPhoto[] }
    >();
    for (const photo of filteredPhotos) {
      const d = new Date(photo.createdAt);
      const now = new Date();
      const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
      const photoDate = new Date(
        d.getFullYear(),
        d.getMonth(),
        d.getDate()
      );
      const diffDays = Math.floor(
        (today.getTime() - photoDate.getTime()) / (1000 * 60 * 60 * 24)
      );
      const dateStr = d.toISOString().split("T")[0];

      let label: string;
      if (diffDays === 0) label = "Today";
      else if (diffDays === 1) label = "Yesterday";
      else if (diffDays < 7)
        label = d.toLocaleDateString("en-US", { weekday: "long" });
      else if (d.getFullYear() === now.getFullYear())
        label = d.toLocaleDateString("en-US", {
          month: "long",
          day: "numeric",
        });
      else
        label = d.toLocaleDateString("en-US", {
          month: "long",
          day: "numeric",
          year: "numeric",
        });

      if (!groups.has(dateStr)) {
        groups.set(dateStr, { label, photos: [] });
      }
      groups.get(dateStr)!.photos.push(photo);
    }

    const sorted = Array.from(groups.entries()).sort((a, b) =>
      sort === "newest"
        ? b[0].localeCompare(a[0])
        : a[0].localeCompare(b[0])
    );

    return sorted.map(([date, { label, photos: p }]) => ({
      date,
      label,
      photos: p,
    }));
  }, [filteredPhotos, dateGroups, filter, sort]);

  const handlePhotoClick = useCallback(
    (_photo: GalleryPhoto, _index: number) => {
      // Find the global index in the full filteredPhotos array
      const globalIndex = filteredPhotos.findIndex(
        (p) => p.fileId === _photo.fileId
      );
      setLightboxIndex(globalIndex >= 0 ? globalIndex : 0);
    },
    [filteredPhotos]
  );

  const handlePhotoSelect = useCallback((photo: GalleryPhoto) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(photo.fileId)) {
        next.delete(photo.fileId);
      } else {
        next.add(photo.fileId);
      }
      return next;
    });
  }, []);

  const handleBulkDelete = useCallback(async () => {
    if (
      !confirm(`Delete ${selectedIds.size} photo(s) permanently?`)
    )
      return;
    for (const id of selectedIds) {
      await deletePhoto(id);
    }
    setSelectedIds(new Set());
    setSelectable(false);
  }, [selectedIds, deletePhoto]);

  const handleLightboxDelete = useCallback(
    async (fileId: string) => {
      await deletePhoto(fileId);
    },
    [deletePhoto]
  );

  const toggleSelect = useCallback(() => {
    setSelectable((s) => {
      if (s) setSelectedIds(new Set());
      return !s;
    });
  }, []);

  const isEmpty = photos.length === 0;
  const isFilteredEmpty = filteredPhotos.length === 0;

  return (
    <div className="relative flex h-full flex-col" {...dropZoneProps}>
      <DropZoneOverlay visible={isDragOver} />
      {/* Header */}
      <div className="flex flex-col gap-3 border-b border-white/[0.06] px-6 py-4 sm:flex-row sm:items-center sm:justify-between">
        <div className="min-w-0">
          <h1 className="text-lg font-semibold tracking-tight text-heading">
            Gallery
          </h1>
          <p className="text-xs text-muted-foreground mt-0.5">
            {filteredPhotos.length} photo{filteredPhotos.length !== 1 ? "s" : ""}
            {filter !== "all" && ` (filtered)`}
          </p>
        </div>

        <div className="flex shrink-0 items-center gap-2">
          <Button
            variant="ghost"
            size="icon"
            onClick={refresh}
            className="h-8 w-8"
          >
            <RefreshCw className="h-4 w-4" />
          </Button>

          {/* Filter */}
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="outline" size="sm" className="rounded-lg">
                <SlidersHorizontal className="h-3.5 w-3.5 mr-1.5" />
                {filter === "all"
                  ? "All"
                  : filter === "photos"
                    ? "Photos"
                    : "Videos"}
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem onClick={() => setFilter("all")}>
                <Image className="h-3.5 w-3.5 mr-2" />
                All media
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => setFilter("photos")}>
                <Camera className="h-3.5 w-3.5 mr-2" />
                Photos only
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => setFilter("videos")}>
                <Film className="h-3.5 w-3.5 mr-2" />
                Videos / GIFs
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>

          {/* Sort */}
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="outline" size="sm" className="rounded-lg">
                <ArrowUpDown className="h-3.5 w-3.5 mr-1.5" />
                {sort === "newest" ? "Newest" : "Oldest"}
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem onClick={() => setSort("newest")}>
                Newest first
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => setSort("oldest")}>
                Oldest first
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>

          {/* Select mode */}
          <Button
            variant={selectable ? "default" : "outline"}
            size="sm"
            className="rounded-lg"
            onClick={toggleSelect}
          >
            {selectable ? (
              <>
                <X className="h-3.5 w-3.5 mr-1.5" />
                Cancel
              </>
            ) : (
              <>
                <CheckSquare className="h-3.5 w-3.5 mr-1.5" />
                Select
              </>
            )}
          </Button>
        </div>
      </div>

      {/* Selection bar */}
      <AnimatePresence>
        {selectable && selectedIds.size > 0 && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            className="overflow-hidden border-b border-white/[0.06] bg-primary/5"
          >
            <div className="flex items-center justify-between px-6 py-2.5">
              <span className="text-sm text-foreground">
                {selectedIds.size} selected
              </span>
              <Button
                variant="destructive"
                size="sm"
                className="rounded-lg"
                onClick={handleBulkDelete}
              >
                <Trash2 className="h-3.5 w-3.5 mr-1.5" />
                Delete selected
              </Button>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Content */}
      <div className="flex-1 overflow-y-auto">
        {loading ? (
          <div className="flex flex-col items-center justify-center py-20">
            <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-secondary/80">
              <Spinner className="h-5 w-5 text-primary" />
            </div>
            <p className="mt-3 text-sm text-muted-foreground">
              Loading photos...
            </p>
          </div>
        ) : error ? (
          <div className="flex flex-col items-center justify-center py-20">
            <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-destructive/10">
              <ImageOff className="h-6 w-6 text-destructive" />
            </div>
            <p className="mt-4 text-sm font-medium text-foreground">
              Failed to load photos
            </p>
            <p className="mt-1 text-xs text-muted-foreground">{error}</p>
            <Button
              variant="outline"
              size="sm"
              className="mt-4 rounded-lg"
              onClick={refresh}
            >
              <RefreshCw className="h-3.5 w-3.5 mr-1.5" />
              Try again
            </Button>
          </div>
        ) : isEmpty ? (
          <div className="flex flex-col items-center justify-center py-20">
            <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-primary/10 ring-1 ring-white/[0.06]">
              <Camera className="h-7 w-7 text-primary" />
            </div>
            <p className="mt-5 text-sm font-semibold text-foreground">
              No photos yet
            </p>
            <p className="mt-1.5 max-w-xs text-center text-xs text-muted-foreground">
              Upload image files from the Files page and they'll appear here
              automatically.
            </p>
          </div>
        ) : isFilteredEmpty ? (
          <div className="flex flex-col items-center justify-center py-20">
            <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-secondary/80">
              <SlidersHorizontal className="h-6 w-6 text-muted-foreground" />
            </div>
            <p className="mt-4 text-sm font-medium text-foreground">
              No matches
            </p>
            <p className="mt-1 text-xs text-muted-foreground">
              No {filter === "photos" ? "photos" : "videos"} found. Try a
              different filter.
            </p>
          </div>
        ) : (
          <div className="px-4 py-4 sm:px-6">
            {filteredGroups.map((group) => (
              <div key={group.date} className="mb-6">
                <h2 className="text-sm font-semibold text-foreground mb-3 sticky top-0 bg-background/80 backdrop-blur-sm py-1 z-10">
                  {group.label}
                  <span className="ml-2 text-xs font-normal text-muted-foreground">
                    {group.photos.length}
                  </span>
                </h2>
                <JustifiedGrid
                  photos={group.photos}
                  targetRowHeight={220}
                  gap={4}
                  onPhotoClick={handlePhotoClick}
                  onPhotoSelect={handlePhotoSelect}
                  selectedIds={selectedIds}
                  selectable={selectable}
                />
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Lightbox */}
      <AnimatePresence>
        {lightboxIndex !== null && (
          <Lightbox
            photos={filteredPhotos}
            initialIndex={lightboxIndex}
            onClose={() => setLightboxIndex(null)}
            onDelete={handleLightboxDelete}
          />
        )}
      </AnimatePresence>

      {/* Transfer panel */}
      <AnimatePresence>
        {uploadTransfers.size > 0 && (
          <TransferPanel
            uploads={uploadTransfers}
            downloads={new Map()}
            onClearUpload={clearUpload}
            onClearDownload={() => {}}
          />
        )}
      </AnimatePresence>
    </div>
  );
}
