import { useState, useCallback, useMemo } from "react";
import { useParams, useNavigate, useSearchParams } from "react-router-dom";
import { motion, AnimatePresence } from "framer-motion";
import {
  Plus,
  ArrowLeft,
  Trash2,
  Share2,
  Link,
  Unlink,
  ImagePlus,
  BookImage,
  FolderOpen,
  X,
  Check,
  Copy,
} from "lucide-react";
import { useAlbums } from "@/hooks/use-albums";
import { useGallery } from "@/hooks/use-gallery";
import { useUpload } from "@/hooks/use-upload";
import { useDropZone } from "@/hooks/use-drop-zone";
import { useImageLoader } from "@/hooks/use-image-loader";
import { JustifiedGrid } from "@/components/gallery/justified-grid";
import { Lightbox } from "@/components/gallery/lightbox";
import { DropZoneOverlay } from "@/components/upload/drop-zone-overlay";
import { TransferPanel } from "@/components/upload/transfer-panel";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import type { GalleryPhoto, Album } from "@app/types";

// ── Album Cover ─────────────────────
function AlbumCover({ fileId }: { fileId: string | null }) {
  if (!fileId) {
    return (
      <div className="flex h-full w-full items-center justify-center bg-gradient-to-br from-surface to-secondary">
        <BookImage className="h-10 w-10 text-muted-foreground/50" />
      </div>
    );
  }
  return <AlbumCoverImage fileId={fileId} />;
}

function AlbumCoverImage({ fileId }: { fileId: string }) {
  const { src, loaded } = useImageLoader(fileId, "thumbnail");
  if (!src) {
    return (
      <div className="h-full w-full animate-pulse bg-gradient-to-br from-surface to-secondary" />
    );
  }
  return (
    <img
      src={src}
      className={`h-full w-full object-cover transition-opacity duration-500 ${loaded ? "opacity-100" : "opacity-0"}`}
      draggable={false}
    />
  );
}

// ── Create Album Dialog ─────────────
function CreateAlbumDialog({
  open,
  onOpenChange,
  onCreate,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onCreate: (name: string) => void;
}) {
  const [name, setName] = useState("");

  const handleCreate = () => {
    if (name.trim()) {
      onCreate(name.trim());
      setName("");
      onOpenChange(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Create Album</DialogTitle>
        </DialogHeader>
        <div className="space-y-4 pt-2">
          <Input
            placeholder="Album name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleCreate()}
            autoFocus
          />
          <div className="flex justify-end gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => onOpenChange(false)}
            >
              Cancel
            </Button>
            <Button
              size="sm"
              onClick={handleCreate}
              disabled={!name.trim()}
            >
              Create
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}

// ── Add Photos Dialog ─────────────
function AddPhotosDialog({
  open,
  onOpenChange,
  allPhotos,
  existingIds,
  onAdd,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  allPhotos: GalleryPhoto[];
  existingIds: Set<string>;
  onAdd: (photoIds: string[]) => void;
}) {
  const [selected, setSelected] = useState<Set<string>>(new Set());

  const available = useMemo(
    () => allPhotos.filter((p) => !existingIds.has(p.fileId)),
    [allPhotos, existingIds]
  );

  const toggleSelect = (id: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const handleAdd = () => {
    onAdd(Array.from(selected));
    setSelected(new Set());
    onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-2xl max-h-[70vh] overflow-hidden flex flex-col">
        <DialogHeader>
          <DialogTitle>Add Photos to Album</DialogTitle>
        </DialogHeader>
        <div className="flex-1 overflow-y-auto py-2">
          {available.length === 0 ? (
            <p className="text-sm text-muted-foreground py-8 text-center">
              No photos available to add. Upload more images first.
            </p>
          ) : (
            <JustifiedGrid
              photos={available}
              targetRowHeight={120}
              gap={3}
              onPhotoClick={(photo) => toggleSelect(photo.fileId)}
              onPhotoSelect={(photo) => toggleSelect(photo.fileId)}
              selectedIds={selected}
              selectable
            />
          )}
        </div>
        <div className="flex items-center justify-between pt-4 border-t border-white/[0.06]">
          <span className="text-xs text-muted-foreground">
            {selected.size} selected
          </span>
          <div className="flex gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => onOpenChange(false)}
            >
              Cancel
            </Button>
            <Button
              size="sm"
              onClick={handleAdd}
              disabled={selected.size === 0}
            >
              <ImagePlus className="h-3.5 w-3.5 mr-1.5" />
              Add {selected.size > 0 ? `(${selected.size})` : ""}
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}

// ── Share Dialog ─────────────────────
function ShareDialog({
  open,
  onOpenChange,
  shareUrl,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  shareUrl: string;
}) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(shareUrl);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Share Album</DialogTitle>
        </DialogHeader>
        <div className="space-y-3 pt-2">
          <p className="text-sm text-muted-foreground">
            Anyone with this link can view the photos in this album.
          </p>
          <div className="flex items-center gap-2">
            <Input value={shareUrl} readOnly className="text-xs font-mono" />
            <Button
              variant="outline"
              size="icon"
              className="shrink-0"
              onClick={handleCopy}
            >
              {copied ? (
                <Check className="h-4 w-4 text-green-400" />
              ) : (
                <Copy className="h-4 w-4" />
              )}
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}

// ── Album Detail View ───────────────
function AlbumDetail({
  album,
  onBack,
}: {
  album: Album;
  onBack: () => void;
}) {
  const { photos: allPhotos, refresh: refreshGallery } = useGallery();
  const {
    updateAlbum,
    deleteAlbum,
    addPhotosToAlbum,
    removePhotoFromAlbum,
    shareAlbum,
    unshareAlbum,
    getShareToken,
  } = useAlbums();

  const {
    transfers: uploadTransfers,
    uploadFile,
    clearTransfer: clearUpload,
  } = useUpload(refreshGallery);

  const handleFileDrop = useCallback(
    async (files: File[]) => {
      const newFileIds: string[] = [];
      for (const file of files) {
        const result = await uploadFile(file, null);
        if (result.failedCount === 0) {
          newFileIds.push(result.fileId);
        }
      }
      if (newFileIds.length > 0) {
        addPhotosToAlbum(album.id, newFileIds);
      }
    },
    [uploadFile, addPhotosToAlbum, album.id]
  );

  const { isDragOver, dropZoneProps } = useDropZone({
    onDrop: handleFileDrop,
  });

  const [lightboxIndex, setLightboxIndex] = useState<number | null>(null);
  const [showAddPhotos, setShowAddPhotos] = useState(false);
  const [showShare, setShowShare] = useState(false);

  const albumPhotos = useMemo(() => {
    const idSet = new Set(album.photoIds);
    return allPhotos.filter((p) => idSet.has(p.fileId));
  }, [allPhotos, album.photoIds]);

  const existingIds = useMemo(
    () => new Set(album.photoIds),
    [album.photoIds]
  );

  const shareToken = getShareToken(album.id);

  const handlePhotoClick = useCallback(
    (_photo: GalleryPhoto, _idx: number) => {
      const globalIdx = albumPhotos.findIndex(
        (p) => p.fileId === _photo.fileId
      );
      setLightboxIndex(globalIdx >= 0 ? globalIdx : 0);
    },
    [albumPhotos]
  );

  const handleAddPhotos = useCallback(
    (photoIds: string[]) => {
      addPhotosToAlbum(album.id, photoIds);
    },
    [addPhotosToAlbum, album.id]
  );

  const handleShare = useCallback(async () => {
    if (!shareToken) {
      try {
        await shareAlbum(album.id);
      } catch (err) {
        // Mint failed (e.g. JWT_SECRET missing on the deploy → 503,
        // or session expired → 401). Surface in the share dialog
        // rather than silently failing — the dialog reads the share
        // token via getShareToken, so the absence renders a clear
        // "no token yet" state.
        // eslint-disable-next-line no-console
        console.error("shareAlbum: mint failed", err);
      }
    }
    setShowShare(true);
  }, [shareToken, shareAlbum, album.id]);

  const handleUnshare = useCallback(() => {
    unshareAlbum(album.id);
  }, [unshareAlbum, album.id]);

  const handleDelete = useCallback(() => {
    if (confirm("Delete this album? Photos won't be deleted.")) {
      deleteAlbum(album.id);
      onBack();
    }
  }, [deleteAlbum, album.id, onBack]);

  const currentShareToken = getShareToken(album.id);
  const shareUrl = currentShareToken
    ? `${window.location.origin}/shared/${currentShareToken}`
    : "";

  return (
    <div className="relative flex h-full flex-col" {...dropZoneProps}>
      <DropZoneOverlay visible={isDragOver} />
      {/* Header */}
      <div className="flex flex-col gap-3 border-b border-white/[0.06] px-6 py-4 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex items-center gap-3 min-w-0">
          <Button variant="ghost" size="icon" onClick={onBack} className="shrink-0">
            <ArrowLeft className="h-4 w-4" />
          </Button>
          <div className="min-w-0">
            <h1 className="text-lg font-semibold tracking-tight text-heading truncate">
              {album.name}
            </h1>
            <p className="text-xs text-muted-foreground">
              {albumPhotos.length} photo{albumPhotos.length !== 1 ? "s" : ""}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            className="rounded-lg"
            onClick={() => setShowAddPhotos(true)}
          >
            <ImagePlus className="h-3.5 w-3.5 mr-1.5" />
            Add photos
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="rounded-lg"
            onClick={handleShare}
          >
            <Share2 className="h-3.5 w-3.5 mr-1.5" />
            Share
          </Button>
          {shareToken && (
            <Button
              variant="ghost"
              size="sm"
              className="rounded-lg text-muted-foreground"
              onClick={handleUnshare}
            >
              <Unlink className="h-3.5 w-3.5 mr-1.5" />
              Unshare
            </Button>
          )}
          <Button
            variant="ghost"
            size="icon"
            className="text-destructive hover:text-destructive"
            onClick={handleDelete}
          >
            <Trash2 className="h-4 w-4" />
          </Button>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto">
        {albumPhotos.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-20">
            <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-primary/10 ring-1 ring-white/[0.06]">
              <ImagePlus className="h-7 w-7 text-primary" />
            </div>
            <p className="mt-5 text-sm font-semibold text-foreground">
              Empty album
            </p>
            <p className="mt-1.5 max-w-xs text-center text-xs text-muted-foreground">
              Add photos from your gallery to this album.
            </p>
            <Button
              size="sm"
              className="mt-4 rounded-lg"
              onClick={() => setShowAddPhotos(true)}
            >
              <ImagePlus className="h-3.5 w-3.5 mr-1.5" />
              Add photos
            </Button>
          </div>
        ) : (
          <div className="px-4 py-4 sm:px-6">
            <JustifiedGrid
              photos={albumPhotos}
              targetRowHeight={220}
              gap={4}
              onPhotoClick={handlePhotoClick}
            />
          </div>
        )}
      </div>

      {/* Dialogs */}
      <AddPhotosDialog
        open={showAddPhotos}
        onOpenChange={setShowAddPhotos}
        allPhotos={allPhotos}
        existingIds={existingIds}
        onAdd={handleAddPhotos}
      />

      <ShareDialog
        open={showShare}
        onOpenChange={setShowShare}
        shareUrl={shareUrl}
      />

      {/* Lightbox */}
      <AnimatePresence>
        {lightboxIndex !== null && (
          <Lightbox
            photos={albumPhotos}
            initialIndex={lightboxIndex}
            onClose={() => setLightboxIndex(null)}
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

// ── Albums Grid ─────────────────────
export function AlbumsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const albumId = searchParams.get("album");
  const { albums, createAlbum, getAlbum, getShareToken } = useAlbums();
  const [showCreate, setShowCreate] = useState(false);

  const {
    transfers: uploadTransfers,
    uploadFile,
    clearTransfer: clearUpload,
  } = useUpload();

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

  const currentAlbum = albumId ? getAlbum(albumId) : undefined;

  const handleCreateAlbum = useCallback(
    (name: string) => {
      createAlbum(name, []);
    },
    [createAlbum]
  );

  const navigateToAlbum = useCallback(
    (id: string) => {
      setSearchParams({ album: id });
    },
    [setSearchParams]
  );

  const navigateBack = useCallback(() => {
    setSearchParams({});
  }, [setSearchParams]);

  if (currentAlbum) {
    return <AlbumDetail album={currentAlbum} onBack={navigateBack} />;
  }

  return (
    <div className="relative flex h-full flex-col" {...dropZoneProps}>
      <DropZoneOverlay visible={isDragOver} />
      {/* Header */}
      <div className="flex items-center justify-between border-b border-white/[0.06] px-6 py-4">
        <div>
          <h1 className="text-lg font-semibold tracking-tight text-heading">
            Albums
          </h1>
          <p className="text-xs text-muted-foreground mt-0.5">
            {albums.length} album{albums.length !== 1 ? "s" : ""}
          </p>
        </div>
        <Button
          size="sm"
          className="rounded-lg"
          onClick={() => setShowCreate(true)}
        >
          <Plus className="h-3.5 w-3.5 mr-1.5" />
          New Album
        </Button>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4 sm:p-6">
        {albums.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-20">
            <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-primary/10 ring-1 ring-white/[0.06]">
              <BookImage className="h-7 w-7 text-primary" />
            </div>
            <p className="mt-5 text-sm font-semibold text-foreground">
              No albums yet
            </p>
            <p className="mt-1.5 max-w-xs text-center text-xs text-muted-foreground">
              Create albums to organize your photos into collections.
            </p>
            <Button
              size="sm"
              className="mt-4 rounded-lg"
              onClick={() => setShowCreate(true)}
            >
              <Plus className="h-3.5 w-3.5 mr-1.5" />
              Create Album
            </Button>
          </div>
        ) : (
          <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5">
            {albums.map((album) => (
              <motion.div
                key={album.id}
                initial={{ opacity: 0, scale: 0.95 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ duration: 0.2 }}
                className="group cursor-pointer"
                onClick={() => navigateToAlbum(album.id)}
              >
                <div className="aspect-square overflow-hidden rounded-xl bg-surface ring-1 ring-white/[0.06] transition-all duration-200 group-hover:ring-primary/50 group-hover:shadow-lg">
                  <AlbumCover fileId={album.coverPhotoId} />
                </div>
                <div className="mt-2 px-1">
                  <p className="text-sm font-medium text-foreground truncate">
                    {album.name}
                  </p>
                  <div className="flex items-center gap-2">
                    <p className="text-xs text-muted-foreground">
                      {album.photoIds.length} photo
                      {album.photoIds.length !== 1 ? "s" : ""}
                    </p>
                    {getShareToken(album.id) && (
                      <Link className="h-3 w-3 text-primary" />
                    )}
                  </div>
                </div>
              </motion.div>
            ))}
          </div>
        )}
      </div>

      <CreateAlbumDialog
        open={showCreate}
        onOpenChange={setShowCreate}
        onCreate={handleCreateAlbum}
      />

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
