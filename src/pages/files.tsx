import { useState, useCallback, useRef } from "react";
import { useSearchParams } from "react-router-dom";
import { motion, AnimatePresence } from "framer-motion";
import {
  FolderPlus,
  Upload,
  RefreshCw,
  FileX,
  CloudUpload,
  Grid3X3,
  List,
  Eye,
  Download,
  Trash2,
} from "lucide-react";
import { useFiles } from "@/hooks/use-files";
import { useUpload } from "@/hooks/use-upload";
import { useDownload } from "@/hooks/use-download";
import { useImageLoader } from "@/hooks/use-image-loader";
import { Button } from "@/components/ui/button";
import { Breadcrumbs } from "@/components/files/breadcrumbs";
import { FolderRow } from "@/components/files/file-row";
import { FileIcon } from "@/components/files/file-icon";
import { CreateFolderDialog } from "@/components/files/create-folder-dialog";
import { UploadZone } from "@/components/upload/upload-zone";
import { TransferPanel } from "@/components/upload/transfer-panel";
import { FilePreviewPanel } from "@/components/files/file-preview";
import { Lightbox } from "@/components/gallery/lightbox";
import { Spinner } from "@/components/ui/spinner";
import { cn, formatBytes, formatDate } from "@/lib/utils";
import type { UserFile, GalleryPhoto } from "@shared/types";

type ViewMode = "list" | "grid";

// Grid thumbnail for image files
function GridImageThumb({ fileId }: { fileId: string }) {
  const { src, loaded } = useImageLoader(fileId, "thumbnail");
  if (!src) {
    return <div className="h-full w-full animate-pulse bg-gradient-to-br from-surface to-secondary" />;
  }
  return (
    <img
      src={src}
      className={cn(
        "h-full w-full object-cover transition-opacity duration-300",
        loaded ? "opacity-100" : "opacity-0"
      )}
      draggable={false}
    />
  );
}

// Small inline thumbnail for list-view image files
function InlineThumb({ fileId }: { fileId: string }) {
  const { src, loaded } = useImageLoader(fileId, "thumbnail");
  if (!src) {
    return <div className="h-10 w-10 animate-pulse rounded-lg bg-gradient-to-br from-surface to-secondary" />;
  }
  return (
    <img
      src={src}
      className={cn(
        "h-10 w-10 rounded-lg object-cover transition-opacity duration-300",
        loaded ? "opacity-100" : "opacity-0"
      )}
      draggable={false}
    />
  );
}

export function FilesPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const parentId = searchParams.get("folder");

  const { files, folders, path, loading, error, refresh, deleteFile, createFolder } =
    useFiles(parentId);

  const {
    transfers: uploadTransfers,
    uploadFile,
    clearTransfer: clearUpload,
  } = useUpload(refresh);

  const {
    transfers: downloadTransfers,
    downloadFile,
    clearTransfer: clearDownload,
  } = useDownload();

  const [showCreateFolder, setShowCreateFolder] = useState(false);
  const [showUploadZone, setShowUploadZone] = useState(false);
  const [viewMode, setViewMode] = useState<ViewMode>("list");
  const [previewFile, setPreviewFile] = useState<UserFile | null>(null);
  const [lightboxIndex, setLightboxIndex] = useState<number | null>(null);

  // Context menu state
  const [contextMenu, setContextMenu] = useState<{
    x: number;
    y: number;
    file: UserFile;
  } | null>(null);

  const navigateToFolder = useCallback(
    (folderId: string | null) => {
      if (folderId) {
        setSearchParams({ folder: folderId });
      } else {
        setSearchParams({});
      }
    },
    [setSearchParams]
  );

  const handleFilesSelected = useCallback(
    async (selectedFiles: File[]) => {
      for (const file of selectedFiles) {
        uploadFile(file, parentId);
      }
      setShowUploadZone(false);
    },
    [uploadFile, parentId]
  );

  const handleDownload = useCallback(
    (file: UserFile) => {
      downloadFile(file.fileId, file.fileName);
    },
    [downloadFile]
  );

  const handleDelete = useCallback(
    async (file: UserFile) => {
      if (previewFile?.fileId === file.fileId) setPreviewFile(null);
      await deleteFile(file.fileId);
    },
    [deleteFile, previewFile]
  );

  // Get image files for lightbox
  const imageFiles = files.filter(
    (f) => f.status === "complete" && f.mimeType.startsWith("image/")
  );

  const imagePhotos: GalleryPhoto[] = imageFiles.map((f) => ({
    fileId: f.fileId,
    fileName: f.fileName,
    fileSize: f.fileSize,
    mimeType: f.mimeType,
    parentId: f.parentId,
    createdAt: f.createdAt,
    updatedAt: f.updatedAt,
  }));

  const handleFileClick = useCallback(
    (file: UserFile) => {
      if (file.status !== "complete") return;

      if (file.mimeType.startsWith("image/")) {
        const idx = imageFiles.findIndex((f) => f.fileId === file.fileId);
        if (idx >= 0) {
          setLightboxIndex(idx);
          return;
        }
      }

      // For non-images, open the preview panel
      setPreviewFile(file);
    },
    [imageFiles]
  );

  const handleContextMenu = useCallback(
    (e: React.MouseEvent, file: UserFile) => {
      e.preventDefault();
      setContextMenu({ x: e.clientX, y: e.clientY, file });
    },
    []
  );

  // Close context menu on click outside
  const handleContainerClick = useCallback(() => {
    if (contextMenu) setContextMenu(null);
  }, [contextMenu]);

  const isEmpty = files.length === 0 && folders.length === 0;

  return (
    <div className="flex h-full" onClick={handleContainerClick}>
      <div className="flex flex-1 flex-col min-w-0">
        {/* Header */}
        <div className="flex flex-col gap-3 border-b border-white/[0.06] px-6 py-4 sm:flex-row sm:items-center sm:justify-between">
          <div className="min-w-0">
            <h1 className="text-lg font-semibold tracking-tight text-heading">Files</h1>
            <Breadcrumbs path={path} onNavigate={navigateToFolder} />
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

            {/* View mode toggle */}
            <div className="flex items-center rounded-lg border border-white/[0.06] p-0.5">
              <Button
                variant={viewMode === "list" ? "secondary" : "ghost"}
                size="icon"
                className="h-7 w-7"
                onClick={() => setViewMode("list")}
              >
                <List className="h-3.5 w-3.5" />
              </Button>
              <Button
                variant={viewMode === "grid" ? "secondary" : "ghost"}
                size="icon"
                className="h-7 w-7"
                onClick={() => setViewMode("grid")}
              >
                <Grid3X3 className="h-3.5 w-3.5" />
              </Button>
            </div>

            <Button
              variant="outline"
              size="sm"
              onClick={() => setShowCreateFolder(true)}
              className="rounded-lg"
            >
              <FolderPlus className="h-4 w-4 mr-1.5" />
              <span className="hidden sm:inline">New Folder</span>
              <span className="sm:hidden">Folder</span>
            </Button>
            <Button
              size="sm"
              onClick={() => setShowUploadZone(!showUploadZone)}
              className="rounded-lg"
            >
              <Upload className="h-4 w-4 mr-1.5" />
              Upload
            </Button>
          </div>
        </div>

        {/* Upload zone */}
        <AnimatePresence>
          {showUploadZone && (
            <motion.div
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: "auto", opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              transition={{ duration: 0.2, ease: "easeInOut" }}
              className="overflow-hidden border-b border-border"
            >
              <div className="p-4">
                <UploadZone onFilesSelected={handleFilesSelected} />
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-4 sm:p-6">
          {loading ? (
            <div className="flex flex-col items-center justify-center py-20">
              <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-secondary/80">
                <Spinner className="h-5 w-5 text-primary" />
              </div>
              <p className="mt-3 text-sm text-muted-foreground">Loading files...</p>
            </div>
          ) : error ? (
            <div className="flex flex-col items-center justify-center py-20">
              <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-destructive/10">
                <FileX className="h-6 w-6 text-destructive" />
              </div>
              <p className="mt-4 text-sm font-medium text-foreground">
                Failed to load files
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
          ) : isEmpty && !showUploadZone ? (
            <div className="flex flex-col items-center justify-center py-20">
              <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-primary/10 ring-1 ring-white/[0.06]">
                <CloudUpload className="h-7 w-7 text-primary" />
              </div>
              <p className="mt-5 text-sm font-semibold text-foreground">
                No files yet
              </p>
              <p className="mt-1.5 max-w-xs text-center text-xs text-muted-foreground">
                Upload files or create a folder to get started with your distributed storage.
              </p>
              <div className="mt-5 flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  className="rounded-lg"
                  onClick={() => setShowCreateFolder(true)}
                >
                  <FolderPlus className="h-4 w-4 mr-1.5" />
                  New Folder
                </Button>
                <Button
                  size="sm"
                  className="rounded-lg"
                  onClick={() => setShowUploadZone(true)}
                >
                  <Upload className="h-4 w-4 mr-1.5" />
                  Upload files
                </Button>
              </div>
            </div>
          ) : viewMode === "list" ? (
            /* ── List View ── */
            <div className="space-y-1">
              <AnimatePresence>
                {folders.map((folder) => (
                  <FolderRow
                    key={folder.folderId}
                    folder={folder}
                    onOpen={navigateToFolder}
                  />
                ))}
                {files.map((file) => (
                  <div
                    key={file.fileId}
                    onContextMenu={(e) => handleContextMenu(e, file)}
                  >
                    <EnhancedFileRow
                      file={file}
                      onDownload={handleDownload}
                      onDelete={handleDelete}
                      onClick={handleFileClick}
                      isPreviewActive={previewFile?.fileId === file.fileId}
                    />
                  </div>
                ))}
              </AnimatePresence>
            </div>
          ) : (
            /* ── Grid View ── */
            <div>
              {/* Folders grid */}
              {folders.length > 0 && (
                <div className="mb-6">
                  <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-3">
                    Folders
                  </h3>
                  <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6">
                    {folders.map((folder) => (
                      <FolderRow
                        key={folder.folderId}
                        folder={folder}
                        onOpen={navigateToFolder}
                      />
                    ))}
                  </div>
                </div>
              )}
              {/* Files grid */}
              {files.length > 0 && (
                <div>
                  {folders.length > 0 && (
                    <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-3">
                      Files
                    </h3>
                  )}
                  <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6">
                    <AnimatePresence>
                      {files.map((file) => (
                        <motion.div
                          key={file.fileId}
                          layout
                          initial={{ opacity: 0, scale: 0.95 }}
                          animate={{ opacity: 1, scale: 1 }}
                          exit={{ opacity: 0, scale: 0.95 }}
                          transition={{ duration: 0.2 }}
                          className={cn(
                            "group cursor-pointer rounded-xl border border-transparent overflow-hidden transition-all duration-200 hover:border-white/[0.06] hover:bg-white/[0.03]",
                            previewFile?.fileId === file.fileId && "border-primary/50 bg-primary/5"
                          )}
                          onClick={() => handleFileClick(file)}
                          onContextMenu={(e) => handleContextMenu(e, file)}
                        >
                          {/* Thumbnail */}
                          <div className="aspect-square overflow-hidden bg-surface">
                            {file.mimeType.startsWith("image/") && file.status === "complete" ? (
                              <GridImageThumb fileId={file.fileId} />
                            ) : (
                              <div className="flex h-full w-full items-center justify-center bg-gradient-to-br from-surface to-secondary/50">
                                <FileIcon mimeType={file.mimeType} className="h-10 w-10 opacity-60" />
                              </div>
                            )}
                          </div>
                          {/* Info */}
                          <div className="p-2.5">
                            <p className="truncate text-xs font-medium text-foreground">
                              {file.fileName}
                            </p>
                            <p className="text-[10px] text-muted-foreground mt-0.5">
                              {formatBytes(file.fileSize)}
                            </p>
                          </div>
                        </motion.div>
                      ))}
                    </AnimatePresence>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Context Menu */}
        <AnimatePresence>
          {contextMenu && (
            <ContextMenuOverlay
              x={contextMenu.x}
              y={contextMenu.y}
              file={contextMenu.file}
              onClose={() => setContextMenu(null)}
              onOpen={() => {
                handleFileClick(contextMenu.file);
                setContextMenu(null);
              }}
              onPreview={() => {
                setPreviewFile(contextMenu.file);
                setContextMenu(null);
              }}
              onDownload={() => {
                handleDownload(contextMenu.file);
                setContextMenu(null);
              }}
              onDelete={() => {
                handleDelete(contextMenu.file);
                setContextMenu(null);
              }}
            />
          )}
        </AnimatePresence>

        {/* Dialogs */}
        <CreateFolderDialog
          open={showCreateFolder}
          onOpenChange={setShowCreateFolder}
          onCreate={createFolder}
        />

        {/* Transfer panel */}
        <AnimatePresence>
          {(uploadTransfers.size > 0 || downloadTransfers.size > 0) && (
            <TransferPanel
              uploads={uploadTransfers}
              downloads={downloadTransfers}
              onClearUpload={clearUpload}
              onClearDownload={clearDownload}
            />
          )}
        </AnimatePresence>

        {/* Lightbox for images */}
        <AnimatePresence>
          {lightboxIndex !== null && (
            <Lightbox
              photos={imagePhotos}
              initialIndex={lightboxIndex}
              onClose={() => setLightboxIndex(null)}
              onDelete={async (fileId) => {
                const file = files.find((f) => f.fileId === fileId);
                if (file) await handleDelete(file);
              }}
            />
          )}
        </AnimatePresence>
      </div>

      {/* Preview side panel */}
      <AnimatePresence>
        {previewFile && (
          <FilePreviewPanel
            file={previewFile}
            onClose={() => setPreviewFile(null)}
            onDownload={handleDownload}
          />
        )}
      </AnimatePresence>
    </div>
  );
}

// ── Enhanced File Row (list view with thumbnails + click) ──
function EnhancedFileRow({
  file,
  onDownload,
  onDelete,
  onClick,
  isPreviewActive,
}: {
  file: UserFile;
  onDownload: (file: UserFile) => void;
  onDelete: (file: UserFile) => void;
  onClick: (file: UserFile) => void;
  isPreviewActive: boolean;
}) {
  const isImage = file.mimeType.startsWith("image/") && file.status === "complete";

  return (
    <motion.div
      layout
      initial={{ opacity: 0, y: 4 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -4 }}
      transition={{ duration: 0.2 }}
      className={cn(
        "group flex items-center gap-4 rounded-xl border border-transparent px-4 py-3 transition-all duration-200 hover:border-white/[0.06] hover:bg-white/[0.03] cursor-pointer",
        isPreviewActive && "border-primary/50 bg-primary/5"
      )}
      onClick={() => onClick(file)}
    >
      {/* Thumbnail or icon */}
      <div className="flex h-10 w-10 shrink-0 items-center justify-center overflow-hidden rounded-xl bg-white/[0.05] transition-colors duration-200 group-hover:bg-white/[0.08]">
        {isImage ? (
          <InlineThumb fileId={file.fileId} />
        ) : (
          <FileIcon mimeType={file.mimeType} />
        )}
      </div>

      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="truncate text-sm font-medium">{file.fileName}</span>
          {file.status !== "complete" && (
            <span className={cn(
              "inline-flex items-center rounded-full px-1.5 py-0.5 text-[10px] font-medium",
              file.status === "uploading" && "bg-primary/10 text-primary",
              file.status === "failed" && "bg-destructive/10 text-destructive",
              file.status === "deleted" && "bg-secondary text-muted-foreground"
            )}>
              {file.status}
            </span>
          )}
        </div>
        <div className="flex items-center gap-3 text-xs text-muted-foreground mt-0.5">
          <span className="tabular-nums">{formatBytes(file.fileSize)}</span>
          <span className="hidden sm:inline">{formatDate(file.createdAt)}</span>
        </div>
      </div>

      <div
        className="flex items-center gap-1 opacity-0 transition-opacity duration-200 group-hover:opacity-100"
        onClick={(e) => e.stopPropagation()}
      >
        {file.status === "complete" && (
          <Button
            variant="ghost"
            size="icon"
            className="h-8 w-8"
            onClick={() => onDownload(file)}
            title="Download"
          >
            <Download className="h-3.5 w-3.5" />
          </Button>
        )}
        <Button
          variant="ghost"
          size="icon"
          className="h-8 w-8 hover:text-destructive"
          onClick={() => {
            if (confirm("Delete this file?")) onDelete(file);
          }}
          title="Delete"
        >
          <Trash2 className="h-3.5 w-3.5" />
        </Button>
      </div>
    </motion.div>
  );
}

// ── Context Menu ──
function ContextMenuOverlay({
  x,
  y,
  file,
  onClose,
  onOpen,
  onPreview,
  onDownload,
  onDelete,
}: {
  x: number;
  y: number;
  file: UserFile;
  onClose: () => void;
  onOpen: () => void;
  onPreview: () => void;
  onDownload: () => void;
  onDelete: () => void;
}) {
  // Adjust position if near edge
  const adjustedX = Math.min(x, window.innerWidth - 180);
  const adjustedY = Math.min(y, window.innerHeight - 200);

  return (
    <>
      <div className="fixed inset-0 z-40" onClick={onClose} />
      <motion.div
        className="fixed z-50 w-44 rounded-xl border border-white/[0.06] bg-[#2d2d2d] p-1 shadow-xl"
        style={{ left: adjustedX, top: adjustedY }}
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        exit={{ opacity: 0, scale: 0.95 }}
        transition={{ duration: 0.1 }}
      >
        {file.status === "complete" && (
          <>
            <ContextMenuItem label="Open" onClick={onOpen} />
            <ContextMenuItem label="Info Panel" onClick={onPreview} />
            <ContextMenuItem label="Download" onClick={onDownload} />
            <div className="my-1 border-t border-white/[0.06]" />
          </>
        )}
        <ContextMenuItem label="Delete" onClick={onDelete} destructive />
      </motion.div>
    </>
  );
}

function ContextMenuItem({
  label,
  onClick,
  destructive,
}: {
  label: string;
  onClick: () => void;
  destructive?: boolean;
}) {
  return (
    <button
      className={cn(
        "flex w-full items-center rounded-lg px-3 py-1.5 text-sm transition-colors",
        destructive
          ? "text-destructive hover:bg-destructive/10"
          : "text-foreground hover:bg-white/[0.05]"
      )}
      onClick={onClick}
    >
      {label}
    </button>
  );
}
