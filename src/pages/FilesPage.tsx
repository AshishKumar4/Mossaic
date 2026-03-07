import { useState } from "react";
import { useParams } from "react-router-dom";
import { FolderPlus, WarningCircle } from "@phosphor-icons/react";
import { Button, Banner, Loader } from "@cloudflare/kumo";
import { useFiles } from "../hooks/useFiles";
import { FileList } from "../components/files/FileList";
import { FolderBreadcrumbs } from "../components/files/FolderBreadcrumbs";
import { CreateFolderDialog } from "../components/files/CreateFolderDialog";
import { UploadButton } from "../components/upload/UploadButton";
import { DropZone } from "../components/upload/DropZone";
import { UploadProgress } from "../components/upload/UploadProgress";

export function FilesPage() {
  const { folderId } = useParams<{ folderId: string }>();
  const {
    files,
    folders,
    folderPath,
    isLoading,
    error,
    uploads,
    downloads,
    upload,
    download,
    deleteFile,
    createFolder,
  } = useFiles(folderId || null);

  const [showCreateFolder, setShowCreateFolder] = useState(false);

  const allTransfers = new Map([...uploads, ...downloads]);

  return (
    <DropZone onDrop={(fileList) => upload(fileList)}>
      <div className="space-y-6">
        {/* Toolbar */}
        <div className="flex items-center justify-between">
          <FolderBreadcrumbs path={folderPath} />

          <div className="flex items-center gap-2">
            <Button
              variant="secondary"
              size="sm"
              icon={FolderPlus}
              onClick={() => setShowCreateFolder(true)}
            >
              New Folder
            </Button>
            <UploadButton onUpload={(fileList) => upload(fileList)} />
          </div>
        </div>

        {/* Upload progress -- the showpiece */}
        <UploadProgress transfers={allTransfers} />

        {/* Error */}
        {error && (
          <Banner
            variant="error"
            icon={<WarningCircle weight="fill" />}
            title={error}
          />
        )}

        {/* Loading */}
        {isLoading ? (
          <div className="flex items-center justify-center py-20">
            <Loader size="lg" />
          </div>
        ) : (
          <FileList
            files={files}
            folders={folders}
            onDownload={download}
            onDelete={deleteFile}
          />
        )}

        {/* Create folder dialog */}
        <CreateFolderDialog
          open={showCreateFolder}
          onClose={() => setShowCreateFolder(false)}
          onCreate={createFolder}
        />
      </div>
    </DropZone>
  );
}
