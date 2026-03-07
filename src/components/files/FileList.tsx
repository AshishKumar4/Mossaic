import type { UserFile, Folder } from "@shared/types";
import {
  Table,
  LayerCard,
  Badge,
  Text,
  Button,
  DropdownMenu,
} from "@cloudflare/kumo";
import {
  Folder as FolderIcon,
  File as FileIcon,
  Image,
  VideoCamera,
  MusicNote,
  FileText,
  Archive,
  DownloadSimple,
  Trash,
  DotsThree,
} from "@phosphor-icons/react";
import { useNavigate } from "react-router-dom";
import { formatBytes, formatDate, getFileCategory } from "../../lib/utils";
import { EmptyState } from "./EmptyState";

interface FileListProps {
  files: UserFile[];
  folders: Folder[];
  onDownload: (fileId: string) => void;
  onDelete: (fileId: string) => void;
}

function getFileIcon(mimeType: string) {
  const category = getFileCategory(mimeType);
  switch (category) {
    case "image":
      return (
        <Image size={18} weight="duotone" className="text-green-500" />
      );
    case "video":
      return (
        <VideoCamera size={18} weight="duotone" className="text-purple-500" />
      );
    case "audio":
      return (
        <MusicNote size={18} weight="duotone" className="text-pink-500" />
      );
    case "document":
      return (
        <FileText size={18} weight="duotone" className="text-blue-500" />
      );
    case "archive":
      return (
        <Archive size={18} weight="duotone" className="text-amber-500" />
      );
    default:
      return (
        <FileIcon size={18} weight="duotone" className="text-kumo-strong" />
      );
  }
}

export function FileList({
  files,
  folders,
  onDownload,
  onDelete,
}: FileListProps) {
  const navigate = useNavigate();

  if (files.length === 0 && folders.length === 0) {
    return <EmptyState />;
  }

  return (
    <LayerCard>
      <LayerCard.Primary className="p-0">
        <Table>
          <Table.Header>
            <Table.Row>
              <Table.Head>Name</Table.Head>
              <Table.Head>Size</Table.Head>
              <Table.Head>Modified</Table.Head>
              <Table.Head className="w-12" />
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {folders.map((folder) => (
              <Table.Row
                key={folder.folderId}
                className="cursor-pointer"
                onClick={() => navigate(`/files/${folder.folderId}`)}
              >
                <Table.Cell>
                  <div className="flex items-center gap-3">
                    <FolderIcon
                      size={18}
                      weight="duotone"
                      className="text-kumo-brand"
                    />
                    <Text bold>{folder.name}</Text>
                  </div>
                </Table.Cell>
                <Table.Cell>
                  <Text variant="secondary">--</Text>
                </Table.Cell>
                <Table.Cell>
                  <Text size="xs" variant="secondary">
                    {new Date(folder.createdAt).toLocaleDateString()}
                  </Text>
                </Table.Cell>
                <Table.Cell />
              </Table.Row>
            ))}

            {files.map((file) => (
              <Table.Row key={file.fileId}>
                <Table.Cell>
                  <div className="flex items-center gap-3 min-w-0">
                    {getFileIcon(file.mimeType)}
                    <Text className="truncate">{file.fileName}</Text>
                    {file.status === "uploading" && (
                      <Badge variant="beta">Uploading</Badge>
                    )}
                    {file.status === "failed" && (
                      <Badge variant="destructive">Failed</Badge>
                    )}
                  </div>
                </Table.Cell>
                <Table.Cell>
                  <Text size="xs" variant="secondary">
                    {formatBytes(file.fileSize)}
                  </Text>
                </Table.Cell>
                <Table.Cell>
                  <Text size="xs" variant="secondary">
                    {formatDate(file.updatedAt)}
                  </Text>
                </Table.Cell>
                <Table.Cell className="text-right">
                  <DropdownMenu>
                    <DropdownMenu.Trigger
                      render={
                        <Button
                          variant="ghost"
                          size="sm"
                          shape="square"
                          aria-label="More options"
                        >
                          <DotsThree weight="bold" size={16} />
                        </Button>
                      }
                    />
                    <DropdownMenu.Content>
                      <DropdownMenu.Item
                        icon={DownloadSimple}
                        onSelect={() => onDownload(file.fileId)}
                      >
                        Download
                      </DropdownMenu.Item>
                      <DropdownMenu.Separator />
                      <DropdownMenu.Item
                        icon={Trash}
                        variant="danger"
                        onSelect={() => onDelete(file.fileId)}
                      >
                        Delete
                      </DropdownMenu.Item>
                    </DropdownMenu.Content>
                  </DropdownMenu>
                </Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        </Table>
      </LayerCard.Primary>
    </LayerCard>
  );
}
