import {
  Folder as FolderIcon,
  Download,
  Trash2,
  MoreHorizontal,
  ChevronRight,
} from "lucide-react";
import { motion } from "framer-motion";
import type { UserFile, Folder } from "@shared/types";
import { cn, formatBytes, formatDate } from "@/lib/utils";
import { FileIcon } from "./file-icon";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

interface FileRowProps {
  file: UserFile;
  onDownload: (file: UserFile) => void;
  onDelete: (file: UserFile) => void;
}

export function FileRow({ file, onDownload, onDelete }: FileRowProps) {
  const statusVariant =
    file.status === "complete"
      ? "success"
      : file.status === "uploading"
        ? "default"
        : file.status === "failed"
          ? "destructive"
          : "secondary";

  return (
    <motion.div
      layout
      initial={{ opacity: 0, y: 4 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -4 }}
      transition={{ duration: 0.2 }}
      className="group flex items-center gap-4 rounded-xl border border-transparent px-4 py-3 transition-all duration-200 hover:border-white/[0.06] hover:bg-white/[0.03]"
    >
      <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-white/[0.05] transition-colors duration-200 group-hover:bg-white/[0.08]">
        <FileIcon mimeType={file.mimeType} />
      </div>

      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="truncate text-sm font-medium">{file.fileName}</span>
          {file.status !== "complete" && (
            <Badge variant={statusVariant} className="text-[10px]">
              {file.status}
            </Badge>
          )}
        </div>
        <div className="flex items-center gap-3 text-xs text-muted-foreground mt-0.5">
          <span className="tabular-nums">{formatBytes(file.fileSize)}</span>
          <span className="hidden sm:inline">{formatDate(file.createdAt)}</span>
        </div>
      </div>

      <div className="flex items-center gap-1 opacity-0 transition-opacity duration-200 group-hover:opacity-100">
        {file.status === "complete" && (
          <Button
            variant="ghost"
            size="icon"
            aria-label="Download"
            className="h-8 w-8 transition-all duration-200"
            onClick={() => onDownload(file)}
          >
            <Download className="h-3.5 w-3.5" />
          </Button>
        )}
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              aria-label="More actions"
              className="h-8 w-8 transition-all duration-200"
            >
              <MoreHorizontal className="h-3.5 w-3.5" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-40">
            {file.status === "complete" && (
              <DropdownMenuItem onClick={() => onDownload(file)}>
                <Download className="h-3.5 w-3.5 mr-2" />
                Download
              </DropdownMenuItem>
            )}
            <DropdownMenuItem
              onClick={() => onDelete(file)}
              className="text-destructive focus:text-destructive"
            >
              <Trash2 className="h-3.5 w-3.5 mr-2" />
              Delete
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </motion.div>
  );
}

interface FolderRowProps {
  folder: Folder;
  onOpen: (folderId: string) => void;
}

export function FolderRow({ folder, onOpen }: FolderRowProps) {
  return (
    <motion.div
      layout
      initial={{ opacity: 0, y: 4 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -4 }}
      transition={{ duration: 0.2 }}
      onClick={() => onOpen(folder.folderId)}
      className="group flex items-center gap-4 rounded-xl border border-transparent px-4 py-3 transition-all duration-200 hover:border-white/[0.06] hover:bg-white/[0.03] cursor-pointer"
    >
      <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-primary/10 transition-colors duration-200 group-hover:bg-primary/15">
        <FolderIcon className="h-5 w-5 text-primary" />
      </div>

      <div className="flex-1 min-w-0">
        <span className="block truncate text-sm font-medium">{folder.name}</span>
        <div className="text-xs text-muted-foreground mt-0.5">
          {formatDate(folder.createdAt)}
        </div>
      </div>

      <ChevronRight className="h-4 w-4 text-muted-foreground opacity-0 transition-all duration-200 group-hover:opacity-100 group-hover:translate-x-0.5" />
    </motion.div>
  );
}
