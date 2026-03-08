import { ChevronRight, Home } from "lucide-react";
import type { Folder } from "@shared/types";

interface BreadcrumbsProps {
  path: Folder[];
  onNavigate: (folderId: string | null) => void;
}

export function Breadcrumbs({ path, onNavigate }: BreadcrumbsProps) {
  return (
    <nav className="flex items-center gap-0.5 text-sm">
      <button
        onClick={() => onNavigate(null)}
        className="flex items-center gap-1.5 rounded-lg px-2 py-1 text-muted-foreground transition-all duration-200 hover:bg-white/[0.05] hover:text-foreground cursor-pointer"
      >
        <Home className="h-3.5 w-3.5" />
        <span>Files</span>
      </button>
      {path.map((folder) => (
        <span key={folder.folderId} className="flex items-center gap-0.5">
          <ChevronRight className="h-3.5 w-3.5 text-muted-foreground/50" />
          <button
            onClick={() => onNavigate(folder.folderId)}
            className="rounded-lg px-2 py-1 text-muted-foreground transition-all duration-200 hover:bg-white/[0.05] hover:text-foreground cursor-pointer"
          >
            {folder.name}
          </button>
        </span>
      ))}
    </nav>
  );
}
