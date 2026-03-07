import { House } from "@phosphor-icons/react";
import { Breadcrumbs } from "@cloudflare/kumo";
import type { Folder } from "@shared/types";

interface FolderBreadcrumbsProps {
  path: Folder[];
}

export function FolderBreadcrumbs({ path }: FolderBreadcrumbsProps) {
  if (path.length === 0) {
    return (
      <Breadcrumbs>
        <Breadcrumbs.Current icon={<House size={16} />}>
          Files
        </Breadcrumbs.Current>
      </Breadcrumbs>
    );
  }

  return (
    <Breadcrumbs>
      <Breadcrumbs.Link href="/files" icon={<House size={16} />}>
        Files
      </Breadcrumbs.Link>
      {path.map((folder, index) => (
        <span key={folder.folderId} className="contents">
          <Breadcrumbs.Separator />
          {index === path.length - 1 ? (
            <Breadcrumbs.Current>{folder.name}</Breadcrumbs.Current>
          ) : (
            <Breadcrumbs.Link href={`/files/${folder.folderId}`}>
              {folder.name}
            </Breadcrumbs.Link>
          )}
        </span>
      ))}
    </Breadcrumbs>
  );
}
