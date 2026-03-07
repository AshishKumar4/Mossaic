import { CloudArrowUp } from "@phosphor-icons/react";
import { Empty } from "@cloudflare/kumo";

export function EmptyState() {
  return (
    <Empty
      icon={<CloudArrowUp size={48} className="text-kumo-inactive" />}
      title="No files yet"
      description="Upload files by dragging them here or using the upload button. Files are split into chunks and distributed across storage shards for maximum throughput."
    />
  );
}
