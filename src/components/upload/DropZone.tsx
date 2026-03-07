import { useState, useCallback, type DragEvent, type ReactNode } from "react";
import { CloudArrowUp } from "@phosphor-icons/react";
import { Text } from "@cloudflare/kumo";

interface DropZoneProps {
  onDrop: (files: FileList) => void;
  children: ReactNode;
}

export function DropZone({ onDrop, children }: DropZoneProps) {
  const [isDragOver, setIsDragOver] = useState(false);

  const handleDragOver = useCallback((e: DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragOver(true);
  }, []);

  const handleDragLeave = useCallback((e: DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragOver(false);
  }, []);

  const handleDrop = useCallback(
    (e: DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      setIsDragOver(false);

      if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
        onDrop(e.dataTransfer.files);
      }
    },
    [onDrop]
  );

  return (
    <div
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
      className="relative min-h-0 flex-1"
    >
      {children}

      {isDragOver && (
        <div className="absolute inset-0 z-40 flex items-center justify-center rounded-xl border-2 border-dashed border-kumo-brand bg-kumo-brand/5 backdrop-blur-sm">
          <div className="flex flex-col items-center gap-3">
            <div className="rounded-2xl bg-kumo-brand/10 p-4">
              <CloudArrowUp
                size={40}
                weight="duotone"
                className="text-kumo-brand"
              />
            </div>
            <Text size="lg" bold>
              Drop files to upload
            </Text>
            <Text variant="secondary" size="sm">
              Files will be chunked and distributed across shards
            </Text>
          </div>
        </div>
      )}
    </div>
  );
}
