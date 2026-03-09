import { useState, useCallback, useRef, type DragEvent } from "react";

interface UseDropZoneOptions {
  onDrop: (files: File[]) => void;
  disabled?: boolean;
}

interface UseDropZoneReturn {
  isDragOver: boolean;
  dropZoneProps: {
    onDragEnter: (e: DragEvent) => void;
    onDragLeave: (e: DragEvent) => void;
    onDragOver: (e: DragEvent) => void;
    onDrop: (e: DragEvent) => void;
  };
}

/**
 * Hook for full-page drag-and-drop file upload.
 *
 * Uses a drag counter to avoid flicker when dragging over child elements:
 * - dragenter on a child increments the counter
 * - dragleave on a child decrements it
 * - isDragOver is true only when counter > 0
 */
export function useDropZone({
  onDrop,
  disabled = false,
}: UseDropZoneOptions): UseDropZoneReturn {
  const [isDragOver, setIsDragOver] = useState(false);
  const dragCounter = useRef(0);

  const handleDragEnter = useCallback(
    (e: DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      if (disabled) return;
      dragCounter.current++;
      if (dragCounter.current === 1) {
        setIsDragOver(true);
      }
    },
    [disabled]
  );

  const handleDragLeave = useCallback(
    (e: DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      if (disabled) return;
      dragCounter.current--;
      if (dragCounter.current === 0) {
        setIsDragOver(false);
      }
    },
    [disabled]
  );

  const handleDragOver = useCallback(
    (e: DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
    },
    []
  );

  const handleDrop = useCallback(
    (e: DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      dragCounter.current = 0;
      setIsDragOver(false);
      if (disabled) return;

      const files = Array.from(e.dataTransfer.files);
      if (files.length > 0) {
        onDrop(files);
      }
    },
    [onDrop, disabled]
  );

  return {
    isDragOver,
    dropZoneProps: {
      onDragEnter: handleDragEnter,
      onDragLeave: handleDragLeave,
      onDragOver: handleDragOver,
      onDrop: handleDrop,
    },
  };
}
