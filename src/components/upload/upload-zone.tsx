import { useState, useCallback, useRef, type DragEvent } from "react";
import { motion } from "framer-motion";
import { Upload } from "lucide-react";
import { cn } from "@/lib/utils";

interface UploadZoneProps {
  onFilesSelected: (files: File[]) => void;
  className?: string;
}

export function UploadZone({ onFilesSelected, className }: UploadZoneProps) {
  const [dragActive, setDragActive] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const handleDrag = useCallback((e: DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  }, []);

  const handleDrop = useCallback(
    (e: DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      setDragActive(false);
      const files = Array.from(e.dataTransfer.files);
      if (files.length) onFilesSelected(files);
    },
    [onFilesSelected]
  );

  const handleClick = () => inputRef.current?.click();

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []);
    if (files.length) onFilesSelected(files);
    e.target.value = "";
  };

  return (
    <div
      onDragEnter={handleDrag}
      onDragLeave={handleDrag}
      onDragOver={handleDrag}
      onDrop={handleDrop}
      onClick={handleClick}
      className={cn(
        "relative flex flex-col items-center justify-center gap-3 rounded-xl border-2 border-dashed p-8 transition-all duration-200 cursor-pointer",
        dragActive
          ? "border-primary/50 bg-primary/5 scale-[1.005]"
          : "border-white/[0.06] hover:border-muted-foreground/40 hover:bg-white/[0.03]",
        className
      )}
    >
      <input
        ref={inputRef}
        type="file"
        multiple
        className="hidden"
        onChange={handleChange}
      />

      <motion.div
        animate={{ y: dragActive ? -4 : 0 }}
        transition={{ type: "spring", stiffness: 300, damping: 20 }}
        className={cn(
          "flex h-12 w-12 items-center justify-center rounded-xl transition-all duration-200",
          dragActive
            ? "bg-primary/15 text-primary"
            : "bg-white/[0.05] text-muted-foreground"
        )}
      >
        <Upload className="h-6 w-6" />
      </motion.div>

      <div className="text-center">
        <p className="text-sm font-medium">
          {dragActive ? "Drop files here" : "Drop files or click to upload"}
        </p>
        <p className="mt-1 text-xs text-muted-foreground">
          No file size limit
        </p>
      </div>
    </div>
  );
}
