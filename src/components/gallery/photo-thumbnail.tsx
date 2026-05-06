import { useState, memo } from "react";
import { motion } from "framer-motion";
import { Check } from "lucide-react";
import { cn } from "@/lib/utils";
import { useImageLoader } from "@/hooks/use-image-loader";

interface PhotoThumbnailProps {
  fileId: string;
  fileName: string;
  onClick: () => void;
  onSelect?: () => void;
  selected?: boolean;
  selectable?: boolean;
  height: number;
  width: number;
}

export const PhotoThumbnail = memo(function PhotoThumbnail({
  fileId,
  fileName,
  onClick,
  onSelect,
  selected,
  selectable,
  height,
  width,
}: PhotoThumbnailProps) {
  const { src, loaded, error } = useImageLoader(fileId, "thumbnail");
  const [hovered, setHovered] = useState(false);

  return (
    <motion.div
      className={cn(
        "relative cursor-pointer overflow-hidden bg-surface",
        selected && "ring-2 ring-primary ring-offset-2 ring-offset-background"
      )}
      style={{ width, height }}
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: 0.3 }}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      onClick={(e) => {
        if (selectable && (e.ctrlKey || e.metaKey || e.shiftKey)) {
          onSelect?.();
        } else {
          onClick();
        }
      }}
    >
      {/* Blur-up placeholder */}
      <div
        className={cn(
          "absolute inset-0 bg-gradient-to-br from-muted to-secondary transition-opacity duration-500",
          loaded ? "opacity-0" : "opacity-100"
        )}
      >
        <div className="absolute inset-0 animate-pulse bg-gradient-to-r from-transparent via-white/5 to-transparent" />
      </div>

      {/* Actual image */}
      {src && (
        <img
          src={src}
          alt={fileName}
          className={cn(
            "h-full w-full object-cover transition-all duration-500",
            loaded ? "opacity-100 scale-100" : "opacity-0 scale-105",
            hovered && !selectable && "scale-[1.03]"
          )}
          draggable={false}
        />
      )}

      {/* Error state */}
      {error && (
        <div className="absolute inset-0 flex items-center justify-center bg-surface text-muted-foreground">
          <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
            <rect x="3" y="3" width="18" height="18" rx="2" />
            <circle cx="8.5" cy="8.5" r="1.5" />
            <path d="M21 15l-5-5L5 21" />
          </svg>
        </div>
      )}

      {/* Hover overlay */}
      {(hovered || selected) && (
        <motion.div
          className="absolute inset-0 bg-black/20"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.15 }}
        />
      )}

      {/* Selection checkbox */}
      {selectable && (hovered || selected) && (
        <motion.button
          className={cn(
            "absolute top-2 left-2 flex h-6 w-6 items-center justify-center rounded-full border-2 transition-colors",
            selected
              ? "border-primary bg-primary text-primary-foreground"
              : "border-white/80 bg-black/30 text-transparent hover:border-white"
          )}
          initial={{ scale: 0.8, opacity: 0 }}
          animate={{ scale: 1, opacity: 1 }}
          transition={{ duration: 0.15 }}
          onClick={(e) => {
            e.stopPropagation();
            onSelect?.();
          }}
        >
          <Check className="h-3.5 w-3.5" strokeWidth={3} />
        </motion.button>
      )}
    </motion.div>
  );
});
