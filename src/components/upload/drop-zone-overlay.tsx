import { motion } from "framer-motion";
import { Upload } from "lucide-react";

interface DropZoneOverlayProps {
  visible: boolean;
}

/**
 * Full-page drag-and-drop overlay. Renders a translucent backdrop with a
 * dashed border and upload icon. Animated in/out with framer-motion.
 *
 * Mount this inside a positioned container (relative) that has the
 * useDropZone event handlers attached. The overlay itself has
 * pointer-events: none so it doesn't interfere with drag events.
 */
export function DropZoneOverlay({ visible }: DropZoneOverlayProps) {
  if (!visible) return null;

  return (
    <motion.div
      className="absolute inset-0 z-50 flex items-center justify-center pointer-events-none"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.15, ease: "easeOut" }}
    >
      {/* Backdrop */}
      <div className="absolute inset-0 bg-background/80 backdrop-blur-sm" />

      {/* Border + content */}
      <div className="relative mx-4 my-4 flex h-[calc(100%-2rem)] w-[calc(100%-2rem)] flex-col items-center justify-center rounded-2xl border-2 border-dashed border-primary/40">
        <motion.div
          initial={{ scale: 0.9, y: 8 }}
          animate={{ scale: 1, y: 0 }}
          transition={{ type: "spring", stiffness: 300, damping: 25 }}
          className="flex flex-col items-center gap-3"
        >
          <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-primary/15">
            <Upload className="h-7 w-7 text-primary" />
          </div>
          <div className="text-center">
            <p className="text-sm font-semibold text-foreground">
              Drop files to upload
            </p>
            <p className="mt-1 text-xs text-muted-foreground">
              Release to start uploading
            </p>
          </div>
        </motion.div>
      </div>
    </motion.div>
  );
}
