import { useState, useEffect, useRef } from "react";
import { motion } from "framer-motion";
import {
  X,
  Download,
  FileText,
  FileCode,
  FileAudio,
  FileVideo,
  File,
} from "lucide-react";
import { cn, formatBytes, formatDate } from "@/lib/utils";
import { useImageLoader } from "@/hooks/use-image-loader";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import type { UserFile } from "@shared/types";

interface FilePreviewPanelProps {
  file: UserFile;
  onClose: () => void;
  onDownload: (file: UserFile) => void;
}

function ImagePreview({ fileId, fileName }: { fileId: string; fileName: string }) {
  const { src, loaded, error } = useImageLoader(fileId, "full");

  if (error) {
    return (
      <div className="flex h-full items-center justify-center text-muted-foreground">
        <p className="text-sm">Failed to load image</p>
      </div>
    );
  }

  return (
    <div className="relative flex h-full items-center justify-center p-4">
      {!loaded && (
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-muted-foreground border-t-primary" />
      )}
      {src && (
        <img
          src={src}
          alt={fileName}
          className={cn(
            "max-h-full max-w-full rounded-lg object-contain transition-opacity duration-300",
            loaded ? "opacity-100" : "opacity-0"
          )}
        />
      )}
    </div>
  );
}

function TextPreview({ fileId }: { fileId: string }) {
  const [content, setContent] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const url = api.getImageUrl(fileId);
        const res = await fetch(url, { headers: api.getImageHeaders() });
        if (!res.ok) throw new Error("Failed");
        if (cancelled) return;
        const text = await res.text();
        if (cancelled) return;
        // Limit display to 50KB
        setContent(text.slice(0, 50000));
        setLoading(false);
      } catch {
        if (!cancelled) {
          setError(true);
          setLoading(false);
        }
      }
    };
    load();
    return () => { cancelled = true; };
  }, [fileId]);

  if (loading) {
    return (
      <div className="flex h-full items-center justify-center">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-muted-foreground border-t-primary" />
      </div>
    );
  }

  if (error || content === null) {
    return (
      <div className="flex h-full items-center justify-center text-muted-foreground">
        <p className="text-sm">Cannot preview this file</p>
      </div>
    );
  }

  return (
    <pre className="h-full overflow-auto p-4 text-xs text-foreground font-mono leading-relaxed whitespace-pre-wrap break-words bg-[#1a1a1a] rounded-lg m-2">
      {content}
    </pre>
  );
}

function MediaPreview({ fileId, mimeType }: { fileId: string; mimeType: string }) {
  const [src, setSrc] = useState<string | null>(null);
  const urlRef = useRef<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const url = api.getImageUrl(fileId);
        const res = await fetch(url, { headers: api.getImageHeaders() });
        if (!res.ok) throw new Error("Failed");
        if (cancelled) return;
        const blob = await res.blob();
        if (cancelled) return;
        const objectUrl = URL.createObjectURL(blob);
        urlRef.current = objectUrl;
        setSrc(objectUrl);
      } catch {
        // silent fail
      }
    };
    load();
    return () => {
      cancelled = true;
      if (urlRef.current) URL.revokeObjectURL(urlRef.current);
    };
  }, [fileId]);

  if (!src) {
    return (
      <div className="flex h-full items-center justify-center">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-muted-foreground border-t-primary" />
      </div>
    );
  }

  if (mimeType.startsWith("video/")) {
    return (
      <div className="flex h-full items-center justify-center p-4">
        <video
          src={src}
          controls
          className="max-h-full max-w-full rounded-lg"
        />
      </div>
    );
  }

  if (mimeType.startsWith("audio/")) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-4 p-4">
        <FileAudio className="h-16 w-16 text-muted-foreground/50" />
        <audio src={src} controls className="w-full max-w-sm" />
      </div>
    );
  }

  if (mimeType === "application/pdf") {
    return (
      <iframe
        src={src}
        className="h-full w-full rounded-lg"
        title="PDF preview"
      />
    );
  }

  return null;
}

function getPreviewType(mimeType: string): "image" | "text" | "media" | "none" {
  if (mimeType.startsWith("image/")) return "image";
  if (mimeType.startsWith("video/") || mimeType.startsWith("audio/") || mimeType === "application/pdf") return "media";
  if (
    mimeType.startsWith("text/") ||
    mimeType.includes("json") ||
    mimeType.includes("xml") ||
    mimeType.includes("javascript") ||
    mimeType.includes("typescript") ||
    mimeType.includes("css") ||
    mimeType.includes("html") ||
    mimeType.includes("yaml") ||
    mimeType.includes("markdown")
  ) return "text";
  return "none";
}

function getPreviewIcon(mimeType: string) {
  if (mimeType.startsWith("video/")) return FileVideo;
  if (mimeType.startsWith("audio/")) return FileAudio;
  if (mimeType.startsWith("text/") || mimeType.includes("json")) return FileText;
  if (mimeType.includes("javascript") || mimeType.includes("css") || mimeType.includes("html"))
    return FileCode;
  return File;
}

export function FilePreviewPanel({ file, onClose, onDownload }: FilePreviewPanelProps) {
  const previewType = getPreviewType(file.mimeType);

  return (
    <motion.div
      className="flex h-full w-80 flex-col border-l border-white/[0.06] bg-sidebar lg:w-96"
      initial={{ width: 0, opacity: 0 }}
      animate={{ width: undefined, opacity: 1 }}
      exit={{ width: 0, opacity: 0 }}
      transition={{ duration: 0.2, ease: "easeInOut" }}
    >
      {/* Header */}
      <div className="flex items-center justify-between border-b border-white/[0.06] px-4 py-3">
        <h3 className="text-sm font-semibold text-foreground truncate pr-2">
          {file.fileName}
        </h3>
        <Button variant="ghost" size="icon" className="h-7 w-7 shrink-0" onClick={onClose}>
          <X className="h-4 w-4" />
        </Button>
      </div>

      {/* Preview area */}
      <div className="flex-1 overflow-hidden">
        {previewType === "image" && (
          <ImagePreview fileId={file.fileId} fileName={file.fileName} />
        )}
        {previewType === "text" && <TextPreview fileId={file.fileId} />}
        {previewType === "media" && (
          <MediaPreview fileId={file.fileId} mimeType={file.mimeType} />
        )}
        {previewType === "none" && (
          <div className="flex h-full flex-col items-center justify-center gap-3 text-muted-foreground">
            <File className="h-12 w-12 opacity-30" />
            <p className="text-sm">No preview available</p>
            <Button
              variant="outline"
              size="sm"
              className="rounded-lg"
              onClick={() => onDownload(file)}
            >
              <Download className="h-3.5 w-3.5 mr-1.5" />
              Download
            </Button>
          </div>
        )}
      </div>

      {/* File details */}
      <div className="border-t border-white/[0.06] p-4 space-y-3">
        <DetailRow label="Size" value={formatBytes(file.fileSize)} />
        <DetailRow label="Type" value={file.mimeType} />
        <DetailRow label="Uploaded" value={formatDate(file.createdAt)} />
        <DetailRow label="Status" value={file.status} />
        <div className="pt-2">
          <Button
            variant="outline"
            size="sm"
            className="w-full rounded-lg"
            onClick={() => onDownload(file)}
          >
            <Download className="h-3.5 w-3.5 mr-1.5" />
            Download
          </Button>
        </div>
      </div>
    </motion.div>
  );
}

function DetailRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-start justify-between gap-4">
      <span className="text-xs text-muted-foreground shrink-0">{label}</span>
      <span className="text-xs text-foreground text-right break-all">{value}</span>
    </div>
  );
}
