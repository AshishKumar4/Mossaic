import {
  File,
  FileImage,
  FileVideo,
  FileAudio,
  FileText,
  FileArchive,
  FileCode,
} from "lucide-react";
import { cn } from "@/lib/utils";

const iconMap: Record<string, typeof File> = {
  image: FileImage,
  video: FileVideo,
  audio: FileAudio,
  text: FileText,
  pdf: FileText,
  archive: FileArchive,
  code: FileCode,
  file: File,
};

/* VSCode Dark-style file type colors */
const colorMap: Record<string, string> = {
  image: "text-[#4fc1ff]",
  video: "text-[#dcdcaa]",
  audio: "text-[#ce9178]",
  text: "text-[#9cdcfe]",
  pdf: "text-[#f44747]",
  archive: "text-[#dcdcaa]",
  code: "text-[#4ec9b0]",
  file: "text-muted-foreground",
};

export function FileIcon({
  mimeType,
  className,
}: {
  mimeType: string;
  className?: string;
}) {
  const kind = getKind(mimeType);
  const Icon = iconMap[kind] || File;
  const color = colorMap[kind] || "text-muted-foreground";

  return <Icon className={cn("h-5 w-5", color, className)} />;
}

function getKind(mime: string): string {
  if (mime.startsWith("image/")) return "image";
  if (mime.startsWith("video/")) return "video";
  if (mime.startsWith("audio/")) return "audio";
  if (mime.includes("pdf")) return "pdf";
  if (
    mime.includes("zip") ||
    mime.includes("archive") ||
    mime.includes("compressed") ||
    mime.includes("tar") ||
    mime.includes("gzip")
  )
    return "archive";
  if (
    mime.includes("javascript") ||
    mime.includes("typescript") ||
    mime.includes("json") ||
    mime.includes("xml") ||
    mime.includes("html") ||
    mime.includes("css")
  )
    return "code";
  if (mime.startsWith("text/")) return "text";
  return "file";
}
