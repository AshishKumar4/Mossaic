import { useState, useCallback, useEffect, useRef } from "react";
import { useNavigate } from "react-router-dom";
import { motion, AnimatePresence } from "framer-motion";
import {
  Search as SearchIcon,
  FileText,
  Image,
  Film,
  Music,
  Archive,
  File,
  Download,
  X,
  Sparkles,
  Settings,
  Eye,
  Image as ImageLucide,
  File as FileLucide,
} from "lucide-react";
import { useSearch } from "@/hooks/use-search";
import { useDownload } from "@/hooks/use-download";
import { cn, formatBytes } from "@/lib/utils";
import { api } from "@/lib/api";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Spinner } from "@/components/ui/spinner";
import {
  Tooltip,
  TooltipTrigger,
  TooltipContent,
} from "@/components/ui/tooltip";
import { SearchProviderConfig } from "@/components/search-provider-config";
import type { SearchResult, SearchResultType } from "@shared/embedding-types";

const container = {
  hidden: { opacity: 0 },
  show: {
    opacity: 1,
    transition: { staggerChildren: 0.04 },
  },
};

const item = {
  hidden: { opacity: 0, y: 8 },
  show: { opacity: 1, y: 0, transition: { duration: 0.25, ease: "easeOut" as const } },
};

export function SearchPage() {
  const { results, loading, error, query, search, clear } = useSearch();
  const { downloadFile } = useDownload();
  const navigate = useNavigate();
  const [inputValue, setInputValue] = useState("");
  const [showConfig, setShowConfig] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  // Focus search input on mount
  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  // Debounced search
  const handleInputChange = useCallback(
    (value: string) => {
      setInputValue(value);
      if (debounceRef.current) clearTimeout(debounceRef.current);
      debounceRef.current = setTimeout(() => {
        search(value);
      }, 300);
    },
    [search]
  );

  const handleClear = useCallback(() => {
    setInputValue("");
    clear();
    inputRef.current?.focus();
  }, [clear]);

  const handleDownload = useCallback(
    (result: SearchResult) => {
      downloadFile(result.fileId, result.fileName);
    },
    [downloadFile]
  );

  const handleNavigate = useCallback(
    (result: SearchResult) => {
      // Navigate to gallery for images, files page otherwise
      if (result.resultType === "image") {
        navigate("/gallery");
      } else {
        navigate("/files");
      }
    },
    [navigate]
  );

  // Count results by type
  const imageCount = results.filter((r) => r.resultType === "image").length;
  const docCount = results.filter((r) => r.resultType !== "image").length;

  return (
    <div className="flex h-full flex-col">
      {/* Header */}
      <div className="flex items-center justify-between border-b border-border px-6 py-4">
        <div>
          <h1 className="text-lg font-semibold tracking-tight text-heading">
            Search
          </h1>
          <p className="text-sm text-muted-foreground">
            Find files by meaning — images, documents, and more
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="icon"
            className="h-8 w-8"
            onClick={() => setShowConfig(!showConfig)}
          >
            <Settings className="h-4 w-4" />
          </Button>
        </div>
      </div>

      {/* Search bar */}
      <div className="border-b border-border px-6 py-3">
        <div className="relative">
          <SearchIcon className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            ref={inputRef}
            value={inputValue}
            onChange={(e) => handleInputChange(e.target.value)}
            placeholder="Search by meaning... (e.g. 'sunset beach', 'project report', 'cat photos')"
            className="pl-9 pr-9"
          />
          {inputValue && (
            <button
              onClick={handleClear}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition-colors"
            >
              <X className="h-4 w-4" />
            </button>
          )}
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto">
        <div className="flex gap-6 p-6">
          {/* Results */}
          <div className="flex-1 min-w-0">
            {loading ? (
              <div className="flex flex-col items-center justify-center py-16">
                <Spinner className="h-5 w-5 text-primary" />
                <p className="mt-3 text-sm text-muted-foreground">
                  Searching across images and documents...
                </p>
              </div>
            ) : error ? (
              <div className="flex flex-col items-center justify-center py-16">
                <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-destructive/10">
                  <SearchIcon className="h-6 w-6 text-destructive" />
                </div>
                <p className="mt-4 text-sm font-medium text-foreground">
                  Search failed
                </p>
                <p className="mt-1 text-xs text-muted-foreground">{error}</p>
              </div>
            ) : query && results.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-16">
                <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-secondary/80">
                  <SearchIcon className="h-6 w-6 text-muted-foreground" />
                </div>
                <p className="mt-4 text-sm font-medium text-foreground">
                  No results found
                </p>
                <p className="mt-1 text-xs text-muted-foreground">
                  Try a different search term or reindex your files
                </p>
              </div>
            ) : !query ? (
              <div className="flex flex-col items-center justify-center py-16">
                <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-primary/10">
                  <Sparkles className="h-6 w-6 text-primary" />
                </div>
                <p className="mt-4 text-sm font-medium text-foreground">
                  Multi-Modal Search
                </p>
                <p className="mt-1 text-xs text-muted-foreground max-w-sm text-center">
                  Search images by visual content (CLIP) and documents by
                  filename and metadata. Try describing what you're looking for.
                </p>
                <div className="mt-4 flex gap-2">
                  <Badge variant="secondary" className="text-[10px]">
                    <ImageLucide className="mr-1 h-2.5 w-2.5" />
                    Image Search
                  </Badge>
                  <Badge variant="secondary" className="text-[10px]">
                    <FileLucide className="mr-1 h-2.5 w-2.5" />
                    Document Search
                  </Badge>
                </div>
              </div>
            ) : (
              <motion.div
                variants={container}
                initial="hidden"
                animate="show"
                className="space-y-2"
              >
                {/* Results summary */}
                <div className="mb-3 flex items-center gap-2">
                  <p className="text-xs text-muted-foreground">
                    {results.length} result{results.length !== 1 ? "s" : ""} for "
                    {query}"
                  </p>
                  {imageCount > 0 && (
                    <Badge variant="secondary" className="text-[10px]">
                      <ImageLucide className="mr-1 h-2.5 w-2.5" />
                      {imageCount} image{imageCount !== 1 ? "s" : ""}
                    </Badge>
                  )}
                  {docCount > 0 && (
                    <Badge variant="secondary" className="text-[10px]">
                      <FileLucide className="mr-1 h-2.5 w-2.5" />
                      {docCount} doc{docCount !== 1 ? "s" : ""}
                    </Badge>
                  )}
                </div>

                {results.map((result) => (
                  <motion.div key={`${result.fileId}-${result.space}`} variants={item}>
                    <SearchResultRow
                      result={result}
                      onDownload={handleDownload}
                      onNavigate={handleNavigate}
                    />
                  </motion.div>
                ))}
              </motion.div>
            )}
          </div>

          {/* Config panel (collapsible) */}
          <AnimatePresence>
            {showConfig && (
              <motion.div
                initial={{ opacity: 0, width: 0 }}
                animate={{ opacity: 1, width: 320 }}
                exit={{ opacity: 0, width: 0 }}
                transition={{ duration: 0.2 }}
                className="shrink-0 overflow-hidden"
              >
                <SearchProviderConfig />
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      </div>
    </div>
  );
}

function SearchResultRow({
  result,
  onDownload,
  onNavigate,
}: {
  result: SearchResult;
  onDownload: (result: SearchResult) => void;
  onNavigate: (result: SearchResult) => void;
}) {
  const Icon = getMimeIcon(result.mimeType);
  const iconColor = getMimeColor(result.mimeType);
  const scorePct = Math.round(result.score * 100);

  return (
    <div
      className="group flex items-center gap-3 rounded-lg bg-white/[0.03] px-4 py-3 transition-colors hover:bg-white/[0.06] cursor-pointer"
      onClick={() => onNavigate(result)}
    >
      {/* Thumbnail or type icon */}
      {result.hasThumbnail ? (
        <div className="relative h-10 w-10 shrink-0 overflow-hidden rounded-lg bg-secondary">
          <img
            src={api.getThumbnailUrl(result.fileId)}
            alt={result.fileName}
            className="h-full w-full object-cover"
            loading="lazy"
            crossOrigin="use-credentials"
          />
        </div>
      ) : (
        <div
          className={cn(
            "flex h-10 w-10 shrink-0 items-center justify-center rounded-lg",
            iconColor
          )}
        >
          <Icon className="h-4 w-4" />
        </div>
      )}

      {/* File info */}
      <div className="min-w-0 flex-1">
        <p className="truncate text-sm font-medium">{result.fileName}</p>
        <div className="flex items-center gap-2 mt-0.5">
          {/* Result type badge */}
          <ResultTypeBadge type={result.resultType} />
          {/* Space indicator */}
          <Badge
            variant="outline"
            className="text-[9px] px-1 py-0 h-4 border-border/50"
          >
            {result.space === "clip" ? "visual" : "text"}
          </Badge>
          {result.fileSize && (
            <span className="text-xs text-muted-foreground">
              {formatBytes(result.fileSize)}
            </span>
          )}
        </div>
      </div>

      {/* Similarity score bar */}
      <Tooltip>
        <TooltipTrigger asChild>
          <div className="shrink-0 flex items-center gap-1.5">
            <div className="w-16 h-1.5 rounded-full bg-secondary overflow-hidden">
              <div
                className={cn(
                  "h-full rounded-full transition-all",
                  scorePct >= 70
                    ? "bg-emerald-500"
                    : scorePct >= 40
                      ? "bg-amber-500"
                      : "bg-zinc-500"
                )}
                style={{ width: `${Math.max(scorePct, 5)}%` }}
              />
            </div>
            <span
              className={cn(
                "text-[10px] tabular-nums font-medium w-8 text-right",
                scorePct >= 70
                  ? "text-emerald-400"
                  : scorePct >= 40
                    ? "text-amber-400"
                    : "text-muted-foreground"
              )}
            >
              {scorePct}%
            </span>
          </div>
        </TooltipTrigger>
        <TooltipContent>
          Similarity: {result.score.toFixed(4)} ({result.space} space)
        </TooltipContent>
      </Tooltip>

      {/* Actions */}
      <div className="shrink-0 flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
        {result.hasThumbnail && (
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                size="icon"
                className="h-7 w-7"
                onClick={(e) => {
                  e.stopPropagation();
                  window.open(api.getImageUrl(result.fileId), "_blank");
                }}
              >
                <Eye className="h-3.5 w-3.5" />
              </Button>
            </TooltipTrigger>
            <TooltipContent>Preview</TooltipContent>
          </Tooltip>
        )}
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7"
              onClick={(e) => {
                e.stopPropagation();
                onDownload(result);
              }}
            >
              <Download className="h-3.5 w-3.5" />
            </Button>
          </TooltipTrigger>
          <TooltipContent>Download</TooltipContent>
        </Tooltip>
      </div>
    </div>
  );
}

function ResultTypeBadge({ type }: { type: SearchResultType }) {
  const config: Record<SearchResultType, { label: string; className: string }> = {
    image: { label: "Image", className: "bg-blue-500/10 text-blue-400 border-blue-500/20" },
    document: { label: "Document", className: "bg-green-500/10 text-green-400 border-green-500/20" },
    video: { label: "Video", className: "bg-purple-500/10 text-purple-400 border-purple-500/20" },
    audio: { label: "Audio", className: "bg-pink-500/10 text-pink-400 border-pink-500/20" },
    archive: { label: "Archive", className: "bg-amber-500/10 text-amber-400 border-amber-500/20" },
    other: { label: "File", className: "bg-zinc-500/10 text-zinc-400 border-zinc-500/20" },
  };

  const { label, className } = config[type] || config.other;

  return (
    <span className={cn("text-[9px] font-medium px-1.5 py-0 rounded border", className)}>
      {label}
    </span>
  );
}

function getMimeIcon(
  mimeType: string
): typeof FileText {
  if (mimeType.startsWith("image/")) return Image;
  if (mimeType.startsWith("video/")) return Film;
  if (mimeType.startsWith("audio/")) return Music;
  if (mimeType.includes("pdf") || mimeType.includes("text") || mimeType.includes("json"))
    return FileText;
  if (mimeType.includes("zip") || mimeType.includes("archive"))
    return Archive;
  return File;
}

function getMimeColor(mimeType: string): string {
  if (mimeType.startsWith("image/")) return "bg-blue-500/10 text-blue-400";
  if (mimeType.startsWith("video/")) return "bg-purple-500/10 text-purple-400";
  if (mimeType.startsWith("audio/")) return "bg-pink-500/10 text-pink-400";
  if (mimeType.includes("pdf")) return "bg-red-500/10 text-red-400";
  if (mimeType.includes("text") || mimeType.includes("json"))
    return "bg-green-500/10 text-green-400";
  if (mimeType.includes("zip") || mimeType.includes("archive"))
    return "bg-amber-500/10 text-amber-400";
  return "bg-secondary text-muted-foreground";
}
