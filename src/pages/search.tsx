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
} from "lucide-react";
import { useSearch } from "@/hooks/use-search";
import { useDownload } from "@/hooks/use-download";
import { cn, formatBytes } from "@/lib/utils";
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
import type { SearchResult } from "@shared/embedding-types";

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
  const { results, loading, error, query, activeProvider, search, clear } =
    useSearch();
  const { startDownload } = useDownload();
  const [inputValue, setInputValue] = useState("");
  const [showConfig, setShowConfig] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>();

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
      startDownload(result.fileId, result.fileName);
    },
    [startDownload]
  );

  return (
    <div className="flex h-full flex-col">
      {/* Header */}
      <div className="flex items-center justify-between border-b border-border px-6 py-4">
        <div>
          <h1 className="text-lg font-semibold tracking-tight text-heading">
            Search
          </h1>
          <p className="text-sm text-muted-foreground">
            Find files by meaning, not just filename
          </p>
        </div>
        <div className="flex items-center gap-2">
          {activeProvider && (
            <Tooltip>
              <TooltipTrigger asChild>
                <Badge
                  variant="secondary"
                  className="text-[10px] cursor-default"
                >
                  <Sparkles className="mr-1 h-2.5 w-2.5" />
                  {activeProvider}
                </Badge>
              </TooltipTrigger>
              <TooltipContent>Active embedding provider</TooltipContent>
            </Tooltip>
          )}
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
            placeholder="Search by meaning... (e.g. 'vacation photos', 'project documents', 'music files')"
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
                  Searching...
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
                  Semantic Search
                </p>
                <p className="mt-1 text-xs text-muted-foreground max-w-sm text-center">
                  Search your files by meaning. Try describing what you're
                  looking for instead of exact filenames.
                </p>
              </div>
            ) : (
              <motion.div
                variants={container}
                initial="hidden"
                animate="show"
                className="space-y-2"
              >
                <p className="mb-3 text-xs text-muted-foreground">
                  {results.length} result{results.length !== 1 ? "s" : ""} for "
                  {query}"
                </p>
                {results.map((result) => (
                  <motion.div key={result.fileId} variants={item}>
                    <SearchResultRow
                      result={result}
                      onDownload={handleDownload}
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
}: {
  result: SearchResult;
  onDownload: (result: SearchResult) => void;
}) {
  const Icon = getMimeIcon(result.mimeType);
  const iconColor = getMimeColor(result.mimeType);
  const scorePct = Math.round(result.score * 100);

  return (
    <div className="group flex items-center gap-3 rounded-lg bg-white/[0.03] px-4 py-3 transition-colors hover:bg-white/[0.06]">
      {/* Type icon */}
      <div
        className={cn(
          "flex h-9 w-9 shrink-0 items-center justify-center rounded-lg",
          iconColor
        )}
      >
        <Icon className="h-4 w-4" />
      </div>

      {/* File info */}
      <div className="min-w-0 flex-1">
        <p className="truncate text-sm font-medium">{result.fileName}</p>
        <div className="flex items-center gap-2 mt-0.5">
          <span className="text-xs text-muted-foreground">
            {result.mimeType}
          </span>
          {result.highlight && (
            <span className="truncate text-xs text-muted-foreground">
              &middot; {result.highlight}
            </span>
          )}
        </div>
      </div>

      {/* Score */}
      <Tooltip>
        <TooltipTrigger asChild>
          <div className="shrink-0">
            <Badge
              variant={scorePct >= 70 ? "success" : scorePct >= 40 ? "default" : "secondary"}
              className="text-[10px] tabular-nums"
            >
              {scorePct}%
            </Badge>
          </div>
        </TooltipTrigger>
        <TooltipContent>
          Similarity score: {result.score.toFixed(4)}
        </TooltipContent>
      </Tooltip>

      {/* Download */}
      <Button
        variant="ghost"
        size="icon"
        className="h-8 w-8 shrink-0 opacity-0 group-hover:opacity-100 transition-opacity"
        onClick={() => onDownload(result)}
      >
        <Download className="h-3.5 w-3.5" />
      </Button>
    </div>
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
