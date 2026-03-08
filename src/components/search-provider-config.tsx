import { useEffect } from "react";
import {
  RefreshCw,
  Settings,
  Database,
  Cpu,
  CheckCircle2,
  XCircle,
  Loader2,
} from "lucide-react";
import { useSearchProviders } from "@/hooks/use-search";
import { cn } from "@/lib/utils";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";

export function SearchProviderConfig() {
  const {
    providers,
    active,
    indexedCount,
    loading,
    error,
    refresh,
    setConfig,
    reindex,
    reindexing,
  } = useSearchProviders();

  useEffect(() => {
    refresh();
  }, [refresh]);

  const embeddingProviders = providers.filter((p) => p.type === "embedding");
  const vectorStores = providers.filter((p) => p.type === "vectorStore");

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2 text-sm font-medium">
            <Settings className="h-4 w-4 text-muted-foreground" />
            Search Configuration
          </CardTitle>
          <Badge variant="secondary" className="text-[10px]">
            {indexedCount} indexed
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {loading ? (
          <div className="flex items-center justify-center py-4">
            <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
          </div>
        ) : error ? (
          <div className="text-center text-sm text-destructive">{error}</div>
        ) : (
          <>
            {/* Embedding Providers */}
            <div>
              <p className="mb-2 text-xs font-medium text-muted-foreground uppercase tracking-wider">
                Embedding Provider
              </p>
              <div className="space-y-1.5">
                {embeddingProviders.map((provider) => (
                  <ProviderRow
                    key={provider.name}
                    name={provider.name}
                    available={provider.available}
                    active={active?.embedding === provider.name}
                    dimensions={provider.dimensions}
                    onClick={() => setConfig({ embedding: provider.name })}
                  />
                ))}
              </div>
            </div>

            <Separator />

            {/* Vector Stores */}
            <div>
              <p className="mb-2 text-xs font-medium text-muted-foreground uppercase tracking-wider">
                Vector Store
              </p>
              <div className="space-y-1.5">
                {vectorStores.map((store) => (
                  <ProviderRow
                    key={store.name}
                    name={store.name}
                    available={store.available}
                    active={active?.vectorStore === store.name}
                    onClick={() => setConfig({ vectorStore: store.name })}
                  />
                ))}
              </div>
            </div>

            <Separator />

            {/* Reindex */}
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium">Reindex All Files</p>
                <p className="text-xs text-muted-foreground">
                  Re-embed all files with the active provider
                </p>
              </div>
              <Button
                variant="outline"
                size="sm"
                onClick={reindex}
                disabled={reindexing}
                className="rounded-lg"
              >
                {reindexing ? (
                  <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />
                ) : (
                  <RefreshCw className="h-3.5 w-3.5 mr-1.5" />
                )}
                {reindexing ? "Reindexing..." : "Reindex"}
              </Button>
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}

function ProviderRow({
  name,
  available,
  active,
  dimensions,
  onClick,
}: {
  name: string;
  available: boolean;
  active: boolean;
  dimensions?: number;
  onClick: () => void;
}) {
  const displayName = formatProviderName(name);

  return (
    <button
      onClick={onClick}
      disabled={!available}
      className={cn(
        "flex w-full items-center justify-between rounded-lg px-3 py-2 text-left transition-colors",
        active
          ? "bg-primary/10 border border-primary/20"
          : available
            ? "bg-white/[0.03] hover:bg-white/[0.06] border border-transparent"
            : "bg-white/[0.02] border border-transparent opacity-50 cursor-not-allowed"
      )}
    >
      <div className="flex items-center gap-2.5">
        {available ? (
          <CheckCircle2
            className={cn(
              "h-3.5 w-3.5",
              active ? "text-primary" : "text-success"
            )}
          />
        ) : (
          <XCircle className="h-3.5 w-3.5 text-muted-foreground" />
        )}
        <div>
          <span className="text-sm font-medium">{displayName}</span>
          {dimensions && (
            <span className="ml-1.5 text-[10px] text-muted-foreground">
              {dimensions}d
            </span>
          )}
        </div>
      </div>
      {active && (
        <Badge variant="default" className="text-[10px]">
          Active
        </Badge>
      )}
    </button>
  );
}

function formatProviderName(name: string): string {
  const names: Record<string, string> = {
    simple: "Simple (Built-in)",
    "cloudflare-ai": "Cloudflare AI",
    ollama: "Ollama",
    "durable-object": "Durable Object",
    vectorize: "Cloudflare Vectorize",
  };
  return names[name] || name;
}
