import { useState, useCallback, useRef } from "react";
import { api, ApiError } from "@/lib/api";
import type { SearchResult, ProviderStatus, SearchProviderConfig } from "@shared/embedding-types";

interface SearchState {
  results: SearchResult[];
  loading: boolean;
  error: string | null;
  query: string;
}

interface SpaceInfo {
  space: string;
  count: number;
  dimensions: number | null;
}

interface ProvidersState {
  providers: ProviderStatus[];
  active: SearchProviderConfig | null;
  indexedCount: number;
  spaces: SpaceInfo[];
  loading: boolean;
  error: string | null;
}

export function useSearch() {
  const [state, setState] = useState<SearchState>({
    results: [],
    loading: false,
    error: null,
    query: "",
  });

  const abortRef = useRef<AbortController | null>(null);

  const search = useCallback(async (query: string, topK?: number) => {
    if (!query.trim()) {
      setState((s) => ({ ...s, results: [], query: "", error: null }));
      return;
    }

    // Cancel any in-flight request
    abortRef.current?.abort();
    abortRef.current = new AbortController();

    setState((s) => ({ ...s, loading: true, error: null, query }));

    try {
      const data = await api.semanticSearch(query, topK);
      setState({
        results: data.results,
        loading: false,
        error: null,
        query,
      });
    } catch (err) {
      if (err instanceof DOMException && err.name === "AbortError") return;
      const message =
        err instanceof ApiError ? err.message : "Search failed";
      setState((s) => ({ ...s, loading: false, error: message }));
    }
  }, []);

  const clear = useCallback(() => {
    abortRef.current?.abort();
    setState({
      results: [],
      loading: false,
      error: null,
      query: "",
    });
  }, []);

  return { ...state, search, clear };
}

export function useSearchProviders() {
  const [state, setState] = useState<ProvidersState>({
    providers: [],
    active: null,
    indexedCount: 0,
    spaces: [],
    loading: true,
    error: null,
  });

  const [reindexing, setReindexing] = useState(false);

  const refresh = useCallback(async () => {
    setState((s) => ({ ...s, loading: true, error: null }));
    try {
      const data = await api.getSearchProviders();
      setState({
        providers: data.providers,
        active: data.active,
        indexedCount: data.indexedCount,
        spaces: data.spaces,
        loading: false,
        error: null,
      });
    } catch (err) {
      const message =
        err instanceof ApiError ? err.message : "Failed to load providers";
      setState((s) => ({ ...s, loading: false, error: message }));
    }
  }, []);

  const setConfig = useCallback(
    async (config: Partial<SearchProviderConfig>) => {
      try {
        await api.setSearchConfig(config);
        await refresh();
      } catch (err) {
        const message =
          err instanceof ApiError ? err.message : "Failed to update config";
        setState((s) => ({ ...s, error: message }));
      }
    },
    [refresh]
  );

  const reindex = useCallback(async () => {
    setReindexing(true);
    try {
      const result = await api.reindexSearch();
      await refresh();
      return result;
    } catch (err) {
      const message =
        err instanceof ApiError ? err.message : "Reindex failed";
      setState((s) => ({ ...s, error: message }));
      return null;
    } finally {
      setReindexing(false);
    }
  }, [refresh]);

  return { ...state, refresh, setConfig, reindex, reindexing };
}
