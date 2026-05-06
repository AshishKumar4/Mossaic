import { useState, useCallback, useEffect } from "react";
import { api, ApiError } from "@/lib/api";
import type { AnalyticsOverview } from "@app/types";

interface AnalyticsState {
  data: AnalyticsOverview | null;
  loading: boolean;
  error: string | null;
}

export function useAnalytics() {
  const [state, setState] = useState<AnalyticsState>({
    data: null,
    loading: true,
    error: null,
  });

  const refresh = useCallback(async () => {
    setState((s) => ({ ...s, loading: true, error: null }));
    try {
      const data = await api.getAnalytics();
      setState({ data, loading: false, error: null });
    } catch (err) {
      const message =
        err instanceof ApiError ? err.message : "Failed to load analytics";
      setState((s) => ({ ...s, loading: false, error: message }));
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  return { ...state, refresh };
}
