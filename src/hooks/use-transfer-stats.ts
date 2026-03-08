import { useSyncExternalStore } from "react";
import {
  getTransferStats,
  subscribeTransferStats,
} from "@/lib/transfer-stats";
import type { CompletedTransferStats } from "@shared/types";

export function useTransferStats(): CompletedTransferStats[] {
  return useSyncExternalStore(subscribeTransferStats, getTransferStats);
}
