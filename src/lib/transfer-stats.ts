import type { CompletedTransferStats } from "@shared/types";

const MAX_HISTORY = 20;
let stats: CompletedTransferStats[] = [];
const listeners = new Set<() => void>();

export function addTransferStats(entry: CompletedTransferStats): void {
  stats = [entry, ...stats.slice(0, MAX_HISTORY - 1)];
  listeners.forEach((fn) => fn());
}

export function getTransferStats(): CompletedTransferStats[] {
  return stats;
}

export function subscribeTransferStats(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}
