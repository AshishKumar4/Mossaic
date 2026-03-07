import { useState, useCallback, useRef } from "react";

export interface SpeedSample {
  timestamp: number;
  bytesPerSecond: number;
  direction: "upload" | "download";
}

export interface TransferStatsState {
  uploadSamples: SpeedSample[];
  downloadSamples: SpeedSample[];
  currentUploadSpeed: number;
  currentDownloadSpeed: number;
  peakUploadSpeed: number;
  peakDownloadSpeed: number;
  totalBytesUploaded: number;
  totalBytesDownloaded: number;
  activeUploads: number;
  activeDownloads: number;
}

const MAX_SAMPLES = 60;

/**
 * Hook that tracks upload/download transfer speeds over time.
 * Records speed samples for sparkline/chart visualization.
 */
export function useTransferStats() {
  const [stats, setStats] = useState<TransferStatsState>({
    uploadSamples: [],
    downloadSamples: [],
    currentUploadSpeed: 0,
    currentDownloadSpeed: 0,
    peakUploadSpeed: 0,
    peakDownloadSpeed: 0,
    totalBytesUploaded: 0,
    totalBytesDownloaded: 0,
    activeUploads: 0,
    activeDownloads: 0,
  });

  const peakUpRef = useRef(0);
  const peakDownRef = useRef(0);

  const recordUploadSpeed = useCallback((bytesPerSecond: number) => {
    const sample: SpeedSample = {
      timestamp: Date.now(),
      bytesPerSecond,
      direction: "upload",
    };

    if (bytesPerSecond > peakUpRef.current) {
      peakUpRef.current = bytesPerSecond;
    }

    setStats((prev) => {
      const uploadSamples = [...prev.uploadSamples, sample].slice(-MAX_SAMPLES);
      return {
        ...prev,
        uploadSamples,
        currentUploadSpeed: bytesPerSecond,
        peakUploadSpeed: peakUpRef.current,
      };
    });
  }, []);

  const recordDownloadSpeed = useCallback((bytesPerSecond: number) => {
    const sample: SpeedSample = {
      timestamp: Date.now(),
      bytesPerSecond,
      direction: "download",
    };

    if (bytesPerSecond > peakDownRef.current) {
      peakDownRef.current = bytesPerSecond;
    }

    setStats((prev) => {
      const downloadSamples = [...prev.downloadSamples, sample].slice(
        -MAX_SAMPLES
      );
      return {
        ...prev,
        downloadSamples,
        currentDownloadSpeed: bytesPerSecond,
        peakDownloadSpeed: peakDownRef.current,
      };
    });
  }, []);

  const recordTransferComplete = useCallback(
    (direction: "upload" | "download", totalBytes: number) => {
      setStats((prev) => ({
        ...prev,
        ...(direction === "upload"
          ? {
              totalBytesUploaded: prev.totalBytesUploaded + totalBytes,
              currentUploadSpeed: 0,
            }
          : {
              totalBytesDownloaded: prev.totalBytesDownloaded + totalBytes,
              currentDownloadSpeed: 0,
            }),
      }));
    },
    []
  );

  const setActiveTransfers = useCallback(
    (direction: "upload" | "download", count: number) => {
      setStats((prev) => ({
        ...prev,
        ...(direction === "upload"
          ? { activeUploads: count }
          : { activeDownloads: count }),
      }));
    },
    []
  );

  const reset = useCallback(() => {
    peakUpRef.current = 0;
    peakDownRef.current = 0;
    setStats({
      uploadSamples: [],
      downloadSamples: [],
      currentUploadSpeed: 0,
      currentDownloadSpeed: 0,
      peakUploadSpeed: 0,
      peakDownloadSpeed: 0,
      totalBytesUploaded: 0,
      totalBytesDownloaded: 0,
      activeUploads: 0,
      activeDownloads: 0,
    });
  }, []);

  return {
    stats,
    recordUploadSpeed,
    recordDownloadSpeed,
    recordTransferComplete,
    setActiveTransfers,
    reset,
  };
}
