import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      singleWorker: true,
      wrangler: { configPath: "./tests/bench/wrangler.bench.jsonc" },
      miniflare: {
        compatibilityDate: "2025-09-06",
        compatibilityFlags: ["nodejs_compat"],
      },
    }),
  ],
  resolve: {
    alias: {
      "@shared": new URL("./shared", import.meta.url).pathname,
      "@core": new URL("./worker/core", import.meta.url).pathname,
      "@app": new URL("./worker/app", import.meta.url).pathname,
    },
  },
  test: {
    include: ["tests/bench/**/*.bench.ts"],
    testTimeout: 300_000,
    hookTimeout: 300_000,
  },
});
