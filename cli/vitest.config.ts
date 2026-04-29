import { defineConfig } from "vitest/config";

/**
 * CLI vitest config — uses the standard Node test runtime (NOT the
 * Cloudflare workers pool used by the root config). E2E tests speak
 * HTTP/WSS to a real deployed Mossaic Service worker.
 */
export default defineConfig({
  // Resolve @mossaic/sdk subpaths via the "workspace" condition (TS source)
  // — matches the SDK's package.json conditional exports added in Phase 14.
  // Without this, Vite falls through to ./dist/*.js which doesn't exist
  // until `pnpm -F @mossaic/sdk build` has been run.
  resolve: {
    conditions: ["workspace", "import", "module", "browser", "default"],
  },
  test: {
    include: ["tests/**/*.test.ts"],
    testTimeout: 60_000,
    hookTimeout: 60_000,
    // Limit parallelism — live tests share a global rate-limit budget
    // even though each test uses its own tenant DO. 4 concurrent
    // suites is a good trade-off for wall-clock vs steady-state load.
    pool: "threads",
    poolOptions: {
      threads: {
        maxThreads: 4,
        minThreads: 1,
      },
    },
    // Fail-fast off — we want to see all failures in one run.
    bail: 0,
  },
});
