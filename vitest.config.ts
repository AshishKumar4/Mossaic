import { defineWorkersConfig } from "@cloudflare/vitest-pool-workers/config";

/**
 * Vitest config using @cloudflare/vitest-pool-workers (v0.8.x for vitest 2.x).
 *
 * Tests run inside a Miniflare-spawned Worker isolate with MOSSAIC_USER and
 * MOSSAIC_SHARD bindings.
 *
 * - `isolatedStorage: false` because our tests use distinct DO names per
 *   test (e.g. "migration:fresh", "refcount:retry-same-slot"), so we don't
 *   need framework-level storage stack rollback. Avoids the
 *   "Failed to pop isolated storage stack" issue when test bodies use
 *   `runInDurableObject` to introspect DO state.
 * - The compat date is bumped down to a date the bundled workerd supports;
 *   production wrangler.jsonc keeps 2026-03-01 separately.
 * - `testTimeout: 15000` (vitest default is 5000) — under singleWorker the
 *   one workerd instance accumulates DO state and module-cache pressure
 *   across ~110 test files. Tests that complete in <1s in isolation can
 *   take 4-9s under suite load, particularly multipart upload flows that
 *   fan out begin → N×putChunk → finalize across multiple SQL transactions
 *   plus shard RPCs. The 5s default produces 5-6 false-positive timeouts
 *   per full suite run; 15s gives 3x headroom over the slowest observed
 *   passing test (~9.7s) without meaningfully delaying genuine failures.
 *   See `tests/integration/rmrf-budget.test.ts:106` for the existing
 *   precedent — a test that explicitly bumped to 30s for the same reason.
 */
export default defineWorkersConfig({
  test: {
    include: ["tests/**/*.test.ts"],
    testTimeout: 15000,
    poolOptions: {
      workers: {
        isolatedStorage: false,
        singleWorker: true,
        miniflare: {
          compatibilityDate: "2025-09-06",
          compatibilityFlags: ["nodejs_compat"],
        },
        wrangler: { configPath: "./tests/wrangler.test.jsonc" },
      },
    },
  },
  resolve: {
    alias: {
      "@shared": new URL("./shared", import.meta.url).pathname,
      "@core": new URL("./worker/core", import.meta.url).pathname,
      "@app": new URL("./worker/app", import.meta.url).pathname,
    },
  },
});
