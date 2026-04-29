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
 */
export default defineWorkersConfig({
  test: {
    include: ["tests/**/*.test.ts"],
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
    },
  },
});
