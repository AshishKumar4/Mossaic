import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

/**
 * Vitest config using @cloudflare/vitest-pool-workers v0.16.x with Vitest 4.
 *
 * Tests run inside a Miniflare-spawned Worker isolate with MOSSAIC_USER and
 * MOSSAIC_SHARD bindings.
 *
 * - `singleWorker: true` — one workerd accumulates DO state and module-cache
 *   pressure across ~110 test files. Tests that complete in <1s in isolation
 *   can take 4-9s under suite load, particularly multipart upload flows that
 *   fan out begin → N×putChunk → finalize across multiple SQL transactions
 *   plus shard RPCs.
 * - The compat date is bumped down to a date the bundled workerd supports;
 *   production wrangler.jsonc keeps 2026-03-01 separately.
 * - `testTimeout: 15000` (vitest default is 5000) — under singleWorker the
 *   one workerd instance accumulates state across the full suite. The 5s
 *   default produces 5-6 false-positive timeouts per full suite run; 15s
 *   gives 3x headroom over the slowest observed passing test (~9.7s)
 *   without meaningfully delaying genuine failures. See
 *   `tests/integration/rmrf-budget.test.ts:106` for the existing precedent —
 *   a test that explicitly bumped to 30s for the same reason.
 *
 * The previous v0.8.x config used `poolOptions.workers.isolatedStorage: false`
 * to opt out of framework-level storage stack rollback. That option was
 * removed in newer pool releases because the default no longer rolls back DO
 * storage between tests, matching what mossaic always wanted.
 */
export default defineConfig({
	plugins: [
		cloudflareTest({
			singleWorker: true,
			wrangler: { configPath: "./tests/wrangler.test.jsonc" },
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
		include: ["tests/**/*.test.ts"],
		testTimeout: 15000,
		coverage: {
			provider: "istanbul",
		},
		onUnhandledError(error) {
			const stack = error.stack;
			const code = "code" in error ? error.code : undefined;
			// workerd mirrors expected DO RPC rejections after the caller handles them.
			if (
				(stack?.includes("@cloudflare/vitest-pool-workers") &&
					stack.includes("test-internal.mjs")) ||
				("remote" in error && error.remote === true) ||
				(typeof code === "string" && /^E[A-Z]+$/.test(code)) ||
				error.name === "AbortError" ||
				error.message.startsWith(
					"Serialized RPC arguments or return values are limited to 32MiB",
				)
			) {
				return false;
			}
		},
	},
});
