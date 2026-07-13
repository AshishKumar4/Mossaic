import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { configDefaults, defineConfig } from "vitest/config";

import { isExpectedWorkerdRpcRejectionMirror } from "./vitest-unhandled-errors";

interface WorkersTestConfigOptions {
	wranglerConfigPath: string;
	include: string[];
	exclude?: string[];
}

export function defineWorkersTestConfig({
	wranglerConfigPath,
	include,
	exclude,
}: WorkersTestConfigOptions) {
	return defineConfig({
		plugins: [
			cloudflareTest({
				singleWorker: true,
				wrangler: { configPath: wranglerConfigPath },
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
			include,
			exclude: [...configDefaults.exclude, ...(exclude ?? [])],
			testTimeout: 15000,
			coverage: {
				provider: "istanbul",
			},
			onUnhandledError(error) {
				if (isExpectedWorkerdRpcRejectionMirror(error)) return false;
			},
		},
	});
}
