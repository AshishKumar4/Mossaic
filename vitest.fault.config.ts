import { defineWorkersTestConfig } from "./vitest.shared";

export default defineWorkersTestConfig({
	wranglerConfigPath: "./tests/wrangler.fault.test.jsonc",
	include: [
		"tests/integration/cleanup-outbox-remaining-paths.test.ts",
		"tests/integration/multipart-shard-transaction.test.ts",
		"tests/integration/multipart-paged-faults.test.ts",
		"tests/integration/ordinary-publication-failures.test.ts",
		"tests/integration/overwrite-cleanup-failures.test.ts",
		"tests/integration/paged-cleanup-faults.test.ts",
		"tests/integration/versioned-publication-failures.test.ts",
	],
});
