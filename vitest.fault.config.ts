import { defineWorkersTestConfig } from "./vitest.shared";

export default defineWorkersTestConfig({
	wranglerConfigPath: "./tests/wrangler.fault.test.jsonc",
	include: [
		"tests/integration/cleanup-outbox-remaining-paths.test.ts",
		"tests/integration/multipart-constant-publication.test.ts",
		"tests/integration/multipart-finalize-steps.test.ts",
		"tests/integration/multipart-paged-abort.test.ts",
		"tests/integration/multipart-poison.test.ts",
		"tests/integration/multipart-shard-transaction.test.ts",
		"tests/integration/ordinary-publication-failures.test.ts",
		"tests/integration/overwrite-cleanup-failures.test.ts",
		"tests/integration/versioned-publication-failures.test.ts",
	],
});
