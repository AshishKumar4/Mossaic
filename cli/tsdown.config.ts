import { defineConfig } from "tsdown";

export default defineConfig({
	entry: {
		bin: "src/bin.ts",
		main: "src/main.ts",
	},
	format: ["esm"],
	target: "node20",
	platform: "node",
	outDir: "dist",
	clean: true,
	dts: false,
	sourcemap: false,
	// tsdown defaults `fixedExtension` to true on the `node` platform,
	// which emits `.mjs`. Force package-type-based extensions so output is
	// `.js` to match the `bin`/`main` paths (./dist/bin.js, ./dist/main.js).
	fixedExtension: false,
	// Externalize every node_modules dependency (including the workspace
	// `@mossaic/sdk` symlink) so users can swap versions and the published
	// bundle stays small. `skipNodeModulesBundle` covers them all, leaving
	// only the `cloudflare:*` virtuals to enumerate — the SDK's transitive
	// `cloudflare:workers` import is handled by a Node loader shim in
	// src/_sdk-shim.ts that dispatches BEFORE the SDK loads.
	// tsdown preserves the shebang from src/bin.ts and grants execute
	// permission to dist/bin.js automatically — no onSuccess chmod needed.
	deps: {
		skipNodeModulesBundle: true,
		neverBundle: ["cloudflare:workers", "cloudflare:email"],
	},
});
