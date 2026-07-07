import path from "node:path";
import { defineConfig } from "tsdown";

/**
 * SDK build config (tsdown / Rolldown).
 *
 * The package re-exports the production `UserDO` and `ShardDO` classes
 * from worker/objects/* so consumer Workers can bind them in their own
 * wrangler.jsonc. The DO classes import from `cloudflare:workers`,
 * which we mark external so the bundler doesn't try to bundle that virtual.
 *
 * The worker-source files use `@shared/*` import aliases. We resolve
 * them via the `alias` option so both the JS bundle and the generated
 * `.d.ts` (rolldown-plugin-dts) can find e.g. `@shared/constants`.
 *
 * Five entry points:
 *   - src/index.ts      → ./dist/index.js       (main; createVFS + DO re-exports)
 *   - src/igit.ts       → ./dist/igit.js        (isomorphic-git fs adapter, optional)
 *   - src/yjs.ts        → ./dist/yjs.js         (yjs runtime, lazy)
 *   - src/http-only.ts  → ./dist/http-only.js
 *   - src/encryption.ts → ./dist/encryption.js  (lazy-loaded via /encryption subpath)
 *
 * Output is ESM only (the SDK is for Workers, which is ESM-native).
 * Rolldown code-splits the dynamic `await import("./yjs")` inside
 * `user-do-core.ts:getYjsRuntime()` into its own chunk, so non-collab
 * consumers never eagerly pull the CRDT runtime.
 */
export default defineConfig({
	entry: [
		"src/index.ts",
		"src/igit.ts",
		"src/yjs.ts",
		"src/http-only.ts",
		"src/encryption.ts",
	],
	format: ["esm"],
	// tsdown defaults `fixedExtension` to true on the `node` platform, which
	// emits `.mjs`/`.d.mts`. Force package-type-based extensions so output is
	// `.js`/`.d.ts` to match the `exports` map (./dist/index.js, .d.ts).
	fixedExtension: false,
	dts: true,
	sourcemap: true,
	clean: true,
	treeshake: true,
	// Externalize every node_modules dependency automatically — the peer
	// deps (`yjs`, `isomorphic-git`, `y-protocols`) must not be bundled, or
	// consumers get two `Y.Doc` constructors with different module
	// identities that silently break edits. `skipNodeModulesBundle` covers
	// all of them, so we only enumerate the `cloudflare:*` virtuals that
	// aren't on disk. The same externals apply to the generated `.d.ts`.
	deps: {
		skipNodeModulesBundle: true,
		neverBundle: ["cloudflare:workers", "cloudflare:email"],
		dts: {
			neverBundle: ["cloudflare:workers", "cloudflare:email"],
		},
	},
	target: "es2022",
	alias: {
		"@shared": path.resolve(import.meta.dirname, "../shared"),
	},
});
