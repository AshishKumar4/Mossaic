import { defineConfig } from "tsup";
import path from "node:path";

/**
 * SDK build config.
 *
 * The package re-exports the production `UserDO` and `ShardDO` classes
 * from worker/objects/* so consumer Workers can bind them in their own
 * wrangler.jsonc. The DO classes import from `cloudflare:workers`,
 * which we mark external so tsup doesn't try to bundle that virtual.
 *
 * The worker-source files use `@shared/*` import aliases. We resolve
 * them via an esbuild plugin so tsup can bundle them. Without this,
 * esbuild can't find e.g. `@shared/constants`.
 *
 * Two entry points:
 *   - src/index.ts → ./dist/index.js     (main; createVFS + DO re-exports)
 *   - src/igit.ts  → ./dist/igit.js      (isomorphic-git fs adapter, optional)
 *
 * Output is ESM only (the SDK is for Workers, which is ESM-native).
 */
export default defineConfig({
  entry: ["src/index.ts", "src/igit.ts", "src/yjs.ts"],
  format: ["esm"],
  dts: true,
  sourcemap: true,
  clean: true,
  splitting: false,
  bundle: true,
  treeshake: true,
  // `yjs` and `isomorphic-git` are peer deps — the consumer brings them.
  // If we bundled `yjs` into our dist we'd ship two copies of the
  // CRDT runtime alongside the consumer's Y.Doc imports, which would
  // silently break edits (the two `Y.Doc` constructors wouldn't be
  // identity-compatible). Mark external.
  external: ["cloudflare:workers", "isomorphic-git", "yjs"],
  target: "es2022",
  esbuildOptions(options) {
    options.alias = {
      ...(options.alias ?? {}),
      "@shared": path.resolve(__dirname, "../shared"),
    };
  },
  tsconfig: "./tsconfig.json",
});
