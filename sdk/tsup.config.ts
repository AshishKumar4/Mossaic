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
  entry: [
    "src/index.ts",
    "src/igit.ts",
    "src/yjs.ts",
    "src/http-only.ts",
    // Lazy-load encryption helpers via /encryption subpath. The
    // main entry's writeFile/readFile use
    // `await import("./encryption")` — splitting:true keeps that
    // out of the main bundle.
    "src/encryption.ts",
  ],
  format: ["esm"],
  dts: true,
  sourcemap: true,
  clean: true,
  // Code-splitting on so the dynamic `await import("./yjs")`
  // inside `user-do-core.ts:getYjsRuntime()`
  // becomes a real lazy chunk. Without splitting, esbuild inlines
  // the dynamic import into index.js and `import { Awareness, ... }
  // from "y-protocols/awareness"` at the top of the file forces
  // every consumer's bundler to eagerly pull y-protocols, even
  // non-collab consumers. With splitting, yjs.ts lives in its own
  // chunk that's only fetched when a tenant uses yjs-mode.
  splitting: true,
  bundle: true,
  treeshake: true,
  // `yjs` and `isomorphic-git` and `y-protocols` are peer deps —
  // the consumer brings them. If we bundled them, we'd ship two
  // copies of the CRDT runtime alongside the consumer's; two
  // `Y.Doc` constructors with different module identities silently
  // break edits. Mark external.
  external: [
    "cloudflare:workers",
    "isomorphic-git",
    "yjs",
    "y-protocols",
    "y-protocols/awareness",
  ],
  target: "es2022",
  esbuildOptions(options) {
    options.alias = {
      ...(options.alias ?? {}),
      "@shared": path.resolve(__dirname, "../shared"),
    };
  },
  tsconfig: "./tsconfig.json",
});
