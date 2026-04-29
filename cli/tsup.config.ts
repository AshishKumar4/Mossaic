import { defineConfig } from "tsup";

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
  // Don't bundle deps — keep them as runtime imports so users can swap
  // versions and so the published bundle stays small. The bin shebang
  // is preserved by tsup.
  bundle: true,
  dts: false,
  sourcemap: false,
  splitting: false,
  shims: false,
  banner: ({ format }) =>
    format === "esm" ? { js: "" } : {},
  esbuildOptions(opts) {
    // Keep `node:` prefix on builtins so node20+ ESM resolves them.
    opts.platform = "node";
  },
  // Externalize runtime deps. @mossaic/sdk is also external — the
  // CLI imports its types and HttpVFS class from the published bundle
  // at runtime. The SDK's `cloudflare:workers` import (transitively
  // pulled in via the UserDO re-export) is handled via a Node loader
  // shim in src/_sdk-shim.ts that dispatches BEFORE the SDK loads.
  external: [
    "@mossaic/sdk",
    "cloudflare:workers",
    "commander",
    "jose",
    "ulid",
    "ws",
    "yjs",
    "y-protocols",
    "y-protocols/awareness",
  ],
  onSuccess: async () => {
    // Make dist/bin.js executable + ensure shebang. tsup preserves the
    // shebang from src/bin.ts; we just chmod +x.
    const { chmod } = await import("node:fs/promises");
    try {
      await chmod("dist/bin.js", 0o755);
    } catch {
      /* ignore on first build */
    }
  },
});
