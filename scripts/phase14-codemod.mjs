#!/usr/bin/env node
/**
 * Phase 14 codemod — convert `@shared/*` imports to relative paths
 * in SDK-reachable files under `worker/core/`.
 *
 * Why: workspace consumers vendor `@mossaic/sdk` as `workspace:*`
 * and consume TS source directly. They must not need `@shared/*`
 * path aliases in their tsconfig. This codemod rewrites every
 * `@shared/*` import in the SDK-reachable closure to its
 * relative-path equivalent so the consumer's tsc resolves them
 * with zero alias config.
 *
 * Out of scope:
 *   - worker/app/**           — App layer keeps `@shared/*` (explicit)
 *   - tests/**                — vitest provides aliases
 *   - lean/**                 — Lean-only refs in JSDoc, no TS imports
 *   - cli/**                  — separate package
 *
 * Idempotent: re-running on already-converted files is a no-op
 * (regex finds nothing, file is not rewritten).
 *
 * Usage:
 *   node scripts/phase14-codemod.mjs           # apply
 *   node scripts/phase14-codemod.mjs --dry-run # show diffs only
 */

import { readFile, writeFile } from "node:fs/promises";
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(__dirname, "..");
const SHARED_DIR = resolve(REPO_ROOT, "shared");

const SDK_REACHABLE_FILES = [
  "worker/core/index.ts",
  "worker/core/lib/auth.ts",
  "worker/core/lib/cursor.ts",
  "worker/core/objects/shard/shard-do.ts",
  "worker/core/objects/user/admin.ts",
  "worker/core/objects/user/copy-file.ts",
  "worker/core/objects/user/list-files.ts",
  "worker/core/objects/user/metadata-tags.ts",
  "worker/core/objects/user/path-walk.ts",
  "worker/core/objects/user/rate-limit.ts",
  "worker/core/objects/user/user-do-core.ts",
  "worker/core/objects/user/vfs-ops.ts",
  "worker/core/objects/user/vfs-versions.ts",
  "worker/core/objects/user/yjs.ts",
  "worker/core/routes/vfs-yjs-ws.ts",
  "worker/core/routes/vfs.ts",
  // Phase 14: SDK src files with type-position `import("@shared/X").Foo`.
  "sdk/src/http.ts",
];

// Match three forms:
//   - static:        from "@shared/X" | from '@shared/X'
//   - dynamic:       import("@shared/X")
//   - type-import:   import("@shared/X").Foo
// All three rewrite to the same relative path; we run three regexes
// over each file and accumulate.
const SHARED_RE_LIST = [
  /from\s+(["'])@shared\/([^"']+)\1/g,
  /import\s*\(\s*(["'])@shared\/([^"']+)\1\s*\)/g,
];
const SHARED_IMPORT_RE = /from\s+(["'])@shared\/([^"']+)\1/g; // legacy alias for back-compat

const dryRun = process.argv.includes("--dry-run");

let totalRewrites = 0;
let totalFilesTouched = 0;

for (const rel of SDK_REACHABLE_FILES) {
  const abs = resolve(REPO_ROOT, rel);
  const original = await readFile(abs, "utf8");
  const fileDir = dirname(abs);
  let rewrites = 0;
  let next = original;
  // Static `from "@shared/X"`
  next = next.replace(SHARED_RE_LIST[0], (_match, quote, subpath) => {
    const target = resolve(SHARED_DIR, subpath);
    let rel = relative(fileDir, target).split("\\").join("/");
    if (!rel.startsWith(".")) rel = "./" + rel;
    rewrites += 1;
    return `from ${quote}${rel}${quote}`;
  });
  // Dynamic `import("@shared/X")` — also covers type-position
  // `import("@shared/X").Foo` because the regex matches the
  // `import("..."")` portion only.
  next = next.replace(SHARED_RE_LIST[1], (_match, quote, subpath) => {
    const target = resolve(SHARED_DIR, subpath);
    let rel = relative(fileDir, target).split("\\").join("/");
    if (!rel.startsWith(".")) rel = "./" + rel;
    rewrites += 1;
    return `import(${quote}${rel}${quote})`;
  });

  if (rewrites > 0) {
    totalRewrites += rewrites;
    totalFilesTouched += 1;
    if (dryRun) {
      console.log(`[dry-run] ${rel} — ${rewrites} rewrite(s)`);
      // Show the first 3 rewritten lines for quick visual diff.
      const origLines = original.split("\n");
      const nextLines = next.split("\n");
      let shown = 0;
      for (let i = 0; i < origLines.length && shown < 3; i++) {
        if (origLines[i] !== nextLines[i]) {
          console.log(`    - ${origLines[i].trim()}`);
          console.log(`    + ${nextLines[i].trim()}`);
          shown += 1;
        }
      }
    } else {
      await writeFile(abs, next, "utf8");
      console.log(`${rel} — ${rewrites} rewrite(s)`);
    }
  }
}

console.log(
  `\n${dryRun ? "[dry-run] " : ""}Total: ${totalRewrites} import(s) across ${totalFilesTouched} file(s).`
);
if (dryRun) {
  console.log("(no files modified — re-run without --dry-run to apply)");
}
