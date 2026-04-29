/**
 * Versioning commands: versions ls | restore | drop | mark.
 */

import type { Command } from "commander";
import { withClient } from "./_run.js";
import { formatVersions } from "../format.js";

export function registerVersions(program: Command): void {
  const versions = program
    .command("versions")
    .description("file-level versioning (S3-style; opt-in per tenant)");

  // ls
  const ls = versions
    .command("ls <path>")
    .description("list historical versions (newest first)")
    .option("--limit <n>", "max rows", "100")
    .option("--user-visible-only", "filter to user-visible versions")
    .option("--include-metadata", "include metadata snapshot per version");
  ls.action(
    withClient<{
      limit: string;
      userVisibleOnly?: boolean;
      includeMetadata?: boolean;
    }>(ls, async (ctx, local, args) => {
      const [p] = args as [string];
      const rows = await ctx.client.vfs.listVersions(p, {
        limit: parseInt(local.limit, 10),
        userVisibleOnly: local.userVisibleOnly,
        includeMetadata: local.includeMetadata,
      });
      process.stdout.write(formatVersions(rows, { json: !!ctx.globals.json }));
    }),
  );

  // restore
  const restore = versions
    .command("restore <path> <versionId>")
    .description("create a new version whose content matches <versionId>");
  restore.action(
    withClient<{}>(restore, async (ctx, _l, args) => {
      const [p, vid] = args as [string, string];
      const r = await ctx.client.vfs.restoreVersion(p, vid);
      const out = { newVersionId: r.id };
      if (ctx.globals.json) {
        process.stdout.write(JSON.stringify(out) + "\n");
      } else {
        process.stdout.write(`new version: ${r.id}\n`);
      }
    }),
  );

  // drop (retention policy)
  const drop = versions
    .command("drop <path>")
    .description("drop versions per a retention policy. Head version is always preserved.")
    .option("--keep-last <n>", "keep N newest in addition to head")
    .option("--older-than <ms-or-iso>", "drop versions with mtimeMs < cutoff")
    .option("--except <id...>", "explicit allowlist of version_ids to preserve");
  drop.action(
    withClient<{
      keepLast?: string;
      olderThan?: string;
      except?: string[];
    }>(drop, async (ctx, local, args) => {
      const [p] = args as [string];
      const policy: Parameters<typeof ctx.client.vfs.dropVersions>[1] = {};
      if (local.keepLast !== undefined) policy.keepLast = parseInt(local.keepLast, 10);
      if (local.olderThan !== undefined) {
        const n = parseInt(local.olderThan, 10);
        policy.olderThan = Number.isFinite(n) && String(n) === local.olderThan
          ? n
          : new Date(local.olderThan).getTime();
      }
      if (local.except !== undefined) policy.exceptVersions = local.except;
      const r = await ctx.client.vfs.dropVersions(p, policy);
      if (ctx.globals.json) {
        process.stdout.write(JSON.stringify(r) + "\n");
      } else {
        process.stdout.write(`dropped=${r.dropped} kept=${r.kept}\n`);
      }
    }),
  );

  // mark
  const mark = versions
    .command("mark <path> <versionId>")
    .description("set per-version label and/or user-visible flag (vfs.markVersion)")
    .option("--label <s>", "label (replaces any prior label)")
    .option("--user-visible", "mark user-visible (default true)")
    .option("--no-user-visible", "REJECTED by server (userVisible is monotonic)");
  mark.action(
    withClient<{ label?: string; userVisible?: boolean }>(mark, async (ctx, local, args) => {
      const [p, vid] = args as [string, string];
      const opts: Parameters<typeof ctx.client.vfs.markVersion>[2] = {};
      if (local.label !== undefined) opts.label = local.label;
      if (local.userVisible === false) opts.userVisible = false;
      else if (local.userVisible === true) opts.userVisible = true;
      await ctx.client.vfs.markVersion(p, vid, opts);
    }),
  );
}
