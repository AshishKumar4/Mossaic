/**
 * Metadata operations: `meta patch` (vfs.patchMetadata) and `find`
 * (vfs.listFiles indexed query). Both surfaces are metadata-driven.
 */

import type { Command } from "commander";
import { withClient } from "./_run.js";
import { formatFindItems, parseMetadataFlag } from "../format.js";
import { TAGS_MAX_PER_LIST_QUERY, LIST_LIMIT_MAX } from "@mossaic/sdk/http";
import { readFile as fsReadFile } from "node:fs/promises";

export function registerMetadata(program: Command): void {
  // meta patch
  const meta = program.command("meta").description("metadata + tag operations");
  const patch = meta
    .command("patch <path>")
    .description("partial-update metadata + tags (vfs.patchMetadata)")
    .option("--patch <json>", "JSON patch object (deep-merge; null leaves remove)")
    .option("--from <local>", "read patch from a local JSON file")
    .option("--null", "clear all metadata (UPDATE files SET metadata = NULL)")
    .option("--add-tag <t...>", "tags to add (idempotent)")
    .option("--remove-tag <t...>", "tags to remove");
  patch.action(
    withClient<{
      patch?: string;
      from?: string;
      null?: boolean;
      addTag?: string[];
      removeTag?: string[];
    }>(patch, async (ctx, local, args) => {
      const [p] = args as [string];
      let patchValue: Record<string, unknown> | null;
      if (local.null) {
        patchValue = null;
      } else if (local.from) {
        const raw = await fsReadFile(local.from, "utf8");
        const parsed = JSON.parse(raw);
        if (parsed === null) patchValue = null;
        else if (typeof parsed === "object" && !Array.isArray(parsed)) patchValue = parsed;
        else throw new Error(`--from: file must contain a JSON object or null`);
      } else if (local.patch !== undefined) {
        const md = parseMetadataFlag(local.patch);
        patchValue = md ?? null;
      } else {
        // Read JSON from stdin.
        const chunks: Buffer[] = [];
        for await (const c of process.stdin) chunks.push(c as Buffer);
        const raw = Buffer.concat(chunks).toString("utf8");
        if (raw.trim().length === 0) {
          throw new Error("meta patch: provide --patch, --from, or pipe JSON via stdin");
        }
        const parsed = JSON.parse(raw);
        if (parsed === null) patchValue = null;
        else if (typeof parsed === "object" && !Array.isArray(parsed)) patchValue = parsed;
        else throw new Error("meta patch: stdin must contain a JSON object or null");
      }
      const opts: { addTags?: readonly string[]; removeTags?: readonly string[] } = {};
      if (local.addTag) opts.addTags = local.addTag;
      if (local.removeTag) opts.removeTags = local.removeTag;
      await ctx.client.vfs.patchMetadata(p, patchValue, opts);
    }),
  );

  // find / listFiles
  const find = program
    .command("find")
    .description("indexed query of files (vfs.listFiles)")
    .option("--prefix <p>", "path prefix")
    .option("--tag <t...>", "filter by tag (AND; up to 8)")
    .option("--metadata <json>", "exact-match metadata filter (post-filter)")
    .option("--limit <n>", "page size (1..1000)", "50")
    .option("--cursor <c>", "opaque cursor from a prior page")
    .option("--order-by <field>", "mtime | name | size", "mtime")
    .option("--direction <dir>", "asc | desc")
    .option("--no-include-stat", "omit stat from items")
    .option("--include-metadata", "include metadata blob on items")
    .option("--all", "auto-paginate and emit a single combined list");
  find.action(
    withClient<{
      prefix?: string;
      tag?: string[];
      metadata?: string;
      limit: string;
      cursor?: string;
      orderBy: "mtime" | "name" | "size";
      direction?: "asc" | "desc";
      includeStat: boolean;
      includeMetadata?: boolean;
      all?: boolean;
    }>(find, async (ctx, local, _args) => {
      if (local.tag && local.tag.length > TAGS_MAX_PER_LIST_QUERY) {
        throw new Error(
          `--tag: ${local.tag.length} exceeds TAGS_MAX_PER_LIST_QUERY=${TAGS_MAX_PER_LIST_QUERY}`,
        );
      }
      const limit = parseInt(local.limit, 10);
      if (!Number.isFinite(limit) || limit < 1 || limit > LIST_LIMIT_MAX) {
        throw new Error(`--limit: must be 1..${LIST_LIMIT_MAX}`);
      }
      const baseOpts: Parameters<typeof ctx.client.vfs.listFiles>[0] = {
        prefix: local.prefix,
        tags: local.tag,
        metadata: parseMetadataFlag(local.metadata) ?? undefined,
        limit,
        orderBy: local.orderBy,
        direction: local.direction,
        includeStat: local.includeStat,
        includeMetadata: local.includeMetadata,
      };
      const json = !!ctx.globals.json;
      if (local.all) {
        const allItems: Awaited<ReturnType<typeof ctx.client.vfs.listFiles>>["items"] = [];
        let cursor: string | undefined = local.cursor;
        for (;;) {
          const page = await ctx.client.vfs.listFiles({ ...baseOpts, cursor });
          allItems.push(...page.items);
          if (!page.cursor) break;
          cursor = page.cursor;
        }
        process.stdout.write(formatFindItems(allItems, { json }));
      } else {
        const page = await ctx.client.vfs.listFiles({ ...baseOpts, cursor: local.cursor });
        if (json) {
          process.stdout.write(
            JSON.stringify({ items: page.items, cursor: page.cursor ?? null }) + "\n",
          );
        } else {
          process.stdout.write(formatFindItems(page.items, { json: false }));
          if (page.cursor) {
            process.stdout.write(`# next cursor: ${page.cursor}\n`);
          }
        }
      }
    }),
  );
}
