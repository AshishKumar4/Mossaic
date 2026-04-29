/**
 * Stdout formatters for human-readable vs --json output.
 *
 * Every list-style command honours --json. When --json is set, the
 * formatter dumps a JSON value to stdout with a trailing newline.
 * Otherwise we render in a Unix-friendly shape (one entry per line,
 * tabular for stat).
 */

import type { VFSStat, ListFilesItem, VersionInfo } from "@mossaic/sdk/http";

export interface OutputOpts {
  json: boolean;
}

export function formatList(entries: string[], o: OutputOpts): string {
  if (o.json) return JSON.stringify(entries) + "\n";
  return entries.length === 0 ? "" : entries.join("\n") + "\n";
}

/** Format a VFSStat for human display. */
export function formatStat(stat: VFSStat, path: string, o: OutputOpts): string {
  if (o.json) {
    return (
      JSON.stringify({
        path,
        type: stat.isFile()
          ? "file"
          : stat.isDirectory()
            ? "directory"
            : stat.isSymbolicLink()
              ? "symlink"
              : "other",
        mode: stat.mode,
        size: stat.size,
        mtimeMs: stat.mtimeMs,
        ino: stat.ino,
      }) + "\n"
    );
  }
  const kind = stat.isFile()
    ? "f"
    : stat.isDirectory()
      ? "d"
      : stat.isSymbolicLink()
        ? "l"
        : "?";
  const mode = stat.mode.toString(8).padStart(4, "0");
  const mtime = new Date(stat.mtimeMs).toISOString();
  return `${kind}  mode=${mode}  size=${stat.size}  mtime=${mtime}  ino=${stat.ino}  ${path}\n`;
}

export function formatStatMany(
  rows: Array<{ path: string; stat: VFSStat | null }>,
  o: OutputOpts,
): string {
  if (o.json) {
    return (
      JSON.stringify(
        rows.map((r) => ({
          path: r.path,
          stat: r.stat
            ? {
                type: r.stat.isFile()
                  ? "file"
                  : r.stat.isDirectory()
                    ? "directory"
                    : r.stat.isSymbolicLink()
                      ? "symlink"
                      : "other",
                mode: r.stat.mode,
                size: r.stat.size,
                mtimeMs: r.stat.mtimeMs,
                ino: r.stat.ino,
              }
            : null,
        })),
      ) + "\n"
    );
  }
  return (
    rows
      .map((r) =>
        r.stat
          ? formatStat(r.stat, r.path, { json: false }).trimEnd()
          : `?  missing  ${r.path}`,
      )
      .join("\n") + "\n"
  );
}

export function formatFindItems(
  items: ListFilesItem[],
  o: OutputOpts,
): string {
  if (o.json) {
    return (
      JSON.stringify(
        items.map((i) => ({
          path: i.path,
          pathId: i.pathId,
          tags: i.tags,
          metadata: i.metadata ?? null,
          size: i.stat?.size,
          mtimeMs: i.stat?.mtimeMs,
        })),
      ) + "\n"
    );
  }
  return (
    items
      .map((i) => {
        const sz = i.stat?.size ?? "-";
        const tags = i.tags.length > 0 ? `[${i.tags.join(",")}]` : "";
        return `${sz}\t${i.path}\t${tags}`;
      })
      .join("\n") + "\n"
  );
}

export function formatVersions(
  versions: VersionInfo[],
  o: OutputOpts,
): string {
  if (o.json) return JSON.stringify(versions) + "\n";
  return (
    versions
      .map((v) => {
        const flags = [
          v.deleted ? "deleted" : "live",
          v.userVisible === false ? "internal" : "visible",
          v.label ? `label=${v.label}` : "",
        ]
          .filter(Boolean)
          .join(",");
        const t = new Date(v.mtimeMs).toISOString();
        return `${v.id}\t${t}\t${v.size}\t${flags}`;
      })
      .join("\n") + "\n"
  );
}

/** Parse `--metadata <json>` and validate it's a plain object. */
export function parseMetadataFlag(
  raw: string | undefined,
): Record<string, unknown> | null | undefined {
  if (raw === undefined) return undefined;
  if (raw === "null") return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error(`--metadata: not valid JSON: ${raw}`);
  }
  if (parsed === null) return null;
  if (typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("--metadata: must be a JSON object or null");
  }
  return parsed as Record<string, unknown>;
}
