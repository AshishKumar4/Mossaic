/**
 * `mossaic preview` — fetch a rendered preview variant for a file
 * and write the bytes to stdout (or `--out <local>`). Drives the
 * server-side renderer pipeline through `vfs.readPreview()`.
 *
 * Examples:
 *   mossaic preview /photos/sunset.jpg --variant=thumb > sunset.webp
 *   mossaic preview /docs/code.ts --variant=medium --out preview.svg
 *   mossaic preview /audio/song.mp3 --width=512 --height=128 --fit=contain
 *
 * Exit codes follow the standard CLI convention; ENOENT → 4,
 * ENOTSUP → 14 (encrypted file), generic error → 1. See
 * `cli/src/exit-codes.ts`.
 */

import type { Command } from "commander";
import { writeFile as fsWriteFile } from "node:fs/promises";
import { withClient } from "./_run.js";
import type {
  Variant,
  ReadPreviewOpts,
} from "@mossaic/sdk/http";

interface PreviewFlags {
  variant?: string;
  width?: string;
  height?: string;
  fit?: string;
  format?: string;
  out?: string;
}

/**
 * Build a `Variant` from the CLI flags. Standard variant labels
 * win if `--variant` is supplied; otherwise a custom variant is
 * synthesized from `--width` (+ optional `--height`/`--fit`).
 * Defaults to `thumb` if neither is supplied.
 */
function buildVariant(local: PreviewFlags): Variant {
  if (local.variant !== undefined) {
    if (
      local.variant !== "thumb" &&
      local.variant !== "medium" &&
      local.variant !== "lightbox"
    ) {
      throw new Error(
        `--variant must be thumb, medium, or lightbox (got "${local.variant}")`
      );
    }
    return local.variant;
  }
  if (local.width !== undefined) {
    const w = parseInt(local.width, 10);
    if (!Number.isFinite(w) || w <= 0) {
      throw new Error(`--width must be a positive integer (got "${local.width}")`);
    }
    const out: Variant = { width: w };
    if (local.height !== undefined) {
      const h = parseInt(local.height, 10);
      if (!Number.isFinite(h) || h <= 0) {
        throw new Error(
          `--height must be a positive integer (got "${local.height}")`
        );
      }
      out.height = h;
    }
    if (local.fit !== undefined) {
      if (
        local.fit !== "cover" &&
        local.fit !== "contain" &&
        local.fit !== "scale-down"
      ) {
        throw new Error(
          `--fit must be cover, contain, or scale-down (got "${local.fit}")`
        );
      }
      out.fit = local.fit;
    }
    return out;
  }
  return "thumb";
}

export function registerPreview(program: Command): void {
  const cmd = program
    .command("preview <path>")
    .description(
      "fetch a rendered preview variant (vfs.readPreview). " +
      "Defaults to --variant=thumb writing bytes to stdout."
    )
    .option(
      "--variant <kind>",
      "standard variant: thumb | medium | lightbox"
    )
    .option(
      "--width <px>",
      "custom variant width (overrides --variant when set)"
    )
    .option("--height <px>", "custom variant height (default: same as width)")
    .option(
      "--fit <mode>",
      "cover | contain | scale-down (custom variants only)"
    )
    .option(
      "--format <mime>",
      "preferred output format (image/webp | image/png | image/avif | image/svg+xml). " +
      "The renderer MAY ignore the hint when the underlying binding can't honor it."
    )
    .option("--out <local>", "write bytes to this local path (default: stdout)");

  cmd.action(
    withClient<PreviewFlags>(cmd, async (ctx, local, args) => {
      const [p] = args as [string];
      const variant = buildVariant(local);
      const opts: ReadPreviewOpts = { variant };
      if (local.format !== undefined) {
        if (
          local.format !== "image/png" &&
          local.format !== "image/webp" &&
          local.format !== "image/avif" &&
          local.format !== "image/svg+xml"
        ) {
          throw new Error(
            `--format must be image/png, image/webp, image/avif, or image/svg+xml`
          );
        }
        opts.format = local.format;
      }
      const result = await ctx.client.vfs.readPreview(p, opts);
      if (local.out !== undefined) {
        await fsWriteFile(local.out, result.bytes);
        if (ctx.globals.json === true) {
          process.stdout.write(
            JSON.stringify({
              path: p,
              out: local.out,
              mimeType: result.mimeType,
              width: result.width,
              height: result.height,
              renderer: result.rendererKind,
              cached: result.fromVariantTable,
              byteSize: result.bytes.byteLength,
            }) + "\n"
          );
        } else {
          process.stderr.write(
            `wrote ${result.bytes.byteLength} bytes to ${local.out} ` +
              `(${result.mimeType}, ${result.width}×${result.height}, ` +
              `renderer=${result.rendererKind}, cached=${result.fromVariantTable})\n`
          );
        }
      } else {
        process.stdout.write(result.bytes);
      }
    })
  );
}
