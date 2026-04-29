/**
 * Token command — mint a fresh VFS-scoped JWT and print it to stdout.
 *
 * Useful for downstream scripts that need a short-lived bearer to
 * call the Mossaic Service worker directly without going through
 * `~/.mossaic/config.json`.
 */

import type { Command } from "commander";
import { resolveProfile } from "../config.js";
import { mintToken } from "../jwt.js";
import { exitCodeFor, formatError } from "../exit-codes.js";
import { readGlobals } from "./index.js";

export function registerToken(program: Command): void {
  const token = program.command("token").description("VFS token utilities");

  const mint = token
    .command("mint")
    .description("mint a VFS-scoped JWT and print it to stdout")
    .option("--ttl <ms>", "TTL in milliseconds", "3600000");
  mint.action(async function (this: Command, opts: { ttl: string }) {
    try {
      const g = readGlobals(this);
      const profile = await resolveProfile(g);
      if (!profile) {
        throw new Error(
          "no profile configured (run `mossaic auth setup` or set MOSSAIC_JWT_SECRET + MOSSAIC_TENANT)",
        );
      }
      const ttl = parseInt(opts.ttl, 10);
      if (!Number.isFinite(ttl) || ttl <= 0) {
        throw new Error(`--ttl: must be a positive integer (ms)`);
      }
      const t = await mintToken({
        secret: profile.jwtSecret,
        ns: profile.scope.ns,
        tenant: profile.scope.tenant,
        sub: profile.scope.sub ?? undefined,
        ttlMs: ttl,
      });
      process.stdout.write(t + "\n");
    } catch (err) {
      console.error(formatError(err));
      process.exit(exitCodeFor(err));
    }
  });
}
