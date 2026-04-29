/**
 * Auth commands: setup, use, whoami.
 *
 * `auth setup` writes ~/.mossaic/config.json (mode 0600) with the
 * supplied endpoint + secret + scope. `auth use` flips the active
 * profile pointer. `auth whoami` round-trips a stat call to confirm
 * the active profile actually works.
 */

import type { Command } from "commander";
import {
  readConfig,
  writeConfig,
  resolveProfile,
  configPath,
  type ConfigFile,
} from "../config.js";
import { buildClient } from "../client.js";
import { exitCodeFor, formatError } from "../exit-codes.js";
import { readGlobals } from "./index.js";

export function registerAuth(program: Command): void {
  const auth = program.command("auth").description("manage CLI auth profiles");

  auth
    .command("setup")
    .description("write or update a profile in ~/.mossaic/config.json")
    .option("--name <name>", "profile name", "default")
    .option(
      "--endpoint <url>",
      "Mossaic Service worker endpoint",
      "https://mossaic-core.ashishkmr472.workers.dev",
    )
    .requiredOption("--secret <s>", "JWT_SECRET (must match the worker's secret)")
    .option("--ns <ns>", "namespace", "default")
    .requiredOption("--tenant <t>", "tenant id")
    .option("--sub <s>", "sub-tenant id")
    .action(async (opts: {
      name: string;
      endpoint: string;
      secret: string;
      ns: string;
      tenant: string;
      sub?: string;
    }) => {
      const existing = (await readConfig()) ?? {
        active: opts.name,
        profiles: {},
      } as ConfigFile;
      existing.profiles[opts.name] = {
        endpoint: opts.endpoint.replace(/\/$/, ""),
        jwtSecret: opts.secret,
        scope: {
          ns: opts.ns,
          tenant: opts.tenant,
          sub: opts.sub ?? null,
        },
      };
      // First-write becomes the active profile.
      if (Object.keys(existing.profiles).length === 1) {
        existing.active = opts.name;
      }
      await writeConfig(existing);
      // eslint-disable-next-line no-console
      console.log(`mossaic: wrote profile "${opts.name}" to ${configPath()}`);
    });

  auth
    .command("use <profile>")
    .description("set active profile")
    .action(async (profile: string) => {
      const cfg = await readConfig();
      if (!cfg) {
        console.error("mossaic: no config; run `mossaic auth setup` first");
        process.exit(1);
      }
      if (!cfg.profiles[profile]) {
        console.error(`mossaic: unknown profile "${profile}"`);
        process.exit(1);
      }
      cfg.active = profile;
      await writeConfig(cfg);
      // eslint-disable-next-line no-console
      console.log(`mossaic: active profile is now "${profile}"`);
    });

  auth
    .command("whoami")
    .description("verify active profile by round-tripping /stat")
    .action(async function (this: Command) {
      const g = readGlobals(this);
      try {
        const profile = await resolveProfile(g);
        if (!profile) {
          console.error(
            "mossaic: no profile configured. Run `mossaic auth setup` or set MOSSAIC_JWT_SECRET + MOSSAIC_TENANT.",
          );
          process.exit(1);
        }
        const c = await buildClient(profile);
        // Health is open; stat exercises auth.
        const h = await fetch(c.endpoint + "/api/health");
        if (!h.ok) throw new Error(`health probe failed: HTTP ${h.status}`);
        // Touch the tenant via stat /; if root doesn't exist that's fine,
        // we expect any structured response from the worker.
        let scopeOk = true;
        try {
          await c.vfs.stat("/");
        } catch (err: unknown) {
          // ENOENT is fine — proves auth + DO routing works.
          const code = (err as { code?: string }).code;
          if (code !== "ENOENT" && code !== "ENOTDIR") scopeOk = false;
          if (!scopeOk) throw err;
        }
        const out = {
          endpoint: c.endpoint,
          scope: c.scope,
          ok: true,
        };
        if (g.json) {
          // eslint-disable-next-line no-console
          console.log(JSON.stringify(out));
        } else {
          // eslint-disable-next-line no-console
          console.log(
            `mossaic: ok\n  endpoint: ${out.endpoint}\n  ns: ${out.scope.ns}\n  tenant: ${out.scope.tenant}\n  sub: ${out.scope.sub ?? "-"}`,
          );
        }
      } catch (err) {
        console.error(formatError(err));
        process.exit(exitCodeFor(err));
      }
    });
}
