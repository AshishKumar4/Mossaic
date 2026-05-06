/**
 * CLI entrypoint — builds the commander program, registers every
 * command, and dispatches.
 *
 * Global flags (every command):
 *   --profile <name>     pick a non-active profile from ~/.mossaic/config.json
 *   --endpoint <url>     override the profile endpoint
 *   --secret <s>         override the JWT_SECRET (NOT recommended; pass via env)
 *   --ns <ns>            override namespace
 *   --tenant <t>         override tenant
 *   --sub <s>            override sub-tenant
 *   --json               JSON output for list-style commands
 */

import { Command } from "commander";
import { registerCommands } from "./commands/index.js";

export async function run(argv: string[] = process.argv): Promise<void> {
  const program = new Command();
  program
    .name("mossaic")
    .description(
      "Command-line interface for Mossaic VFS. Mints VFS tokens locally and speaks to a deployed Mossaic Service worker over HTTP/WSS.",
    )
    .version("0.1.0")
    // Global options: only orthogonal-to-subcommand flags. Per-call
    // scope overrides (ns/tenant/sub/secret/endpoint) come from env
    // vars (MOSSAIC_NS / MOSSAIC_TENANT / MOSSAIC_SUB / MOSSAIC_JWT_SECRET
    // / MOSSAIC_ENDPOINT) so they don't collide with subcommand
    // options of the same name (commander hoists same-name options
    // up the chain, which would steal values from `auth setup`).
    .option("--profile <name>", "config profile name (default: active)")
    .option("--json", "JSON output for list-style commands")
    .showHelpAfterError(true);

  registerCommands(program);

  // Parse + dispatch. Commander throws CommanderError on bad args; we
  // let those bubble — commander prints help and exits with code 1.
  await program.parseAsync(argv);
}
