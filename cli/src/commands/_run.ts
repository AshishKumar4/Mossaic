/**
 * Shared command runner — resolves the profile, builds the client,
 * invokes the action, and uniformly handles errors → exit codes.
 */

import { resolveProfile, type Profile } from "../config.js";
import { buildClient, type BuiltClient } from "../client.js";
import { exitCodeFor, formatError } from "../exit-codes.js";
import { readGlobals, type GlobalOpts } from "./index.js";
import type { Command } from "commander";

export interface CmdCtx {
  globals: GlobalOpts;
  profile: Profile;
  client: BuiltClient;
}

/**
 * Wrap a command action with profile resolution + error handling.
 *
 * Commander invokes action handlers with positional args (one per
 * registered argument; variadic args are passed as a single array)
 * followed by the local-options object and finally the Command
 * instance. We pass the positional list through as `args: ArgList`
 * so each command can destructure with explicit shape.
 *
 * Per-command callers cast slots to the right concrete type
 * (string for plain `<arg>`; string[] for variadic `<arg...>`).
 */
export type ArgList = ReadonlyArray<string | string[]>;

export function withClient<TLocal>(
  cmd: Command,
  fn: (ctx: CmdCtx, local: TLocal, args: ArgList) => Promise<void> | void,
): (...args: unknown[]) => Promise<void> {
  return async (...args: unknown[]) => {
    const localOpts = (args[args.length - 2] ?? {}) as TLocal;
    const positional = args.slice(0, -2) as ArgList;
    const globals = readGlobals(cmd);
    try {
      const profile = await resolveProfile(globals);
      if (!profile) {
        throw new Error(
          "no profile configured. Run `mossaic auth setup` or set MOSSAIC_JWT_SECRET + MOSSAIC_TENANT.",
        );
      }
      const client = await buildClient(profile);
      await fn({ globals, profile, client }, localOpts, positional);
    } catch (err) {
      console.error(formatError(err));
      process.exit(exitCodeFor(err));
    }
  };
}
