/**
 * Command registry — wires every verb into the commander program.
 * Grouped by area for grepability; each registrar receives the root
 * Command and attaches its subcommands.
 */

import type { Command } from "commander";
import { registerAuth } from "./auth.js";
import { registerFileOps } from "./fileops.js";
import { registerPhase12 } from "./phase12.js";
import { registerVersions } from "./versions.js";
import { registerYjs } from "./yjs.js";
import { registerToken } from "./token.js";

export function registerCommands(program: Command): void {
  registerAuth(program);
  registerFileOps(program);
  registerPhase12(program);
  registerVersions(program);
  registerYjs(program);
  registerToken(program);
}

// Shared types/helpers used across command files.
export interface GlobalOpts {
  profile?: string;
  json?: boolean;
}

export function readGlobals(cmd: import("commander").Command): GlobalOpts {
  // Walk up to the root program to read global options.
  let cur: import("commander").Command | null = cmd;
  while (cur && cur.parent) cur = cur.parent;
  const opts = (cur?.opts() ?? {}) as GlobalOpts;
  return opts;
}
