/**
 * Map SDK errors to CLI process exit codes.
 *
 * Per the build spec:
 *   VFSError                → 1
 *   MossaicUnavailableError → 2
 *
 * Everything else (programmer error, JSON parse failure, etc.) → 1
 * with a stack trace on stderr.
 *
 * The richer POSIX-style codes (ENOENT=2, EACCES=13, ...) are surfaced
 * on stderr alongside the exit code as `[code: ENOENT]` for
 * debuggability, but the process exit code stays binary (1 or 2)
 * to match the build spec.
 */

import { MossaicUnavailableError, VFSFsError } from "@mossaic/sdk/http";

export function exitCodeFor(err: unknown): number {
  if (err instanceof MossaicUnavailableError) return 2;
  if (err instanceof VFSFsError) return 1;
  return 1;
}

/**
 * Format an error for stderr with code annotation. Used by
 * `runCommand` in `main.ts` so every command surfaces errors
 * consistently.
 */
export function formatError(err: unknown): string {
  if (err instanceof VFSFsError) {
    const path = err.path ? ` '${err.path}'` : "";
    const syscall = err.syscall ? `, ${err.syscall}` : "";
    return `mossaic: ${err.code}${syscall}${path}: ${humanize(err)}`;
  }
  if (err instanceof Error) {
    return `mossaic: ${err.message}`;
  }
  return `mossaic: ${String(err)}`;
}

function humanize(err: VFSFsError): string {
  // Strip any leading "CODE: " from the message so we don't repeat
  // ourselves when the formatter prepends the code.
  const msg = err.message;
  const stripped = msg.replace(new RegExp(`^${err.code}:\\s*`), "");
  return stripped;
}
