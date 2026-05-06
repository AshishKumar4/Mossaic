/**
 * encryption CLI commands.
 *
 *   mossaic encrypt <local-file> <remote-path> [--password <pw>] [--mode convergent|random] [--key-id <id>]
 *     Encrypts a local file with a PBKDF2-derived master key, then
 *     POSTs the envelope to the remote endpoint with the
 *     X-Mossaic-Encryption header. Reads password from --password,
 *     env MOSSAIC_PASSWORD, or interactive prompt (TTY only).
 *
 *   mossaic decrypt-readback <remote-path> [--password <pw>] [--key-id <id>]
 *     Reads the envelope from the remote endpoint, decrypts with
 *     PBKDF2(password) → master key, prints plaintext to stdout.
 *     Useful for debugging — production consumers should use the
 *     SDK directly.
 *
 *   mossaic rotate-key --old-password <pw> --new-password <pw> [--prefix <p>]
 *     For every encrypted file under the prefix, decrypt with the old
 *     master key and re-encrypt with the new. Best-effort, per-file
 *     atomic via the supersede semantics. Costly: O(total encrypted
 *     bytes).
 *
 * The CLI reads the tenant-salt from the active profile's
 * `encryptionSalt` field (32 bytes hex). If missing, the user is
 * prompted to set one — the salt is per-tenant + stable for the
 * tenant's lifetime.
 *
 * No master key is ever persisted. Passwords are NEVER echoed.
 */

import type { Command } from "commander";
import { writeFile as fsWriteFile, readFile as fsReadFile } from "node:fs/promises";
import { withClient } from "./_run.js";
import {
  encryptChunk,
  decryptChunk,
  deriveMasterFromPassword,
  hexToBytes,
} from "@mossaic/sdk/encryption";
import { promisify } from "node:util";
import * as readlineSync from "node:readline";

interface EncryptFlags {
  password?: string;
  mode?: "convergent" | "random";
  keyId?: string;
  salt?: string;
}

interface DecryptReadbackFlags {
  password?: string;
  keyId?: string;
  salt?: string;
}

interface RotateKeyFlags {
  oldPassword?: string;
  newPassword?: string;
  prefix?: string;
  salt?: string;
}

/**
 * Prompt the user for a password from the TTY. Returns the entered
 * string; never echoes. Throws on non-TTY (avoid silent insecurity).
 */
async function promptPassword(prompt: string): Promise<string> {
  if (!process.stdin.isTTY) {
    throw new Error(
      `password required; use --password=... or set MOSSAIC_PASSWORD (TTY required for interactive prompt)`
    );
  }
  // Use readline + raw mode to suppress echo. node:readline supports
  // creating an interface; we toggle terminal echo by writing to
  // stdin's underlying TTY.
  const rl = readlineSync.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: true,
  });
  const ttyWrite = process.stdout.write.bind(process.stdout);
  ttyWrite(prompt);
  return new Promise<string>((resolve) => {
    // Hide input by intercepting the writeable on stdout.
    const origWrite = (rl as unknown as { _writeToOutput?: (s: string) => void })
      ._writeToOutput;
    (rl as unknown as { _writeToOutput?: (s: string) => void })._writeToOutput =
      function () {
        // Suppress echo entirely.
      };
    rl.question("", (answer) => {
      (
        rl as unknown as { _writeToOutput?: (s: string) => void }
      )._writeToOutput = origWrite;
      ttyWrite("\n");
      rl.close();
      resolve(answer);
    });
  });
}

async function resolvePassword(
  flag: string | undefined,
  envName: string,
  prompt: string
): Promise<string> {
  if (flag !== undefined && flag.length > 0) return flag;
  const fromEnv = process.env[envName];
  if (typeof fromEnv === "string" && fromEnv.length > 0) return fromEnv;
  return promptPassword(prompt);
}

function resolveSalt(flag: string | undefined): Uint8Array {
  const hex = flag ?? process.env.MOSSAIC_ENCRYPTION_SALT;
  if (!hex || typeof hex !== "string" || hex.length !== 64) {
    throw new Error(
      `tenant salt required: pass --salt=<64-hex-chars> or set MOSSAIC_ENCRYPTION_SALT`
    );
  }
  return hexToBytes(hex);
}

export function registerEncryption(program: Command): void {
  program
    .command("encrypt")
    .description(
      "Encrypt a local file and upload it to a remote Mossaic VFS path"
    )
    .argument("<local>", "local file to read")
    .argument("<remote>", "remote VFS path")
    .option("--password <pw>", "password for PBKDF2 master-key derivation")
    .option("--mode <mode>", "encryption mode (convergent|random)", "convergent")
    .option("--key-id <id>", "opaque key label (≤128B UTF-8)")
    .option("--salt <hex>", "32-byte tenant salt (64 hex chars)")
    .action(
      withClient<EncryptFlags>(
        program,
        async (ctx, local, args) => {
          const localPath = args[0] as string;
          const remotePath = args[1] as string;
          if (local.mode !== "convergent" && local.mode !== "random") {
            throw new Error(
              `--mode: must be 'convergent' or 'random' (got '${local.mode}')`
            );
          }
          const salt = resolveSalt(local.salt);
          const password = await resolvePassword(
            local.password,
            "MOSSAIC_PASSWORD",
            "Master password (NEVER recoverable; lose = lose data): "
          );
          const masterKey = await deriveMasterFromPassword(password, salt);
          const plaintext = await fsReadFile(localPath);
          const envelope = await encryptChunk({
            plaintext: new Uint8Array(plaintext),
            masterRaw: masterKey,
            tenantSalt: salt,
            mode: local.mode,
            aadTag: "ck",
            ...(local.keyId !== undefined ? { keyId: local.keyId } : {}),
          });
          // POST directly. The SDK's HttpVFS doesn't expose the
          // header, so we use raw fetch.
          const url = `${ctx.client.endpoint}/api/vfs/writeFile?path=${encodeURIComponent(remotePath)}`;
          const headers: Record<string, string> = {
            Authorization: `Bearer ${ctx.client.token}`,
            "Content-Type": "application/octet-stream",
            "X-Mossaic-Encryption": JSON.stringify({
              mode: local.mode,
              ...(local.keyId !== undefined ? { keyId: local.keyId } : {}),
            }),
          };
          const r = await fetch(url, {
            method: "POST",
            headers,
            body: envelope,
          });
          if (!r.ok) {
            const text = await r.text();
            throw new Error(
              `writeFile failed: ${r.status} ${text.slice(0, 200)}`
            );
          }
          // Best-effort zeroize.
          masterKey.fill(0);
          process.stdout.write(
            `OK: encrypted ${plaintext.length} bytes → ${remotePath} (mode=${local.mode}, envelope=${envelope.byteLength} bytes)\n`
          );
        }
      )
    );

  program
    .command("decrypt-readback")
    .description(
      "Read + decrypt a remote encrypted file and print plaintext to stdout"
    )
    .argument("<remote>", "remote VFS path")
    .option("--password <pw>", "password for PBKDF2 master-key derivation")
    .option("--key-id <id>", "opaque key label (informational; not validated)")
    .option("--salt <hex>", "32-byte tenant salt (64 hex chars)")
    .action(
      withClient<DecryptReadbackFlags>(
        program,
        async (ctx, local, args) => {
          const remotePath = args[0] as string;
          const salt = resolveSalt(local.salt);
          const password = await resolvePassword(
            local.password,
            "MOSSAIC_PASSWORD",
            "Master password: "
          );
          const masterKey = await deriveMasterFromPassword(password, salt);
          const url = `${ctx.client.endpoint}/api/vfs/readFile`;
          const r = await fetch(url, {
            method: "POST",
            headers: {
              Authorization: `Bearer ${ctx.client.token}`,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({ path: remotePath }),
          });
          if (!r.ok) {
            const text = await r.text();
            throw new Error(
              `readFile failed: ${r.status} ${text.slice(0, 200)}`
            );
          }
          const envelope = new Uint8Array(await r.arrayBuffer());
          // Verify the file is actually encrypted via response header.
          const encHdr = r.headers.get("X-Mossaic-Encryption");
          if (encHdr === null) {
            // Plaintext file — write through.
            process.stdout.write(envelope);
            masterKey.fill(0);
            return;
          }
          const plaintext = await decryptChunk({
            envelope,
            masterRaw: masterKey,
            tenantSalt: salt,
            expectedAadTag: "ck",
          });
          process.stdout.write(plaintext);
          masterKey.fill(0);
        }
      )
    );

  program
    .command("rotate-key")
    .description(
      "Rotate the master key for every encrypted file under a prefix (re-encrypts each one)"
    )
    .option("--old-password <pw>", "current master password")
    .option("--new-password <pw>", "new master password")
    .option("--prefix <p>", "path prefix (default '/')", "/")
    .option("--salt <hex>", "32-byte tenant salt (64 hex chars)")
    .action(
      withClient<RotateKeyFlags>(
        program,
        async (ctx, local, _args) => {
          const salt = resolveSalt(local.salt);
          const oldPw = await resolvePassword(
            local.oldPassword,
            "MOSSAIC_OLD_PASSWORD",
            "OLD master password: "
          );
          const newPw = await resolvePassword(
            local.newPassword,
            "MOSSAIC_NEW_PASSWORD",
            "NEW master password: "
          );
          const oldKey = await deriveMasterFromPassword(oldPw, salt);
          const newKey = await deriveMasterFromPassword(newPw, salt);

          // Walk via vfs.listFiles.
          const list = await ctx.client.vfs.listFiles({
            prefix: local.prefix ?? "/",
            limit: 10000,
          });
          let rotated = 0;
          let skipped = 0;
          const failed: string[] = [];

          for (const item of list.items) {
            // Only re-key encrypted files.
            const stat = await ctx.client.vfs.stat(item.path);
            if (!stat.encryption) {
              skipped++;
              continue;
            }
            try {
              // Read envelope (raw bytes via HTTP).
              const rUrl = `${ctx.client.endpoint}/api/vfs/readFile`;
              const rr = await fetch(rUrl, {
                method: "POST",
                headers: {
                  Authorization: `Bearer ${ctx.client.token}`,
                  "Content-Type": "application/json",
                },
                body: JSON.stringify({ path: item.path }),
              });
              if (!rr.ok)
                throw new Error(`read failed: ${rr.status}`);
              const oldEnv = new Uint8Array(await rr.arrayBuffer());
              const plaintext = await decryptChunk({
                envelope: oldEnv,
                masterRaw: oldKey,
                tenantSalt: salt,
                expectedAadTag: "ck",
              });
              const newEnv = await encryptChunk({
                plaintext,
                masterRaw: newKey,
                tenantSalt: salt,
                mode: stat.encryption.mode,
                aadTag: "ck",
                ...(stat.encryption.keyId !== undefined
                  ? { keyId: stat.encryption.keyId }
                  : {}),
              });
              const wUrl = `${ctx.client.endpoint}/api/vfs/writeFile?path=${encodeURIComponent(item.path)}`;
              const wr = await fetch(wUrl, {
                method: "POST",
                headers: {
                  Authorization: `Bearer ${ctx.client.token}`,
                  "Content-Type": "application/octet-stream",
                  "X-Mossaic-Encryption": JSON.stringify({
                    mode: stat.encryption.mode,
                    ...(stat.encryption.keyId !== undefined
                      ? { keyId: stat.encryption.keyId }
                      : {}),
                  }),
                },
                body: newEnv,
              });
              if (!wr.ok) throw new Error(`write failed: ${wr.status}`);
              rotated++;
            } catch (err) {
              failed.push(item.path);
              process.stderr.write(
                `[rotate-key] FAILED ${item.path}: ${err instanceof Error ? err.message : String(err)}\n`
              );
            }
          }

          oldKey.fill(0);
          newKey.fill(0);
          const json = ctx.globals.json ?? false;
          if (json) {
            process.stdout.write(
              JSON.stringify({ rotated, skipped, failed }) + "\n"
            );
          } else {
            process.stdout.write(
              `OK: rotated=${rotated} skipped=${skipped} failed=${failed.length}\n`
            );
            if (failed.length > 0) {
              process.stderr.write(
                `Failed paths:\n${failed.map((p) => `  ${p}`).join("\n")}\n`
              );
            }
          }
        }
      )
    );
}

// Suppress unused-imports if any helpers aren't referenced in this file's
// final form. (promisify reserved for any future async-readline migration.)
void promisify;
