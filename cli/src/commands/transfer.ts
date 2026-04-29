/**
 * Phase 16 — multipart parallel transfer CLI commands.
 *
 *   mossaic upload <local> <remote> [--concurrency=N] [--no-endgame]
 *     Streams a local file to the remote VFS path via the
 *     parallelUpload engine. Adaptive concurrency, endgame, resume
 *     hooks all enabled by default. Progress is rendered to stderr at
 *     ≤10 Hz so it doesn't spam scripted invocations.
 *
 *   mossaic download <remote> <local> [--concurrency=N] [--no-endgame]
 *     Symmetric — parallelDownload pulled into a local file. Streams
 *     to stdout when <local> is "-".
 *
 *   mossaic upload-resume <local> <remote> --upload-id=<id>
 *     Resume a previously interrupted upload by passing the
 *     uploadId. The SDK re-derives the local chunk hashes for
 *     already-landed indices so we can finalize without re-PUTting.
 *
 *   mossaic upload-status <upload-id>
 *     Print landed[] / total / bytesUploaded for a session.
 *
 * All commands route through the shared `withClient` helper so they
 * speak the same Bearer-token-auth + endpoint resolution as the
 * existing `put`/`get` verbs. The HTTP client (`HttpVFS`) already
 * implements the Phase 16 wire calls — we just reach into it.
 */

import type { Command } from "commander";
import { writeFile as fsWriteFile, readFile as fsReadFile } from "node:fs/promises";
import { createWriteStream } from "node:fs";
import { withClient } from "./_run.js";
import {
  parallelUpload,
  parallelDownload,
  beginUpload,
  finalizeUpload,
  statusUpload,
  type ParallelUploadOpts,
  type ParallelDownloadOpts,
} from "@mossaic/sdk/http";

interface UploadFlags {
  concurrency?: string;
  initialConcurrency?: string;
  endgame?: boolean;
  endgameThreshold?: string;
  endgameMaxFanout?: string;
  uploadId?: string;
  noEndgame?: boolean;
  noProgress?: boolean;
}

interface DownloadFlags {
  concurrency?: string;
  initialConcurrency?: string;
  endgame?: boolean;
  noEndgame?: boolean;
  noProgress?: boolean;
}

function parseConcurrency(
  flag: string | undefined,
  initialFlag: string | undefined,
  defaults: { initial: number; min: number; max: number }
): { initial?: number; min?: number; max?: number } | undefined {
  const out: { initial?: number; min?: number; max?: number } = {};
  if (flag) {
    const m = flag.match(/^(\d+):(\d+):(\d+)$/);
    if (m) {
      out.min = parseInt(m[1], 10);
      out.initial = parseInt(m[2], 10);
      out.max = parseInt(m[3], 10);
    } else {
      const n = parseInt(flag, 10);
      if (!Number.isFinite(n) || n < 1) {
        throw new Error(
          `--concurrency: expected N or MIN:INITIAL:MAX (got '${flag}')`
        );
      }
      out.max = n;
      out.initial = Math.min(n, defaults.initial);
      out.min = 1;
    }
  }
  if (initialFlag) {
    const n = parseInt(initialFlag, 10);
    if (!Number.isFinite(n) || n < 1) {
      throw new Error(`--initial-concurrency: expected positive integer`);
    }
    out.initial = n;
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

function progressBar(
  uploaded: number,
  total: number,
  parallelism: number,
  endgame: boolean
): string {
  const pct = total > 0 ? Math.floor((uploaded / total) * 100) : 100;
  const w = 30;
  const filled = Math.floor((pct / 100) * w);
  const bar = "#".repeat(filled) + ".".repeat(w - filled);
  const mb = (uploaded / 1_048_576).toFixed(1);
  const totMb = (total / 1_048_576).toFixed(1);
  const tag = endgame ? "EG" : `${parallelism}p`;
  return `[${bar}] ${pct}% ${mb}/${totMb} MB (${tag})`;
}

export function registerTransfer(program: Command): void {
  // upload — local file → remote via parallelUpload
  const up = program
    .command("upload <local> <remote>")
    .description(
      "parallel multipart upload of a local file to a remote VFS path (Phase 16)"
    )
    .option("--concurrency <n|MIN:INIT:MAX>", "parallelism budget (default 4:64:64)")
    .option("--initial-concurrency <n>", "initial lane count")
    .option("--no-endgame", "disable endgame mode")
    .option("--endgame-threshold <p>", "endgame trigger fraction 0..1 (default 0.9)")
    .option("--endgame-max-fanout <n>", "max extra lanes at endgame (default 8)")
    .option("--upload-id <id>", "resume a prior session by uploadId")
    .option("--no-progress", "suppress stderr progress bar");
  up.action(
    withClient<UploadFlags>(up, async (ctx, local, args) => {
      const [src, dest] = args as [string, string];
      const data = await fsReadFile(src);
      const concurrency = parseConcurrency(
        local.concurrency,
        local.initialConcurrency,
        { initial: 4, min: 1, max: 64 }
      );
      const opts: ParallelUploadOpts = {};
      if (concurrency) opts.concurrency = concurrency;
      if (local.noEndgame) {
        opts.endgameThreshold = 1.0;
        opts.endgameMaxFanout = 0;
      } else {
        if (local.endgameThreshold) {
          opts.endgameThreshold = Math.max(
            0,
            Math.min(1, parseFloat(local.endgameThreshold))
          );
        }
        if (local.endgameMaxFanout) {
          opts.endgameMaxFanout = parseInt(local.endgameMaxFanout, 10);
        }
      }
      if (local.uploadId) opts.resumeUploadId = local.uploadId;
      if (!local.noProgress && process.stderr.isTTY) {
        let lastTs = 0;
        opts.onProgress = (e) => {
          const now = Date.now();
          if (now - lastTs < 100 && e.chunksDone < e.chunksTotal) return;
          lastTs = now;
          const line = progressBar(
            e.uploaded,
            e.total,
            e.currentParallelism,
            e.endgameActive
          );
          process.stderr.write(`\r${line}`);
        };
      }
      const t0 = Date.now();
      const r = await parallelUpload(
        ctx.client.vfs,
        dest,
        new Uint8Array(data),
        opts
      );
      if (!local.noProgress && process.stderr.isTTY) process.stderr.write("\n");
      const elapsed = (Date.now() - t0) / 1000;
      const mb = (data.length / 1_048_576).toFixed(2);
      const mbps = (data.length / 1_048_576 / Math.max(elapsed, 0.001)).toFixed(1);
      if (ctx.globals.json) {
        process.stdout.write(
          JSON.stringify({
            fileId: r.fileId,
            uploadId: r.uploadId,
            size: r.size,
            fileHash: r.fileHash,
            elapsedSec: elapsed,
            throughputMBs: parseFloat(mbps),
          }) + "\n"
        );
      } else {
        process.stdout.write(
          `OK uploaded ${dest}\n  uploadId=${r.uploadId}\n  fileId=${r.fileId}\n  size=${mb} MB\n  elapsed=${elapsed.toFixed(2)}s\n  throughput=${mbps} MB/s\n`
        );
      }
    })
  );

  // download — parallelDownload → local file (or stdout)
  const dn = program
    .command("download <remote> <local>")
    .description(
      "parallel multipart download of a remote VFS path to a local file (Phase 16). Use '-' for stdout."
    )
    .option("--concurrency <n|MIN:INIT:MAX>", "parallelism budget (default 4:64:64)")
    .option("--initial-concurrency <n>", "initial lane count")
    .option("--no-endgame", "disable endgame mode")
    .option("--no-progress", "suppress stderr progress bar");
  dn.action(
    withClient<DownloadFlags>(dn, async (ctx, local, args) => {
      const [remote, target] = args as [string, string];
      const concurrency = parseConcurrency(
        local.concurrency,
        local.initialConcurrency,
        { initial: 4, min: 1, max: 64 }
      );
      const opts: ParallelDownloadOpts = {};
      if (concurrency) opts.concurrency = concurrency;
      if (local.noEndgame) {
        opts.endgameThreshold = 1.0;
        opts.endgameMaxFanout = 0;
      }
      if (!local.noProgress && process.stderr.isTTY) {
        let lastTs = 0;
        opts.onProgress = (e) => {
          const now = Date.now();
          if (now - lastTs < 100 && e.chunksDone < e.chunksTotal) return;
          lastTs = now;
          const line = progressBar(
            e.uploaded,
            e.total,
            e.currentParallelism,
            e.endgameActive
          );
          process.stderr.write(`\r${line}`);
        };
      }
      const t0 = Date.now();
      const buf = await parallelDownload(ctx.client.vfs, remote, opts);
      if (!local.noProgress && process.stderr.isTTY) process.stderr.write("\n");
      if (target === "-") {
        process.stdout.write(buf);
      } else {
        await fsWriteFile(target, buf);
      }
      const elapsed = (Date.now() - t0) / 1000;
      const mb = (buf.length / 1_048_576).toFixed(2);
      const mbps = (
        buf.length /
        1_048_576 /
        Math.max(elapsed, 0.001)
      ).toFixed(1);
      if (ctx.globals.json) {
        process.stdout.write(
          JSON.stringify({
            size: buf.length,
            elapsedSec: elapsed,
            throughputMBs: parseFloat(mbps),
            target,
          }) + "\n"
        );
      } else if (target !== "-") {
        process.stderr.write(
          `OK downloaded ${remote} → ${target} (${mb} MB in ${elapsed.toFixed(2)}s, ${mbps} MB/s)\n`
        );
      }
    })
  );

  // upload-status — list landed chunks for a session
  const stat = program
    .command("upload-status <local> <remote>")
    .description(
      "probe a multipart session: returns landed[], total, bytesUploaded, expiresAt. Requires the local file (for size) and remote path (for begin)."
    )
    .option("--upload-id <id>", "session uploadId to probe (required)");
  stat.action(
    withClient<{ uploadId?: string }>(stat, async (ctx, local, args) => {
      const [src, dest] = args as [string, string];
      if (!local.uploadId) {
        throw new Error("--upload-id is required");
      }
      const data = await fsReadFile(src);
      // Use beginUpload with resumeFrom to get a fresh session-token
      // for the existing uploadId — server checks that the session
      // is still open and returns landed[].
      const session = await beginUpload(ctx.client.vfs, dest, {
        size: data.length,
        resumeFrom: local.uploadId,
      });
      const status = await statusUpload(ctx.client.vfs, session);
      if (ctx.globals.json) {
        process.stdout.write(
          JSON.stringify({
            uploadId: session.uploadId,
            chunkSize: session.chunkSize,
            totalChunks: session.totalChunks,
            landed: status.landed.length,
            total: status.total,
            bytesUploaded: status.bytesUploaded,
            expiresAtMs: status.expiresAtMs,
          }) + "\n"
        );
      } else {
        const pct =
          status.total > 0
            ? ((status.landed.length / status.total) * 100).toFixed(1)
            : "100.0";
        process.stdout.write(
          `Upload status: ${session.uploadId}\n  chunkSize=${session.chunkSize}\n  landed=${status.landed.length}/${status.total} (${pct}%)\n  bytesUploaded=${status.bytesUploaded}\n  expiresAt=${new Date(status.expiresAtMs).toISOString()}\n`
        );
      }
    })
  );

  // upload-finalize — finalize an open session given a hash list
  // (advanced; rarely needed manually but useful for debugging).
  const fin = program
    .command("upload-finalize <local> <remote>")
    .description(
      "finalize an existing multipart session by computing local hashes and POSTing finalize. Use after a crash where chunks landed but finalize never ran."
    )
    .requiredOption("--upload-id <id>", "session uploadId");
  fin.action(
    withClient<{ uploadId: string }>(fin, async (ctx, local, args) => {
      const [src, dest] = args as [string, string];
      const data = await fsReadFile(src);
      const session = await beginUpload(ctx.client.vfs, dest, {
        size: data.length,
        resumeFrom: local.uploadId,
      });
      const cs = session.chunkSize;
      const hashes: string[] = new Array(session.totalChunks);
      // Compute SHA-256 hex per chunk via Node 20+'s WebCrypto.
      // Mirrors `shared/crypto.ts:hashChunk` exactly.
      async function sha256Hex(bytes: Uint8Array): Promise<string> {
        const buf = await crypto.subtle.digest("SHA-256", bytes);
        return Array.from(new Uint8Array(buf))
          .map((b) => b.toString(16).padStart(2, "0"))
          .join("");
      }
      for (let i = 0; i < session.totalChunks; i++) {
        const start = i * cs;
        const end = Math.min(start + cs, data.length);
        hashes[i] = await sha256Hex(
          new Uint8Array(data.buffer, data.byteOffset + start, end - start)
        );
      }
      const r = await finalizeUpload(ctx.client.vfs, session, hashes);
      if (ctx.globals.json) {
        process.stdout.write(JSON.stringify(r) + "\n");
      } else {
        process.stdout.write(
          `OK finalized ${dest}\n  fileId=${r.fileId}\n  size=${r.size}\n  fileHash=${r.fileHash}\n`
        );
      }
    })
  );

  // Suppress unused-import warnings if helpers stay reserved.
  void createWriteStream;
}
