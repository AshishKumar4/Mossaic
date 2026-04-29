/**
 * File ops: ls, cat, write, put, get, stream-put, stream-get, rm, mv,
 * cp, mkdir, rmdir, rm-rf, stat, ln, readlink, chmod, exists.
 *
 * Every command reads the global flags (profile/endpoint/secret/ns/tenant/sub/json)
 * via `readGlobals` and goes through `withClient` which resolves the
 * profile, mints a token, builds an HttpVFS, and uniformly handles
 * errors → exit codes.
 */

import type { Command } from "commander";
import { writeFile as fsWriteFile, readFile as fsReadFile, stat as fsStat } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { withClient } from "./_run.js";
import { formatList, formatStat, formatStatMany } from "../format.js";
import {
  TAGS_MAX_PER_FILE,
  TAG_MAX_LEN,
  METADATA_MAX_BYTES,
  VFS_MODE_YJS_BIT,
} from "@mossaic/sdk/http";
import { parseMetadataFlag } from "../format.js";

interface WriteFlags {
  text?: string;
  from?: string;
  mode?: string;
  mime?: string;
  metadata?: string;
  tag?: string[];
  versionLabel?: string;
  noUserVisible?: boolean;
}

function parseWriteOpts(local: WriteFlags): {
  mode?: number;
  mimeType?: string;
  metadata?: Record<string, unknown> | null;
  tags?: readonly string[];
  version?: { label?: string; userVisible?: boolean };
} {
  const opts: ReturnType<typeof parseWriteOpts> = {};
  if (local.mode) {
    const m = local.mode.startsWith("0o")
      ? parseInt(local.mode.slice(2), 8)
      : parseInt(local.mode, 8);
    if (!Number.isFinite(m)) throw new Error(`--mode: not an octal: ${local.mode}`);
    opts.mode = m;
  }
  if (local.mime) opts.mimeType = local.mime;
  const md = parseMetadataFlag(local.metadata);
  if (md !== undefined) {
    if (md !== null) {
      const size = new TextEncoder().encode(JSON.stringify(md)).length;
      if (size > METADATA_MAX_BYTES) {
        throw new Error(
          `--metadata: ${size} bytes exceeds METADATA_MAX_BYTES=${METADATA_MAX_BYTES}`,
        );
      }
    }
    opts.metadata = md;
  }
  if (local.tag !== undefined && local.tag.length > 0) {
    if (local.tag.length > TAGS_MAX_PER_FILE) {
      throw new Error(
        `--tag: ${local.tag.length} exceeds TAGS_MAX_PER_FILE=${TAGS_MAX_PER_FILE}`,
      );
    }
    for (const t of local.tag) {
      if (t.length === 0 || t.length > TAG_MAX_LEN || !/^[A-Za-z0-9._:/-]+$/.test(t)) {
        throw new Error(`--tag: invalid value "${t}" (charset [A-Za-z0-9._:/-]{1,${TAG_MAX_LEN}})`);
      }
    }
    opts.tags = local.tag;
  }
  if (local.versionLabel !== undefined || local.noUserVisible) {
    opts.version = {
      label: local.versionLabel,
      userVisible: local.noUserVisible ? false : undefined,
    };
  }
  return opts;
}

export function registerFileOps(program: Command): void {
  // ls
  const ls = program
    .command("ls <path>")
    .description("list directory entries (vfs.readdir)");
  ls.action(
    withClient<{}>(ls, async (ctx, _local, args) => {
      const [p] = args as [string];
      const entries = await ctx.client.vfs.readdir(p);
      process.stdout.write(formatList(entries.sort(), { json: !!ctx.globals.json }));
    }),
  );

  // cat
  const cat = program
    .command("cat <path>")
    .description("read a file to stdout (vfs.readFile)")
    .option("--version <id>", "read a specific historical version")
    .option("--encoding <enc>", "utf8 | binary (default binary)", "binary");
  cat.action(
    withClient<{ version?: string; encoding: string }>(cat, async (ctx, local, args) => {
      const [p] = args as [string];
      let buf: Uint8Array;
      if (local.version) {
        // HttpVFS does not expose `{version}` on its readFile overloads;
        // hit /api/vfs/readFile directly with the versionId in the body.
        const r = await fetch(ctx.client.endpoint + "/api/vfs/readFile", {
          method: "POST",
          headers: {
            Authorization: `Bearer ${ctx.client.token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ path: p, versionId: local.version }),
        });
        if (!r.ok) {
          const body = await r.text();
          throw new Error(`cat --version: HTTP ${r.status} ${body}`);
        }
        const ct = r.headers.get("Content-Type") ?? "";
        if (ct.includes("application/json")) {
          // Server returned the encoded form; decode.
          const j = (await r.json()) as { data?: string };
          buf = new TextEncoder().encode(j.data ?? "");
        } else {
          buf = new Uint8Array(await r.arrayBuffer());
        }
      } else {
        buf = await ctx.client.vfs.readFile(p);
      }
      if (local.encoding === "utf8") {
        process.stdout.write(new TextDecoder().decode(buf));
      } else {
        process.stdout.write(buf);
      }
    }),
  );

  // write
  const write = program
    .command("write <path>")
    .description("write a file (vfs.writeFile). Reads stdin when neither --text nor --from is given.")
    .option("--text <s>", "literal text payload")
    .option("--from <local>", "read payload from a local file")
    .option("--mode <octal>", "POSIX mode (e.g. 0o644)")
    .option("--mime <type>", "MIME type")
    .option("--metadata <json>", "metadata JSON object (or 'null' to clear)")
    .option("--tag <t...>", "tag (repeatable)")
    .option("--version-label <s>", "label for the new version row")
    .option("--no-user-visible", "mark new version userVisible:false");
  write.action(
    withClient<WriteFlags>(write, async (ctx, local, args) => {
      const [p] = args as [string];
      let data: Uint8Array | string;
      if (local.text !== undefined) {
        data = local.text;
      } else if (local.from) {
        data = await fsReadFile(local.from);
      } else {
        // Read stdin to buffer.
        const chunks: Buffer[] = [];
        for await (const c of process.stdin) chunks.push(c as Buffer);
        data = new Uint8Array(Buffer.concat(chunks));
      }
      const opts = parseWriteOpts(local);
      await ctx.client.vfs.writeFile(p, data, opts);
    }),
  );

  // put — local file → remote path
  const put = program
    .command("put <local> <remote>")
    .description("upload a local file (vfs.writeFile)")
    .option("--mode <octal>", "POSIX mode")
    .option("--mime <type>", "MIME type")
    .option("--metadata <json>", "metadata JSON")
    .option("--tag <t...>", "tag (repeatable)")
    .option("--version-label <s>")
    .option("--no-user-visible");
  put.action(
    withClient<WriteFlags>(put, async (ctx, local, args) => {
      const [src, dest] = args as [string, string];
      const data = await fsReadFile(src);
      const opts = parseWriteOpts(local);
      await ctx.client.vfs.writeFile(dest, new Uint8Array(data), opts);
    }),
  );

  // get — remote path → local file (or stdout)
  const get = program
    .command("get <remote> [local]")
    .description("download a remote file (vfs.readFile)")
    .option("-o, --output <path>", "explicit output path");
  get.action(
    withClient<{ output?: string }>(get, async (ctx, local, args) => {
      const [remote, posLocal] = args as [string, string | undefined];
      const target = local.output ?? posLocal;
      const buf = await ctx.client.vfs.readFile(remote);
      if (target) {
        await fsWriteFile(target, buf);
      } else {
        process.stdout.write(buf);
      }
    }),
  );

  // stream-put — buffered stdin → remote (HTTP fallback can't stream)
  const sput = program
    .command("stream-put <remote>")
    .description("upload stdin to a remote path (buffered; HTTP fallback)")
    .option("--mode <octal>")
    .option("--mime <type>")
    .option("--metadata <json>")
    .option("--tag <t...>")
    .option("--version-label <s>")
    .option("--no-user-visible")
    .option("--max-size <bytes>", "buffer cap (default 100 MB)", "104857600");
  sput.action(
    withClient<WriteFlags & { maxSize: string }>(sput, async (ctx, local, args) => {
      const [remote] = args as [string];
      const cap = parseInt(local.maxSize, 10);
      const chunks: Buffer[] = [];
      let total = 0;
      for await (const c of process.stdin) {
        const b = c as Buffer;
        total += b.length;
        if (total > cap) {
          throw new Error(`stream-put: input exceeds --max-size=${cap}`);
        }
        chunks.push(b);
      }
      const data = new Uint8Array(Buffer.concat(chunks));
      const opts = parseWriteOpts(local);
      await ctx.client.vfs.writeFile(remote, data, opts);
    }),
  );

  // stream-get — openManifest + readChunk loop (works on HTTP fallback)
  const sget = program
    .command("stream-get <remote>")
    .description("download a remote file via openManifest + readChunk loop")
    .option("-o, --output <path>", "write to local path (else stdout)");
  sget.action(
    withClient<{ output?: string }>(sget, async (ctx, local, args) => {
      const [remote] = args as [string];
      const m = await ctx.client.vfs.openManifest(remote);
      const sink = local.output
        ? createWriteSink(local.output)
        : { write: (b: Uint8Array): Promise<void> => new Promise((res) => { process.stdout.write(b, () => res()); }), close: async (): Promise<void> => {} };
      try {
        if (m.inlined) {
          // Inlined files have no manifest chunks; read whole body.
          const buf = await ctx.client.vfs.readFile(remote);
          await sink.write(buf);
        } else {
          for (let i = 0; i < m.chunkCount; i++) {
            const chunk = await ctx.client.vfs.readChunk(remote, i);
            await sink.write(chunk);
          }
        }
      } finally {
        await sink.close();
      }
    }),
  );

  // rm
  const rm = program
    .command("rm <path>")
    .description("remove a file (vfs.unlink). Use -r for removeRecursive.")
    .option("-r, --recursive", "recursive (vfs.removeRecursive)");
  rm.action(
    withClient<{ recursive?: boolean }>(rm, async (ctx, local, args) => {
      const [p] = args as [string];
      if (local.recursive) {
        await ctx.client.vfs.removeRecursive(p);
      } else {
        await ctx.client.vfs.unlink(p);
      }
    }),
  );

  // mv (rename)
  const mv = program
    .command("mv <src> <dst>")
    .description("rename a path (vfs.rename)");
  mv.action(
    withClient<{}>(mv, async (ctx, _l, args) => {
      const [src, dst] = args as [string, string];
      await ctx.client.vfs.rename(src, dst);
    }),
  );

  // cp (copyFile)
  const cp = program
    .command("cp <src> <dest>")
    .description("copy a file (vfs.copyFile)")
    .option("--no-overwrite", "fail with EEXIST if dest exists")
    .option("--metadata <json>", "REPLACE metadata on dest")
    .option("--tag <t...>", "REPLACE tags on dest")
    .option("--version-label <s>");
  cp.action(
    withClient<WriteFlags & { overwrite: boolean }>(cp, async (ctx, local, args) => {
      const [src, dest] = args as [string, string];
      const opts: Parameters<typeof ctx.client.vfs.copyFile>[2] = {
        overwrite: local.overwrite,
      };
      const md = parseMetadataFlag(local.metadata);
      if (md !== undefined) opts.metadata = md;
      if (local.tag) opts.tags = local.tag;
      if (local.versionLabel) opts.version = { label: local.versionLabel };
      await ctx.client.vfs.copyFile(src, dest, opts);
    }),
  );

  // mkdir
  const mkdir = program
    .command("mkdir <path>")
    .description("create a directory (vfs.mkdir)")
    .option("-p, --recursive", "create parents")
    .option("--mode <octal>");
  mkdir.action(
    withClient<{ recursive?: boolean; mode?: string }>(mkdir, async (ctx, local, args) => {
      const [p] = args as [string];
      const opts: { recursive?: boolean; mode?: number } = {};
      if (local.recursive) opts.recursive = true;
      if (local.mode) {
        opts.mode = parseInt(local.mode.replace(/^0o/, ""), 8);
      }
      await ctx.client.vfs.mkdir(p, opts);
    }),
  );

  // rmdir
  const rmdir = program
    .command("rmdir <path>")
    .description("remove an empty directory (vfs.rmdir)");
  rmdir.action(
    withClient<{}>(rmdir, async (ctx, _l, args) => {
      const [p] = args as [string];
      await ctx.client.vfs.rmdir(p);
    }),
  );

  // rm-rf
  const rmrf = program
    .command("rm-rf <path>")
    .description("paginated removeRecursive (vfs.removeRecursive)");
  rmrf.action(
    withClient<{}>(rmrf, async (ctx, _l, args) => {
      const [p] = args as [string];
      await ctx.client.vfs.removeRecursive(p);
    }),
  );

  // stat — variadic path. Commander passes the full list as args[0].
  const stat = program
    .command("stat <path...>")
    .description("display file metadata (vfs.stat / lstat / readManyStat)")
    .option("--lstat", "use lstat (don't follow symlinks)")
    .option("--many", "force readManyStat even with one path");
  stat.action(
    withClient<{ lstat?: boolean; many?: boolean }>(stat, async (ctx, local, args) => {
      const paths = args[0] as string[];
      const json = !!ctx.globals.json;
      if (paths.length > 1 || local.many) {
        const stats = await ctx.client.vfs.readManyStat(paths);
        const rows = paths.map((p, i) => ({ path: p, stat: stats[i] }));
        process.stdout.write(formatStatMany(rows, { json }));
      } else {
        const p = paths[0];
        const s = local.lstat
          ? await ctx.client.vfs.lstat(p)
          : await ctx.client.vfs.stat(p);
        process.stdout.write(formatStat(s, p, { json }));
      }
    }),
  );

  // ln -s / symlink
  const ln = program
    .command("ln <target> <path>")
    .description("create a symlink (vfs.symlink). The -s flag is implicit (only symlinks supported).")
    .option("-s, --symbolic", "(default; symlinks are the only kind supported)");
  ln.action(
    withClient<{}>(ln, async (ctx, _l, args) => {
      const [target, path] = args as [string, string];
      await ctx.client.vfs.symlink(target, path);
    }),
  );

  // readlink
  const readlink = program
    .command("readlink <path>")
    .description("read symlink target (vfs.readlink)");
  readlink.action(
    withClient<{}>(readlink, async (ctx, _l, args) => {
      const [p] = args as [string];
      const target = await ctx.client.vfs.readlink(p);
      process.stdout.write(target + "\n");
    }),
  );

  // chmod
  const chmod = program
    .command("chmod <mode> <path>")
    .description("change POSIX mode (vfs.chmod). Use --yjs to flip the yjs-mode bit.")
    .option("--yjs", "treat <mode> as boolean for setYjsMode (true|false|on|off|1|0)");
  chmod.action(
    withClient<{ yjs?: boolean }>(chmod, async (ctx, local, args) => {
      const [mode, p] = args as [string, string];
      if (local.yjs) {
        const enabled = ["true", "1", "on", "yes"].includes(mode.toLowerCase());
        // Use the HTTP fallback's setYjsMode (we exposed a /api/vfs/setYjsMode route).
        const r = await fetch(ctx.client.endpoint + "/api/vfs/setYjsMode", {
          method: "POST",
          headers: {
            Authorization: `Bearer ${ctx.client.token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ path: p, enabled }),
        });
        if (!r.ok) {
          const body = await r.text();
          throw new Error(`setYjsMode: HTTP ${r.status} ${body}`);
        }
        // Note constant import to assert link dependency stays sane.
        void VFS_MODE_YJS_BIT;
      } else {
        const m = parseInt(mode.replace(/^0o/, ""), 8);
        if (!Number.isFinite(m)) throw new Error(`chmod: not an octal: ${mode}`);
        await ctx.client.vfs.chmod(p, m);
      }
    }),
  );

  // exists
  const exists = program
    .command("exists <path>")
    .description("test existence (vfs.exists). Exit 0 if exists, 1 otherwise.")
    .option("-q, --quiet", "no stdout output");
  exists.action(
    withClient<{ quiet?: boolean }>(exists, async (ctx, local, args) => {
      const [p] = args as [string];
      const r = await ctx.client.vfs.exists(p);
      if (!local.quiet) {
        process.stdout.write(r ? "true\n" : "false\n");
      }
      if (!r) process.exit(1);
    }),
  );

  // suppress unused import warning
  void fsStat;
  void createReadStream;
}

function createWriteSink(path: string): {
  write(b: Uint8Array): Promise<void>;
  close(): Promise<void>;
} {
  // Buffer chunks, flush on close. Simpler than a real stream and
  // perfectly fine for files up to ~hundreds of MB on a Node CLI.
  const chunks: Uint8Array[] = [];
  return {
    async write(b) {
      chunks.push(b);
    },
    async close() {
      const total = chunks.reduce((n, c) => n + c.byteLength, 0);
      const buf = new Uint8Array(total);
      let o = 0;
      for (const c of chunks) {
        buf.set(c, o);
        o += c.byteLength;
      }
      await fsWriteFile(path, buf);
    },
  };
}
