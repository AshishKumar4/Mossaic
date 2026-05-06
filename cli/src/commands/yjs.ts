/**
 * Yjs commands: yjs init | edit | awareness | flush.
 *
 * `init` flips the per-file yjs-mode bit. `edit` opens a live socket
 * and accepts text from stdin → Y.Text("content"). `awareness`
 * prints inbound awareness changes (presence) for diagnostic /
 * test use. `flush` triggers a server-side compaction → user-visible
 * version row.
 */

import type { Command } from "commander";
import { withClient } from "./_run.js";
import { openYDocOverWs } from "../yjs-ws.js";

export function registerYjs(program: Command): void {
  const yjs = program.command("yjs").description("live editing via Yjs CRDT");

  // init — promote a regular file to yjs-mode.
  const init = yjs
    .command("init <path>")
    .description("flip the yjs-mode bit on a file (vfs.setYjsMode)");
  init.action(
    withClient<{}>(init, async (ctx, _l, args) => {
      const [p] = args as [string];
      // No public SDK method on HttpVFS for setYjsMode; reach the
      // /api/vfs/setYjsMode HTTP route directly.
      const r = await fetch(ctx.client.endpoint + "/api/vfs/setYjsMode", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${ctx.client.token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path: p, enabled: true }),
      });
      if (!r.ok) {
        const body = await r.text();
        throw new Error(`yjs init: HTTP ${r.status} ${body}`);
      }
    }),
  );

  // edit — read stdin, append to Y.Text("content"), close.
  const edit = yjs
    .command("edit <path>")
    .description("append stdin to the yjs-mode file's Y.Text('content')")
    .option("--flush", "trigger flush() on close")
    .option("--label <s>", "version label for the flush");
  edit.action(
    withClient<{ flush?: boolean; label?: string }>(edit, async (ctx, local, args) => {
      const [p] = args as [string];
      const handle = await openYDocOverWs({
        endpoint: ctx.client.endpoint,
        token: ctx.client.token,
        path: p,
      });
      try {
        await handle.synced;
        const chunks: Buffer[] = [];
        for await (const c of process.stdin) chunks.push(c as Buffer);
        const text = Buffer.concat(chunks).toString("utf8");
        if (text.length > 0) {
          handle.doc.transact(() => {
            const yText = handle.doc.getText("content");
            yText.insert(yText.length, text);
          });
          // Give the outbound update pump a moment.
          await new Promise((res) => setTimeout(res, 200));
        }
        if (local.flush) {
          const r = await handle.flush({ label: local.label });
          if (ctx.globals.json) {
            process.stdout.write(JSON.stringify(r) + "\n");
          } else {
            process.stdout.write(
              `flushed: versionId=${r.versionId} checkpointSeq=${r.checkpointSeq}\n`,
            );
          }
        }
      } finally {
        await handle.close();
      }
    }),
  );

  // awareness — print inbound awareness states for N seconds.
  const aware = yjs
    .command("awareness <path>")
    .description("subscribe to awareness updates and print remote state")
    .option("--name <s>", "local presence name", `mossaic-cli-${process.pid}`)
    .option("--watch <s>", "seconds to watch (default 5)", "5");
  aware.action(
    withClient<{ name: string; watch: string }>(aware, async (ctx, local, args) => {
      const [p] = args as [string];
      const seconds = parseInt(local.watch, 10);
      const handle = await openYDocOverWs({
        endpoint: ctx.client.endpoint,
        token: ctx.client.token,
        path: p,
      });
      try {
        await handle.synced;
        handle.awareness.setLocalState({ name: local.name, ts: Date.now() });
        const json = !!ctx.globals.json;
        handle.awareness.on("change", () => {
          const states = Array.from(handle.awareness.getStates().entries()).map(
            ([id, st]) => ({ clientID: id, state: st }),
          );
          if (json) {
            process.stdout.write(JSON.stringify(states) + "\n");
          } else {
            for (const s of states) {
              process.stdout.write(
                `peer ${s.clientID}: ${JSON.stringify(s.state)}\n`,
              );
            }
            process.stdout.write("---\n");
          }
        });
        await new Promise((res) => setTimeout(res, seconds * 1000));
      } finally {
        await handle.close();
      }
    }),
  );

  // flush — trigger a server-side checkpoint emit.
  const flush = yjs
    .command("flush <path>")
    .description("trigger Yjs compaction → user-visible version row")
    .option("--label <s>");
  flush.action(
    withClient<{ label?: string }>(flush, async (ctx, local, args) => {
      const [p] = args as [string];
      const r = await fetch(ctx.client.endpoint + "/api/vfs/flushYjs", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${ctx.client.token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path: p, label: local.label }),
      });
      if (!r.ok) {
        const body = await r.text();
        throw new Error(`yjs flush: HTTP ${r.status} ${body}`);
      }
      const body = (await r.json()) as { versionId: string | null; checkpointSeq: number };
      if (ctx.globals.json) {
        process.stdout.write(JSON.stringify(body) + "\n");
      } else {
        process.stdout.write(
          `flushed: versionId=${body.versionId} checkpointSeq=${body.checkpointSeq}\n`,
        );
      }
    }),
  );
}
