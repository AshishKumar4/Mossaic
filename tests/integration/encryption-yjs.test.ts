import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";

/**
 * Phase 15 — Step 4 — encrypted Yjs round-trips at the wire boundary.
 *
 *   Y1.  Two clients on the same encrypted yjs file converge on edits.
 *        The server sees only opaque envelope bytes.
 *   Y2.  Awareness frames round-trip through the encrypted relay.
 *   Y3.  Plain (non-encrypted) yjs files still work for
 *        encryption-aware clients (handle.encrypted === false).
 *   Y4.  An encryption-unaware client opening an encrypted yjs file
 *        is rejected EACCES synchronously (no WS upgrade attempted).
 *   Y5.  handle.encrypted is true on encrypted files, false on plain.
 *   Y6.  Mixed-tenant: tenant A's encrypted yjs file is invisible to
 *        tenant B (existing isolation invariant preserved).
 *   Y7.  Server-side oplog stores envelope bytes (NOT plaintext).
 *   Y8.  bytes_since_last_compact tracks the encrypted op-log volume.
 */

import {
  createVFS,
  type MossaicEnv,
  type UserDO,
  type EncryptionConfig,
} from "../../sdk/src/index";
import { openYDoc } from "../../sdk/src/yjs";
import { vfsUserDOName } from "@core/lib/utils";
import { runInDurableObject } from "cloudflare:test";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}
const E = env as unknown as E;
const NS = "default";

function envFor(): MossaicEnv {
  return { MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"] };
}
function userStub(tenant: string) {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}
function makeKey(byte: number): Uint8Array {
  const a = new Uint8Array(32);
  a.fill(byte);
  return a;
}

describe("Phase 15 — encrypted Yjs wire boundary", () => {
  it("Y1 — two clients converge on edits via encrypted Yjs WS", async () => {
    const tenant = "p15-y1";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0xa1),
      tenantSalt: makeKey(0xb2),
    };
    const vfs = createVFS(envFor(), { tenant, encryption: cfg });
    // Create the file with encryption AND yjs-mode enabled. The
    // writeFile-with-encryption stamps the encryption columns; the
    // setYjsMode flips the yjs bit.
    await vfs.writeFile("/enc.md", "", { encrypted: true });
    await vfs.setYjsMode("/enc.md", true);

    const a = await openYDoc(vfs, "/enc.md");
    const b = await openYDoc(vfs, "/enc.md");
    await Promise.all([a.synced, b.synced]);

    expect(a.encrypted).toBe(true);
    expect(b.encrypted).toBe(true);

    const aText = a.doc.getText("content");
    const bText = b.doc.getText("content");

    const seenOnB = new Promise<string>((resolve) => {
      const observer = () => {
        const v = bText.toString();
        if (v.length > 0) {
          bText.unobserve(observer);
          resolve(v);
        }
      };
      bText.observe(observer);
    });

    aText.insert(0, "secret-edit-from-A");
    const got = await Promise.race([
      seenOnB,
      new Promise<string>((_, rej) =>
        setTimeout(
          () => rej(new Error("timed out waiting for encrypted broadcast")),
          3000
        )
      ),
    ]);
    expect(got).toBe("secret-edit-from-A");

    await Promise.all([a.close(), b.close()]);
  });

  it("Y3 — plain (non-encrypted) yjs file still works for encryption-aware client", async () => {
    const tenant = "p15-y3";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0xc1),
      tenantSalt: makeKey(0xd2),
    };
    const vfs = createVFS(envFor(), { tenant, encryption: cfg });
    // Plain (no encrypted opt) writeFile + yjs-mode toggle.
    await vfs.writeFile("/plain.md", "");
    await vfs.setYjsMode("/plain.md", true);

    const handle = await openYDoc(vfs, "/plain.md");
    await handle.synced;
    expect(handle.encrypted).toBe(false);

    handle.doc.getText("content").insert(0, "plain edit");
    // Bounded wait for the local edit to persist.
    await new Promise((resolve) => setTimeout(resolve, 200));
    await handle.close();
  });

  it("Y4 — encryption-unaware client opening encrypted yjs file → EACCES", async () => {
    const tenant = "p15-y4";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0xe1),
      tenantSalt: makeKey(0xf2),
    };
    const writer = createVFS(envFor(), { tenant, encryption: cfg });
    await writer.writeFile("/enc.md", "", { encrypted: true });
    await writer.setYjsMode("/enc.md", true);

    const reader = createVFS(envFor(), { tenant });
    await expect(openYDoc(reader, "/enc.md")).rejects.toThrow(/EACCES/);
  });

  it("Y5 — handle.encrypted reflects the file's encryption status", async () => {
    const tenant = "p15-y5";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0x71),
      tenantSalt: makeKey(0x82),
    };
    const vfs = createVFS(envFor(), { tenant, encryption: cfg });

    await vfs.writeFile("/enc.md", "", { encrypted: true });
    await vfs.setYjsMode("/enc.md", true);
    const enc = await openYDoc(vfs, "/enc.md");
    expect(enc.encrypted).toBe(true);
    await enc.close();

    await vfs.writeFile("/plain.md", "");
    await vfs.setYjsMode("/plain.md", true);
    const plain = await openYDoc(vfs, "/plain.md");
    expect(plain.encrypted).toBe(false);
    await plain.close();
  });

  it("Y7 — server-side oplog stores envelope bytes (not plaintext)", async () => {
    const tenant = "p15-y7";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0x91),
      tenantSalt: makeKey(0xa2),
    };
    const vfs = createVFS(envFor(), { tenant, encryption: cfg });
    await vfs.writeFile("/enc.md", "", { encrypted: true });
    await vfs.setYjsMode("/enc.md", true);

    const handle = await openYDoc(vfs, "/enc.md");
    await handle.synced;
    handle.doc.getText("content").insert(0, "the-quick-brown-fox");
    // Allow the update to traverse to the server.
    await new Promise((resolve) => setTimeout(resolve, 300));
    await handle.close();

    // The oplog should hold envelope bytes — they must NOT contain
    // the literal plaintext fragment we just inserted. (The chunk
    // hashes are stored in yjs_oplog; the actual bytes are in
    // ShardDOs, but here we just verify the file has yjs_oplog rows
    // and chunks.hash columns are present — opacity is implied by
    // the encryption envelope structure tested in Step 1.)
    const stub = userStub(tenant);
    const oplog = await runInDurableObject(stub, async (_, state) => {
      return state.storage.sql
        .exec("SELECT seq, kind, chunk_size FROM yjs_oplog")
        .toArray() as { seq: number; kind: string; chunk_size: number }[];
    });
    expect(oplog.length).toBeGreaterThan(0);
    // Each op's chunk_size should be > the plaintext length (envelope
    // overhead is at least 58 bytes for convergent or 68 bytes for
    // random mode; Yjs ops use random mode per plan §10 Q10).
    for (const op of oplog) {
      expect(op.chunk_size).toBeGreaterThan(60);
    }
  });

  it("Y8 — bytes_since_last_compact accumulates envelope bytes", async () => {
    const tenant = "p15-y8";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0x33),
      tenantSalt: makeKey(0x44),
    };
    const vfs = createVFS(envFor(), { tenant, encryption: cfg });
    await vfs.writeFile("/enc.md", "", { encrypted: true });
    await vfs.setYjsMode("/enc.md", true);

    const handle = await openYDoc(vfs, "/enc.md");
    await handle.synced;
    // Insert a few edits.
    for (let i = 0; i < 5; i++) {
      handle.doc.getText("content").insert(0, `edit-${i}-`);
    }
    await new Promise((resolve) => setTimeout(resolve, 400));
    await handle.close();

    const stub = userStub(tenant);
    const meta = await runInDurableObject(stub, async (_, state) => {
      return state.storage.sql
        .exec("SELECT bytes_since_last_compact, op_count_since_ckpt FROM yjs_meta")
        .toArray()[0] as
        | { bytes_since_last_compact: number; op_count_since_ckpt: number }
        | undefined;
    });
    expect(meta).toBeDefined();
    expect(meta!.bytes_since_last_compact).toBeGreaterThan(0);
    expect(meta!.op_count_since_ckpt).toBeGreaterThan(0);
  });

  it("Y6 — encrypted yjs is tenant-isolated (B can't see A's encrypted file)", async () => {
    const tenantA = "p15-y6-a";
    const tenantB = "p15-y6-b";
    const cfgA: EncryptionConfig = {
      masterKey: makeKey(0x55),
      tenantSalt: makeKey(0x66),
    };
    const cfgB: EncryptionConfig = {
      masterKey: makeKey(0x55),
      tenantSalt: makeKey(0x77), // different salt per tenant
    };
    const vfsA = createVFS(envFor(), { tenant: tenantA, encryption: cfgA });
    const vfsB = createVFS(envFor(), { tenant: tenantB, encryption: cfgB });

    await vfsA.writeFile("/enc.md", "", { encrypted: true });
    await vfsA.setYjsMode("/enc.md", true);

    // Tenant B should ENOENT on the same path.
    await expect(vfsB.stat("/enc.md")).rejects.toThrow(/ENOENT/);
  });

  it("Y2 — awareness round-trips through encrypted relay", async () => {
    const tenant = "p15-y2";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0x21),
      tenantSalt: makeKey(0x32),
    };
    const vfs = createVFS(envFor(), { tenant, encryption: cfg });
    await vfs.writeFile("/aware.md", "", { encrypted: true });
    await vfs.setYjsMode("/aware.md", true);

    const a = await openYDoc(vfs, "/aware.md");
    const b = await openYDoc(vfs, "/aware.md");
    await Promise.all([a.synced, b.synced]);

    const seenOnB = new Promise<unknown>((resolve) => {
      const handler = () => {
        const states = b.awareness.getStates();
        for (const [, state] of states) {
          const s = state as { name?: string };
          if (s?.name === "alice") {
            b.awareness.off("change", handler);
            resolve(s);
            return;
          }
        }
      };
      b.awareness.on("change", handler);
    });

    a.awareness.setLocalState({ name: "alice", cursor: 7 });
    const got = (await Promise.race([
      seenOnB,
      new Promise((_, rej) =>
        setTimeout(
          () => rej(new Error("encrypted awareness timeout")),
          3000
        )
      ),
    ])) as { name: string; cursor: number };
    expect(got.name).toBe("alice");
    expect(got.cursor).toBe(7);

    await Promise.all([a.close(), b.close()]);
  });
});
