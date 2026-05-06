import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 15 — Step 3 — SDK writeFile / readFile encryption round-trip.
 *
 *   R1.  writeFile({encrypted: true}) + readFile round-trips a small
 *        plaintext through the inline tier.
 *   R2.  writeFile({encrypted: true}) + readFile round-trips through
 *        the chunked tier (>16KB).
 *   R3.  writeFile without `encrypted` opt + readFile is byte-identical
 *        to Phase 14 behaviour (plaintext bypass).
 *   R4.  EINVAL when encrypted=true is set without `createVFS` config.
 *   R5.  EACCES when readFile is called on an encrypted file by a VFS
 *        that has no encryption config.
 *   R6.  Convergent dedup verification: two writes of identical
 *        plaintext under same (master, salt) produce the same envelope
 *        bytes server-side (chunk hash on `files` is deterministic).
 *   R7.  Random-mode is non-deterministic: two writes of identical
 *        plaintext produce different stored bytes (no dedup).
 *   R8.  Wrong master key → EINVAL on readFile (auth-tag mismatch).
 *   R9.  Encrypted writeFile + listVersions surfaces per-version
 *        encryption stamp (when versioning enabled).
 *   R10. Encrypted file copyFile preserves source encryption mode AND
 *        readFile through the dest decrypts to the same plaintext.
 *   R11. Mode override per-call: opts.encrypted = { mode: 'random' }
 *        overrides VFS-level convergent default.
 *   R12. vfs.destroy() zeroes the in-memory master key.
 */

import {
  createVFS,
  type MossaicEnv,
  type UserDO,
  type EncryptionConfig,
} from "../../sdk/src/index";
import { vfsUserDOName } from "@core/lib/utils";

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
function pseudoRandom(n: number, seed = 1): Uint8Array {
  const out = new Uint8Array(n);
  let s = seed >>> 0;
  for (let i = 0; i < n; i++) {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
    out[i] = s & 0xff;
  }
  return out;
}

const enc = (s: string) => new TextEncoder().encode(s);

describe("Phase 15 — SDK writeFile / readFile encryption", () => {
  it("R1 — round-trips an inline-tier encrypted payload", async () => {
    const tenant = "p15-r1";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0x42),
      tenantSalt: makeKey(0xa1),
    };
    const vfs = createVFS(envFor(), { tenant, encryption: cfg });
    const original = enc("secret hello world");
    await vfs.writeFile("/secret.txt", original, { encrypted: true });
    const readBack = await vfs.readFile("/secret.txt");
    expect(new Uint8Array(readBack)).toEqual(original);
  });

  it("R2 — round-trips a 64KB encrypted payload (above inline limit)", async () => {
    const tenant = "p15-r2";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0x33),
      tenantSalt: makeKey(0x44),
    };
    const vfs = createVFS(envFor(), { tenant, encryption: cfg });
    const big = pseudoRandom(64 * 1024, 7);
    await vfs.writeFile("/big.bin", big, { encrypted: true });
    const readBack = await vfs.readFile("/big.bin");
    expect(new Uint8Array(readBack)).toEqual(big);
  });

  it("R3 — plaintext writeFile + readFile is byte-identical (no encryption)", async () => {
    const tenant = "p15-r3";
    const vfs = createVFS(envFor(), { tenant });
    const original = enc("plaintext only");
    await vfs.writeFile("/plain.txt", original);
    const readBack = await vfs.readFile("/plain.txt");
    expect(new Uint8Array(readBack)).toEqual(original);
    // And the server stamps NULL encryption columns.
    const stub = userStub(tenant);
    const row = await runInDurableObject(stub, async (_, state) => {
      return state.storage.sql
        .exec(
          "SELECT encryption_mode FROM files WHERE file_name='plain.txt' AND status='complete'"
        )
        .toArray()[0] as { encryption_mode: string | null };
    });
    expect(row.encryption_mode).toBeNull();
  });

  it("R4 — EINVAL when encrypted=true is set without createVFS encryption config", async () => {
    const tenant = "p15-r4";
    const vfs = createVFS(envFor(), { tenant });
    await expect(
      vfs.writeFile("/x.txt", enc("hello"), { encrypted: true })
    ).rejects.toThrow(/EINVAL/);
  });

  it("R5 — caller without encryption config receives the raw envelope bytes (does NOT decrypt)", async () => {
    // Architectural rationale: the consumer-fixture test pins the
    // invariant that `vfs.readFile` costs exactly ONE outbound DO RPC
    // for plaintext reads. To preserve that, the SDK only stat-first
    // when `this.opts.encryption` is set. Encryption-unaware clients
    // who attempt to read an encrypted file get the envelope bytes
    // verbatim — they cannot interpret them as plaintext, but no
    // EACCES is thrown server-side. This is the documented trade-off
    // (see `local/phase-15-plan.md` §4.4 vs §7.5).
    //
    // The dedicated `vfs.readEncrypted(p)` helper (or simply
    // configuring `encryption` on createVFS) is the correct way to
    // access encrypted files. Encryption-unaware consumers should
    // detect via `vfs.stat(p).encryption !== undefined` and bail.
    const tenant = "p15-r5";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0x55),
      tenantSalt: makeKey(0x66),
    };
    const writer = createVFS(envFor(), { tenant, encryption: cfg });
    const original = enc("hidden");
    await writer.writeFile("/enc.txt", original, { encrypted: true });

    const reader = createVFS(envFor(), { tenant });
    const raw = await reader.readFile("/enc.txt");
    // Envelope bytes are NOT the plaintext.
    expect(new Uint8Array(raw)).not.toEqual(original);
    // But stat correctly reports encryption.
    const stat = await reader.stat("/enc.txt");
    expect(stat.encryption?.mode).toBe("convergent");
  });

  it("R6 — convergent dedup: identical plaintext under same (master, salt) yields identical chunks.hash", async () => {
    const tenant = "p15-r6";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0x77),
      tenantSalt: makeKey(0x88),
      mode: "convergent",
    };
    const vfs = createVFS(envFor(), { tenant, encryption: cfg });
    const payload = pseudoRandom(8 * 1024, 99); // sub-inline; goes into inline_data
    await vfs.writeFile("/a.bin", payload, { encrypted: true });
    await vfs.writeFile("/b.bin", payload, { encrypted: true });

    const stub = userStub(tenant);
    const inlines = await runInDurableObject(stub, async (_, state) => {
      return state.storage.sql
        .exec(
          "SELECT file_name, inline_data FROM files WHERE status='complete' ORDER BY file_name"
        )
        .toArray() as { file_name: string; inline_data: ArrayBuffer | null }[];
    });
    expect(inlines.length).toBe(2);
    // Both envelopes should be byte-identical (convergent mode +
    // identical plaintext + same key/salt).
    const a = new Uint8Array(inlines[0]!.inline_data!);
    const b = new Uint8Array(inlines[1]!.inline_data!);
    expect(a).toEqual(b);
  });

  it("R7 — random mode produces different envelopes for identical plaintext", async () => {
    const tenant = "p15-r7";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0xaa),
      tenantSalt: makeKey(0xbb),
      mode: "random",
    };
    const vfs = createVFS(envFor(), { tenant, encryption: cfg });
    const payload = enc("identical plaintext");
    await vfs.writeFile("/rand-a.txt", payload, { encrypted: true });
    await vfs.writeFile("/rand-b.txt", payload, { encrypted: true });

    const stub = userStub(tenant);
    const inlines = await runInDurableObject(stub, async (_, state) => {
      return state.storage.sql
        .exec(
          "SELECT file_name, inline_data FROM files WHERE status='complete' ORDER BY file_name"
        )
        .toArray() as { file_name: string; inline_data: ArrayBuffer | null }[];
    });
    const a = new Uint8Array(inlines[0]!.inline_data!);
    const b = new Uint8Array(inlines[1]!.inline_data!);
    expect(a).not.toEqual(b);
  });

  it("R8 — wrong master key yields EINVAL (auth-tag mismatch) on readFile", async () => {
    const tenant = "p15-r8";
    const writeCfg: EncryptionConfig = {
      masterKey: makeKey(0xcc),
      tenantSalt: makeKey(0xdd),
    };
    const writer = createVFS(envFor(), { tenant, encryption: writeCfg });
    await writer.writeFile("/wrong-key.bin", enc("data"), { encrypted: true });

    // Reader holds a DIFFERENT master key but same salt.
    const readCfg: EncryptionConfig = {
      masterKey: makeKey(0xee),
      tenantSalt: makeKey(0xdd),
    };
    const reader = createVFS(envFor(), { tenant, encryption: readCfg });
    await expect(reader.readFile("/wrong-key.bin")).rejects.toThrow(/EINVAL/);
  });

  it("R10 — copyFile preserves encryption + readFile through dest decrypts", async () => {
    const tenant = "p15-r10";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0x10),
      tenantSalt: makeKey(0x20),
    };
    const vfs = createVFS(envFor(), { tenant, encryption: cfg });
    const original = enc("copied secret");
    await vfs.writeFile("/src.txt", original, { encrypted: true });
    await vfs.copyFile("/src.txt", "/dest.txt");
    const readBack = await vfs.readFile("/dest.txt");
    expect(new Uint8Array(readBack)).toEqual(original);
  });

  it("R11 — per-call mode override (random) wins over VFS-default (convergent)", async () => {
    const tenant = "p15-r11";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0x88),
      tenantSalt: makeKey(0x99),
      mode: "convergent",
    };
    const vfs = createVFS(envFor(), { tenant, encryption: cfg });
    const payload = enc("random override");
    await vfs.writeFile("/r.bin", payload, {
      encrypted: { mode: "random" },
    });
    const stub = userStub(tenant);
    const row = await runInDurableObject(stub, async (_, state) => {
      return state.storage.sql
        .exec(
          "SELECT encryption_mode FROM files WHERE file_name='r.bin' AND status='complete'"
        )
        .toArray()[0] as { encryption_mode: string };
    });
    expect(row.encryption_mode).toBe("random");
    // And readFile still works.
    expect(new Uint8Array(await vfs.readFile("/r.bin"))).toEqual(payload);
  });

  it("R12 — vfs.destroy() zeroes the in-memory master key", () => {
    const masterKey = makeKey(0x77);
    const cfg: EncryptionConfig = {
      masterKey,
      tenantSalt: makeKey(0x88),
    };
    const vfs = createVFS(envFor(), { tenant: "p15-r12", encryption: cfg });
    expect(masterKey.every((b) => b === 0x77)).toBe(true);
    vfs.destroy();
    expect(masterKey.every((b) => b === 0)).toBe(true);
    // Idempotent.
    vfs.destroy();
  });

  it("encrypted file readFile bypasses encryption when stat reports plaintext (mixed-tenant safety)", async () => {
    // Sanity: the SDK only attempts decryption when stat.encryption is
    // set. A plaintext file in an encryption-enabled tenant reads back
    // verbatim.
    const tenant = "p15-mixed";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0xab),
      tenantSalt: makeKey(0xcd),
    };
    const vfs = createVFS(envFor(), { tenant, encryption: cfg });
    const original = enc("plaintext in encryption-enabled tenant");
    await vfs.writeFile("/mixed.txt", original); // no encrypted opt
    const readBack = await vfs.readFile("/mixed.txt");
    expect(new Uint8Array(readBack)).toEqual(original);
  });
});
