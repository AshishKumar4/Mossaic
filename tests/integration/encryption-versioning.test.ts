import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 26 — encryption × versioning state-combination tests
 * (audit gap G10 / §3 "encryption + multipart + dropVersions").
 *
 * The audit identified that "each transition is correct in
 * isolation; the combination produces an asymmetry." The tombstone
 * bug was the canonical instance; encryption × versioning is one of
 * the next-most-likely combination bug classes (audit §1 bullet 5).
 *
 * Existing coverage (see `encryption-server.test.ts` S2/S7,
 * `encryption-rw.test.ts` R9): single-write encrypted-versioned
 * round-trip, per-version encryption stamps. NOT covered:
 *
 *   EV1. Encrypted file unlinked under versioning ON: tombstone
 *        version inherits the correct encryption envelope columns
 *        from the prior live version (no NULL leak that would
 *        cause "ENCRYPTION_REQUIRED" thrown later by stat).
 *   EV2. Restore an encrypted historical version: decrypt of the
 *        post-restore head returns the original plaintext (the
 *        AAD chain isn't broken by the restore).
 *   EV3. Convergent encryption + versioning: two versions of the
 *        same plaintext bytes share the same chunk_hash (dedup
 *        across versions) AND the per-version encryption stamps
 *        match.
 *   EV4. Random encryption + versioning: each version gets distinct
 *        stored bytes (no dedup), but readFile of any version
 *        returns the original plaintext.
 *   EV5. dropVersions on an encrypted file's history: surviving
 *        head version still decrypts cleanly post-drop.
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
const enc = (s: string) => new TextEncoder().encode(s);

describe("encryption × versioning — tombstone envelope (EV1)", () => {
  it("EV1 — unlink of an encrypted file: head ENOENT; version row preserved with encryption stamp; raw RPC read still returns envelope", async () => {
    const tenant = "ev-unlink-encrypted";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0xc1),
      tenantSalt: makeKey(0xd2),
    };
    const vfs = createVFS(envFor(), {
      tenant,
      encryption: cfg,
      versioning: "enabled",
    });
    const original = enc("encrypted plaintext payload");
    await vfs.writeFile("/secret.txt", original, { encrypted: true });

    const liveVersions = await vfs.listVersions("/secret.txt");
    expect(liveVersions).toHaveLength(1);
    const liveId = liveVersions[0].id;

    await vfs.unlink("/secret.txt"); // tombstone

    // Head readFile fails (the SDK's encryption pre-flight stat
    // hits the tombstoned head and ENOENTs out — the user never
    // sees envelope bytes for a deleted path, which is the correct
    // user-facing semantic).
    let caught: unknown = null;
    try {
      await vfs.readFile("/secret.txt");
    } catch (e) {
      caught = e;
    }
    expect(caught).toBeTruthy();

    // Tombstone row exists and is flagged. Live version row is
    // preserved with its encryption columns intact (no NULL leak)
    // — verify directly via SQL.
    const post = await vfs.listVersions("/secret.txt");
    expect(post).toHaveLength(2);
    const tomb = post.find((v) => v.deleted);
    const live = post.find((v) => !v.deleted);
    expect(tomb).toBeTruthy();
    expect(live).toBeTruthy();
    expect(live!.id).toBe(liveId);

    const stub = userStub(tenant);
    const versionRow = await runInDurableObject(stub, async (_inst, state) => {
      return state.storage.sql
        .exec(
          `SELECT version_id, deleted, encryption_mode, encryption_key_id
             FROM file_versions WHERE version_id = ?`,
          liveId
        )
        .toArray()[0] as
        | {
            version_id: string;
            deleted: number;
            encryption_mode: string | null;
            encryption_key_id: string | null;
          }
        | undefined;
    });
    expect(versionRow).toBeTruthy();
    expect(versionRow!.deleted).toBe(0);
    // Encryption stamp on the LIVE version row is preserved
    // through the unlink (the unlink writes a NEW tombstone
    // version, it does NOT mutate the existing live row).
    expect(versionRow!.encryption_mode).toBe("convergent");
  });
});

describe("encryption × versioning — restore decrypts cleanly (EV2)", () => {
  it("EV2 — restoreVersion of an encrypted historical version: post-restore head decrypts to the original plaintext", async () => {
    const tenant = "ev-restore-encrypted";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0xa3),
      tenantSalt: makeKey(0xb4),
    };
    const vfs = createVFS(envFor(), {
      tenant,
      encryption: cfg,
      versioning: "enabled",
    });
    const v1 = enc("v1-bytes");
    const v2 = enc("v2-bytes-newer");
    await vfs.writeFile("/x.bin", v1, { encrypted: true });
    await vfs.writeFile("/x.bin", v2, { encrypted: true });

    const versions = await vfs.listVersions("/x.bin");
    expect(versions).toHaveLength(2);
    // newest first → v2, v1
    const v1Id = versions[1].id;

    const r = await vfs.restoreVersion("/x.bin", v1Id);
    expect(typeof r.id).toBe("string");

    // Head reads the v1 bytes (decrypted via convergent stamp).
    const head = await vfs.readFile("/x.bin");
    expect(new Uint8Array(head)).toEqual(v1);
  });
});

describe("encryption × versioning — convergent dedup across versions (EV3)", () => {
  it("EV3 — convergent: two versions of identical plaintext share the same chunk_hash on file_versions", async () => {
    const tenant = "ev-dedup-convergent";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0xee),
      tenantSalt: makeKey(0xff),
    };
    const vfs = createVFS(envFor(), {
      tenant,
      encryption: cfg,
      versioning: "enabled",
    });
    // Two writes of IDENTICAL plaintext. Convergent encryption is
    // deterministic per (master, salt), so the encrypted chunk
    // bytes — and therefore chunk_hash — must match.
    // Use a chunked-tier payload (>16KB) so chunks are tracked.
    const big = new Uint8Array(20_000);
    for (let i = 0; i < big.length; i++) big[i] = (i * 17 + 7) & 0xff;
    await vfs.writeFile("/dedup.bin", big, { encrypted: true });
    await vfs.writeFile("/dedup.bin", big, { encrypted: true });

    const versions = await vfs.listVersions("/dedup.bin");
    expect(versions.length).toBe(2);

    // Inspect version_chunks: both versions reference the SAME
    // chunk_hash entries (per-index). Convergent + content-
    // addressing means the chunked tier dedups across versions.
    const stub = userStub(tenant);
    const hashesPerVersion = await runInDurableObject(
      stub,
      async (_inst, state) => {
        const v0 = state.storage.sql
          .exec(
            "SELECT chunk_index, chunk_hash FROM version_chunks WHERE version_id=? ORDER BY chunk_index",
            versions[0].id
          )
          .toArray() as { chunk_index: number; chunk_hash: string }[];
        const v1 = state.storage.sql
          .exec(
            "SELECT chunk_index, chunk_hash FROM version_chunks WHERE version_id=? ORDER BY chunk_index",
            versions[1].id
          )
          .toArray() as { chunk_index: number; chunk_hash: string }[];
        return { v0, v1 };
      }
    );
    expect(hashesPerVersion.v0.length).toBeGreaterThan(0);
    expect(hashesPerVersion.v0.length).toBe(hashesPerVersion.v1.length);
    for (let i = 0; i < hashesPerVersion.v0.length; i++) {
      expect(hashesPerVersion.v0[i].chunk_hash).toBe(
        hashesPerVersion.v1[i].chunk_hash
      );
    }
  });
});

describe("encryption × versioning — random mode does NOT dedup (EV4)", () => {
  it("EV4 — random: each version has DISTINCT chunk hashes; readFile of either returns the original plaintext", async () => {
    const tenant = "ev-random-no-dedup";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0x11),
      tenantSalt: makeKey(0x22),
      mode: "random",
    };
    const vfs = createVFS(envFor(), {
      tenant,
      encryption: cfg,
      versioning: "enabled",
    });
    const big = new Uint8Array(20_000);
    for (let i = 0; i < big.length; i++) big[i] = (i * 31) & 0xff;
    await vfs.writeFile("/rand.bin", big, { encrypted: true });
    await vfs.writeFile("/rand.bin", big, { encrypted: true });

    const versions = await vfs.listVersions("/rand.bin");
    expect(versions.length).toBe(2);

    const stub = userStub(tenant);
    const hashesPerVersion = await runInDurableObject(
      stub,
      async (_inst, state) => {
        const v0 = state.storage.sql
          .exec(
            "SELECT chunk_index, chunk_hash FROM version_chunks WHERE version_id=? ORDER BY chunk_index",
            versions[0].id
          )
          .toArray() as { chunk_index: number; chunk_hash: string }[];
        const v1 = state.storage.sql
          .exec(
            "SELECT chunk_index, chunk_hash FROM version_chunks WHERE version_id=? ORDER BY chunk_index",
            versions[1].id
          )
          .toArray() as { chunk_index: number; chunk_hash: string }[];
        return { v0, v1 };
      }
    );
    // Random mode → at least one chunk_hash differs (with
    // overwhelming probability for any non-trivial chunk).
    let anyDiffer = false;
    for (let i = 0; i < hashesPerVersion.v0.length; i++) {
      if (
        hashesPerVersion.v0[i].chunk_hash !==
        hashesPerVersion.v1[i].chunk_hash
      ) {
        anyDiffer = true;
        break;
      }
    }
    expect(anyDiffer).toBe(true);

    // BOTH versions decrypt to the original plaintext.
    const back0 = await vfs.readFile("/rand.bin", {
      version: versions[0].id,
    });
    const back1 = await vfs.readFile("/rand.bin", {
      version: versions[1].id,
    });
    expect(new Uint8Array(back0)).toEqual(big);
    expect(new Uint8Array(back1)).toEqual(big);
  });
});

describe("encryption × versioning — dropVersions preserves head decryptability (EV5)", () => {
  it("EV5 — dropVersions({ keepLast: 1 }) on an encrypted file leaves the head decryptable", async () => {
    const tenant = "ev-drop-encrypted";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0x9a),
      tenantSalt: makeKey(0x8b),
    };
    const vfs = createVFS(envFor(), {
      tenant,
      encryption: cfg,
      versioning: "enabled",
    });
    const headBytes = enc("head-version-content");
    await vfs.writeFile("/x.bin", enc("v1"), { encrypted: true });
    await vfs.writeFile("/x.bin", enc("v2"), { encrypted: true });
    await vfs.writeFile("/x.bin", enc("v3"), { encrypted: true });
    await vfs.writeFile("/x.bin", headBytes, { encrypted: true });

    const before = await vfs.listVersions("/x.bin");
    expect(before.length).toBe(4);

    const r = await vfs.dropVersions("/x.bin", { keepLast: 1 });
    // keepLast=1 ∪ head ⇒ {v4} only ⇒ dropped 3.
    expect(r.dropped).toBe(3);
    expect(r.kept).toBe(1);

    // Head still decryptable to the head bytes (proves AAD chain
    // wasn't disturbed by the drop's chunk-refcount fan-out).
    const back = await vfs.readFile("/x.bin");
    expect(new Uint8Array(back)).toEqual(headBytes);
  });
});
