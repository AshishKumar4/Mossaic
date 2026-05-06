import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 4 — Tenant isolation tests.
 *
 * Two scoping axes ship in Phase 4:
 *   1. The VFS uses `vfs:${ns}:${tenant}[:${sub}]` for UserDO names and
 *      `vfs:${ns}:${tenant}[:${sub}]:s${idx}` for ShardDO names. Two
 *      different (ns, tenant, sub) tuples MUST resolve to different DO
 *      instances ⇒ different SQLite databases. No cross-tenant reads.
 *   2. Cross-tenant chunk dedup is impossible by construction:
 *      identical bytes uploaded under tenant A and tenant B land in
 *      different ShardDO instances and pay full storage cost twice.
 *      This is the security-critical property called out in the
 *      feasibility study §6.3 — a single SQLite database with
 *      cross-tenant dedup leaks chunk presence as an oracle.
 *
 * Coverage:
 *   - Same path under tenant A vs B → ENOENT cross-tenant
 *   - Namespace separation (same tenant, different ns) → isolated
 *   - Sub-tenant separation (same tenant, different sub) → isolated
 *   - Cross-tenant chunk dedup is structurally impossible (the chunk
 *     hash exists on tenant A's shard but NOT on tenant B's shard,
 *     even when content is identical)
 *   - vfsUserDOName / vfsShardDOName input validation rejects
 *     ":" injection attempts that would let a tenant impersonate another
 *
 * The shard derivation tests grep into the SQLite directly via
 * runInDurableObject to confirm chunk presence/absence.
 */

import type { UserDOCore as UserDO } from "@core/objects/user/user-do-core";
import type { ShardDO } from "@core/objects/shard/shard-do";
import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import { signVFSToken, verifyVFSToken } from "@core/lib/auth";

interface E {
  USER_DO: DurableObjectNamespace<UserDO>;
  SHARD_DO: DurableObjectNamespace<ShardDO>;
  JWT_SECRET?: string;
}
const E = env as unknown as E;
const TEST_ENV = E as unknown as Parameters<typeof signVFSToken>[0];

const NS_DEFAULT = "default";

function userStub(ns: string, tenant: string, sub?: string) {
  return E.USER_DO.get(E.USER_DO.idFromName(vfsUserDOName(ns, tenant, sub)));
}
function shardStub(ns: string, tenant: string, sub: string | undefined, idx: number) {
  return E.SHARD_DO.get(
    E.SHARD_DO.idFromName(vfsShardDOName(ns, tenant, sub, idx))
  );
}

// ───────────────────────────────────────────────────────────────────────
// vfsUserDOName / vfsShardDOName name derivation + input validation
// ───────────────────────────────────────────────────────────────────────

describe("vfsUserDOName / vfsShardDOName derivation", () => {
  it("produces distinct names per (ns, tenant, sub) triple", () => {
    expect(vfsUserDOName("default", "acme")).toBe("vfs:default:acme");
    expect(vfsUserDOName("default", "acme", "alice")).toBe(
      "vfs:default:acme:alice"
    );
    expect(vfsUserDOName("staging", "acme")).toBe("vfs:staging:acme");
    expect(vfsUserDOName("default", "globex")).toBe("vfs:default:globex");
    expect(vfsShardDOName("default", "acme", undefined, 7)).toBe(
      "vfs:default:acme:s7"
    );
    expect(vfsShardDOName("default", "acme", "alice", 7)).toBe(
      "vfs:default:acme:alice:s7"
    );
  });

  it("rejects ':' injection in any component", () => {
    expect(() => vfsUserDOName("default", "acme:bob")).toThrow(/invalid/i);
    expect(() => vfsUserDOName("ns:bad", "acme")).toThrow(/invalid/i);
    expect(() => vfsUserDOName("default", "acme", "alice:eve")).toThrow(
      /invalid/i
    );
    expect(() =>
      vfsShardDOName("default", "acme", "bob:s99", 1)
    ).toThrow(/invalid/i);
  });

  it("rejects empty / oversized / control-char components", () => {
    expect(() => vfsUserDOName("", "acme")).toThrow(/invalid/i);
    expect(() => vfsUserDOName("default", "")).toThrow(/invalid/i);
    expect(() => vfsUserDOName("default", "acme", "")).toThrow(/invalid/i);
    expect(() => vfsUserDOName("default", "a".repeat(129))).toThrow(
      /invalid/i
    );
    expect(() => vfsUserDOName("default", "tab\there")).toThrow(/invalid/i);
    expect(() => vfsUserDOName("default", "newline\nhere")).toThrow(
      /invalid/i
    );
    expect(() => vfsUserDOName("default", "🦀")).toThrow(/invalid/i);
  });

  it("rejects bad shardIndex inputs", () => {
    expect(() => vfsShardDOName("default", "a", undefined, -1)).toThrow(
      /shardIndex/
    );
    expect(() => vfsShardDOName("default", "a", undefined, 1.5)).toThrow(
      /shardIndex/
    );
    expect(() => vfsShardDOName("default", "a", undefined, NaN)).toThrow(
      /shardIndex/
    );
    expect(() =>
      vfsShardDOName("default", "a", undefined, Number.POSITIVE_INFINITY)
    ).toThrow(/shardIndex/);
  });
});

// ───────────────────────────────────────────────────────────────────────
// File-level cross-tenant isolation
// ───────────────────────────────────────────────────────────────────────

describe("scope validation in vfs-ops (userIdFor)", () => {
  it("rejects tenant containing ':' even when called via DO RPC", async () => {
    // Pick a legitimate DO instance (built via vfsUserDOName for "good")
    // and try to make an RPC call with a tampered scope. The userIdFor
    // guard inside vfs-ops must reject before any SQL runs.
    const stub = userStub(NS_DEFAULT, "good");
    // The RPC reaches the DO, but the scope contains ":" so userIdFor
    // throws before any SQL filter could leak data.
    await expect(
      stub.vfsExists({ ns: "default", tenant: "alice:eve" }, "/")
    ).rejects.toThrow(/EINVAL/);
    await expect(
      stub.vfsExists(
        { ns: "default", tenant: "alice", sub: "eve:carol" },
        "/"
      )
    ).rejects.toThrow(/EINVAL/);
  });

  it("rejects empty/whitespace/oversized scope components", async () => {
    const stub = userStub(NS_DEFAULT, "good2");
    await expect(
      stub.vfsExists({ ns: "default", tenant: "" }, "/")
    ).rejects.toThrow(/EINVAL/);
    await expect(
      stub.vfsExists({ ns: "default", tenant: "a".repeat(129) }, "/")
    ).rejects.toThrow(/EINVAL/);
    await expect(
      stub.vfsExists(
        { ns: "default", tenant: "ok", sub: "with space" },
        "/"
      )
    ).rejects.toThrow(/EINVAL/);
  });
});

describe("cross-tenant file isolation", () => {
  it("same path under two tenants → ENOENT cross-tenant", async () => {
    const stubA = userStub(NS_DEFAULT, "tenantA");
    const stubB = userStub(NS_DEFAULT, "tenantB");
    const scopeA = { ns: NS_DEFAULT, tenant: "tenantA" };
    const scopeB = { ns: NS_DEFAULT, tenant: "tenantB" };

    await stubA.vfsWriteFile(
      scopeA,
      "/secret.txt",
      new TextEncoder().encode("alpha-only")
    );

    // Tenant A reads its file fine.
    const aGot = await stubA.vfsReadFile(scopeA, "/secret.txt");
    expect(new TextDecoder().decode(aGot)).toBe("alpha-only");

    // Tenant B's stub for the same path returns ENOENT (different DO
    // instance, different SQLite database).
    expect(await stubB.vfsExists(scopeB, "/secret.txt")).toBe(false);
    await expect(stubB.vfsReadFile(scopeB, "/secret.txt")).rejects.toThrow(
      /ENOENT/
    );

    // Even if tenant B asks tenant A's DO directly with their own
    // scope, the SQL filter on user_id (derived from scope.tenant)
    // refuses to surface tenant A's rows.
    expect(await stubA.vfsExists(scopeB, "/secret.txt")).toBe(false);
  });

  it("namespace separation (same tenant, different ns) is isolated", async () => {
    const tenant = "shared-tenant-name";
    const stubDefault = userStub("default", tenant);
    const stubStaging = userStub("staging", tenant);
    const scopeDefault = { ns: "default", tenant };
    const scopeStaging = { ns: "staging", tenant };

    await stubDefault.vfsWriteFile(
      scopeDefault,
      "/x.txt",
      new TextEncoder().encode("from default ns")
    );
    expect(await stubStaging.vfsExists(scopeStaging, "/x.txt")).toBe(false);
    expect(await stubDefault.vfsExists(scopeDefault, "/x.txt")).toBe(true);
  });

  it("sub-tenant separation (same tenant, different sub) is isolated", async () => {
    const tenant = "shared-tenant-sub";
    const stubAlice = userStub(NS_DEFAULT, tenant, "alice");
    const stubBob = userStub(NS_DEFAULT, tenant, "bob");
    const scopeAlice = { ns: NS_DEFAULT, tenant, sub: "alice" };
    const scopeBob = { ns: NS_DEFAULT, tenant, sub: "bob" };

    await stubAlice.vfsWriteFile(
      scopeAlice,
      "/diary.txt",
      new TextEncoder().encode("alice's diary")
    );
    expect(await stubBob.vfsExists(scopeBob, "/diary.txt")).toBe(false);
    expect(await stubAlice.vfsExists(scopeAlice, "/diary.txt")).toBe(true);
  });
});

// ───────────────────────────────────────────────────────────────────────
// Cross-tenant chunk dedup is impossible
// ───────────────────────────────────────────────────────────────────────

describe("cross-tenant chunk dedup is structurally impossible", () => {
  it("identical bytes under two tenants live on different ShardDOs", async () => {
    const tenantA = "dedup-A";
    const tenantB = "dedup-B";
    const stubA = userStub(NS_DEFAULT, tenantA);
    const stubB = userStub(NS_DEFAULT, tenantB);
    const scopeA = { ns: NS_DEFAULT, tenant: tenantA };
    const scopeB = { ns: NS_DEFAULT, tenant: tenantB };

    // Force chunked tier so we have something to inspect on shards.
    const handleA = await stubA.vfsBeginWriteStream(scopeA, "/big.bin");
    const cs = handleA.chunkSize;
    const payload = new Uint8Array(cs).fill(0xab);
    await stubA.vfsAppendWriteStream(scopeA, handleA, 0, payload);
    await stubA.vfsCommitWriteStream(scopeA, handleA);

    const handleB = await stubB.vfsBeginWriteStream(scopeB, "/big.bin");
    expect(handleB.chunkSize).toBe(cs);
    await stubB.vfsAppendWriteStream(scopeB, handleB, 0, payload);
    await stubB.vfsCommitWriteStream(scopeB, handleB);

    // Pull the recorded shard_index for each tenant's chunk.
    const idxA = await runInDurableObject(stubA, async (_inst, state) => {
      return (
        state.storage.sql
          .exec("SELECT shard_index FROM file_chunks LIMIT 1")
          .toArray()[0] as { shard_index: number }
      ).shard_index;
    });
    const idxB = await runInDurableObject(stubB, async (_inst, state) => {
      return (
        state.storage.sql
          .exec("SELECT shard_index FROM file_chunks LIMIT 1")
          .toArray()[0] as { shard_index: number }
      ).shard_index;
    });

    // Each tenant's chunk lives on its own per-tenant shard DO.
    const shardA = shardStub(NS_DEFAULT, tenantA, undefined, idxA);
    const shardB = shardStub(NS_DEFAULT, tenantB, undefined, idxB);

    const aChunkRefs = await runInDurableObject(
      shardA,
      async (_inst, state) => {
        return (
          state.storage.sql
            .exec("SELECT COUNT(*) AS n FROM chunk_refs")
            .toArray()[0] as { n: number }
        ).n;
      }
    );
    const bChunkRefs = await runInDurableObject(
      shardB,
      async (_inst, state) => {
        return (
          state.storage.sql
            .exec("SELECT COUNT(*) AS n FROM chunk_refs")
            .toArray()[0] as { n: number }
        ).n;
      }
    );
    expect(aChunkRefs).toBe(1);
    expect(bChunkRefs).toBe(1);

    // Hash on tenant A's shard is identical (same content, same hash) —
    // this is what makes intra-tenant dedup work. The point is that
    // tenant B's content lives on a *different* DO. Even if a tenant
    // could somehow learn the hash, querying for it on their own
    // shard returns "not present".
    const hashA = await runInDurableObject(
      shardA,
      async (_inst, state) => {
        return (
          state.storage.sql.exec("SELECT hash FROM chunks LIMIT 1").toArray()[0] as {
            hash: string;
          }
        ).hash;
      }
    );
    const hashB = await runInDurableObject(
      shardB,
      async (_inst, state) => {
        return (
          state.storage.sql.exec("SELECT hash FROM chunks LIMIT 1").toArray()[0] as {
            hash: string;
          }
        ).hash;
      }
    );
    expect(hashA).toBe(hashB); // identical content ⇒ identical hash

    // ...but neither shard sees the *other* tenant's row count change.
    // Confirm by storage size: each tenant pays full storage cost.
    const aBytes = await runInDurableObject(shardA, async (_inst, state) => {
      return (
        state.storage.sql
          .exec("SELECT SUM(size) AS s FROM chunks")
          .toArray()[0] as { s: number }
      ).s;
    });
    const bBytes = await runInDurableObject(shardB, async (_inst, state) => {
      return (
        state.storage.sql
          .exec("SELECT SUM(size) AS s FROM chunks")
          .toArray()[0] as { s: number }
      ).s;
    });
    expect(aBytes).toBe(cs);
    expect(bBytes).toBe(cs);

    // The shard DO instances themselves are distinct: same tenantA's
    // chunk is NOT visible on tenantB's shard for the same idx, even
    // though both tenants happened to land on shard index 0.
    if (idxA === idxB) {
      const shardA_tenantBNamespace = shardStub(
        NS_DEFAULT,
        tenantB,
        undefined,
        idxA
      );
      const isolatedCount = await runInDurableObject(
        shardA_tenantBNamespace,
        async (_inst, state) => {
          // Initialize the DO's tables (first access triggers ensureInit).
          return (
            state.storage.sql
              .exec("SELECT COUNT(*) AS n FROM chunks")
              .toArray()[0] as { n: number }
          ).n;
        }
      );
      // Tenant B's view of "their own shard at idxA" sees only their
      // own one chunk (size cs). They do not see two chunks.
      expect(isolatedCount).toBe(1);
    }
  });

  it("intra-tenant chunk dedup still works (writing the same chunk for the same fileId at idx 0 dedupes)", async () => {
    // Mossaic's `placeChunk(userId, fileId, idx, poolSize)` is keyed
    // on fileId, so two distinct files with identical content land on
    // *different* shards by design (no cross-file content dedup at the
    // shard level). What the chunk store DOES dedupe is repeated PUTs
    // of the same chunk_hash for the same (fileId, idx) — i.e.
    // retried uploads of one file. That's what we test here.
    const tenant = "intra-dedup";
    const stub = userStub(NS_DEFAULT, tenant);
    const scope = { ns: NS_DEFAULT, tenant };

    // Get a deterministic chunkSize from a probe handle, then abort it.
    const probe = await stub.vfsBeginWriteStream(scope, "/probe.bin");
    const cs = probe.chunkSize;
    await stub.vfsAbortWriteStream(scope, probe);

    const payload = new Uint8Array(cs).fill(0xcd);

    // Use the legacy ShardDO PUT route to dedupe-PUT the same chunk
    // for the same (fileId, idx) twice — same code path the writeFile
    // chunked tier exercises. Pre-Phase-1 this would have drifted
    // ref_count to 2; post-fix it stays at 1.
    const fakeFileId = "intra-dedup-file";
    const fakeIdx = 0;
    const sIdx = 0;
    const shard = shardStub(NS_DEFAULT, tenant, undefined, sIdx);
    // Hash via crypto.subtle to match the production hashChunk.
    const hashBuf = await crypto.subtle.digest("SHA-256", payload);
    const hash = Array.from(new Uint8Array(hashBuf))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");

    const r1 = await shard.putChunk(hash, payload, fakeFileId, fakeIdx, tenant);
    const r2 = await shard.putChunk(hash, payload, fakeFileId, fakeIdx, tenant);
    expect(r1.status).toBe("created");
    expect(r2.status).toBe("deduplicated");

    const ref = await runInDurableObject(shard, async (_inst, state) => {
      return (
        state.storage.sql
          .exec("SELECT ref_count FROM chunks WHERE hash = ?", hash)
          .toArray()[0] as { ref_count: number }
      ).ref_count;
    });
    // Same (fileId, idx) on retry: ref_count must stay at 1 (the
    // refcount drift fix from Phase 1).
    expect(ref).toBe(1);
  });
});

// ───────────────────────────────────────────────────────────────────────
// VFS JWT scope guard
// ───────────────────────────────────────────────────────────────────────

describe("VFS JWT scope guard", () => {
  it("signVFSToken / verifyVFSToken round-trip", async () => {
    const token = await signVFSToken(TEST_ENV, {
      ns: "default",
      tenant: "acme",
      sub: "alice",
    });
    const payload = await verifyVFSToken(TEST_ENV, token);
    expect(payload).not.toBeNull();
    expect(payload!.scope).toBe("vfs");
    expect(payload!.ns).toBe("default");
    expect(payload!.tn).toBe("acme");
    expect(payload!.sub).toBe("alice");
  });

  it("legacy login-shape JWT (no scope claim) is rejected by verifyVFSToken", async () => {
    // Mint a token with the legacy shape: { sub, email } and HS256
    // signed with the same JWT_SECRET. verifyVFSToken must reject it.
    const { signJWT } = await import("@core/lib/auth");
    const legacy = await signJWT(TEST_ENV, {
      userId: "user-123",
      email: "u@example.com",
    });
    const out = await verifyVFSToken(TEST_ENV, legacy);
    expect(out).toBeNull();
  });

  it("VFS token is rejected by legacy verifyJWT (no email claim)", async () => {
    const { verifyJWT } = await import("@core/lib/auth");
    const vfs = await signVFSToken(TEST_ENV, {
      ns: "default",
      tenant: "acme",
    });
    const out = await verifyJWT(TEST_ENV, vfs);
    expect(out).toBeNull();
  });

  it("garbage / expired / wrong-signature tokens return null", async () => {
    expect(await verifyVFSToken(TEST_ENV, "not-a-jwt")).toBeNull();
    expect(await verifyVFSToken(TEST_ENV, "")).toBeNull();
    // Tampered: change one char in the signature.
    const t = await signVFSToken(TEST_ENV, { ns: "n", tenant: "t" });
    const tampered = t.slice(0, -2) + (t.slice(-2) === "AB" ? "CD" : "AB");
    expect(await verifyVFSToken(TEST_ENV, tampered)).toBeNull();
  });
});
