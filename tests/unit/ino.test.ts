import { describe, it, expect } from "vitest";
import { gidFromTenant, inoFromId, uidFromTenant } from "@shared/ino";

/**
 * `ino` synthesis (sdk-impl-plan §9).
 *
 * Verifies stable 53-bit safe integers, deterministic per id, low
 * collision rate at the expected scale.
 */

describe("inoFromId", () => {
  it("is deterministic", () => {
    expect(inoFromId("abc")).toBe(inoFromId("abc"));
    expect(inoFromId("01J0000000XYZ")).toBe(inoFromId("01J0000000XYZ"));
  });

  it("returns a Number.MAX_SAFE_INTEGER-safe positive integer", () => {
    for (const id of [
      "",
      "a",
      "01J9ABCDEFGHJK",
      "verylongidwithlotsofcharsverylongidwithlotsofchars",
      "🦀-utf8-id-🚀",
    ]) {
      const ino = inoFromId(id);
      expect(Number.isInteger(ino)).toBe(true);
      expect(ino).toBeGreaterThanOrEqual(0);
      expect(ino).toBeLessThan(Number.MAX_SAFE_INTEGER);
    }
  });

  it("survives JSON round-trip without precision loss", () => {
    const ino = inoFromId("01J9ABCDEFGHJK");
    const round = JSON.parse(JSON.stringify({ ino }));
    expect(round.ino).toBe(ino);
  });

  it("collisions are rare across 100k random ids", () => {
    const N = 100_000;
    const seen = new Set<number>();
    let collisions = 0;
    for (let i = 0; i < N; i++) {
      // Mix of ULID-ish base36 timestamps + random suffix, mirroring
      // worker/lib/utils.ts:generateId.
      const id =
        Date.now().toString(36) +
        Math.random().toString(36).slice(2, 14) +
        i.toString(36);
      const ino = inoFromId(id);
      if (seen.has(ino)) collisions++;
      else seen.add(ino);
    }
    // Birthday at p=0.5 ≈ √(2 · 2^53) ≈ 1.34e8; at 1e5 we expect ≪ 1.
    // Allow some slack in case the hash distribution has minor bias.
    expect(collisions).toBeLessThan(5);
  });

  it("differs for very similar ids (avalanche)", () => {
    expect(inoFromId("a")).not.toBe(inoFromId("b"));
    expect(inoFromId("foo")).not.toBe(inoFromId("foo "));
    expect(inoFromId("file:1")).not.toBe(inoFromId("file:2"));
  });
});

describe("uidFromTenant / gidFromTenant", () => {
  it("are stable per tenant", () => {
    expect(uidFromTenant("acme")).toBe(uidFromTenant("acme"));
    expect(gidFromTenant("acme")).toBe(gidFromTenant("acme"));
  });

  it("uid and gid differ for the same tenant (different seeds)", () => {
    expect(uidFromTenant("acme")).not.toBe(gidFromTenant("acme"));
  });

  it("are 32-bit unsigned integers", () => {
    for (const t of ["acme", "globex", "🦀", ""]) {
      const u = uidFromTenant(t);
      const g = gidFromTenant(t);
      expect(Number.isInteger(u)).toBe(true);
      expect(u).toBeGreaterThanOrEqual(0);
      expect(u).toBeLessThanOrEqual(0xffffffff);
      expect(Number.isInteger(g)).toBe(true);
      expect(g).toBeGreaterThanOrEqual(0);
      expect(g).toBeLessThanOrEqual(0xffffffff);
    }
  });
});
