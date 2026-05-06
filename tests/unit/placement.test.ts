import { describe, it, expect } from "vitest";
import {
  canonicalPlacement,
  legacyAppPlacement,
  placeChunk,
  shardDOName,
  type Placement,
} from "@shared/placement";
import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";

/**
 * Phase 17.5 — Placement abstraction unit tests.
 *
 *   P1.  canonicalPlacement.shardDOName matches vfsShardDOName(...) byte-exact
 *        for every (ns, tenant, sub, idx) combo.
 *   P2.  legacyAppPlacement.shardDOName matches shardDOName(...) byte-exact.
 *   P3.  canonicalPlacement.placeChunk returns the SAME integer as the
 *        existing placeChunk() for canonical inputs.
 *   P4.  legacyAppPlacement.placeChunk returns the SAME integer as
 *        canonicalPlacement.placeChunk for the SAME `(scope, fileId, idx,
 *        poolSize)` — score-template invariance per §1.4.
 *   P5.  Both placements satisfy injectivity of shardDOName: 1000 random
 *        (scope, idx) combos → no name collisions.
 *
 * The score-template invariance test (P4) is the load-bearing
 * regression guard: if anyone changes the legacy score template, this
 * test fires and prevents shipping a release that would orphan every
 * existing chunk.
 */

describe("Phase 17.5 — Placement contract", () => {
  it("P1 — canonicalPlacement.shardDOName matches vfsShardDOName byte-exact", () => {
    const cases = [
      { ns: "default", tenant: "alice", sub: undefined, idx: 0 },
      { ns: "default", tenant: "alice", sub: undefined, idx: 31 },
      { ns: "prod", tenant: "acme", sub: "user-1", idx: 7 },
      { ns: "ns-with-dot.x", tenant: "tenant_y", sub: "sub-z", idx: 100 },
      { ns: "default", tenant: "T", sub: undefined, idx: 0 },
    ];
    for (const c of cases) {
      const got = canonicalPlacement.shardDOName(
        { ns: c.ns, tenant: c.tenant, sub: c.sub },
        c.idx
      );
      const want = vfsShardDOName(c.ns, c.tenant, c.sub, c.idx);
      expect(got).toBe(want);
    }
  });

  it("P1b — canonicalPlacement.userDOName matches vfsUserDOName byte-exact", () => {
    const cases = [
      { ns: "default", tenant: "alice", sub: undefined },
      { ns: "prod", tenant: "acme", sub: "user-1" },
      { ns: "ns-with-dot.x", tenant: "tenant_y", sub: "sub-z" },
    ];
    for (const c of cases) {
      const got = canonicalPlacement.userDOName({
        ns: c.ns,
        tenant: c.tenant,
        sub: c.sub,
      });
      const want = vfsUserDOName(c.ns, c.tenant, c.sub);
      expect(got).toBe(want);
    }
  });

  it("P2 — legacyAppPlacement.shardDOName matches shardDOName(userId, idx) byte-exact", () => {
    const cases = [
      { tenant: "user-123", idx: 0 },
      { tenant: "user-123", idx: 5 },
      { tenant: "alice", idx: 31 },
      // Sub-tenant scope: legacy userId is `${tenant}::${sub}`.
      { tenant: "user-99", sub: "alice", idx: 7 },
    ] as const;
    for (const c of cases) {
      const scope = { ns: "default", tenant: c.tenant, sub: c.sub };
      const got = legacyAppPlacement.shardDOName(scope, c.idx);
      const userId = c.sub ? `${c.tenant}::${c.sub}` : c.tenant;
      const want = shardDOName(userId, c.idx);
      expect(got).toBe(want);
    }
  });

  it("P2b — legacyAppPlacement.userDOName matches `user:${userId}` byte-exact", () => {
    expect(
      legacyAppPlacement.userDOName({ ns: "default", tenant: "user-123" })
    ).toBe("user:user-123");
    expect(
      legacyAppPlacement.userDOName({
        ns: "default",
        tenant: "user-123",
        sub: "alice",
      })
    ).toBe("user:user-123::alice");
  });

  it("P3 — canonicalPlacement.placeChunk returns same integer as placeChunk() for canonical inputs", () => {
    const scope = { ns: "default", tenant: "acme", sub: "alice" };
    const userId = "acme::alice";
    for (const fileId of ["f1", "f2", "f3-x"]) {
      for (const idx of [0, 1, 5, 10, 31]) {
        for (const poolSize of [1, 4, 32, 64]) {
          const got = canonicalPlacement.placeChunk(
            scope,
            fileId,
            idx,
            poolSize
          );
          const want = placeChunk(userId, fileId, idx, poolSize);
          expect(got).toBe(want);
        }
      }
    }
  });

  it("P4 — score-template invariance: legacy and canonical placeChunk return the same integer for the same (scope, fileId, idx, poolSize)", () => {
    // The load-bearing regression guard. If anyone forks the score
    // template, this test fires.
    const scopes = [
      { ns: "default", tenant: "user-123" },
      { ns: "default", tenant: "user-99", sub: "alice" },
      { ns: "prod", tenant: "acme", sub: undefined },
    ];
    for (const scope of scopes) {
      for (const fileId of ["a", "b", "12345-abcd"]) {
        for (const idx of [0, 7, 31]) {
          for (const poolSize of [4, 32]) {
            const a = canonicalPlacement.placeChunk(
              scope,
              fileId,
              idx,
              poolSize
            );
            const b = legacyAppPlacement.placeChunk(
              scope,
              fileId,
              idx,
              poolSize
            );
            expect(a).toBe(b);
          }
        }
      }
    }
  });

  it("P5 — injectivity of shardDOName: 1000 random (scope, idx) combos → no collisions for either placement", () => {
    function rnd(): string {
      return Math.random().toString(36).slice(2, 10);
    }
    function makeCase() {
      return {
        ns: rnd(),
        tenant: rnd(),
        sub: Math.random() > 0.5 ? rnd() : undefined,
        idx: Math.floor(Math.random() * 200),
      };
    }
    for (const placement of [canonicalPlacement, legacyAppPlacement]) {
      const seen = new Set<string>();
      const dupes = new Set<string>();
      for (let i = 0; i < 1000; i++) {
        const c = makeCase();
        const name = placement.shardDOName(
          { ns: c.ns, tenant: c.tenant, sub: c.sub },
          c.idx
        );
        if (seen.has(name)) dupes.add(name);
        seen.add(name);
      }
      // The probability of a collision under random `rnd()` is
      // negligible; observing one indicates a bug in the name builder.
      expect([...dupes]).toEqual([]);
    }
  });

  it("Placement is structurally typed — both placements assign to type Placement", () => {
    // Structural typing smoke. If either placement drops a method, tsc
    // flags it; this test exists for runtime documentation.
    const all: Placement[] = [canonicalPlacement, legacyAppPlacement];
    expect(all.length).toBe(2);
    for (const p of all) {
      expect(typeof p.placeChunk).toBe("function");
      expect(typeof p.shardDOName).toBe("function");
      expect(typeof p.userDOName).toBe("function");
    }
  });
});
