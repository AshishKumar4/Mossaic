import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import { vfsUserDOName } from "@core/lib/utils";
import {
  OPERATION_CLAIM_COLUMNS_DDL,
  OPERATION_RETRY_COLUMNS_DDL,
  commitOperationTransition,
  drainReadyOperations,
  isPoisonous,
  isTerminalPhase,
  operationPhaseRank,
  retryDelayMs,
  runOperationPages,
  type OperationPage,
  type OperationUnit,
  type DurableSqlStore,
  type PagedOperationTable,
  type ReadyOperationBatch,
} from "@core/lib/paged-operation";
import type { UserDO } from "@app/objects/user/user-do";

/**
 * Oracle for the durable paged-operation control plane.
 *
 * The chunk cleanup outbox covers the claim plane through the fault-injection
 * suite, but it is a single-phase operation, so the phase lattice, the
 * lexicographic cursor rule and the terminal guard need their own scratch
 * operation to exercise. The table below is deliberately shaped like the ones
 * the real machines use: an identity, a claim state, a monotone phase, an
 * outer and an inner cursor, and the shared claim / retry column set.
 */

type ScratchPhase = "start" | "middle" | "end" | "done";

const SCRATCH_TABLE: PagedOperationTable<ScratchPhase> = {
  table: "test_paged_ops",
  keyColumns: ["op_id", "part"],
  phases: {
    column: "phase",
    forward: ["start", "middle", "end", "done"],
    terminal: ["done"],
  },
  cursorColumns: ["cursor_outer", "cursor_inner"],
  retry: { baseMs: 1_000, maxMs: 60_000, maxDoublings: 4 },
};

const CLAIM = {
  column: "state",
  ready: "ready",
  claimed: "claimed",
  leaseMs: 30_000,
} as const;

interface ScratchRow extends Record<string, SqlStorageValue> {
  op_id: string;
  part: number;
  state: string;
  phase: ScratchPhase;
  cursor_outer: number;
  cursor_inner: number;
  generation: number;
  attempts: number;
  next_attempt_at: number;
  last_error: string | null;
}

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}

const E = env as unknown as TestEnv;

function userStub(tenant: string): DurableObjectStub<UserDO> {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
  );
}

interface Scratch {
  readonly store: DurableSqlStore;
  seed(rows: readonly Partial<ScratchRow>[]): void;
  read(): ScratchRow[];
}

/** Run `body` against a private scratch table inside a real DO's SQLite. */
function withScratch<T>(
  tenant: string,
  body: (scratch: Scratch) => Promise<T> | T
): Promise<T> {
  return runInDurableObject(userStub(tenant), async (_instance, state) => {
    const sql = state.storage.sql;
    sql.exec(`
      CREATE TABLE IF NOT EXISTS test_paged_ops (
        op_id        TEXT NOT NULL,
        part         INTEGER NOT NULL,
        state        TEXT NOT NULL DEFAULT 'ready',
        ${OPERATION_CLAIM_COLUMNS_DDL},
        phase        TEXT NOT NULL DEFAULT 'start',
        cursor_outer INTEGER NOT NULL DEFAULT 0,
        cursor_inner INTEGER NOT NULL DEFAULT 0,
        created_at   INTEGER NOT NULL,
        ${OPERATION_RETRY_COLUMNS_DDL},
        PRIMARY KEY (op_id, part)
      )
    `);
    sql.exec("DELETE FROM test_paged_ops");
    const now = Date.now();
    return body({
      store: { storage: state.storage, sql },
      seed(rows) {
        for (const [index, row] of rows.entries()) {
          sql.exec(
            `INSERT INTO test_paged_ops
               (op_id, part, state, generation, phase, cursor_outer,
                cursor_inner, created_at, updated_at, next_attempt_at,
                attempts, last_error)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
            row.op_id ?? `op-${index}`,
            row.part ?? 0,
            row.state ?? CLAIM.ready,
            row.generation ?? 0,
            row.phase ?? "start",
            row.cursor_outer ?? 0,
            row.cursor_inner ?? 0,
            now + index,
            now,
            row.next_attempt_at ?? 0,
            row.attempts ?? 0
          );
        }
      },
      read() {
        return sql
          .exec<ScratchRow>(
            "SELECT * FROM test_paged_ops ORDER BY op_id, part"
          )
          .toArray();
      },
    });
  });
}

function unitOf(row: ScratchRow): OperationUnit<ScratchRow> {
  return [row];
}

describe("paged-operation retry and poison policies", () => {
  it("doubles the delay per failure and saturates at the ceiling", () => {
    const policy = { baseMs: 60_000, maxMs: 6 * 60 * 60 * 1000, maxDoublings: 16 };
    expect(retryDelayMs(policy, 1)).toBe(60_000);
    expect(retryDelayMs(policy, 2)).toBe(120_000);
    expect(retryDelayMs(policy, 4)).toBe(480_000);
    expect(retryDelayMs(policy, 40)).toBe(6 * 60 * 60 * 1000);
    // A zeroth failure cannot shorten the first delay.
    expect(retryDelayMs(policy, 0)).toBe(60_000);
  });

  it("stops doubling at maxDoublings before the ceiling is reached", () => {
    const policy = { baseMs: 1_000, maxMs: 10 * 60 * 1000, maxDoublings: 3 };
    expect(retryDelayMs(policy, 4)).toBe(8_000);
    expect(retryDelayMs(policy, 9)).toBe(8_000);
  });

  it("poisons only deterministic local corruption at or past the cap", () => {
    class LocalCorruption extends Error {}
    const policy = {
      maxAttempts: 3,
      isLocalCorruption: (error: unknown) => error instanceof LocalCorruption,
    };
    const remote = new Error("Network connection lost.");
    const local = new LocalCorruption("cursor is inconsistent");

    expect(isPoisonous(policy, local, 2)).toBe(false);
    expect(isPoisonous(policy, local, 3)).toBe(true);
    expect(isPoisonous(policy, remote, 3)).toBe(false);
    expect(isPoisonous(policy, remote, 3_000)).toBe(false);
  });
});

describe("paged-operation phase lattice", () => {
  it("ranks phases in declaration order and knows the terminal set", () => {
    const phases = SCRATCH_TABLE.phases;
    if (phases === undefined) throw new Error("scratch table declares phases");
    expect(operationPhaseRank(phases, "start")).toBe(0);
    expect(operationPhaseRank(phases, "done")).toBe(3);
    expect(isTerminalPhase(phases, "end")).toBe(false);
    expect(isTerminalPhase(phases, "done")).toBe(true);
    expect(() => operationPhaseRank(phases, "nonsense")).toThrow(
      /unknown phase/
    );
  });
});

describe("commitOperationTransition", () => {
  it("advances the phase and cursors it fenced on", async () => {
    await withScratch("paged-op-advance", (scratch) => {
      scratch.seed([{ op_id: "a", cursor_outer: 4 }]);
      const committed = commitOperationTransition(
        scratch.store,
        SCRATCH_TABLE,
        { op_id: "a", part: 0 },
        { phase: "start", cursor_outer: 4, generation: 0 },
        { phase: "middle", cursor_outer: 5, cursor_inner: 0, generation: 1 }
      );
      expect(committed).toBe(true);
      expect(scratch.read()).toMatchObject([
        { phase: "middle", cursor_outer: 5, cursor_inner: 0, generation: 1 },
      ]);
    });
  });

  it("reports a stale writer instead of overwriting", async () => {
    await withScratch("paged-op-stale", (scratch) => {
      scratch.seed([{ op_id: "a", generation: 7 }]);
      const committed = commitOperationTransition(
        scratch.store,
        SCRATCH_TABLE,
        { op_id: "a", part: 0 },
        { phase: "start", generation: 6 },
        { phase: "middle", generation: 7 }
      );
      expect(committed).toBe(false);
      expect(scratch.read()).toMatchObject([
        { phase: "start", generation: 7 },
      ]);
    });
  });

  it("resets an inner cursor only when an outer component advances", async () => {
    await withScratch("paged-op-lexicographic", (scratch) => {
      scratch.seed([{ op_id: "a", cursor_outer: 2, cursor_inner: 9 }]);
      expect(
        commitOperationTransition(
          scratch.store,
          SCRATCH_TABLE,
          { op_id: "a", part: 0 },
          { phase: "start", cursor_outer: 2, cursor_inner: 9 },
          { cursor_outer: 3, cursor_inner: 0 }
        )
      ).toBe(true);
      expect(scratch.read()).toMatchObject([
        { cursor_outer: 3, cursor_inner: 0 },
      ]);

      expect(() =>
        commitOperationTransition(
          scratch.store,
          SCRATCH_TABLE,
          { op_id: "a", part: 0 },
          { phase: "start", cursor_outer: 3, cursor_inner: 0 },
          { cursor_outer: 3, cursor_inner: -1 }
        )
      ).toThrow(/cursor_inner would regress/);
      expect(scratch.read()).toMatchObject([
        { cursor_outer: 3, cursor_inner: 0 },
      ]);
    });
  });

  it("rejects a phase regression and a reused generation", async () => {
    await withScratch("paged-op-regression", (scratch) => {
      scratch.seed([{ op_id: "a", phase: "end", generation: 2 }]);
      expect(() =>
        commitOperationTransition(
          scratch.store,
          SCRATCH_TABLE,
          { op_id: "a", part: 0 },
          { phase: "end" },
          { phase: "middle" }
        )
      ).toThrow(/phase would regress/);
      expect(() =>
        commitOperationTransition(
          scratch.store,
          SCRATCH_TABLE,
          { op_id: "a", part: 0 },
          { phase: "end", generation: 2 },
          { phase: "done", generation: 2 }
        )
      ).toThrow(/generation must advance/);
      expect(scratch.read()).toMatchObject([{ phase: "end", generation: 2 }]);
    });
  });

  it("refuses to move an operation out of a terminal phase", async () => {
    await withScratch("paged-op-terminal", (scratch) => {
      scratch.seed([{ op_id: "a", phase: "done" }]);
      expect(() =>
        commitOperationTransition(
          scratch.store,
          SCRATCH_TABLE,
          { op_id: "a", part: 0 },
          { phase: "done" },
          { phase: "done", cursor_outer: 1 }
        )
      ).toThrow(/phase 'done' is terminal/);
      expect(scratch.read()).toMatchObject([{ phase: "done", cursor_outer: 0 }]);
    });
  });
});

describe("runOperationPages", () => {
  it("stops on completion, on abandonment, and at the page bound", async () => {
    const calls: string[] = [];
    const pages = (outcomes: readonly OperationPage[]) => {
      let index = 0;
      return async (): Promise<OperationPage> => {
        calls.push(`page-${index}`);
        return outcomes[index++] ?? { kind: "advanced" };
      };
    };

    expect(
      await runOperationPages(5, pages([{ kind: "advanced" }, { kind: "completed" }]))
    ).toEqual({ done: true });
    expect(calls).toEqual(["page-0", "page-1"]);

    calls.length = 0;
    expect(
      await runOperationPages(5, pages([{ kind: "abandoned" }]))
    ).toEqual({ done: false });
    expect(calls).toEqual(["page-0"]);

    calls.length = 0;
    expect(await runOperationPages(2, pages([]))).toEqual({ done: false });
    expect(calls).toEqual(["page-0", "page-1"]);
  });
});

describe("drainReadyOperations", () => {
  it("discards or retains each completed row per its disposition", async () => {
    await withScratch("paged-op-drain-settle", async (scratch) => {
      scratch.seed([
        { op_id: "keep", part: 0 },
        { op_id: "drop", part: 0 },
      ]);
      const advanced: string[] = [];
      await drainReadyOperations<ScratchRow>(scratch.store, {
        table: SCRATCH_TABLE,
        claim: CLAIM,
        fanOut: 1,
        maxPages: 1,
        selectReady: (dueAt) => selectScratch(scratch, dueAt),
        advance: async (unit) => {
          advanced.push(unit[0].op_id);
          return { kind: "completed" };
        },
        disposition: (row) =>
          row.op_id === "keep"
            ? { kind: "retain", state: "settled" }
            : { kind: "discard" },
        armAlarmAt: async () => {},
      });
      expect(advanced).toEqual(["keep", "drop"]);
      expect(scratch.read()).toMatchObject([
        { op_id: "keep", state: "settled", generation: 2, last_error: null },
      ]);
    });
  });

  it("backs a failed unit off and arms the alarm at its deadline", async () => {
    await withScratch("paged-op-drain-failure", async (scratch) => {
      scratch.seed([{ op_id: "a", attempts: 2 }]);
      const armed: number[] = [];
      const before = Date.now();
      await drainReadyOperations<ScratchRow>(scratch.store, {
        table: SCRATCH_TABLE,
        claim: CLAIM,
        fanOut: 1,
        maxPages: 1,
        selectReady: (dueAt) => selectScratch(scratch, dueAt),
        advance: async () => {
          throw new Error("shard unreachable");
        },
        disposition: () => ({ kind: "discard" }),
        armAlarmAt: async (at) => {
          armed.push(at);
        },
      });

      const [row] = scratch.read();
      if (row === undefined) throw new Error("row survives a failure");
      expect(row).toMatchObject({
        state: CLAIM.ready,
        attempts: 3,
        generation: 2,
        last_error: "shard unreachable",
      });
      // Third failure of a 1s base with 4 doublings available.
      expect(row.next_attempt_at).toBeGreaterThanOrEqual(before + 4_000);
      expect(armed).toEqual([row.next_attempt_at]);
    });
  });

  it("persists an advanced page and runs the next one under a fresh claim", async () => {
    await withScratch("paged-op-drain-pages", async (scratch) => {
      scratch.seed([{ op_id: "a" }]);
      const seen: { phase: ScratchPhase; cursor: number; generation: number }[] =
        [];
      await drainReadyOperations<ScratchRow>(scratch.store, {
        table: SCRATCH_TABLE,
        claim: CLAIM,
        fanOut: 1,
        maxPages: 3,
        selectReady: (dueAt) => selectScratch(scratch, dueAt),
        advance: async (unit) => {
          const row = unit[0];
          seen.push({
            phase: row.phase,
            cursor: row.cursor_outer,
            generation: row.generation,
          });
          if (seen.length === 1) {
            return { kind: "advanced", next: { cursor_outer: 1 } };
          }
          if (seen.length === 2) {
            return {
              kind: "advanced",
              next: { phase: "middle", cursor_outer: 0 },
            };
          }
          return { kind: "completed" };
        },
        disposition: () => ({ kind: "retain", state: "settled" }),
        armAlarmAt: async () => {},
      });

      // Each page sees what the previous one persisted, and each claim plus
      // release burns two generations.
      expect(seen).toEqual([
        { phase: "start", cursor: 0, generation: 1 },
        { phase: "start", cursor: 1, generation: 3 },
        { phase: "middle", cursor: 0, generation: 5 },
      ]);
      expect(scratch.read()).toMatchObject([
        {
          op_id: "a",
          state: "settled",
          phase: "middle",
          cursor_outer: 0,
          generation: 6,
        },
      ]);
    });
  });

  it("rejects a page that tries to rewind its own cursor", async () => {
    await withScratch("paged-op-drain-rewind", async (scratch) => {
      scratch.seed([{ op_id: "a", cursor_outer: 3 }]);
      await expect(
        drainReadyOperations<ScratchRow>(scratch.store, {
          table: SCRATCH_TABLE,
          claim: CLAIM,
          fanOut: 1,
          maxPages: 2,
          selectReady: (dueAt) => selectScratch(scratch, dueAt),
          advance: async () => ({
            kind: "advanced",
            next: { cursor_outer: 2 },
          }),
          disposition: () => ({ kind: "discard" }),
          armAlarmAt: async () => {},
        })
      ).rejects.toThrow(/cursor_outer would regress/);
      // The claim survives with its lease, so a later invocation retries it.
      expect(scratch.read()).toMatchObject([
        { op_id: "a", state: CLAIM.claimed, cursor_outer: 3, generation: 1 },
      ]);
    });
  });

  it("reclaims an expired lease and fences the straggler out", async () => {
    await withScratch("paged-op-drain-lease", async (scratch) => {
      scratch.seed([
        {
          op_id: "a",
          state: CLAIM.claimed,
          generation: 4,
          next_attempt_at: 1,
        },
      ]);
      await drainReadyOperations<ScratchRow>(scratch.store, {
        table: SCRATCH_TABLE,
        claim: CLAIM,
        fanOut: 1,
        maxPages: 1,
        // The reclaim runs before selection, so an empty batch still proves
        // the lease came back.
        selectReady: () => ({ units: [], saturated: false }),
        advance: async () => ({ kind: "completed" }),
        disposition: () => ({ kind: "discard" }),
        armAlarmAt: async () => {},
      });

      expect(scratch.read()).toMatchObject([
        { op_id: "a", state: CLAIM.ready, generation: 5 },
      ]);
      // The straggler still believes it holds generation 4.
      expect(
        commitOperationTransition(
          scratch.store,
          SCRATCH_TABLE,
          { op_id: "a", part: 0 },
          { state: CLAIM.claimed, generation: 4 },
          { state: "settled", generation: 5 }
        )
      ).toBe(false);
      expect(scratch.read()).toMatchObject([
        { op_id: "a", state: CLAIM.ready, generation: 5 },
      ]);
    });
  });

  it("skips a unit whose claim was taken by another writer", async () => {
    await withScratch("paged-op-drain-lost-claim", async (scratch) => {
      scratch.seed([{ op_id: "a" }]);
      const rows = scratch.read();
      const stale = rows[0];
      if (stale === undefined) throw new Error("seeded row");
      scratch.store.sql.exec(
        "UPDATE test_paged_ops SET generation = 9 WHERE op_id = 'a'"
      );

      let advances = 0;
      const armed: number[] = [];
      await drainReadyOperations<ScratchRow>(scratch.store, {
        table: SCRATCH_TABLE,
        claim: CLAIM,
        fanOut: 1,
        maxPages: 1,
        selectReady: () => ({ units: [unitOf(stale)], saturated: false }),
        advance: async () => {
          advances++;
          return { kind: "completed" };
        },
        disposition: () => ({ kind: "discard" }),
        armAlarmAt: async (at) => {
          armed.push(at);
        },
      });

      expect(advances).toBe(0);
      expect(armed).toEqual([]);
      expect(scratch.read()).toMatchObject([
        { op_id: "a", state: CLAIM.ready, generation: 9 },
      ]);
    });
  });

  it("caps concurrent units at the declared fan-out", async () => {
    await withScratch("paged-op-drain-fanout", async (scratch) => {
      scratch.seed([
        { op_id: "a" },
        { op_id: "b" },
        { op_id: "c" },
        { op_id: "d" },
        { op_id: "e" },
      ]);
      let inFlight = 0;
      let maxInFlight = 0;
      let release = (): void => {};
      const parked = new Promise<void>((resolve) => {
        release = resolve;
      });

      await drainReadyOperations<ScratchRow>(scratch.store, {
        table: SCRATCH_TABLE,
        claim: CLAIM,
        fanOut: 2,
        maxPages: 1,
        selectReady: (dueAt) => selectScratch(scratch, dueAt),
        advance: async () => {
          inFlight++;
          maxInFlight = Math.max(maxInFlight, inFlight);
          if (inFlight >= 2) release();
          await parked;
          inFlight--;
          return { kind: "completed" };
        },
        disposition: () => ({ kind: "discard" }),
        armAlarmAt: async () => {},
      });

      expect(maxInFlight).toBe(2);
      expect(scratch.read()).toEqual([]);
    });
  });

  it("arms the alarm when the selection saturated its own bound", async () => {
    await withScratch("paged-op-drain-saturated", async (scratch) => {
      scratch.seed([{ op_id: "a" }, { op_id: "b", next_attempt_at: 5_000 }]);
      const rows = scratch.read();
      const first = rows[0];
      if (first === undefined) throw new Error("seeded rows");
      const armed: number[] = [];
      await drainReadyOperations<ScratchRow>(scratch.store, {
        table: SCRATCH_TABLE,
        claim: CLAIM,
        fanOut: 1,
        maxPages: 1,
        selectReady: () => ({ units: [unitOf(first)], saturated: true }),
        advance: async () => ({ kind: "completed" }),
        disposition: () => ({ kind: "discard" }),
        armAlarmAt: async (at) => {
          armed.push(at);
        },
      });
      expect(armed).toEqual([5_000]);
    });
  });
});

function selectScratch(
  scratch: Scratch,
  dueAt: number
): ReadyOperationBatch<ScratchRow> {
  const rows = scratch.store.sql
    .exec<ScratchRow>(
      `SELECT * FROM test_paged_ops
        WHERE state = ? AND next_attempt_at <= ?
        ORDER BY next_attempt_at, created_at, op_id, part`,
      CLAIM.ready,
      dueAt
    )
    .toArray();
  return { units: rows.map(unitOf), saturated: false };
}
