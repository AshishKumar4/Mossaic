/**
 * Control plane for durable, resumable, bounded operations.
 *
 * Every long-running mutation in a SQLite-backed Durable Object here follows
 * the same shape: a durable row remembers how far the operation got, one
 * invocation performs a bounded amount of work, the advance is persisted under
 * compare-and-set so a stale writer cannot rewind it, failures back off, and
 * an alarm brings the next invocation. Only the *domain* differs — which rows
 * to page over, which shard RPC to issue, what "done" means.
 *
 * This module owns exactly the shared part:
 *
 *   - `commitOperationTransition` — compare-and-set persistence of a
 *     transition, rejecting regressions and any move out of a terminal phase.
 *   - `runOperationPages` — the bounded page loop.
 *   - `drainReadyOperations` — the claim plane: due-ordered bounded batch,
 *     generation + lease fencing, exponential backoff, terminal disposition,
 *     capped fan-out, alarm re-arming at the earliest due deadline.
 *   - `retryDelayMs` / `isPoisonous` — the retry and poison policies.
 *   - `OPERATION_CLAIM_COLUMNS_DDL` / `OPERATION_RETRY_COLUMNS_DDL` — the
 *     column set those policies read and write.
 *
 * It deliberately owns no table of its own. Each feature keeps its typed
 * table, its own domain cursors, and its own SQL for domain mutations; the
 * driver only fences and sequences them.
 *
 * ── the progress tuple ───────────────────────────────────────────────
 *
 * A transition is legal when `(phase, cursor₁ … cursorₙ)` — the columns
 * declared on `PagedOperationTable`, outermost first — does not regress
 * lexicographically, and the row is not already terminal. That single rule
 * covers every machine below: an inner cursor may reset to zero exactly when
 * an outer component advances (a new phase restarts its page counter; a new
 * plan generation restarts the whole scan), which is what makes replayed pages
 * idempotent instead of merely tolerated.
 *
 * `generation` is not part of that tuple. It fences concurrent writers, so it
 * advances on every transition — including a failure that makes no progress —
 * and is checked on its own.
 *
 * Terminal is final *per operation*, not per row: re-staging work at the same
 * key starts a new operation there and bumps `generation`, which invalidates
 * every claim held against the old one.
 *
 * ── replay ───────────────────────────────────────────────────────────
 *
 * Terminal results stay in the feature's own columns (`finalize_result`,
 * `dropped`/`kept`, a journal row), because only the feature knows their
 * shape. What the control plane guarantees is that a terminal operation can
 * never run another page, so reading those columns and returning them is
 * always safe on replay.
 *
 * ── how the durable operations in this codebase map on ───────────────
 *
 * multipart finalize (`vfsFinalizeMultipartStep`)
 *   table    upload_sessions, key (upload_id, user_id)
 *   phase    finalize_phase: fencing → verifying → preparing → publishing →
 *            cleaning_old_manifest → cleaning → done (terminal). Publication
 *            is constant-size: verification copies each verified page into the
 *            destination manifest, preparation routes the shards the switch
 *            orphans, and what the switch leaves owed is paged off after it.
 *   cursors  finalize_fence_cursor, finalize_chunk_cursor,
 *            finalize_verify_shard_cursor, finalize_old_manifest_cursor,
 *            finalize_old_cleanup_cursor, finalize_cleanup_cursor. Only one
 *            pair has to be ordered: finalize_verify_shard_cursor sits inside
 *            finalize_chunk_cursor, because finishing a chunk page restarts
 *            the shard fan-out at zero. The rest belong to one phase each and
 *            only ever advance.
 *   fence    `status` ('finalizing' / 'finalized') and `finalize_context`
 *            passed in `expected`, so a session that aborted, or whose frozen
 *            decision was rewritten, changes zero rows
 *   terminal phase `done`; result decoded from `finalize_result`
 *   bounded  one shard page, one chunk page, one displaced-manifest page or
 *            one scratch page per invocation
 *
 * multipart abort (`vfsAbortMultipartStep`, `advanceMultipartAbortPage`)
 *   table    upload_sessions, key (upload_id, user_id)
 *   phase    abort_phase: fencing → intents → cleanup → old_intents →
 *            local → done (terminal)
 *   cursors  abort_fence_cursor, abort_intent_cursor, abort_cleanup_cursor,
 *            abort_old_intent_cursor
 *   fence    `status = 'aborting'` in `expected`
 *   terminal phase `done` with `status = 'aborted'` and `terminal_at` stamped;
 *            `status = 'poisoned'` when `isPoisonous` holds
 *   bounded  `runOperationPages` with five pages per invocation
 *
 * chunk cleanup outbox (`drainChunkCleanupIntents`, the caller migrated here)
 *   table    chunk_cleanup_intents, key (ref_id, shard_index)
 *   phase    none yet — one page finishes a ref's shard cleanup. Paging the
 *            shard side adds cleanup_phase: chunks → staging with
 *            cleanup_cursor, which is why `OperationPage.advanced` carries the
 *            columns to persist.
 *   cursors  none yet (cleanup_cursor once shard cleanup is paged)
 *   fence    `generation` plus a `next_attempt_at` lease over
 *            `state` pending ⇄ in_flight
 *   terminal `state = 'cleaned'` retained as a publication guard for
 *            provisional intents, row discarded otherwise
 *   bounded  200 rows per invocation, six concurrent units, one page per claim
 *
 * version retention (`dropVersions`)
 *   table    version_retention_operations, key (operation_id)
 *   phase    status: running → expiring → done (terminal)
 *   cursors  plan_generation, mirrored from files.version_generation. Its
 *            per-version bookkeeping — the descending (cursor_mtime_ms,
 *            cursor_version_id) seek, pending_metadata_deleted, manifest_cursor
 *            — is *not* declared, because it legitimately restarts for each
 *            version without any declared outer component advancing. It stays
 *            domain state written inside the transition's transaction, which is
 *            the boundary the rule above draws: declare a cursor only when it
 *            rewinds no further than a declared outer component allows.
 *   fence    (status, plan_generation) in `expected`; the whole step runs in
 *            one synchronous transaction, so no second writer can interleave
 *   terminal status `done`; result decoded from the persisted dropped / kept
 *   bounded  128 versions, 200 manifest rows, 128 cleanup intents
 *
 * yjs cleanup (`dropOpsBefore`)
 *   table    yjs_cleanup_operations, key (path_id)
 *   phase    none — a single draining phase
 *   cursors  cursor_seq
 *   fence    none; the UserDO is the only writer and a concurrent request
 *            merges its cutoff with MAX() instead of racing
 *   terminal row discarded, then yjs_meta purged when the request asked for it
 *   bounded  256 oplog rows per page
 *
 * shard cleanup pages (`ShardDO.runCleanupPage`)
 *   table    shard_cleanup_progress, key (cleanup_kind, ref_id,
 *            cleanup_generation)
 *   phase    the reference tracks a `done` integer flag; on the shared plane it
 *            becomes a text phase (paging → done) so the terminal guard is
 *            mechanical instead of an inline comparison, which is the one
 *            schema adjustment this mapping asks of the reference machines
 *   cursors  next_cursor
 *   fence    the caller's `cleanup_generation` is part of the key, and its
 *            `request_cursor` becomes `expected: { next_cursor }`, so a caller
 *            replaying an older cursor changes zero rows instead of double
 *            counting
 *   terminal phase `done`; the page journal replays a completed page verbatim
 *   bounded  256 refs per page
 */

/**
 * Storage capability every SQLite-backed Durable Object here satisfies. Both
 * the UserDO and the ShardDO expose it, so the control plane never has to
 * import either.
 */
export interface DurableSqlStore {
  readonly storage: DurableObjectStorage;
  readonly sql: SqlStorage;
}

/** Rows affected by the statement that just ran on `sql`. */
export function sqlRowsChanged(sql: SqlStorage): number {
  return sql.exec<{ n: number }>("SELECT changes() AS n").toArray()[0]?.n ?? 0;
}

// ── retry and claim columns ───────────────────────────────────────────

/**
 * Claim fence. Every persisted transition bumps it, so a writer that read
 * generation G can only commit while the row is still at G. Tables whose
 * operations are addressed directly (one caller per key) fence on their phase
 * and cursors instead and omit this column.
 */
export const OPERATION_CLAIM_COLUMNS_DDL =
  "generation      INTEGER NOT NULL DEFAULT 0";

/**
 * Retry bookkeeping. `next_attempt_at` is the row's due time while it waits
 * and its claim lease deadline while it runs, so one ordering serves both
 * eligibility and lease recovery. `last_error` keeps the most recent failure
 * for operators; it is never parsed.
 */
export const OPERATION_RETRY_COLUMNS_DDL = `updated_at      INTEGER NOT NULL,
        next_attempt_at INTEGER NOT NULL DEFAULT 0,
        attempts        INTEGER NOT NULL DEFAULT 0,
        last_error      TEXT`;

const GENERATION_COLUMN = "generation";
const UPDATED_AT_COLUMN = "updated_at";
const NEXT_ATTEMPT_AT_COLUMN = "next_attempt_at";
const ATTEMPTS_COLUMN = "attempts";
const LAST_ERROR_COLUMN = "last_error";

/** Longest failure string kept in `last_error`. */
const LAST_ERROR_MAX_LENGTH = 2_000;

/** Normalise a thrown value for `last_error`. */
function operationErrorMessage(error: unknown): string {
  return (error instanceof Error ? error.message : String(error)).slice(
    0,
    LAST_ERROR_MAX_LENGTH
  );
}

// ── retry and poison policies ─────────────────────────────────────────

/** Exponential backoff between attempts of one durable operation. */
export interface RetryPolicy {
  /** Delay after the first failure. */
  readonly baseMs: number;
  /** Ceiling the delay saturates at. */
  readonly maxMs: number;
  /** Doublings applied before the delay stops growing. */
  readonly maxDoublings: number;
}

/** Delay before the next attempt. `failureCount` is 1-based. */
export function retryDelayMs(
  policy: RetryPolicy,
  failureCount: number
): number {
  const doublings = Math.min(
    Math.max(failureCount - 1, 0),
    policy.maxDoublings
  );
  return Math.min(policy.baseMs * 2 ** doublings, policy.maxMs);
}

/**
 * When an operation may be abandoned instead of retried.
 *
 * Only a deterministic local-corruption failure qualifies: state that this
 * Durable Object can inspect and has proven inconsistent, where every replay
 * would fail the same way. A remote failure — an unreachable shard, a lost
 * response — never qualifies, however often it recurs, because the work is
 * still owed.
 */
export interface PoisonPolicy {
  /** Failures tolerated before a corrupt operation is abandoned. */
  readonly maxAttempts: number;
  readonly isLocalCorruption: (error: unknown) => boolean;
}

/** `failureCount` is 1-based, matching `retryDelayMs`. */
export function isPoisonous(
  policy: PoisonPolicy,
  error: unknown,
  failureCount: number
): boolean {
  return (
    failureCount >= policy.maxAttempts && policy.isLocalCorruption(error)
  );
}

// ── phases ───────────────────────────────────────────────────────────

/**
 * Forward-only phase set. `forward` is the rank order — a transition may skip
 * ahead but never regress — and `terminal` phases admit no transition at all.
 */
export interface OperationPhases<TPhase extends string> {
  readonly column: string;
  readonly forward: readonly TPhase[];
  readonly terminal: readonly TPhase[];
}

export function operationPhaseRank(
  phases: OperationPhases<string>,
  phase: string
): number {
  const rank = phases.forward.indexOf(phase);
  if (rank < 0) {
    throw new Error(`unknown ${phases.column} '${phase}'`);
  }
  return rank;
}

export function isTerminalPhase(
  phases: OperationPhases<string>,
  phase: string
): boolean {
  return phases.terminal.includes(phase);
}

// ── the operation table ──────────────────────────────────────────────

/**
 * Where a feature keeps the control-plane columns of one operation. The
 * feature owns the table itself, including every domain column; this only
 * names the parts the driver fences on.
 */
export interface PagedOperationTable<TPhase extends string = never> {
  readonly table: string;
  /** Identity columns. A transition never writes them. */
  readonly keyColumns: readonly string[];
  /** Monotone phase column, for operations that have more than one phase. */
  readonly phases?: OperationPhases<TPhase>;
  /**
   * Integer domain cursors, outermost first. Declare a cursor only when it
   * rewinds no further than a declared outer component allows — per-item
   * bookkeeping that restarts on its own is domain state, not a cursor.
   */
  readonly cursorColumns?: readonly string[];
  readonly retry: RetryPolicy;
}

type OperationColumns = Readonly<Record<string, SqlStorageValue>>;

/** Progress tuple, outermost first. */
function progressColumns(
  table: PagedOperationTable<string>
): readonly string[] {
  return [
    ...(table.phases === undefined ? [] : [table.phases.column]),
    ...(table.cursorColumns ?? []),
  ];
}

function compareProgress(
  table: PagedOperationTable<string>,
  column: string,
  from: SqlStorageValue,
  to: SqlStorageValue
): number {
  const phases = table.phases;
  if (phases !== undefined && column === phases.column) {
    if (typeof from !== "string" || typeof to !== "string") {
      throw new Error(`${table.table}.${column} must be text`);
    }
    return operationPhaseRank(phases, to) - operationPhaseRank(phases, from);
  }
  if (typeof from !== "number" || typeof to !== "number") {
    throw new Error(`${table.table}.${column} must be numeric`);
  }
  return to - from;
}

/**
 * Reject a transition that rewinds progress, leaves a terminal phase, or
 * reuses a claim fence. A column absent from either record is unconstrained by
 * this transition and compares equal.
 *
 * `generation` is checked separately from the progress tuple: it fences
 * concurrent writers and so advances on every transition, including a failure
 * that makes no progress at all.
 */
function assertForwardTransition(
  table: PagedOperationTable<string>,
  expected: OperationColumns,
  next: OperationColumns
): void {
  const phases = table.phases;
  if (phases !== undefined) {
    const current = expected[phases.column];
    if (typeof current === "string" && isTerminalPhase(phases, current)) {
      throw new Error(
        `${table.table}: ${phases.column} '${current}' is terminal`
      );
    }
  }
  const fenceFrom = expected[GENERATION_COLUMN];
  const fenceTo = next[GENERATION_COLUMN];
  if (fenceFrom !== undefined && fenceTo !== undefined) {
    if (typeof fenceFrom !== "number" || typeof fenceTo !== "number") {
      throw new Error(`${table.table}.${GENERATION_COLUMN} must be numeric`);
    }
    if (fenceTo <= fenceFrom) {
      throw new Error(`${table.table}: ${GENERATION_COLUMN} must advance`);
    }
  }
  for (const column of progressColumns(table)) {
    const from = expected[column];
    const to = next[column];
    if (from === undefined || to === undefined) continue;
    const advance = compareProgress(table, column, from, to);
    if (advance < 0) {
      throw new Error(`${table.table}: ${column} would regress`);
    }
    if (advance > 0) return;
  }
}

function equalityClause(columns: readonly string[]): string {
  return columns.map((column) => `${column} = ?`).join(" AND ");
}

function operationKey(
  table: PagedOperationTable<string>,
  row: OperationColumns
): OperationColumns {
  const key: Record<string, SqlStorageValue> = {};
  for (const column of table.keyColumns) {
    const value = row[column];
    if (value === undefined) {
      throw new Error(`${table.table}: row is missing key column '${column}'`);
    }
    key[column] = value;
  }
  return key;
}

/**
 * Persist one transition under compare-and-set.
 *
 * `expected` carries the guarded columns exactly as the page read them, so
 * columns present only there act as pure guards; `next` carries what the
 * transition writes. A stale writer matches zero rows and gets `false` back —
 * addressed callers translate that into their own "changed underneath me"
 * error, while the claim plane treats it as "another writer owns this row now"
 * and moves on.
 *
 * Must run inside the same transaction as the domain mutations it commits, so
 * a rejected transition rolls them back.
 */
export function commitOperationTransition(
  store: DurableSqlStore,
  table: PagedOperationTable<string>,
  key: OperationColumns,
  expected: OperationColumns,
  next: OperationColumns
): boolean {
  assertForwardTransition(table, expected, next);
  const keyColumns = Object.keys(key);
  const expectedColumns = Object.keys(expected);
  const nextColumns = Object.keys(next);
  store.sql.exec(
    `UPDATE ${table.table}
        SET ${nextColumns.map((column) => `${column} = ?`).join(", ")}
      WHERE ${equalityClause([...keyColumns, ...expectedColumns])}`,
    ...nextColumns.map((column) => next[column]),
    ...keyColumns.map((column) => key[column]),
    ...expectedColumns.map((column) => expected[column])
  );
  return sqlRowsChanged(store.sql) === 1;
}

/**
 * The declared progress a row currently holds, as a guard for its next
 * transition.
 *
 * An addressed page reads its row, computes one page of work, and commits;
 * passing this as `expected` says "nothing moved underneath me", which is the
 * same fence the claim plane puts on a released unit. Columns the row does not
 * carry are simply absent, so a projection that selected only part of the
 * progress tuple still guards the part it read.
 */
export function heldProgress(
  table: PagedOperationTable<string>,
  row: OperationColumns
): OperationColumns {
  const held: Record<string, SqlStorageValue> = {};
  for (const column of progressColumns(table)) {
    const value = row[column];
    if (value !== undefined) held[column] = value;
  }
  return held;
}

/** Drop a terminal operation row, fenced the same way as a transition. */
function discardOperation(
  store: DurableSqlStore,
  table: PagedOperationTable<string>,
  key: OperationColumns,
  expected: OperationColumns
): boolean {
  const keyColumns = Object.keys(key);
  const expectedColumns = Object.keys(expected);
  store.sql.exec(
    `DELETE FROM ${table.table}
      WHERE ${equalityClause([...keyColumns, ...expectedColumns])}`,
    ...keyColumns.map((column) => key[column]),
    ...expectedColumns.map((column) => expected[column])
  );
  return sqlRowsChanged(store.sql) === 1;
}

// ── bounded page loop ────────────────────────────────────────────────

/** Outcome of one bounded page. */
export type OperationPage =
  /** More pages remain. `next` carries the columns the advance persists. */
  | { readonly kind: "advanced"; readonly next?: OperationColumns }
  /** The operation reached its terminal state. */
  | { readonly kind: "completed" }
  /** No progress is possible in this invocation; leave the row for later. */
  | { readonly kind: "abandoned" };

/**
 * Run at most `maxPages` pages, stopping as soon as the operation completes or
 * gives up. `done` distinguishes "nothing left to do" from "resume later".
 */
export async function runOperationPages(
  maxPages: number,
  page: () => Promise<OperationPage>
): Promise<{ done: boolean }> {
  for (let index = 0; index < maxPages; index++) {
    const outcome = await page();
    if (outcome.kind === "completed") return { done: true };
    if (outcome.kind === "abandoned") return { done: false };
  }
  return { done: false };
}

// ── claim plane ──────────────────────────────────────────────────────

/** Control-plane columns the claim plane requires a selected row to carry. */
export interface ClaimableOperationRow
  extends Record<string, SqlStorageValue> {
  readonly generation: number;
  readonly attempts: number;
}

/** Claim states the driver moves a row between, and how long a claim holds. */
export interface OperationClaim {
  /** Claim-state column. Distinct from the phase column: a claim comes back. */
  readonly column: string;
  /** Rows in this state are eligible once `next_attempt_at` is due. */
  readonly ready: string;
  /** Rows in this state are being worked on and hold a lease. */
  readonly claimed: string;
  /** How long a claim survives before another invocation may reclaim it. */
  readonly leaseMs: number;
}

/** What happens to a row whose operation completed. */
export type OperationDisposition =
  /** Keep the row in a terminal state so later readers observe the outcome. */
  | { readonly kind: "retain"; readonly state: string }
  /** Drop the row; nothing needs to observe the outcome. */
  | { readonly kind: "discard" };

/** One or more rows advanced together under a single claim. */
export type OperationUnit<TRow> = readonly [TRow, ...TRow[]];

/** One bounded, due-ordered selection of ready work. */
export interface ReadyOperationBatch<TRow> {
  /** Units advanced independently; rows within a unit share one claim. */
  readonly units: readonly OperationUnit<TRow>[];
  /** The selection hit its own bound, so more ready rows may exist. */
  readonly saturated: boolean;
}

export interface DrainReadyOperationsSpec<TRow extends ClaimableOperationRow> {
  readonly table: PagedOperationTable<string>;
  readonly claim: OperationClaim;
  /**
   * Bounded, due-ordered ready work. The predicate is the feature's — the
   * driver only requires that every row is in the ready state and projects
   * the key, `generation` and `attempts` columns.
   *
   * Called once, after expired leases have been reclaimed, so a feature that
   * has to re-arm rows before taking them can do it here and see the reclaimed
   * ones.
   */
  readonly selectReady: (dueAt: number) => ReadyOperationBatch<TRow>;
  /** Units worked on concurrently. */
  readonly fanOut: number;
  /** Pages one unit may run before the batch moves on. */
  readonly maxPages: number;
  /** One bounded page of domain work for a claimed unit. */
  readonly advance: (unit: OperationUnit<TRow>) => Promise<OperationPage>;
  /** Terminal disposition of each row of a completed unit. */
  readonly disposition: (row: TRow) => OperationDisposition;
  /** Arm the Durable Object's alarm no later than `at`. */
  readonly armAlarmAt: (at: number) => Promise<void>;
}

/**
 * Drive one bounded batch of ready operations.
 *
 * Reclaims leases that outlived their deadline, claims each unit under
 * generation fencing, runs bounded pages with a capped fan-out, records
 * failures with exponential backoff, applies each completed row's
 * disposition, and re-arms the alarm at the earliest deadline still owed.
 *
 * A unit whose page throws, or that loses its claim, stays durable for the next
 * invocation, so one unreachable shard cannot stall the rest of the batch. A
 * page that violates the progress rule is a different matter and does surface:
 * that is a caller bug, not a failure the operation can retry its way out of.
 */
export async function drainReadyOperations<TRow extends ClaimableOperationRow>(
  store: DurableSqlStore,
  spec: DrainReadyOperationsSpec<TRow>
): Promise<void> {
  const dueAt = Date.now();
  reclaimExpiredClaims(store, spec, dueAt);

  const batch = spec.selectReady(dueAt);
  let moreWorkOwed = batch.saturated;
  let nextUnit = 0;

  const runUnit = async (unit: OperationUnit<TRow>): Promise<void> => {
    // Every transition rewrites the rows it fenced on, so the unit is carried
    // forward as the values now persisted — a second page sees the cursor the
    // first page committed, not the one the batch selected.
    let claimed = unit;
    await runOperationPages(spec.maxPages, async () => {
      const held = claimUnit(store, spec, claimed);
      if (held === null) return { kind: "abandoned" };
      claimed = held;
      let outcome: OperationPage;
      try {
        outcome = await spec.advance(claimed);
      } catch (error) {
        moreWorkOwed = true;
        recordUnitFailure(store, spec, claimed, error);
        return { kind: "abandoned" };
      }
      if (outcome.kind === "completed") {
        settleUnit(store, spec, claimed);
        return outcome;
      }
      if (outcome.kind === "advanced") {
        moreWorkOwed = true;
        claimed = releaseUnit(store, spec, claimed, outcome.next);
      }
      return outcome;
    });
  };

  const lane = async (): Promise<void> => {
    for (;;) {
      const unit = batch.units[nextUnit++];
      if (unit === undefined) return;
      await runUnit(unit);
    }
  };

  await Promise.all(
    Array.from({ length: Math.min(spec.fanOut, batch.units.length) }, lane)
  );

  if (!moreWorkOwed) return;
  const nextDueAt = earliestDueAt(store, spec);
  if (nextDueAt !== undefined) await spec.armAlarmAt(nextDueAt);
}

/**
 * Return claims whose lease expired to the ready state. Bumping `generation`
 * invalidates the original claim, so a straggler that finally answers cannot
 * acknowledge work another invocation has taken over.
 */
function reclaimExpiredClaims<TRow extends ClaimableOperationRow>(
  store: DurableSqlStore,
  spec: DrainReadyOperationsSpec<TRow>,
  dueAt: number
): void {
  store.storage.transactionSync(() => {
    store.sql.exec(
      `UPDATE ${spec.table.table}
          SET ${spec.claim.column} = ?, ${GENERATION_COLUMN} = ${GENERATION_COLUMN} + 1,
              ${UPDATED_AT_COLUMN} = ?, ${NEXT_ATTEMPT_AT_COLUMN} = ?
        WHERE ${spec.claim.column} = ? AND ${NEXT_ATTEMPT_AT_COLUMN} <= ?`,
      spec.claim.ready,
      dueAt,
      dueAt,
      spec.claim.claimed,
      dueAt
    );
  });
}

/** Carry a committed transition into the in-memory view of the unit. */
function withCommitted<TRow extends ClaimableOperationRow>(
  unit: OperationUnit<TRow>,
  committed: (row: TRow) => OperationColumns
): OperationUnit<TRow> {
  const [first, ...rest] = unit;
  return [
    Object.assign({}, first, committed(first)),
    ...rest.map((row) => Object.assign({}, row, committed(row))),
  ];
}

/**
 * Claim every row of a unit, or none of them, and return the unit as claimed.
 * Verifying the whole unit before writing keeps a grouped remote call from
 * acting on a partially claimed unit.
 */
function claimUnit<TRow extends ClaimableOperationRow>(
  store: DurableSqlStore,
  spec: DrainReadyOperationsSpec<TRow>,
  unit: OperationUnit<TRow>
): OperationUnit<TRow> | null {
  const claimedAt = Date.now();
  const claim = (row: TRow): OperationColumns => ({
    [spec.claim.column]: spec.claim.claimed,
    [GENERATION_COLUMN]: row.generation + 1,
    [UPDATED_AT_COLUMN]: claimedAt,
    [NEXT_ATTEMPT_AT_COLUMN]: claimedAt + spec.claim.leaseMs,
  });
  const held = store.storage.transactionSync(() => {
    for (const row of unit) {
      const key = operationKey(spec.table, row);
      const current = store.sql
        .exec<{ state: SqlStorageValue; generation: number }>(
          `SELECT ${spec.claim.column} AS state, ${GENERATION_COLUMN} AS generation
             FROM ${spec.table.table}
            WHERE ${equalityClause(Object.keys(key))}`,
          ...Object.values(key)
        )
        .toArray()[0];
      if (
        current === undefined ||
        current.state !== spec.claim.ready ||
        current.generation !== row.generation
      ) {
        return false;
      }
    }
    for (const row of unit) {
      commitOperationTransition(
        store,
        spec.table,
        operationKey(spec.table, row),
        {
          [spec.claim.column]: spec.claim.ready,
          [GENERATION_COLUMN]: row.generation,
        },
        claim(row)
      );
    }
    return true;
  });
  return held ? withCommitted(unit, claim) : null;
}

/** Release a claim back to the ready state with the failure recorded. */
function recordUnitFailure<TRow extends ClaimableOperationRow>(
  store: DurableSqlStore,
  spec: DrainReadyOperationsSpec<TRow>,
  unit: OperationUnit<TRow>,
  error: unknown
): void {
  const failedAt = Date.now();
  const message = operationErrorMessage(error);
  store.storage.transactionSync(() => {
    for (const row of unit) {
      const failureCount = row.attempts + 1;
      commitOperationTransition(
        store,
        spec.table,
        operationKey(spec.table, row),
        {
          [spec.claim.column]: spec.claim.claimed,
          [GENERATION_COLUMN]: row.generation,
        },
        {
          [spec.claim.column]: spec.claim.ready,
          [GENERATION_COLUMN]: row.generation + 1,
          [ATTEMPTS_COLUMN]: failureCount,
          [UPDATED_AT_COLUMN]: failedAt,
          [NEXT_ATTEMPT_AT_COLUMN]:
            failedAt + retryDelayMs(spec.table.retry, failureCount),
          [LAST_ERROR_COLUMN]: message,
        }
      );
    }
  });
}

/**
 * Release a claim back to the ready state after a page made progress, and
 * return the unit as released so the next page starts from the new cursor.
 *
 * This is the one transition the claim plane makes that writes progress, so it
 * fences on the progress the row already holds — the advance is then checked
 * against the same lexicographic rule an addressed page gets.
 */
function releaseUnit<TRow extends ClaimableOperationRow>(
  store: DurableSqlStore,
  spec: DrainReadyOperationsSpec<TRow>,
  unit: OperationUnit<TRow>,
  next: OperationColumns | undefined
): OperationUnit<TRow> {
  const releasedAt = Date.now();
  const release = (row: TRow): OperationColumns => ({
    ...next,
    [spec.claim.column]: spec.claim.ready,
    [GENERATION_COLUMN]: row.generation + 1,
    [UPDATED_AT_COLUMN]: releasedAt,
    [NEXT_ATTEMPT_AT_COLUMN]: releasedAt,
    [LAST_ERROR_COLUMN]: null,
  });
  store.storage.transactionSync(() => {
    for (const row of unit) {
      commitOperationTransition(
        store,
        spec.table,
        operationKey(spec.table, row),
        {
          ...heldProgress(spec.table, row),
          [spec.claim.column]: spec.claim.claimed,
          [GENERATION_COLUMN]: row.generation,
        },
        release(row)
      );
    }
  });
  return withCommitted(unit, release);
}

/** Apply each row's terminal disposition. */
function settleUnit<TRow extends ClaimableOperationRow>(
  store: DurableSqlStore,
  spec: DrainReadyOperationsSpec<TRow>,
  unit: OperationUnit<TRow>
): void {
  const settledAt = Date.now();
  store.storage.transactionSync(() => {
    for (const row of unit) {
      const key = operationKey(spec.table, row);
      const expected = {
        [spec.claim.column]: spec.claim.claimed,
        [GENERATION_COLUMN]: row.generation,
      };
      const disposition = spec.disposition(row);
      if (disposition.kind === "discard") {
        discardOperation(store, spec.table, key, expected);
        continue;
      }
      commitOperationTransition(store, spec.table, key, expected, {
        [spec.claim.column]: disposition.state,
        [GENERATION_COLUMN]: row.generation + 1,
        [UPDATED_AT_COLUMN]: settledAt,
        [LAST_ERROR_COLUMN]: null,
      });
    }
  });
}

/** Earliest deadline still owed by a row that is not terminal. */
function earliestDueAt<TRow extends ClaimableOperationRow>(
  store: DurableSqlStore,
  spec: DrainReadyOperationsSpec<TRow>
): number | undefined {
  const dueAt = store.sql
    .exec<{ next_attempt_at: number | null }>(
      `SELECT MIN(${NEXT_ATTEMPT_AT_COLUMN}) AS ${NEXT_ATTEMPT_AT_COLUMN}
         FROM ${spec.table.table}
        WHERE ${spec.claim.column} IN (?, ?)`,
      spec.claim.ready,
      spec.claim.claimed
    )
    .toArray()[0]?.next_attempt_at;
  return dueAt === null || dueAt === undefined ? undefined : dueAt;
}
