/**
 * Structured logger for Mossaic Worker code.
 *
 * Three concerns this module addresses:
 *
 *   1. **Operator visibility.** Free-form
 *      `console.{warn,error}(`message: ${err}`)` is ingested by
 *      Logpush as opaque text — operators can't grep for "every
 *      error from tenant X" or "every error from request Y". The
 *      structured shape `{ts, level, msg, ...fields}` is
 *      machine-parseable — Logpush + Workers Logs both surface
 *      structured JSON natively.
 *
 *   2. **Request-id correlation.** A single user-facing request
 *      may fan out to multiple DOs (UserDO + N ShardDOs); each
 *      logs from its own isolate. Without a shared request-id,
 *      correlating "which user's PUT triggered this ShardDO
 *      error?" requires log timestamp triangulation. A request-id
 *      is assigned at the Worker edge via `requestIdMiddleware`
 *      and threaded through (a) Hono context for route-layer
 *      logs, and (b) the `X-Mossaic-Request-Id` header for
 *      cross-DO calls.
 *
 *   3. **Alarm-handler error visibility.** Three bare `catch {}`
 *      sites in `user-do-core.ts:alarm()` swallowed every error,
 *      including permanent failures (a corrupted tmp row that
 *      blocks every subsequent alarm tick). Replacement pattern:
 *      log via `logger.error` AND increment the
 *      `vfs_meta.alarm_failures` counter; never re-throw (alarms
 *      have at-least-once semantics; a thrown alarm gets
 *      retried).
 *
 * Output format: a single `console.{info,warn,error}` call with a
 * JSON-stringified payload. Workers' Logs ingestion pipeline
 * recognises `console.*` JSON output and parses it into structured
 * fields without further configuration. Cloudflare Logpush v2
 * surfaces the same payload to operator destinations
 * (R2 / S3 / queues / Splunk).
 *
 * Levels: `info` for steady-state observability,
 * `warn` for recoverable degradation, `error` for failures the
 * operator should investigate.
 */

/**
 * Header name for cross-DO request-id propagation. Mossaic-prefixed
 * to avoid collision with anything in the standard request-id
 * conventions (`X-Request-Id`, `X-Correlation-Id`) the Cloudflare
 * edge or upstream proxies might use.
 */
export const REQUEST_ID_HEADER = "X-Mossaic-Request-Id";

/**
 * Hono variable key under which the request-id middleware
 * stashes the per-request id. Routes that want to log with
 * request-id read `c.var.requestId` after declaring
 * `Variables: { requestId: string }` on their Hono builder.
 */
export const REQUEST_ID_VAR = "requestId" as const;

/**
 * Common fields every log entry carries. Callers may pass
 * additional fields via the `extra` parameter on `log*` functions;
 * those are merged in at the JSON top level so Logpush filters
 * see them as first-class searchable columns.
 */
export interface LogContext {
  /** Edge-assigned UUID. Empty string when not in request scope. */
  requestId?: string;
  /** Tenant scope (e.g. `default::user-id-123`). Empty when N/A. */
  tenantId?: string;
  /** Optional event-class discriminator for grep-friendly filtering. */
  event?: string;
}

/**
 * Generate a request-id. UUIDv4 via `crypto.randomUUID()` —
 * available in Workers + Miniflare. ~22 chars after dashes
 * removed so the header is short.
 */
export function generateRequestId(): string {
  return crypto.randomUUID().replace(/-/g, "");
}

interface LogPayload {
  ts: number;
  level: "info" | "warn" | "error";
  msg: string;
  requestId?: string;
  tenantId?: string;
  event?: string;
  errCode?: string;
  errMsg?: string;
  errStack?: string;
  [key: string]: unknown;
}

function emit(payload: LogPayload): void {
  // Workers Logs + Logpush both parse JSON-stringified single-line
  // console.* output. Stringify here once so callers don't pay
  // serialization for log calls below their level threshold.
  const json = JSON.stringify(payload);
  switch (payload.level) {
    case "error":
      console.error(json);
      break;
    case "warn":
      console.warn(json);
      break;
    default:
      console.log(json);
  }
}

/**
 * Log at `info` level. Use for steady-state observability:
 * "destructive op happened", "alarm completed", "cache wired".
 */
export function logInfo(
  msg: string,
  ctx: LogContext = {},
  extra: Record<string, unknown> = {}
): void {
  emit({
    ts: Date.now(),
    level: "info",
    msg,
    ...(ctx.requestId ? { requestId: ctx.requestId } : {}),
    ...(ctx.tenantId ? { tenantId: ctx.tenantId } : {}),
    ...(ctx.event ? { event: ctx.event } : {}),
    ...extra,
  });
}

/**
 * Log at `warn` level. Use for recoverable degradation:
 * "approaching cap", "shard at soft cap", "first cap crossing".
 */
export function logWarn(
  msg: string,
  ctx: LogContext = {},
  extra: Record<string, unknown> = {}
): void {
  emit({
    ts: Date.now(),
    level: "warn",
    msg,
    ...(ctx.requestId ? { requestId: ctx.requestId } : {}),
    ...(ctx.tenantId ? { tenantId: ctx.tenantId } : {}),
    ...(ctx.event ? { event: ctx.event } : {}),
    ...extra,
  });
}

/**
 * Log at `error` level. Use for failures: alarm-handler errors,
 * indexing failures, share-mint exceptions. Pass the actual
 * `Error` (or unknown) via `err`; the helper extracts code +
 * message + stack into structured fields so Logpush filters can
 * group by error class.
 */
export function logError(
  msg: string,
  ctx: LogContext = {},
  err?: unknown,
  extra: Record<string, unknown> = {}
): void {
  let errCode: string | undefined;
  let errMsg: string | undefined;
  let errStack: string | undefined;
  if (err !== undefined) {
    if (err instanceof Error) {
      // Mossaic's VFSError carries `.code` separately; surface it.
      const maybeCoded = err as Error & { code?: unknown };
      if (typeof maybeCoded.code === "string") {
        errCode = maybeCoded.code;
      }
      errMsg = err.message;
      errStack = err.stack;
    } else {
      errMsg = String(err);
    }
  }
  emit({
    ts: Date.now(),
    level: "error",
    msg,
    ...(ctx.requestId ? { requestId: ctx.requestId } : {}),
    ...(ctx.tenantId ? { tenantId: ctx.tenantId } : {}),
    ...(ctx.event ? { event: ctx.event } : {}),
    ...(errCode ? { errCode } : {}),
    ...(errMsg ? { errMsg } : {}),
    ...(errStack ? { errStack } : {}),
    ...extra,
  });
}

// ─────────────────────────────────────────────────────────────────────
// Hono middleware — request-id assignment + propagation.
// ─────────────────────────────────────────────────────────────────────

/**
 * Hono middleware that assigns a request-id to every incoming
 * request, mirroring it onto both:
 *   - `c.var.requestId` for route handlers (declare
 *     `Variables: { requestId: string }` on the Hono builder).
 *   - Response header `X-Mossaic-Request-Id` so clients can
 *     correlate their request to server logs.
 *
 * Honors a caller-supplied `X-Mossaic-Request-Id` request header
 * if present (matches the standard request-id semantics:
 * upstream wins, edge mints if absent). This lets the SDK or
 * a wrapping proxy thread its own correlation id through to
 * server logs.
 *
 * Cost: one `crypto.randomUUID()` per request (~microseconds).
 * Deliberately NOT registered on `/api/health` to keep the
 * health-check log noise minimal — health checks fire ~once
 * per second per region. Mount this middleware at the app root
 * BEFORE `cors()` so even pre-flight OPTIONS responses carry
 * the header.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function requestIdMiddleware(): (c: any, next: () => Promise<void>) => Promise<void> {
  return async (c, next) => {
    const incoming = c.req.header(REQUEST_ID_HEADER);
    const requestId =
      typeof incoming === "string" && /^[a-zA-Z0-9-]{8,128}$/.test(incoming)
        ? incoming
        : generateRequestId();
    c.set(REQUEST_ID_VAR, requestId);
    await next();
    // Mirror the id onto the response so clients can correlate.
    // `c.res` is mutable inside Hono middleware after `await next()`.
    c.res.headers.set(REQUEST_ID_HEADER, requestId);
  };
}

/**
 * Helper that pulls (requestId, tenantId) off a Hono context for
 * `LogContext`. Routes that have already populated
 * `c.var.scope` (every VFS / app route) can call this to build a
 * one-shot log context without typing each field.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function ctxFromHono(c: any): LogContext {
  const requestId = c.var?.[REQUEST_ID_VAR];
  const scope = c.var?.scope;
  let tenantId: string | undefined;
  if (scope && typeof scope === "object") {
    const ns = (scope as { ns?: unknown }).ns;
    const tenant = (scope as { tenant?: unknown }).tenant;
    const sub = (scope as { sub?: unknown }).sub;
    if (typeof ns === "string" && typeof tenant === "string") {
      tenantId =
        typeof sub === "string" ? `${ns}::${tenant}::${sub}` : `${ns}::${tenant}`;
    }
  } else if (typeof c.get === "function") {
    const userId = c.get("userId");
    if (typeof userId === "string") {
      tenantId = `default::${userId}`;
    }
  }
  return {
    ...(typeof requestId === "string" && requestId.length > 0
      ? { requestId }
      : {}),
    ...(tenantId ? { tenantId } : {}),
  };
}
