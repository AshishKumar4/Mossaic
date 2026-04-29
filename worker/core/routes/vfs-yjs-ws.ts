/**
 * Public Yjs WebSocket upgrade route —.
 *
 * Background: added a per-file CRDT mode reachable via the
 * UserDOCore method `_fetchWebSocketUpgrade(request)` at the synthetic
 * URL `/yjs/ws?path=...&ns=...&tenant=...[&sub=...]`. That handler
 * was reachable only from a sibling Worker calling `stub.fetch(req)`
 * — Cloudflare DO RPC cannot serialize a Response that carries a
 * `webSocket` field across the RPC boundary, so Yjs upgrade traffic
 * has to flow through the DO's own `fetch` handler.
 *
 * Service-mode (`worker/core/index.ts`) historically mounted only
 * `/api/vfs/*` (HTTP fallback) and `/api/health`, leaving the DO's
 * WebSocket entry unreachable from outside Cloudflare. This is fine
 * for the SDK's binding-mode consumer (which holds a stub and can
 * call `stub.fetch` directly) but blocks any external client —
 * including the new `@mossaic/cli` CLI — from speaking Yjs against
 * the live Service worker.
 *
 * This route closes that gap. It validates a Bearer VFS token (same
 * `verifyVFSToken` shape as `/api/vfs/*`), then forwards the upgrade
 * to the per-tenant UserDOCore stub via `stub.fetch()` to the
 * synthetic `/yjs/ws` URL the DO already understands.
 *
 * Auth: two parallel mechanisms, in priority order.
 *
 *   1. `Authorization: Bearer <token>` header. Preferred for tooling
 *      that controls headers (Node CLI, server-side scripts).
 *   2. `Sec-WebSocket-Protocol: bearer.<token>` subprotocol.
 *      Required for browsers (the WebSocket constructor cannot set
 *      Authorization). The server echoes the matched protocol back
 *      so the client constructor's protocol-list contract is
 *      satisfied.
 *
 * Either path validates the token via the existing
 * `verifyVFSToken(env, token)` helper. The token's claims (ns, tn,
 * sub) drive DO routing — request body / query parameters do NOT
 * control scope. Cross-tenant escape via parameter manipulation is
 * impossible by construction.
 *
 * Forwarding: we reconstruct an internal URL `http://internal/yjs/ws`
 * with `path`, `ns`, `tenant`, `sub` query params lifted from the
 * verified token and the inbound `?path=` query. The DO's existing
 * `_fetchWebSocketUpgrade` then validates `path` non-empty, calls
 * `vfsOpenYjsSocket`, and returns the `101 Switching Protocols`
 * response carrying the WebSocket. We forward that response verbatim
 * — Cloudflare permits returning a WebSocket-bearing Response from a
 * Worker (it is only DO-RPC serialization that cannot carry one).
 */

import { Hono } from "hono";
import type { Env } from "../../../shared/types";
import { verifyVFSToken, VFSConfigError } from "../lib/auth";
import { vfsUserDOName } from "../lib/utils";

const yjsWs = new Hono<{ Bindings: Env }>();

/**
 * Extract the Bearer token from either the `Authorization` header
 * or a `Sec-WebSocket-Protocol: bearer.<token>` subprotocol entry.
 * Returns `{ token, matchedProtocol }` so the caller can echo the
 * matched subprotocol back when subprotocol auth was used.
 */
function extractBearer(headers: Headers): {
  token: string | null;
  matchedProtocol: string | null;
} {
  const auth = headers.get("Authorization");
  if (auth?.startsWith("Bearer ")) {
    return { token: auth.slice(7), matchedProtocol: null };
  }
  const proto = headers.get("Sec-WebSocket-Protocol");
  if (proto) {
    // The header is a comma-separated list of subprotocols. We pick
    // the first `bearer.<jwt>` entry. JWTs don't contain commas, so
    // a naive split is safe.
    for (const raw of proto.split(",")) {
      const p = raw.trim();
      if (p.startsWith("bearer.")) {
        return { token: p.slice(7), matchedProtocol: p };
      }
    }
  }
  return { token: null, matchedProtocol: null };
}

yjsWs.get("/ws", async (c) => {
  // Reject non-upgrade requests cleanly so misconfigured callers see
  // a 426 + JSON body rather than a confusing 500.
  const upgrade = c.req.header("Upgrade");
  if (upgrade?.toLowerCase() !== "websocket") {
    return c.json(
      { code: "EINVAL", message: "WebSocket upgrade required" },
      426,
    );
  }

  const { token, matchedProtocol } = extractBearer(
    c.req.raw.headers,
  );
  if (!token) {
    return c.json(
      {
        code: "EACCES",
        message:
          "Bearer token required (Authorization header or Sec-WebSocket-Protocol: bearer.<jwt>)",
      },
      401,
    );
  }

  let payload;
  try {
    payload = await verifyVFSToken(c.env, token);
  } catch (err) {
    // VFSConfigError = JWT_SECRET missing on the deploy. Surface as
    // 503 mirroring the /api/vfs/* router's behaviour.
    if (err instanceof VFSConfigError) {
      return c.json(
        { code: "EMOSSAIC_UNAVAILABLE", message: err.message },
        503,
      );
    }
    throw err;
  }
  if (!payload) {
    return c.json(
      { code: "EACCES", message: "Invalid or expired VFS token" },
      401,
    );
  }

  const path = c.req.query("path");
  if (!path || path.length === 0) {
    return c.json(
      { code: "EINVAL", message: "?path=... required" },
      400,
    );
  }

  // Construct the internal URL the DO's _fetchWebSocketUpgrade expects.
  const u = new URL("http://internal/yjs/ws");
  u.searchParams.set("path", path);
  u.searchParams.set("ns", payload.ns);
  u.searchParams.set("tenant", payload.tn);
  if (payload.sub) u.searchParams.set("sub", payload.sub);

  const name = vfsUserDOName(payload.ns, payload.tn, payload.sub);
  const id = c.env.MOSSAIC_USER.idFromName(name);
  // The runtime DO stub's `fetch` method exists even though the
  // workers-types DurableObjectNamespace<T> generic is structurally
  // incompatible with our cast targets. Treat it as a callable.
  const stub = c.env.MOSSAIC_USER.get(id) as unknown as {
    fetch(req: Request): Promise<Response>;
  };

  // Forward the upgrade. We must pass through the original request's
  // upgrade-related headers so the DO and ultimately the WebSocket
  // pair instantiation see the right protocol/key fields.
  const forwardHeaders = new Headers();
  forwardHeaders.set("Upgrade", "websocket");
  // Echo the matched subprotocol back so the client's
  // new WebSocket(url, ["bearer.<jwt>"]) constructor sees a matching
  // protocol in the 101 response. Browsers reject the upgrade
  // otherwise.
  if (matchedProtocol) {
    forwardHeaders.set("Sec-WebSocket-Protocol", matchedProtocol);
  }

  const upstream = await stub.fetch(
    new Request(u, { headers: forwardHeaders }),
  );

  // The DO returns a 101 with `webSocket` on success or a JSON 400/404
  // body on error. Forward verbatim.
  return upstream;
});

export default yjsWs;
