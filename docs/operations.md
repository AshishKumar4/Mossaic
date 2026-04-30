# Mossaic operations

Concise operator reference. For the API surface see [`docs/integration-guide.md`](./integration-guide.md); for SDK consumer DX see [`sdk/README.md`](../sdk/README.md).

---

## Required secrets

| Secret | Purpose |
|---|---|
| `JWT_SECRET` | HS256 signing for session JWTs, VFS Bearer tokens, and the `listFiles` HMAC pagination cursor. **No dev fallback in source** — without it every `/api/vfs/*` route returns `503 EMOSSAIC_UNAVAILABLE`. |
| `JWT_SECRET_PREVIOUS` *(optional)* | Set during a graceful rotation window (see below). |

```bash
wrangler secret put JWT_SECRET
```

## Pre-deploy checklist

- [ ] `pnpm test` — 534 worker + 46 cli unit pass.
- [ ] `pnpm typecheck` — 0 errors.
- [ ] `pnpm lean:build` — 0 `sorry`, 0 project axiom.
- [ ] `JWT_SECRET` set on the target environment (`wrangler secret list`).
- [ ] `wrangler deploy --dry-run` — bundle clean, migrations match.
- [ ] Smoke against the deployed worker:
  ```bash
  curl -s -o /dev/null -w "%{http_code}\n" https://<worker>/api/health   # → 200
  curl -s -o /dev/null -w "%{http_code}\n" \
    -H "Authorization: Bearer bogus" https://<worker>/api/vfs/exists \
    -X POST -H "Content-Type: application/json" -d '{"path":"/"}'        # → 401
  ```

## Monitoring

Recommended `wrangler tail` filters:

```bash
wrangler tail --format json | jq 'select(.outcome != "ok")'              # any non-ok request
wrangler tail --format json | jq 'select(.logs[]?.message[]? | contains("EMOSSAIC_UNAVAILABLE"))'   # JWT_SECRET drift
wrangler tail --format json | jq 'select(.logs[]?.message[]? | contains("EAGAIN"))'                # rate-limit pressure
```

SLOs (from prod traffic on `mossaic.ashishkumarsingh.com`):

| Operation | Target | Notes |
|---|---|---|
| `/api/health` | p99 < 50 ms | regional cache |
| `/api/vfs/stat` | p99 < 80 ms | one DO RPC |
| `/api/vfs/readFile` (≤ 1 MB) | p99 < 250 ms | single chunk fan-in |
| `/api/vfs/writeFile` (1 MB) | p99 < 350 ms | tmp insert + commitRename |
| `/api/vfs/multipart/finalize` | p99 < 600 ms | shard fan-in + commitRename |

## Common failure modes

### `503 EMOSSAIC_UNAVAILABLE`

`JWT_SECRET` missing or empty. `wrangler secret put JWT_SECRET`; ≤ 60 s propagation.

### `401 Invalid or expired token` after rotation

If you rotated `JWT_SECRET` without staging `JWT_SECRET_PREVIOUS` first, every outstanding token is invalidated. SPA users are logged out; CLI profiles need `mossaic auth setup --secret <newvalue>`. Use the graceful path (below) for routine rotations.

### `409 EBUSY` on writes for a single tenant

UNIQUE-INDEX corruption during a partial migration. Run:

```bash
# operator-only RPC — drives admin.dedupePaths(userId, scope)
mossaic admin dedupe --tenant <userId>
```

### Storm of `EAGAIN`

Per-tenant rate limit (default 100 ops/s, burst 200). Either back off the client or raise the limit on the UserDO row directly.

---

## Graceful `JWT_SECRET` rotation (zero-downtime)

`verifyJWT` and `verifyVFSToken` accept tokens signed with **either** `env.JWT_SECRET` (current) or `env.JWT_SECRET_PREVIOUS` (rotation-window-only). Signing always uses `env.JWT_SECRET`. This lets you rotate without invalidating any outstanding session.

1. **Generate the new secret**:
   ```bash
   openssl rand -base64 48
   ```
2. **Stage the previous-secret slot**:
   ```bash
   wrangler secret put JWT_SECRET_PREVIOUS  # paste the CURRENT JWT_SECRET value
   ```
3. **Promote the new secret**:
   ```bash
   wrangler secret put JWT_SECRET           # paste the NEW value
   ```
4. **Wait for the drain window** — the longer of `JWT_EXPIRATION_MS` (≈ 30 days for session JWTs; see `shared/constants.ts`) and the 15-minute VFS-Bearer TTL.
5. **Drop the previous-secret slot**:
   ```bash
   wrangler secret delete JWT_SECRET_PREVIOUS
   ```

### Hard cutover (emergency only)

After a confirmed leak: skip the multi-secret window — `wrangler secret put JWT_SECRET <new>` and notify users that re-auth is required. Every outstanding token returns 401 immediately.

---

## Sign-off

Fill out per deploy and store in your incident-tracking system:

```
Deploy:        <wrangler deployment id>
Date / time:   <UTC>
Operator:      <name>
Commit SHA:    <git rev-parse HEAD>
Pre-deploy:    [✓] tests, [✓] typecheck, [✓] lean, [✓] dry-run, [✓] JWT_SECRET set
Smoke:         [✓] /api/health 200, [✓] /api/vfs/exists 401 on bogus token
Rollback ref:  <previous wrangler deployment id> + <git tag>
```
