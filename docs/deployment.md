# Mossaic deployment

The default deploy target is the **Seal staging** Cloudflare account
(`f42e5aacc251bd8ca6eadc04fb4ed13c`). Both `wrangler.jsonc` (App
mode) and `deployments/service/wrangler.jsonc` (Service mode) carry
that account id at the top level; `wrangler deploy` from a clean
checkout publishes there with no further flags.

The legacy personal-account deployment at
`mossaic.ashishkumarsingh.com` (account
`f44999d1ddda7012e9a87729eba250f1`) is no longer the default. The
custom domain is owned by the personal account and stays attached
to whatever was last deployed there; pushing the current branch to
that account is intentionally a manual override (see
[Reviving the personal deployment](#reviving-the-personal-deployment)).

---

## App mode &mdash; the user-facing photo library

```bash
wrangler deploy
```

Reads `wrangler.jsonc`. Builds the SPA bundle into `dist/` (run
`pnpm build` first; the wrangler `assets` block reads from there).
Publishes to `mossaic.<staging-subdomain>.workers.dev` &mdash; check
the wrangler output for the canonical URL.

Bindings:
- `MOSSAIC_USER` &harr; `UserDO`
- `MOSSAIC_SHARD` &harr; `ShardDO`
- `SEARCH_DO` &harr; `SearchDO`
- `IMAGES`
- `AI`
- `ASSETS` (for the SPA bundle)

Migrations apply on first deploy: `v1` (UserDO + ShardDO) and `v2`
(SearchDO).

## Service mode &mdash; the SDK-essential surface

```bash
wrangler deploy --config deployments/service/wrangler.jsonc
```

Reads `deployments/service/wrangler.jsonc`. Publishes
`mossaic-core` to its own workers.dev URL. SDK consumers bind via
`script_name: "mossaic-core"` to get a turn-key Mossaic backend
without re-exporting the DO classes themselves.

Bindings:
- `MOSSAIC_USER` &harr; `UserDOCore` (NOT `UserDO`)
- `MOSSAIC_SHARD` &harr; `ShardDO`
- `IMAGES`

Class names differ from App mode (`UserDOCore` vs `UserDO`) so the
two deployments are in **distinct DO namespaces** even on the same
account. Service-mode SQLite databases are independent of App-mode
SQLite databases.

## Required secrets

Both deployments need `JWT_SECRET`. Without it, `/api/vfs/*` refuses
with `503 EMOSSAIC_UNAVAILABLE` (auth.ts:VFSConfigError). Legacy
`/api/upload`/`/api/download` routes that don't touch JWT continue
to work, so a partial rollout doesn't dark-mode the entire Worker.

```bash
# App mode
wrangler secret put JWT_SECRET

# Service mode
wrangler secret put JWT_SECRET --config deployments/service/wrangler.jsonc
```

Multi-secret rotation via `JWT_SECRET_PREVIOUS` is honored by every
verifier (`verifyJWT`, `verifyVFSToken`, `verifyVFSMultipartToken`,
`verifyVFSDownloadToken`, `verifyShareToken`, `verifyPreviewToken`).
See `./operations.md` for the rotation procedure.

## Smoke after deploy

```bash
APP_URL="https://mossaic.<staging>.workers.dev"
SVC_URL="https://mossaic-core.<staging>.workers.dev"

# Should return 200
curl -s -o /dev/null -w "%{http_code}\n" "$APP_URL/api/health"
curl -s -o /dev/null -w "%{http_code}\n" "$SVC_URL/api/health"

# Auth-required routes 401 without a Bearer
curl -s -o /dev/null -w "%{http_code}\n" "$APP_URL/api/files"
curl -s -o /dev/null -w "%{http_code}\n" \
  -X POST -H "Content-Type: application/json" -d '{"path":"/"}' \
  "$SVC_URL/api/vfs/exists"

# Account delete unauth 401
curl -s -o /dev/null -w "%{http_code}\n" -X DELETE "$APP_URL/api/auth/account"
```

All of those produce the expected status code on a healthy deploy.

## Reviving the personal deployment

The custom domain `mossaic.ashishkumarsingh.com` is still bound on
the personal Cloudflare account (`f44999d1ddda7012e9a87729eba250f1`)
to whatever was last published there. To deploy the current branch
to that account instead of staging, use the wrangler `--account-id`
override:

```bash
wrangler deploy --account-id f44999d1ddda7012e9a87729eba250f1
wrangler deploy --account-id f44999d1ddda7012e9a87729eba250f1 \
  --config deployments/service/wrangler.jsonc
```

To re-attach the custom domain, restore the `routes` block in
`wrangler.jsonc` for that one deploy. The recommended pattern when
both targets are needed is to add a `[env.personal]` block to
`wrangler.jsonc` rather than flip the top-level `account_id` &mdash;
that lets `wrangler deploy --env personal` and `wrangler deploy`
coexist cleanly.

## Load testing

Load tests run against staging only. The 100 GB single-tenant load
test lives at `scripts/load-test/100gb-single-tenant.mjs`. It mints
a fresh tenant, uploads 100 GB through the SDK's `parallelUpload`
engine, reads back a sample of files for byte-equality, deletes
half, and re-uploads to exercise content-addressed dedup. Output
goes to `local/100gb-load-test-report.md`.

Never run the load test against the personal-account deployment.

## CI gate

`pnpm ci:check` runs `typecheck` &rarr; `build:sdk` &rarr;
`lint:no-phase-tags`. Run before every deploy. `pnpm verify:proofs`
re-checks the Lean invariants (224 theorems, 0 axioms, 0 sorrys).
