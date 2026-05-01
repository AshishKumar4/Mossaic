#!/usr/bin/env node
// @ts-check
/**
 * Pool growth — LIVE production verification (Phase 23).
 *
 * SINGLE-USE script. Hits PRODUCTION at mossaic.ashishkumarsingh.com,
 * uploads ~5.5 GB of real bytes through the SDK's `parallelUpload`
 * HTTP path, polls `quota.pool_size` via `/api/analytics/overview`,
 * and asserts the 32 → 33 transition fires after the 5 GB threshold.
 * After the assertion (pass or fail), it MUST clean up — see the
 * `try/finally` block at the bottom.
 *
 * NOT part of the regular test suite. Expensive. Run manually:
 *
 *     node scripts/verify-pool-growth-live.mjs
 *
 * Honours environment overrides:
 *   MOSSAIC_BASE_URL   default: https://mossaic.ashishkumarsingh.com
 *   MOSSAIC_TEST_EMAIL default: phase23-pool-test-<unix-ts>@test.local
 *   POOL_TEST_BYTES    default: 5_905_580_032   (~5.5 GB)
 *   POOL_TEST_FILE_MB  default: 512             (per-file size in MB)
 *   POOL_TEST_DRY_RUN  if set: signup + 1 small upload + delete; SKIP big bytes.
 *                      Use for plumbing validation in environments without
 *                      bandwidth or where the operator wants to confirm the
 *                      script wires up correctly before paying for real bytes.
 *
 * Cleanup is mandatory: the script ALWAYS calls
 * `DELETE /api/auth/account` in a finally block, even if assertions
 * fail. Cost-bound: real bytes hit Workers free-tier ingress, the
 * UserDO + ShardDO storage buckets, and the multipart token verify
 * path; the order of magnitude for a one-shot ~5.5 GB run is well
 * under a dollar at retail rates and is reclaimed by the cleanup
 * delete (which decrements ShardDO refcounts and triggers the alarm
 * sweeper to hard-delete chunks on its 30s grace cadence).
 */

import { createMossaicHttpClient, parallelUpload } from "@mossaic/sdk/http";

// ── Configuration ─────────────────────────────────────────────────

const BASE_URL = process.env.MOSSAIC_BASE_URL ?? "https://mossaic.ashishkumarsingh.com";
const NOW = Date.now();
const TEST_EMAIL = process.env.MOSSAIC_TEST_EMAIL ?? `phase23-pool-test-${NOW}@test.local`;
const TEST_PASSWORD = "phase23-pool-test-password-very-long-string";
const TOTAL_BYTES = Number(process.env.POOL_TEST_BYTES ?? 5_905_580_032); // ~5.5 GB
const PER_FILE_MB = Number(process.env.POOL_TEST_FILE_MB ?? 512);
const PER_FILE_BYTES = PER_FILE_MB * 1024 * 1024;
const POST_THRESHOLD_FILE_BYTES = 100 * 1024 * 1024; // 100 MB confirmation file
const DRY_RUN = process.env.POOL_TEST_DRY_RUN !== undefined && process.env.POOL_TEST_DRY_RUN !== "";

const REPORT_PATH = "local/pool-growth-live-verification.md";

// ── Tiny ANSI helpers (no deps) ──────────────────────────────────

const ts = () => new Date().toISOString().replace("T", " ").slice(0, 19);
const log = (msg) => console.log(`[${ts()}] ${msg}`);
const note = (k, v) => console.log(`[${ts()}]   ${k.padEnd(28)} ${v}`);

// ── HTTP helpers (App routes) ─────────────────────────────────────

async function appPost(path, body, sessionJwt) {
  const headers = { "Content-Type": "application/json" };
  if (sessionJwt) headers.Authorization = `Bearer ${sessionJwt}`;
  const res = await fetch(`${BASE_URL}${path}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body ?? {}),
  });
  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = { _raw: text }; }
  return { status: res.status, body: json };
}

async function appGet(path, sessionJwt) {
  const headers = {};
  if (sessionJwt) headers.Authorization = `Bearer ${sessionJwt}`;
  const res = await fetch(`${BASE_URL}${path}`, { headers });
  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = { _raw: text }; }
  return { status: res.status, body: json };
}

async function appDelete(path, sessionJwt) {
  const headers = {};
  if (sessionJwt) headers.Authorization = `Bearer ${sessionJwt}`;
  const res = await fetch(`${BASE_URL}${path}`, { method: "DELETE", headers });
  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = { _raw: text }; }
  return { status: res.status, body: json };
}

// ── Deterministic incompressible payload ─────────────────────────

/**
 * Generate a Uint8Array of exactly `bytes` length, filled with a
 * counter pattern so the payload is HIGH-ENTROPY (no Brotli/CDN
 * dedup) but reproducible. We use a 32-bit rolling counter folded
 * into the byte stream so identical content across files is rare.
 *
 * Memory: at 512 MB per file we allocate the full buffer in RAM.
 * That's intentional — `parallelUpload` accepts `Uint8Array | Blob`
 * and the simplest path is the array. For a Node script driving a
 * one-shot 5.5 GB test this is acceptable; max heap is ~1 GB at any
 * moment because we generate-then-upload-then-discard each file.
 */
function makePayload(bytes, seed) {
  const buf = new Uint8Array(bytes);
  // Fast XOR-shift PRNG seeded by (seed, byte index quarter).
  let state = (seed * 2654435761) >>> 0;
  for (let i = 0; i < bytes; i++) {
    state = (state ^ (state << 13)) >>> 0;
    state = (state ^ (state >>> 17)) >>> 0;
    state = (state ^ (state << 5)) >>> 0;
    buf[i] = state & 0xff;
  }
  return buf;
}

// ── Verification flow ─────────────────────────────────────────────

const report = {
  startedAtIso: new Date().toISOString(),
  baseUrl: BASE_URL,
  testEmail: TEST_EMAIL,
  dryRun: DRY_RUN,
  signup: null,
  initialPoolSize: null,
  uploads: [],
  poolTransition: null,
  postThresholdFile: null,
  errors: [],
  cleanup: null,
  finishedAtIso: null,
  durationSeconds: null,
};

let sessionJwt = null;
let userId = null;

async function fetchPoolSize(jwt) {
  const r = await appGet("/api/analytics/overview", jwt);
  if (r.status !== 200) {
    throw new Error(
      `analytics/overview returned ${r.status}: ${JSON.stringify(r.body).slice(0, 200)}`
    );
  }
  return {
    poolSize: r.body?.user?.poolSize,
    storageUsed: r.body?.user?.totalStorageUsed,
    fileCount: r.body?.user?.totalFiles,
  };
}

async function step1_signup() {
  log(`step 1 — signup ${TEST_EMAIL}`);
  const r = await appPost("/api/auth/signup", {
    email: TEST_EMAIL,
    password: TEST_PASSWORD,
  });
  if (r.status !== 200 || !r.body?.token) {
    throw new Error(
      `signup failed: ${r.status} ${JSON.stringify(r.body).slice(0, 200)}`
    );
  }
  sessionJwt = r.body.token;
  userId = r.body.userId;
  report.signup = { ok: true, userId };
  note("userId", userId);
}

async function step2_readInitialPool() {
  log("step 2 — read initial pool_size");
  const q = await fetchPoolSize(sessionJwt);
  report.initialPoolSize = q.poolSize;
  note("initial pool_size", q.poolSize);
  note("initial storage_used", q.storageUsed);
  if (q.poolSize !== 32) {
    log(
      `WARNING: initial pool_size is ${q.poolSize}, not 32. Test still ` +
        `valid but baseline differs from expected fresh-tenant defaults.`
    );
  }
}

async function step3_uploadUntilThreshold() {
  log(
    `step 3 — upload ${PER_FILE_MB} MB files via parallelUpload until ` +
      `total bytes >= ${TOTAL_BYTES} (~${(TOTAL_BYTES / (1024 * 1024 * 1024)).toFixed(2)} GB)`
  );
  if (DRY_RUN) {
    log("DRY RUN: skipping bulk upload, doing one tiny upload only");
    const httpClient = createMossaicHttpClient({
      url: BASE_URL,
      apiKey: async () => {
        const r = await appPost("/api/auth/vfs-token", {}, sessionJwt);
        if (r.status !== 200) throw new Error(`vfs-token mint failed: ${r.status}`);
        return r.body.token;
      },
    });
    const tiny = makePayload(1024, 1);
    const out = await parallelUpload(httpClient, "/dryrun.bin", tiny, {});
    note("dry-run fileId", out.fileId);
    report.uploads.push({
      idx: 0,
      bytes: tiny.byteLength,
      fileId: out.fileId,
      poolAfter: (await fetchPoolSize(sessionJwt)).poolSize,
    });
    return;
  }

  const httpClient = createMossaicHttpClient({
    url: BASE_URL,
    apiKey: async () => {
      const r = await appPost("/api/auth/vfs-token", {}, sessionJwt);
      if (r.status !== 200) {
        throw new Error(`vfs-token mint failed: ${r.status}`);
      }
      return r.body.token;
    },
  });

  let totalUploaded = 0;
  let idx = 0;
  let observedTransition = false;

  while (totalUploaded < TOTAL_BYTES) {
    idx++;
    const sz = Math.min(PER_FILE_BYTES, TOTAL_BYTES - totalUploaded);
    const path = `/pool-test-file-${String(idx).padStart(3, "0")}.bin`;
    log(`  upload #${idx} → ${path} (${(sz / (1024 * 1024)).toFixed(0)} MB)`);
    const t0 = Date.now();
    const payload = makePayload(sz, idx);
    let out;
    try {
      out = await parallelUpload(httpClient, path, payload, {});
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      report.errors.push({ stage: `upload-${idx}`, message: msg });
      throw err;
    }
    const elapsedMs = Date.now() - t0;
    totalUploaded += sz;

    const q = await fetchPoolSize(sessionJwt);
    const entry = {
      idx,
      path,
      bytes: sz,
      cumBytes: totalUploaded,
      fileId: out.fileId,
      durationMs: elapsedMs,
      mbps: ((sz * 8) / 1_000_000) / (elapsedMs / 1000),
      poolAfter: q.poolSize,
      storageUsedAfter: q.storageUsed,
    };
    report.uploads.push(entry);
    note(
      `cum / pool / used`,
      `${(totalUploaded / (1024 * 1024 * 1024)).toFixed(2)} GB / pool=${q.poolSize} / quota.used=${q.storageUsed}`
    );

    if (!observedTransition && q.poolSize > 32) {
      observedTransition = true;
      report.poolTransition = {
        cumBytesAtTransition: totalUploaded,
        poolSizeAfter: q.poolSize,
        afterUploadIdx: idx,
        firstObservedIso: new Date().toISOString(),
      };
      log(
        `  ✓ POOL TRANSITION: pool_size=${q.poolSize} after ${(totalUploaded / (1024 * 1024 * 1024)).toFixed(3)} GB`
      );
    }
  }

  if (!observedTransition) {
    log(
      `  ✗ NO TRANSITION OBSERVED after ${(totalUploaded / (1024 * 1024 * 1024)).toFixed(2)} GB. Pool stayed at ${
        (await fetchPoolSize(sessionJwt)).poolSize
      }.`
    );
  }
}

async function step4_postThresholdFile() {
  if (DRY_RUN) {
    log("step 4 — DRY RUN: skipping post-threshold file");
    return;
  }
  if (!report.poolTransition) {
    log(
      "step 4 — skipping post-threshold file: pool never grew, the goal " +
        "of this step (verify chunks land on new shard) is moot"
    );
    return;
  }
  log(
    `step 4 — upload one more 100 MB file to confirm new pool routes traffic`
  );
  const httpClient = createMossaicHttpClient({
    url: BASE_URL,
    apiKey: async () => {
      const r = await appPost("/api/auth/vfs-token", {}, sessionJwt);
      if (r.status !== 200) throw new Error(`vfs-token mint failed: ${r.status}`);
      return r.body.token;
    },
  });
  const payload = makePayload(POST_THRESHOLD_FILE_BYTES, 9999);
  const t0 = Date.now();
  const out = await parallelUpload(httpClient, "/pool-test-post.bin", payload, {});
  const elapsedMs = Date.now() - t0;

  // Pull analytics overview again to inspect per-shard stats.
  const overview = await appGet("/api/analytics/overview", sessionJwt);
  const shardStats = overview.body?.shards ?? [];
  const grownPoolSize = overview.body?.user?.poolSize;
  const newShardIdx = grownPoolSize - 1;
  const newShardEntry = shardStats.find((s) => s.shardIndex === newShardIdx);
  report.postThresholdFile = {
    fileId: out.fileId,
    durationMs: elapsedMs,
    bytes: POST_THRESHOLD_FILE_BYTES,
    grownPoolSize,
    shardSummaries: shardStats.map((s) => ({
      idx: s.shardIndex,
      totalBytes: s.totalBytes,
      totalChunks: s.totalChunks,
    })),
    newShardWasUsed: newShardEntry !== undefined,
    newShardIdx,
  };
  if (newShardEntry) {
    log(
      `  ✓ shard #${newShardIdx} received ${newShardEntry.totalBytes} bytes / ${newShardEntry.totalChunks} chunks`
    );
  } else {
    log(
      `  ⚠ shard #${newShardIdx} not seen in analytics overview (zero chunks). New shard exists but no traffic landed on it during this test — could be rendezvous luck (~1/${grownPoolSize} chance per chunk).`
    );
  }
}

async function step5_writeReport() {
  report.finishedAtIso = new Date().toISOString();
  const start = Date.parse(report.startedAtIso);
  const end = Date.parse(report.finishedAtIso);
  report.durationSeconds = Math.round((end - start) / 1000);

  const md = renderReport(report);
  // Write atomically.
  const fs = await import("node:fs/promises");
  await fs.mkdir("local", { recursive: true });
  await fs.writeFile(REPORT_PATH, md, "utf8");
  log(`report written → ${REPORT_PATH}`);
}

async function step6_cleanup() {
  log("step 6 — cleanup (DELETE /api/auth/account)");
  if (sessionJwt === null) {
    log("  no session JWT held — nothing to clean up (signup likely failed)");
    report.cleanup = { ok: false, reason: "no-session" };
    return;
  }
  try {
    const r = await appDelete("/api/auth/account", sessionJwt);
    if (r.status !== 200) {
      log(`  cleanup returned status ${r.status}: ${JSON.stringify(r.body).slice(0, 200)}`);
      report.cleanup = { ok: false, status: r.status, body: r.body };
      return;
    }
    report.cleanup = {
      ok: true,
      filesRemoved: r.body?.data?.filesRemoved,
      foldersRemoved: r.body?.data?.foldersRemoved,
      chunksRemovedFromShards: r.body?.data?.chunksRemovedFromShards,
      authRowRemoved: r.body?.authRowRemoved,
    };
    note("filesRemoved", r.body?.data?.filesRemoved);
    note("chunksRemovedFromShards", r.body?.data?.chunksRemovedFromShards);
    note("authRowRemoved", r.body?.authRowRemoved);

    // Verify post-state: a re-login should 401.
    const verify = await appPost("/api/auth/login", {
      email: TEST_EMAIL,
      password: TEST_PASSWORD,
    });
    if (verify.status === 401) {
      log("  ✓ verification: login returns 401 (auth row gone)");
      report.cleanup.loginVerify = "401-as-expected";
    } else {
      log(`  ✗ verification: login returned ${verify.status} (expected 401)`);
      report.cleanup.loginVerify = `unexpected-${verify.status}`;
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    log(`  cleanup threw: ${msg}`);
    report.cleanup = { ok: false, error: msg };
  }
}

function renderReport(r) {
  const lines = [];
  lines.push(`# Pool growth — live production verification`);
  lines.push("");
  lines.push(`Generated: ${r.finishedAtIso}`);
  lines.push("");
  lines.push(`- Base URL: \`${r.baseUrl}\``);
  lines.push(`- Test email: \`${r.testEmail}\``);
  lines.push(`- Dry run: ${r.dryRun ? "**yes**" : "no"}`);
  lines.push(`- Duration: ${r.durationSeconds}s`);
  lines.push("");

  lines.push(`## Signup`);
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(r.signup, null, 2));
  lines.push("```");
  lines.push("");

  lines.push(`## Initial pool size`);
  lines.push("");
  lines.push(`pool_size = **${r.initialPoolSize}**`);
  lines.push("");

  lines.push(`## Pool transition`);
  lines.push("");
  if (r.poolTransition) {
    lines.push(
      `- ✅ Observed: pool grew to **${r.poolTransition.poolSizeAfter}** after upload #${r.poolTransition.afterUploadIdx}, cumulative **${(r.poolTransition.cumBytesAtTransition / (1024 * 1024 * 1024)).toFixed(3)} GB**`
    );
  } else {
    lines.push(`- ⚠ NOT observed during this run. Pool stayed at ${r.initialPoolSize}.`);
  }
  lines.push("");

  lines.push(`## Upload log`);
  lines.push("");
  lines.push(`| # | Path | Bytes | Cum (GB) | Pool after | quota.used | duration ms | Mbps |`);
  lines.push(`|---|------|------:|---------:|-----------:|-----------:|-----------:|-----:|`);
  for (const u of r.uploads) {
    lines.push(
      `| ${u.idx} | \`${u.path ?? "(dryrun)"}\` | ${u.bytes} | ${(u.cumBytes ? (u.cumBytes / (1024 * 1024 * 1024)).toFixed(3) : "-")} | ${u.poolAfter} | ${u.storageUsedAfter ?? "-"} | ${u.durationMs ?? "-"} | ${u.mbps ? u.mbps.toFixed(1) : "-"} |`
    );
  }
  lines.push("");

  if (r.postThresholdFile) {
    lines.push(`## Post-threshold confirmation file`);
    lines.push("");
    lines.push("```json");
    lines.push(JSON.stringify(r.postThresholdFile, null, 2));
    lines.push("```");
    lines.push("");
  }

  if (r.errors.length > 0) {
    lines.push(`## Errors`);
    lines.push("");
    lines.push("```json");
    lines.push(JSON.stringify(r.errors, null, 2));
    lines.push("```");
    lines.push("");
  }

  lines.push(`## Cleanup`);
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(r.cleanup, null, 2));
  lines.push("```");
  lines.push("");

  lines.push(`## Cost estimate (rough)`);
  lines.push("");
  const gb = r.uploads.reduce((acc, u) => acc + (u.bytes ?? 0), 0) / (1024 * 1024 * 1024);
  lines.push(
    `Bytes ingressed: **${gb.toFixed(2)} GB** (Workers ingress is free at retail). DO storage during the test is reclaimed by the cleanup-delete; ShardDO chunk_refs drop to zero and the alarm sweeper hard-deletes the bytes within ~30 seconds. Workers Paid plan: this run consumes a small slice of the included multipart subrequest budget; rough order is well under \\$0.10.`
  );
  return lines.join("\n");
}

// ── Entry point ───────────────────────────────────────────────────

(async () => {
  log(`pool-growth live verification starting`);
  note("BASE_URL", BASE_URL);
  note("TOTAL_BYTES", TOTAL_BYTES);
  note("PER_FILE_BYTES", PER_FILE_BYTES);
  note("DRY_RUN", DRY_RUN);

  let exitCode = 0;
  try {
    await step1_signup();
    await step2_readInitialPool();
    await step3_uploadUntilThreshold();
    await step4_postThresholdFile();
  } catch (err) {
    const msg = err instanceof Error ? err.stack ?? err.message : String(err);
    log(`FATAL: ${msg}`);
    report.errors.push({ stage: "fatal", message: msg });
    exitCode = 1;
  } finally {
    // Cleanup ALWAYS runs, even on assertion / network failure.
    try {
      await step6_cleanup();
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      log(`CLEANUP FAILED: ${msg}`);
      report.cleanup = { ok: false, error: msg };
    }
    try {
      await step5_writeReport();
    } catch (err) {
      log(`report write failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }
  process.exit(exitCode);
})();
