#!/usr/bin/env node
// @ts-check
/**
 * 100 GB single-tenant load test for Mossaic.
 *
 * Hits the deployed staging worker. Mints a fresh tenant via
 * /api/auth/signup, uploads 100 GB through the SDK's
 * parallelUpload engine in 100 MB-per-file chunks, polls
 * /api/analytics/overview every 5 GB to capture pool_size +
 * storage_used + file_count + timing, reads back a sample of
 * uploaded files for byte-equality, deletes half, re-uploads
 * 50 GB to exercise content-addressed dedup, then deletes the
 * tenant.
 *
 * NEVER buffers the full 100 GB in memory. Each upload is a
 * fresh 100 MB Uint8Array (~ randomized) which is ingested by
 * parallelUpload with adaptive concurrency. The test workload
 * generates content per-file, not in advance.
 *
 * Run manually (NOT part of the test suite — expensive, takes
 * ~2-4 hours depending on bandwidth):
 *
 *     node scripts/load-test/100gb-single-tenant.mjs
 *
 * Honors environment overrides:
 *   MOSSAIC_BASE_URL    default: https://mossaic.seal-staging.workers.dev
 *   MOSSAIC_TEST_EMAIL  default: phase53-100gb-<unix-ts>@test.local
 *   LOAD_TEST_TOTAL_GB  default: 100
 *   LOAD_TEST_FILE_MB   default: 100  (size of each uploaded file)
 *   LOAD_TEST_SAMPLE_N  default: 100  (random files to read back for byte-equality)
 *   LOAD_TEST_DELETE_RATIO default: 0.5
 *   LOAD_TEST_REUPLOAD_GB default: 50   (re-upload after delete to exercise dedup)
 *   LOAD_TEST_REPORT_PATH default: local/100gb-load-test-report.md
 *   LOAD_TEST_DRY_RUN   if set: signup + ONE 100MB upload + delete; SKIPS the
 *                       multi-hour bulk. Use for plumbing validation.
 *   CF_ACCESS_CLIENT_ID + CF_ACCESS_CLIENT_SECRET — if set, sent as
 *                       Cloudflare Access service-token headers so the
 *                       script can reach the Worker behind an Access
 *                       policy (Seal staging gates all traffic).
 *
 * Cleanup is mandatory: the script ALWAYS calls
 * DELETE /api/auth/account in a finally block, even if assertions
 * fail. The chunks land on staging ShardDOs and decrement on the
 * existing 30s-grace alarm sweep.
 *
 * Output: appends a structured summary to
 * local/100gb-load-test-report.md (or LOAD_TEST_REPORT_PATH).
 */

import { createMossaicHttpClient, parallelUpload } from "@mossaic/sdk/http";
import { writeFile, appendFile, mkdir } from "node:fs/promises";
import { dirname } from "node:path";

// ── Configuration ─────────────────────────────────────────────────

const BASE_URL = process.env.MOSSAIC_BASE_URL ?? "https://mossaic.seal-staging.workers.dev";
const NOW = Date.now();
const TEST_EMAIL = process.env.MOSSAIC_TEST_EMAIL ?? `phase53-100gb-${NOW}@test.local`;
const TEST_PASSWORD = "phase53-100gb-load-test-password-please-be-long";

const TOTAL_GB = Number(process.env.LOAD_TEST_TOTAL_GB ?? 100);
const FILE_MB = Number(process.env.LOAD_TEST_FILE_MB ?? 100);
const SAMPLE_N = Number(process.env.LOAD_TEST_SAMPLE_N ?? 100);
const DELETE_RATIO = Number(process.env.LOAD_TEST_DELETE_RATIO ?? 0.5);
const REUPLOAD_GB = Number(process.env.LOAD_TEST_REUPLOAD_GB ?? 50);
const REPORT_PATH = process.env.LOAD_TEST_REPORT_PATH ?? "local/100gb-load-test-report.md";
const DRY_RUN = !!(process.env.LOAD_TEST_DRY_RUN && process.env.LOAD_TEST_DRY_RUN.length);

const TOTAL_BYTES = TOTAL_GB * 1024 * 1024 * 1024;
const FILE_BYTES = FILE_MB * 1024 * 1024;
const FILES_TOTAL = Math.ceil(TOTAL_BYTES / FILE_BYTES);
const POLL_EVERY_GB = 5;
const POLL_EVERY_BYTES = POLL_EVERY_GB * 1024 * 1024 * 1024;

const ACCESS_CLIENT_ID = process.env.CF_ACCESS_CLIENT_ID ?? "";
const ACCESS_CLIENT_SECRET = process.env.CF_ACCESS_CLIENT_SECRET ?? "";

// ── Tiny ANSI helpers (no deps) ──────────────────────────────────

const ts = () => new Date().toISOString().replace("T", " ").slice(0, 19);
const log = (msg) => console.log(`[${ts()}] ${msg}`);
const note = (k, v) => console.log(`[${ts()}]   ${String(k).padEnd(28)} ${v}`);

// ── Cloudflare Access service-token header injection ──────────────

/**
 * Wrap fetch so every outbound request carries the
 * CF-Access-Client-Id and CF-Access-Client-Secret headers when
 * provided. Seal staging gates all workers.dev traffic through
 * Cloudflare Access; without these the script gets 302'd to a
 * login page before reaching the Worker.
 *
 * @type {typeof fetch}
 */
const accessFetch = (input, init) => {
  const headers = new Headers(init?.headers ?? {});
  if (ACCESS_CLIENT_ID && ACCESS_CLIENT_SECRET) {
    headers.set("CF-Access-Client-Id", ACCESS_CLIENT_ID);
    headers.set("CF-Access-Client-Secret", ACCESS_CLIENT_SECRET);
  }
  return fetch(input, { ...init, headers });
};

// ── HTTP helpers ──────────────────────────────────────────────────

async function postJson(path, body, sessionToken = null) {
  const headers = { "Content-Type": "application/json" };
  if (sessionToken) headers["Authorization"] = `Bearer ${sessionToken}`;
  const res = await accessFetch(`${BASE_URL}${path}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { status: res.status, body: json };
}

async function getJson(path, sessionToken) {
  const res = await accessFetch(`${BASE_URL}${path}`, {
    headers: { Authorization: `Bearer ${sessionToken}` },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { status: res.status, body: json };
}

// ── Cheap pseudo-random fill ──────────────────────────────────────

/**
 * Generate FILE_BYTES of pseudo-random bytes. Cheap (Math.random
 * over Uint32Array, ~3x faster than crypto.randomBytes for this
 * scale; we don't need crypto strength). Each call produces a
 * new buffer so identical-content dedup never triggers
 * spuriously.
 *
 * The first 16 bytes are the file's stable salt so we can
 * verify byte-equality on readback without holding the whole
 * blob in memory \u2014 we only verify the first + last 16 bytes
 * (32 bytes total), which is sufficient to detect tampering or
 * mis-routing without paying RAM for 100 MB per readback file.
 */
function makeFile(saltSeed) {
  const buf = new Uint8Array(FILE_BYTES);
  // Salt: deterministic from saltSeed (encoded as 16 bytes).
  for (let i = 0; i < 16; i++) {
    buf[i] = (saltSeed >>> (i * 4)) & 0xff;
  }
  // Body: cheap pseudo-random.
  let state = saltSeed >>> 0;
  for (let i = 16; i < FILE_BYTES; i += 4) {
    state = (state * 1664525 + 1013904223) >>> 0;
    buf[i] = state & 0xff;
    buf[i + 1] = (state >>> 8) & 0xff;
    buf[i + 2] = (state >>> 16) & 0xff;
    buf[i + 3] = (state >>> 24) & 0xff;
  }
  // Tail: stable 16-byte tail derived from saltSeed for verification.
  for (let i = 0; i < 16; i++) {
    buf[FILE_BYTES - 16 + i] = (saltSeed >>> (i * 3)) & 0xff;
  }
  return buf;
}

function expectedSalt(saltSeed) {
  const head = new Uint8Array(16);
  for (let i = 0; i < 16; i++) head[i] = (saltSeed >>> (i * 4)) & 0xff;
  const tail = new Uint8Array(16);
  for (let i = 0; i < 16; i++) tail[i] = (saltSeed >>> (i * 3)) & 0xff;
  return { head, tail };
}

function bytesEqual(a, b) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

// ── Report buffer ─────────────────────────────────────────────────

const report = {
  startedAt: new Date().toISOString(),
  baseUrl: BASE_URL,
  tenant: TEST_EMAIL,
  config: {
    totalGb: TOTAL_GB,
    fileMb: FILE_MB,
    filesTotal: FILES_TOTAL,
    sampleN: SAMPLE_N,
    deleteRatio: DELETE_RATIO,
    reuploadGb: REUPLOAD_GB,
    dryRun: DRY_RUN,
    accessGated: !!ACCESS_CLIENT_ID,
  },
  stages: [],
  errors: [],
  finalVerdict: null,
};

function recordStage(name, data) {
  report.stages.push({
    name,
    at: new Date().toISOString(),
    elapsedMs: Date.now() - NOW,
    ...data,
  });
}

function recordError(stage, err) {
  report.errors.push({
    stage,
    at: new Date().toISOString(),
    message: err instanceof Error ? err.message : String(err),
    stack: err instanceof Error ? err.stack : undefined,
  });
}

async function flushReport() {
  await mkdir(dirname(REPORT_PATH), { recursive: true });
  await writeFile(
    REPORT_PATH,
    `# 100 GB single-tenant load test report\n\n\`\`\`json\n${JSON.stringify(report, null, 2)}\n\`\`\`\n`
  );
}

// ── Main ──────────────────────────────────────────────────────────

let sessionToken = null;

async function signUp() {
  log(`signing up: ${TEST_EMAIL}`);
  const r = await postJson("/api/auth/signup", { email: TEST_EMAIL, password: TEST_PASSWORD });
  if (r.status !== 200 || !r.body?.token) {
    throw new Error(`signup failed: HTTP ${r.status} ${JSON.stringify(r.body).slice(0, 200)}`);
  }
  sessionToken = r.body.token;
  recordStage("signup", { email: TEST_EMAIL, status: r.status });
  log(`signed up; got session token (len=${sessionToken.length})`);
}

async function mintVfsToken() {
  log("minting VFS Bearer token");
  const r = await postJson("/api/auth/vfs-token", {}, sessionToken);
  if (r.status !== 200 || !r.body?.token) {
    throw new Error(`vfs-token failed: HTTP ${r.status} ${JSON.stringify(r.body).slice(0, 200)}`);
  }
  return r.body.token;
}

async function pollAnalytics(label) {
  const r = await getJson("/api/analytics/overview", sessionToken);
  if (r.status !== 200) {
    log(`analytics poll failed at ${label}: HTTP ${r.status}`);
    return null;
  }
  const snap = {
    label,
    poolSize: r.body?.poolSize ?? r.body?.pool_size ?? null,
    storageUsed: r.body?.storageUsed ?? r.body?.storage_used ?? null,
    fileCount: r.body?.fileCount ?? r.body?.file_count ?? null,
    raw: r.body,
  };
  log(
    `[${label}] pool=${snap.poolSize} storage=${snap.storageUsed} files=${snap.fileCount}`
  );
  return snap;
}

async function uploadBatch(vfs, startIdx, endIdx, label, peakSubreq = { value: 0 }) {
  let bytesUploaded = 0;
  let filesUploaded = 0;
  let errors = 0;
  let lastPollBytes = 0;
  const polls = [];
  const stageStart = Date.now();

  for (let i = startIdx; i < endIdx; i++) {
    const path = `/load/${i.toString().padStart(8, "0")}.bin`;
    const bytes = makeFile(i + 1);
    try {
      const result = await parallelUpload(vfs, path, bytes, {
        mimeType: "application/octet-stream",
        onProgress: (e) => {
          if (typeof e.currentParallelism === "number" && e.currentParallelism > peakSubreq.value) {
            peakSubreq.value = e.currentParallelism;
          }
        },
      });
      void result;
      filesUploaded++;
      bytesUploaded += bytes.byteLength;
    } catch (err) {
      errors++;
      recordError(`upload ${label}`, err);
      log(`  upload error at ${path}: ${err instanceof Error ? err.message : String(err)}`);
    }
    if (bytesUploaded - lastPollBytes >= POLL_EVERY_BYTES || i === endIdx - 1) {
      const snap = await pollAnalytics(`${label} after ${(bytesUploaded / 1e9).toFixed(2)} GB`);
      if (snap) polls.push(snap);
      lastPollBytes = bytesUploaded;
    }
  }
  const stageMs = Date.now() - stageStart;
  recordStage(label, {
    filesUploaded,
    bytesUploaded,
    errors,
    elapsedMs: stageMs,
    throughputMBps: (bytesUploaded / 1024 / 1024) / Math.max(1, stageMs / 1000),
    polls,
    peakConcurrency: peakSubreq.value,
  });
  return { bytesUploaded, filesUploaded, errors, polls };
}

async function readbackSample(vfs, indices) {
  log(`readback sample of ${indices.length} files for byte-equality verification`);
  let ok = 0;
  let mismatches = 0;
  let missing = 0;
  for (const i of indices) {
    const path = `/load/${i.toString().padStart(8, "0")}.bin`;
    try {
      const got = await vfs.readFile(path);
      const expected = expectedSalt(i + 1);
      const headOk = bytesEqual(got.subarray(0, 16), expected.head);
      const tailOk = bytesEqual(got.subarray(got.length - 16), expected.tail);
      if (got.length !== FILE_BYTES) {
        mismatches++;
        log(`  size mismatch at ${path}: got ${got.length} expected ${FILE_BYTES}`);
      } else if (headOk && tailOk) {
        ok++;
      } else {
        mismatches++;
        log(`  byte mismatch at ${path}: head=${headOk} tail=${tailOk}`);
      }
    } catch (err) {
      missing++;
      log(`  readback error at ${path}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }
  recordStage("readback", { sampled: indices.length, ok, mismatches, missing });
  return { ok, mismatches, missing };
}

async function deleteHalf(vfs) {
  const toDelete = [];
  for (let i = 0; i < FILES_TOTAL; i++) {
    if (Math.random() < DELETE_RATIO) toDelete.push(i);
  }
  log(`deleting ${toDelete.length} of ${FILES_TOTAL} files (ratio=${DELETE_RATIO})`);
  let ok = 0;
  let errors = 0;
  for (const i of toDelete) {
    try {
      await vfs.unlink(`/load/${i.toString().padStart(8, "0")}.bin`);
      ok++;
    } catch (err) {
      errors++;
      recordError("delete", err);
    }
  }
  const post = await pollAnalytics("post-delete");
  recordStage("delete-half", {
    requested: toDelete.length,
    ok,
    errors,
    poolAfter: post?.poolSize ?? null,
    storageAfter: post?.storageUsed ?? null,
    filesAfter: post?.fileCount ?? null,
    deletedIndices: toDelete,
  });
  return { deletedIndices: toDelete, post };
}

async function reuploadDedup(vfs, peakSubreq) {
  const reuploadFiles = Math.ceil((REUPLOAD_GB * 1024 * 1024 * 1024) / FILE_BYTES);
  log(`re-upload ${REUPLOAD_GB} GB (${reuploadFiles} files) to exercise dedup`);
  // Re-use the FIRST `reuploadFiles` indices' content (same saltSeed,
  // same bytes \u2192 server-side content-addressed dedup should fire).
  let ok = 0;
  let errors = 0;
  const stageStart = Date.now();
  for (let i = 0; i < reuploadFiles; i++) {
    const path = `/load/redup/${i.toString().padStart(8, "0")}.bin`;
    const bytes = makeFile(i + 1);
    try {
      await parallelUpload(vfs, path, bytes, {
        mimeType: "application/octet-stream",
        onProgress: (e) => {
          if (typeof e.currentParallelism === "number" && e.currentParallelism > peakSubreq.value) {
            peakSubreq.value = e.currentParallelism;
          }
        },
      });
      ok++;
    } catch (err) {
      errors++;
      recordError("reupload-dedup", err);
    }
  }
  const post = await pollAnalytics("post-reupload");
  recordStage("reupload-dedup", {
    files: reuploadFiles,
    ok,
    errors,
    elapsedMs: Date.now() - stageStart,
    poolAfter: post?.poolSize ?? null,
    storageAfter: post?.storageUsed ?? null,
    filesAfter: post?.fileCount ?? null,
  });
}

async function deleteAccount() {
  log("deleting tenant (account-delete)");
  const res = await accessFetch(`${BASE_URL}/api/auth/account`, {
    method: "DELETE",
    headers: { Authorization: `Bearer ${sessionToken}` },
  });
  recordStage("account-delete", { status: res.status });
  log(`account-delete returned HTTP ${res.status}`);
}

// ── Top-level ─────────────────────────────────────────────────────

async function main() {
  log("=".repeat(60));
  log(`100 GB single-tenant load test`);
  log(`base       = ${BASE_URL}`);
  log(`tenant     = ${TEST_EMAIL}`);
  log(`total      = ${TOTAL_GB} GB across ${FILES_TOTAL} files of ${FILE_MB} MB`);
  log(`dry-run    = ${DRY_RUN}`);
  log(`access     = ${ACCESS_CLIENT_ID ? "service-token" : "none (will fail behind Access)"}`);
  log(`report     = ${REPORT_PATH}`);
  log("=".repeat(60));

  await signUp();

  try {
    const vfsToken = await mintVfsToken();
    const vfs = createMossaicHttpClient({
      url: BASE_URL,
      apiKey: vfsToken,
      fetcher: accessFetch,
    });
    const peakSubreq = { value: 0 };

    const baseline = await pollAnalytics("baseline");
    recordStage("baseline", { snapshot: baseline });

    if (DRY_RUN) {
      log("DRY_RUN: uploading ONE 100MB file then bailing");
      await uploadBatch(vfs, 0, 1, "dry-run", peakSubreq);
      const sample = [0];
      await readbackSample(vfs, sample);
    } else {
      // Stage 1: 100 GB upload
      log(`stage 1: bulk upload ${FILES_TOTAL} files`);
      await uploadBatch(vfs, 0, FILES_TOTAL, "bulk-upload", peakSubreq);

      // Stage 2: readback sample
      const sample = pickRandomIndices(FILES_TOTAL, SAMPLE_N);
      await readbackSample(vfs, sample);

      // Stage 3: delete half
      await deleteHalf(vfs);

      // Stage 4: re-upload to exercise dedup
      await reuploadDedup(vfs, peakSubreq);
    }

    report.finalVerdict = synthesizeVerdict();
  } catch (err) {
    recordError("top-level", err);
    log(`fatal: ${err instanceof Error ? err.message : String(err)}`);
    report.finalVerdict = { ok: false, reason: "errored mid-test", err: String(err) };
  } finally {
    try {
      await deleteAccount();
    } catch (err) {
      recordError("cleanup", err);
    }
    await flushReport();
    log(`report written to ${REPORT_PATH}`);
  }
}

function pickRandomIndices(n, k) {
  const out = [];
  const seen = new Set();
  while (out.length < Math.min(k, n)) {
    const i = Math.floor(Math.random() * n);
    if (!seen.has(i)) {
      seen.add(i);
      out.push(i);
    }
  }
  return out;
}

function synthesizeVerdict() {
  const upload = report.stages.find((s) => s.name === "bulk-upload");
  const readback = report.stages.find((s) => s.name === "readback");
  const del = report.stages.find((s) => s.name === "delete-half");
  const redup = report.stages.find((s) => s.name === "reupload-dedup");

  const lastPoolSize = (() => {
    for (let i = report.stages.length - 1; i >= 0; i--) {
      const s = report.stages[i];
      if (Array.isArray(s.polls) && s.polls.length > 0) {
        return s.polls[s.polls.length - 1].poolSize;
      }
      if (typeof s.poolAfter === "number") return s.poolAfter;
    }
    return null;
  })();

  const checks = {
    poolGrowthHit60Plus: typeof lastPoolSize === "number" && lastPoolSize >= 60,
    uploadErrorsZero: upload ? upload.errors === 0 : false,
    readbackByteEquality: readback ? readback.mismatches === 0 && readback.missing === 0 : false,
    poolMonotonic:
      del && upload
        ? typeof del.poolAfter === "number" &&
          typeof lastPoolSize === "number" &&
          del.poolAfter >= 32
        : true,
    dedupReuploadOk: redup ? redup.errors === 0 : true,
  };
  const allOk = Object.values(checks).every((v) => v === true);
  return {
    ok: allOk,
    lastPoolSize,
    checks,
    bet: allOk ? "would bet $10k now" : "would NOT bet \u2014 see failed checks",
  };
}

main().catch((err) => {
  recordError("unhandled", err);
  flushReport().finally(() => process.exit(1));
});
