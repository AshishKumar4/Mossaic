import { describe, it, expect } from "vitest";
import { SELF, env } from "cloudflare:test";

/**
 * Phase 15 — Step 7 — HTTP fallback `X-Mossaic-Encryption` header.
 *
 *   H1.  POST /api/vfs/writeFile with `X-Mossaic-Encryption` header
 *        and an octet-stream body stamps the encryption columns
 *        on the file row server-side.
 *   H2.  POST /api/vfs/readFile responds with `X-Mossaic-Encryption`
 *        header when the file is encrypted, and omits it otherwise.
 *   H3.  Invalid `X-Mossaic-Encryption` header → 400 EINVAL.
 *
 * The HTTP-fallback consumer is responsible for client-side
 * encryption / decryption — the server just relays the bytes and the
 * mode metadata. These tests exercise the metadata routing only.
 */

import { signVFSToken } from "@core/lib/auth";

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
  JWT_SECRET?: string;
}
const TEST_ENV = env as unknown as E;

async function mintToken(
  ns: string,
  tenant: string,
  sub?: string
): Promise<string> {
  return signVFSToken(TEST_ENV, { ns, tenant, sub });
}

describe("Phase 15 — HTTP fallback X-Mossaic-Encryption header", () => {
  it("H1 — writeFile with X-Mossaic-Encryption stamps the column", async () => {
    const ns = "default";
    const tenant = "p15-h1";
    const apiKey = await mintToken(ns, tenant);
    // Send some opaque bytes (a "fake envelope" — the server doesn't
    // care about the structure for this test).
    const payload = new Uint8Array([
      0xfa, 0xfa, 0xfa, 0xfa, 0xfa, 0xfa, 0xfa, 0xfa,
    ]);
    const r = await SELF.fetch(
      `https://mossaic.test/api/vfs/writeFile?path=/encrypted.bin`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/octet-stream",
          "X-Mossaic-Encryption": JSON.stringify({
            mode: "convergent",
            keyId: "http-key-v1",
          }),
        },
        body: payload,
      }
    );
    expect(r.status).toBe(200);
    // Now stat to confirm the columns.
    const sr = await SELF.fetch(`https://mossaic.test/api/vfs/stat`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/encrypted.bin" }),
    });
    expect(sr.status).toBe(200);
    const sb = (await sr.json()) as {
      stat: { encryption?: { mode: string; keyId?: string } };
    };
    expect(sb.stat.encryption?.mode).toBe("convergent");
    expect(sb.stat.encryption?.keyId).toBe("http-key-v1");
  });

  it("H2 — readFile surfaces X-Mossaic-Encryption response header for encrypted files", async () => {
    const ns = "default";
    const tenant = "p15-h2";
    const apiKey = await mintToken(ns, tenant);
    // Seed: write encrypted via the binding (avoids HTTP round-trip
    // for setup). Then read back via HTTP.
    const payload = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
    const wr = await SELF.fetch(
      `https://mossaic.test/api/vfs/writeFile?path=/secret.bin`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/octet-stream",
          "X-Mossaic-Encryption": JSON.stringify({ mode: "random" }),
        },
        body: payload,
      }
    );
    expect(wr.status).toBe(200);

    const rr = await SELF.fetch(`https://mossaic.test/api/vfs/readFile`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/secret.bin" }),
    });
    expect(rr.status).toBe(200);
    const encHdr = rr.headers.get("X-Mossaic-Encryption");
    expect(encHdr).not.toBeNull();
    const parsed = JSON.parse(encHdr!);
    expect(parsed.mode).toBe("random");
    // And the bytes round-trip (server is byte-faithful).
    const back = new Uint8Array(await rr.arrayBuffer());
    expect(back).toEqual(payload);

    // Plaintext file must NOT have the header.
    const wp = await SELF.fetch(
      `https://mossaic.test/api/vfs/writeFile?path=/plain.bin`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/octet-stream",
        },
        body: payload,
      }
    );
    expect(wp.status).toBe(200);
    const rp = await SELF.fetch(`https://mossaic.test/api/vfs/readFile`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/plain.bin" }),
    });
    expect(rp.status).toBe(200);
    expect(rp.headers.get("X-Mossaic-Encryption")).toBeNull();
  });

  it("H3 — invalid X-Mossaic-Encryption header → 400 EINVAL", async () => {
    const ns = "default";
    const tenant = "p15-h3";
    const apiKey = await mintToken(ns, tenant);
    const payload = new Uint8Array([0xfa]);

    // Invalid JSON.
    const r1 = await SELF.fetch(
      `https://mossaic.test/api/vfs/writeFile?path=/x.bin`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/octet-stream",
          "X-Mossaic-Encryption": "{not-json",
        },
        body: payload,
      }
    );
    expect(r1.status).toBe(400);
    const b1 = (await r1.json()) as { code: string };
    expect(b1.code).toBe("EINVAL");

    // Invalid mode.
    const r2 = await SELF.fetch(
      `https://mossaic.test/api/vfs/writeFile?path=/x.bin`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/octet-stream",
          "X-Mossaic-Encryption": JSON.stringify({ mode: "totally-not-real" }),
        },
        body: payload,
      }
    );
    expect(r2.status).toBe(400);
  });
});
