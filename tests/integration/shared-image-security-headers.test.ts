import { describe, it, expect } from "vitest";
import { SELF } from "cloudflare:test";
import { setupTenant } from "./_helpers";

async function writeFileViaHttp(
  vfsToken: string,
  path: string,
  data: string,
  mimeType: string
): Promise<void> {
  const res = await SELF.fetch("https://test/api/vfs/writeFile", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${vfsToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ path, data, mimeType }),
  });
  expect(res.status).toBe(200);
}

async function resolveFileId(sessionJwt: string, path: string): Promise<string> {
  const res = await SELF.fetch("https://test/api/index/file", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${sessionJwt}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ path }),
  });
  expect(res.status).toBe(201);
  const body = (await res.json()) as { fileId: string };
  expect(body.fileId.length).toBeGreaterThan(0);
  return body.fileId;
}

async function mintShareToken(
  sessionJwt: string,
  fileIds: string[]
): Promise<string> {
  const res = await SELF.fetch("https://test/api/auth/share-token", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${sessionJwt}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fileIds, albumName: "security-regression" }),
  });
  expect(res.status).toBe(200);
  const body = (await res.json()) as { token: string };
  expect(body.token.length).toBeGreaterThan(0);
  return body.token;
}

describe("shared/gallery image route hardening", () => {
  it("forces attachment + nosniff + CSP sandbox for non-image share bytes", async () => {
    const tenant = await setupTenant("security-shared-html@test.example");
    await writeFileViaHttp(
      tenant.vfsToken,
      "/poc.html",
      "<html><script>window.pwned=1</script></html>",
      "text/html"
    );

    const fileId = await resolveFileId(tenant.jwt, "/poc.html");
    const shareToken = await mintShareToken(tenant.jwt, [fileId]);

    const res = await SELF.fetch(
      `https://test/api/shared/${shareToken}/image/${fileId}`
    );
    expect(res.status).toBe(200);
    expect(res.headers.get("Content-Type")).toBe("application/octet-stream");
    expect(res.headers.get("Content-Disposition")).toBe("attachment");
    expect(res.headers.get("X-Content-Type-Options")).toBe("nosniff");
    expect(res.headers.get("Content-Security-Policy")).toBe(
      "sandbox; default-src 'none'"
    );
  });

  it("allows inline SVG with nosniff + CSP sandbox", async () => {
    const tenant = await setupTenant("security-shared-svg@test.example");
    await writeFileViaHttp(
      tenant.vfsToken,
      "/preview.svg",
      "<svg xmlns='http://www.w3.org/2000/svg'><rect width='2' height='2'/></svg>",
      "image/svg+xml"
    );

    const fileId = await resolveFileId(tenant.jwt, "/preview.svg");
    const shareToken = await mintShareToken(tenant.jwt, [fileId]);

    const res = await SELF.fetch(
      `https://test/api/shared/${shareToken}/image/${fileId}`
    );
    expect(res.status).toBe(200);
    expect(res.headers.get("Content-Type")).toBe("image/svg+xml");
    expect(res.headers.get("Content-Disposition")).toBeNull();
    expect(res.headers.get("X-Content-Type-Options")).toBe("nosniff");
    expect(res.headers.get("Content-Security-Policy")).toBe(
      "sandbox; default-src 'none'"
    );
  });

  it("hardens authenticated gallery image route for non-image mime", async () => {
    const tenant = await setupTenant("security-gallery-html@test.example");
    await writeFileViaHttp(
      tenant.vfsToken,
      "/gallery-poc.html",
      "<html><script>window.pwned=1</script></html>",
      "text/html"
    );

    const fileId = await resolveFileId(tenant.jwt, "/gallery-poc.html");

    const res = await SELF.fetch(`https://test/api/gallery/image/${fileId}`, {
      headers: { Authorization: `Bearer ${tenant.jwt}` },
    });
    expect(res.status).toBe(200);
    expect(res.headers.get("Content-Type")).toBe("application/octet-stream");
    expect(res.headers.get("Content-Disposition")).toBe("attachment");
    expect(res.headers.get("X-Content-Type-Options")).toBe("nosniff");
    expect(res.headers.get("Content-Security-Policy")).toBe(
      "sandbox; default-src 'none'"
    );
  });

  it("applies hardening headers on shared-image Range/206 responses", async () => {
    const tenant = await setupTenant("security-shared-range@test.example");
    await writeFileViaHttp(
      tenant.vfsToken,
      "/range.html",
      "<html><script>window.pwned=1</script></html>",
      "text/html"
    );

    const fileId = await resolveFileId(tenant.jwt, "/range.html");
    const shareToken = await mintShareToken(tenant.jwt, [fileId]);

    const res = await SELF.fetch(
      `https://test/api/shared/${shareToken}/image/${fileId}`,
      {
        headers: { Range: "bytes=0-5" },
      }
    );
    expect(res.status).toBe(206);
    expect(res.headers.get("Content-Type")).toBe("application/octet-stream");
    expect(res.headers.get("Content-Disposition")).toBe("attachment");
    expect(res.headers.get("X-Content-Type-Options")).toBe("nosniff");
    expect(res.headers.get("Content-Security-Policy")).toBe(
      "sandbox; default-src 'none'"
    );
    expect(res.headers.get("Accept-Ranges")).toBe("bytes");
  });

  it("applies hardening headers on gallery-image Range/206 responses", async () => {
    const tenant = await setupTenant("security-gallery-range@test.example");
    await writeFileViaHttp(
      tenant.vfsToken,
      "/gallery-range.html",
      "<html><script>window.pwned=1</script></html>",
      "text/html"
    );

    const fileId = await resolveFileId(tenant.jwt, "/gallery-range.html");

    const res = await SELF.fetch(`https://test/api/gallery/image/${fileId}`, {
      headers: {
        Authorization: `Bearer ${tenant.jwt}`,
        Range: "bytes=0-5",
      },
    });
    expect(res.status).toBe(206);
    expect(res.headers.get("Content-Type")).toBe("application/octet-stream");
    expect(res.headers.get("Content-Disposition")).toBe("attachment");
    expect(res.headers.get("X-Content-Type-Options")).toBe("nosniff");
    expect(res.headers.get("Content-Security-Policy")).toBe(
      "sandbox; default-src 'none'"
    );
    expect(res.headers.get("Accept-Ranges")).toBe("bytes");
  });
});
