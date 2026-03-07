/**
 * Password hashing using SubtleCrypto PBKDF2.
 * Workers-compatible — no bcryptjs needed.
 */

const PBKDF2_ITERATIONS = 100_000;
const SALT_LENGTH = 16;
const KEY_LENGTH = 32;

/**
 * Hash a password using PBKDF2-SHA256.
 * Returns "base64salt:base64hash".
 */
export async function hashPassword(password: string): Promise<string> {
  const salt = crypto.getRandomValues(new Uint8Array(SALT_LENGTH));
  const key = await deriveKey(password, salt);
  const hash = await crypto.subtle.exportKey("raw", key);
  const saltB64 = btoa(String.fromCharCode(...salt));
  const hashB64 = btoa(String.fromCharCode(...new Uint8Array(hash)));
  return `${saltB64}:${hashB64}`;
}

/**
 * Verify a password against a stored hash.
 */
export async function verifyPassword(
  password: string,
  stored: string
): Promise<boolean> {
  const [saltB64, expectedB64] = stored.split(":");
  if (!saltB64 || !expectedB64) return false;

  const salt = Uint8Array.from(atob(saltB64), (c) => c.charCodeAt(0));
  const key = await deriveKey(password, salt);
  const hash = await crypto.subtle.exportKey("raw", key);
  const actualB64 = btoa(String.fromCharCode(...new Uint8Array(hash)));
  return actualB64 === expectedB64;
}

async function deriveKey(
  password: string,
  salt: Uint8Array
): Promise<CryptoKey> {
  const encoder = new TextEncoder();
  const keyMaterial = await crypto.subtle.importKey(
    "raw",
    encoder.encode(password),
    "PBKDF2",
    false,
    ["deriveBits", "deriveKey"]
  );

  return crypto.subtle.deriveKey(
    {
      name: "PBKDF2",
      salt,
      iterations: PBKDF2_ITERATIONS,
      hash: "SHA-256",
    },
    keyMaterial,
    { name: "AES-GCM", length: KEY_LENGTH * 8 },
    true,
    ["encrypt", "decrypt"]
  );
}
