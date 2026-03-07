import type { UserDO } from "./user-do";
import { hashPassword, verifyPassword } from "../../lib/crypto";
import { generateId } from "../../lib/utils";

export interface AuthResult {
  userId: string;
  email: string;
}

/**
 * Handle user signup. Creates auth record, quota record.
 */
export async function handleSignup(
  durableObject: UserDO,
  email: string,
  password: string
): Promise<AuthResult> {
  // Check if email already exists
  const existing = durableObject.sql
    .exec("SELECT user_id FROM auth WHERE email = ?", email)
    .toArray();
  if (existing.length > 0) {
    throw new Error("Email already registered");
  }

  const userId = generateId();
  const passwordHash = await hashPassword(password);
  const now = Date.now();

  durableObject.sql.exec(
    `INSERT INTO auth (user_id, email, password_hash, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?)`,
    userId,
    email,
    passwordHash,
    now,
    now
  );

  durableObject.sql.exec(
    `INSERT INTO quota (user_id, storage_used, storage_limit, file_count, pool_size)
     VALUES (?, 0, 107374182400, 0, 32)`,
    userId
  );

  return { userId, email };
}

/**
 * Handle user login. Verifies credentials.
 */
export async function handleLogin(
  durableObject: UserDO,
  email: string,
  password: string
): Promise<AuthResult> {
  const rows = durableObject.sql
    .exec(
      "SELECT user_id, email, password_hash FROM auth WHERE email = ?",
      email
    )
    .toArray();

  if (rows.length === 0) {
    throw new Error("Invalid credentials");
  }

  const user = rows[0] as { user_id: string; email: string; password_hash: string };
  const valid = await verifyPassword(password, user.password_hash);
  if (!valid) {
    throw new Error("Invalid credentials");
  }

  return { userId: user.user_id, email: user.email };
}
