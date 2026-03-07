/**
 * Generate a ULID-like sortable unique ID.
 * Uses timestamp prefix + random suffix.
 */
export function generateId(): string {
  const timestamp = Date.now().toString(36);
  const random = crypto.getRandomValues(new Uint8Array(8));
  const randomStr = Array.from(random)
    .map((b) => b.toString(36).padStart(2, "0"))
    .join("")
    .slice(0, 12);
  return `${timestamp}${randomStr}`;
}

/**
 * Create a JSON error response.
 */
export function errorResponse(
  message: string,
  status: number = 400
): Response {
  return Response.json({ error: message }, { status });
}

/**
 * Create a JSON success response.
 */
export function jsonResponse<T>(data: T, status: number = 200): Response {
  return Response.json(data, { status });
}

/**
 * Build the UserDO name for a given userId.
 */
export function userDOName(userId: string): string {
  return `user:${userId}`;
}

/**
 * Build the ShardDO name.
 */
export function shardDOName(userId: string, shardIndex: number): string {
  return `shard:${userId}:${shardIndex}`;
}
