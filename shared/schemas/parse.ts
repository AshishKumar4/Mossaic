import type { z } from "zod/v4";
import { VFSError } from "../vfs-types";

export function parseRequestBody<T>(
	schema: z.ZodType<T>,
	body: unknown,
	context: string,
): T {
	const parsed = schema.safeParse(body);
	if (!parsed.success) {
		throw new VFSError(
			"EINVAL",
			`${context}: invalid request body: ${parsed.error.message}`,
		);
	}
	return parsed.data;
}

export function parseResponse<T>(
	schema: z.ZodType<T>,
	raw: unknown,
	context: string,
): T {
	const parsed = schema.safeParse(raw);
	if (!parsed.success) {
		throw new TypeError(
			`${context}: invalid response body: ${parsed.error.message}`,
			{ cause: parsed.error },
		);
	}
	return parsed.data;
}
