import type { UserDOCore } from "@core/objects/user/user-do-core";
import { transactionSync } from "@core/objects/user/internal-storage";

declare const durableObject: UserDOCore;

transactionSync(durableObject, () => 1);

// @ts-expect-error transaction callbacks must complete synchronously.
transactionSync(durableObject, async () => 1);
