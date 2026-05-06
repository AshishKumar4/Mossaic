#!/usr/bin/env node
import { run } from "./main.js";
run().catch((err) => {
  // Unexpected throw bubbling out of main — should never happen because
  // main wraps every command in try/catch. If it does, surface the
  // stack so debugging is straightforward.
  // eslint-disable-next-line no-console
  console.error(err);
  process.exit(1);
});
