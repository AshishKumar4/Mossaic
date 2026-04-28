#!/usr/bin/env bash
# Verify zero `sorry` in must-have Lean proofs.
#
# Strategy: use `lake env lean --print-axioms` indirectly via grep on the
# build output. Simpler: rely on Lean's compiler — if any `sorry` is used
# as a tactic in a definition, `lake build` emits a warning of the form
# `declaration uses 'sorry'`. We grep `lake build` output for that.
#
# This is the load-bearing check: if a proof secretly uses `sorry`, lake's
# warning will catch it.

set -euo pipefail

cd "$(dirname "$0")/.."

# Run `lake build` and capture warnings about sorry.
output=$(lake build 2>&1 || true)

# Filter for files we care about (must-have only).
must_have_pattern='Mossaic/Vfs/(Common|Tenant|Refcount|Gc)\.lean|Mossaic/Generated/(ShardDO|UserDO|Placement)\.lean'

# Look for "declaration uses 'sorry'" warnings on must-have files.
sorry_warnings=$(echo "$output" | grep -E "declaration uses .sorry." | grep -E "$must_have_pattern" || true)

if [[ -n "$sorry_warnings" ]]; then
  echo "FAIL: sorry detected in must-have proofs:"
  echo "$sorry_warnings"
  exit 1
fi

# Sanity check: confirm `lake build` succeeded.
if echo "$output" | grep -q "Build completed successfully"; then
  echo "OK: lake build succeeded; zero sorry in must-have proofs."
  exit 0
elif echo "$output" | grep -q "build failed"; then
  echo "FAIL: lake build failed."
  echo "$output" | tail -20
  exit 1
fi

# Fall-through: build neither succeeded nor failed visibly. Treat as OK
# only if the output had a "Built" indicator and no error: lines.
if echo "$output" | grep -q "Built Mossaic" && ! echo "$output" | grep -q "^error:"; then
  echo "OK: zero sorry in must-have proofs."
  exit 0
fi

echo "WARN: lake build output unrecognized:"
echo "$output" | tail -10
exit 1
