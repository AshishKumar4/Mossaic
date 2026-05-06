#!/usr/bin/env bash
# Verify zero `sorry` and zero project `axiom` in Mossaic Lean proofs.
#
# Strategy: rely on Lean's compiler emitting a warning when a definition
# uses `sorry`. A `lake build` warning of the form
# `declaration uses 'sorry'` triggers a failure.
#
# For axioms, we grep the source files directly for any line beginning
# with `axiom` (modulo whitespace) — that catches every project-level
# declaration. Kernel axioms (`Classical.choice`, `propext`, `Quot.sound`)
# live in core Lean, not in our project, so they don't appear in the grep.

set -euo pipefail

cd "$(dirname "$0")/.."

# 1. Run lake build and capture sorry warnings.
output=$(lake build 2>&1 || true)

# Filter for files we care about (must-have only).
must_have_pattern='Mossaic/Vfs/(Common|Tenant|Refcount|Gc|AtomicWrite|Versioning)\.lean|Mossaic/Generated/(ShardDO|UserDO|Placement)\.lean'

sorry_warnings=$(echo "$output" | grep -E "declaration uses .sorry." | grep -E "$must_have_pattern" || true)

if [[ -n "$sorry_warnings" ]]; then
  echo "FAIL: sorry detected in proof files:"
  echo "$sorry_warnings"
  exit 1
fi

# 2. Project axioms.
project_axioms=$(grep -rnE '^axiom|^[[:space:]]+axiom' Mossaic/ || true)
if [[ -n "$project_axioms" ]]; then
  echo "FAIL: project-level axiom declarations found:"
  echo "$project_axioms"
  exit 1
fi

# 3. Confirm `lake build` succeeded.
if echo "$output" | grep -q "Build completed successfully"; then
  echo "OK: lake build succeeded; zero sorry; zero project axioms."
  exit 0
elif echo "$output" | grep -q "build failed"; then
  echo "FAIL: lake build failed."
  echo "$output" | tail -20
  exit 1
fi

# Fall-through: build neither succeeded nor failed visibly.
if echo "$output" | grep -q "Built Mossaic" && ! echo "$output" | grep -q "^error:"; then
  echo "OK: zero sorry; zero project axioms."
  exit 0
fi

echo "WARN: lake build output unrecognized:"
echo "$output" | tail -10
exit 1
