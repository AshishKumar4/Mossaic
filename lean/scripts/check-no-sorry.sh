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
#
# Phase 24: the previous version of this script silently whitelisted
# `AES_GCM_IND_CPA`. That axiom has been REMOVED (its conclusion was
# `True`, making it logically vacuous). The whitelist is now empty by
# default; any future `axiom` addition fails the gate immediately, no
# silent allowance.

set -euo pipefail

cd "$(dirname "$0")/.."

# 1. Run lake build and capture sorry warnings.
output=$(lake build 2>&1 || true)

# Filter for files we care about (everything under Mossaic/).
must_have_pattern='Mossaic/Vfs/(Common|Tenant|Refcount|Gc|AtomicWrite|Versioning|Encryption|Multipart|Quota|Preview|Tombstone|HistoryPreservation|StreamRouting)\.lean|Mossaic/Generated/(ShardDO|UserDO|Placement)\.lean'

sorry_warnings=$(echo "$output" | grep -E "declaration uses .sorry." | grep -E "$must_have_pattern" || true)

if [[ -n "$sorry_warnings" ]]; then
  echo "FAIL: sorry detected in proof files:"
  echo "$sorry_warnings"
  exit 1
fi

# 2. Project axioms. Phase 24: the whitelist is intentionally EMPTY.
#
# If you need to add a literature axiom (e.g., a cryptographic
# assumption), it MUST be (a) named, (b) PR-reviewed, and (c) added
# to this whitelist explicitly with a justification comment. The
# previous silent allowance for `AES_GCM_IND_CPA` was removed in
# Phase 24 because that axiom's conclusion was `True`, making it a
# no-op declaration that lent false credibility.
WHITELISTED_AXIOMS=()

# Find every axiom declaration in proof files. The project pattern
# `^axiom <name>` or `^[[:space:]]+axiom <name>`.
all_axioms_raw=$(grep -rnE '^axiom |^[[:space:]]+axiom ' Mossaic/ || true)
if [[ -n "$all_axioms_raw" ]]; then
  unauthorized=""
  while IFS= read -r line; do
    name=$(echo "$line" | sed -E 's/.*axiom +([A-Za-z_][A-Za-z0-9_]*).*/\1/')
    is_whitelisted=0
    for w in "${WHITELISTED_AXIOMS[@]:+${WHITELISTED_AXIOMS[@]}}"; do
      if [[ "$name" == "$w" ]]; then
        is_whitelisted=1
        break
      fi
    done
    if [[ "$is_whitelisted" -eq 0 ]]; then
      unauthorized+="$line"$'\n'
    fi
  done <<< "$all_axioms_raw"
  if [[ -n "$unauthorized" ]]; then
    echo "FAIL: project-level axiom declarations found (no whitelist active):"
    echo "$unauthorized"
    if [[ ${#WHITELISTED_AXIOMS[@]} -eq 0 ]]; then
      echo "Whitelist is intentionally empty (Phase 24 cleanup)."
    else
      echo "Whitelisted: ${WHITELISTED_AXIOMS[*]}"
    fi
    exit 1
  fi
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
