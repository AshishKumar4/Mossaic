#!/usr/bin/env bash
#
# Gate that fails the build when stale "Phase NN" narration leaks
# into runtime code. Phase 44 stripped 307 markers to 0; Phase 46
# reintroduced 60+. Phase 47 adds this gate so future phases can't
# regress silently.
#
# Scope: production code only. Tests intentionally use Phase NN as
# stable test IDs (e.g. "Phase 42 — alarm-failures counter") and
# are excluded. Documentation (docs/, README.md, AGENTS.md, lean/,
# local/) is also out of scope — those track historical narrative
# deliberately.
#
# Pattern: `Phase ?[0-9]` covers `Phase 1`, `Phase 12`, `Phase 27.5`,
# `Phase-32` (rare hyphenated form).
#
# Exit:
#   0 — no markers found (clean)
#   1 — at least one marker found (CI fails)
#
# Usage:
#   bash scripts/check-no-phase-tags.sh

set -euo pipefail

# Directories to scan (runtime code only).
readonly SCOPED_DIRS=(
  worker
  sdk/src
  sdk/templates
  src
  shared
  deployments
)

# Single-file targets.
readonly SCOPED_FILES=(
  wrangler.jsonc
  sdk/tsup.config.ts
)

# File extensions to consider.
readonly EXTS=("ts" "tsx" "jsonc")

INCLUDE_ARGS=()
for ext in "${EXTS[@]}"; do
  INCLUDE_ARGS+=(--include="*.${ext}")
done

EXISTING_DIRS=()
for d in "${SCOPED_DIRS[@]}"; do
  [[ -d "$d" ]] && EXISTING_DIRS+=("$d")
done

EXISTING_FILES=()
for f in "${SCOPED_FILES[@]}"; do
  [[ -f "$f" ]] && EXISTING_FILES+=("$f")
done

if [[ ${#EXISTING_DIRS[@]} -eq 0 && ${#EXISTING_FILES[@]} -eq 0 ]]; then
  echo "ERROR: no scoped paths found; run from repo root" >&2
  exit 2
fi

# `grep -E` with the union pattern; -r recursive over directories,
# direct match for individual files.
PATTERN='Phase[ -]?[0-9]'

found=0
for d in "${EXISTING_DIRS[@]}"; do
  if grep -rEn "$PATTERN" "${INCLUDE_ARGS[@]}" "$d" 2>/dev/null; then
    found=1
  fi
done
for f in "${EXISTING_FILES[@]}"; do
  if grep -En "$PATTERN" "$f" 2>/dev/null; then
    found=1
  fi
done

if [[ "$found" -ne 0 ]]; then
  echo "" >&2
  echo "FAIL: stale 'Phase NN' narration found in runtime code." >&2
  echo "      Strip the phase tag — preserve the WHY, not the history." >&2
  echo "      See AGENTS.md or the Phase 44 commit for guidance." >&2
  exit 1
fi

echo "OK: no Phase NN markers in runtime code."
