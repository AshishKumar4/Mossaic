#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")/.."

set +e
output="$(lake build 2>&1)"
build_status=$?
set -e

if [[ $build_status -ne 0 ]]; then
  echo "FAIL: lake build failed."
  printf '%s\n' "$output"
  exit "$build_status"
fi

sorry_warnings="$(printf '%s\n' "$output" | grep -E "Mossaic.*declaration uses .sorry.|declaration uses .sorry.*Mossaic" || true)"
if [[ -n "$sorry_warnings" ]]; then
  echo "FAIL: sorry-backed declarations detected:"
  printf '%s\n' "$sorry_warnings"
  exit 1
fi

placeholders="$(grep -InE '^[[:space:]]*(sorry|admit)([[:space:];]|$)|:=[^/]*(sorry|admit)([[:space:];]|$)|\bby[[:space:]]+(sorry|admit)([[:space:];]|$)' Mossaic.lean || true; grep -RInE '^[[:space:]]*(sorry|admit)([[:space:];]|$)|:=[^/]*(sorry|admit)([[:space:];]|$)|\bby[[:space:]]+(sorry|admit)([[:space:];]|$)' Mossaic --include='*.lean' || true)"
if [[ -n "$placeholders" ]]; then
  echo "FAIL: executable sorry/admit token detected:"
  printf '%s\n' "$placeholders"
  exit 1
fi

axioms="$(grep -InE '^[[:space:]]*axiom[[:space:]]+' Mossaic.lean || true; grep -RInE '^[[:space:]]*axiom[[:space:]]+' Mossaic --include='*.lean' || true)"
if [[ -n "$axioms" ]]; then
  echo "FAIL: project-level axiom declarations detected:"
  printf '%s\n' "$axioms"
  exit 1
fi

echo "OK: lake build succeeded; zero executable sorry/admit; zero project axiom declarations."
