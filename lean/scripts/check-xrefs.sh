#!/usr/bin/env bash
# Verify that every `@lean-invariant Foo.Bar.baz` annotation in TS source
# resolves to an actual `theorem baz` in `lean/Foo/Bar.lean`.
#
# This is the drift-detection gate: if a TS source comment references a
# Lean theorem that has been deleted or renamed, this script fails CI.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

failed=0

# Find all `@lean-invariant <full.theorem.name>` annotations in TS.
# Format expected: `@lean-invariant Mossaic.{Vfs,Generated}.<Module>.<theorem>`
xrefs=$(grep -rEn '@lean-invariant\s+\S+' worker/ shared/ sdk/ src/ 2>/dev/null | \
  sed -E 's/.*@lean-invariant[[:space:]]+([A-Za-z0-9._]+).*/\1/' | \
  sort -u || true)

if [[ -z "$xrefs" ]]; then
  echo "WARN: no @lean-invariant annotations found. Skipping xref check."
  exit 0
fi

while IFS= read -r theorem_path; do
  # Parse: Mossaic.Generated.ShardDO.alarm_safe → file=lean/Mossaic/Generated/ShardDO.lean, name=alarm_safe
  if [[ ! "$theorem_path" =~ ^([A-Za-z0-9._]+)\.([A-Za-z0-9_]+)$ ]]; then
    echo "ERROR: malformed xref: $theorem_path"
    failed=1
    continue
  fi
  module_path="${BASH_REMATCH[1]}"
  theorem_name="${BASH_REMATCH[2]}"
  # Module path → file path: Mossaic.Generated.ShardDO → lean/Mossaic/Generated/ShardDO.lean
  file_path="lean/$(echo "$module_path" | tr '.' '/').lean"

  if [[ ! -f "$file_path" ]]; then
    echo "ERROR: xref '$theorem_path' references missing file: $file_path"
    failed=1
    continue
  fi

  # Check the theorem exists in the file. We grep for `theorem <name>` or `theorem <name> :`.
  if ! grep -qE "^theorem[[:space:]]+${theorem_name}\b" "$file_path"; then
    echo "ERROR: xref '$theorem_path' references missing theorem: ${theorem_name} in ${file_path}"
    failed=1
    continue
  fi
done <<< "$xrefs"

if [[ $failed -ne 0 ]]; then
  echo
  echo "FAIL: TS↔Lean cross-references are stale. Either fix the TS comment"
  echo "      or restore the Lean theorem to its expected name."
  exit 1
fi

echo "OK: all @lean-invariant xrefs resolve."
