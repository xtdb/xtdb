#!/usr/bin/env bash
# check-actions-consistency.sh
# Scans .github/workflows for `uses:` references and verifies that each action
# (grouped by owner/repo, ignoring any subpath) is pinned to a single, consistent
# version/ref across all workflows. Reports any mismatches.
#
# Usage:
#   ./scripts/check-actions-consistency.sh [--strict]
#
# Options:
#   --strict   Exit with non-zero status if inconsistencies are found.
#
# Notes:
# - Local actions (paths like ./.github/actions/...) and reusable workflows (./.github/workflows/*.yml)
#   are ignored.
# - For actions with subpaths (e.g., gradle/actions/setup-gradle@v4), we normalize the key to
#   the base repository (gradle/actions) for consistency.
# - This script only compares the literal ref (e.g., v4 or a SHA). It does not resolve tags.

set -euo pipefail

STRICT=0
if [[ ${1-} == "--strict" ]]; then
  STRICT=1
fi

export LC_ALL=C

# Collect all action usages into a temporary file: "repo version file"
TMP_FILE=$(mktemp)
trap 'rm -f "$TMP_FILE"' EXIT

find .github/workflows -type f \( -name '*.yml' -o -name '*.yaml' \) | while IFS= read -r file; do
  # Read only lines with `uses:` and extract the reference portion
  (grep -E "^[[:space:]]*uses:[[:space:]]*" "$file" || true) | while IFS= read -r line; do
    # Extract the reference (owner/repo[/path]@ref OR local path OR reusable workflow)
    ref=$(sed -E "s/^[[:space:]]*uses:[[:space:]]*['\"]?([^'\"[:space:]]+)['\"]?.*$/\1/" <<<"$line")

    # Skip local/composite and reusable workflow references
    case "$ref" in
      ./*|../*|/*|:*) continue;;
    esac

    # Must contain an @ to split action and version
    [[ "$ref" != *@* ]] && continue

    action_full="${ref%@*}"      # may be owner/repo or owner/repo/path
    version_ref="${ref#*@}"

    # Normalize to base repo (owner/repo)
    repo=$(echo "$action_full" | awk -F'/' '{print $1"/"$2}')
    [[ -z "$repo" ]] && continue

    printf '%s %s %s\n' "$repo" "$version_ref" "$file" >> "$TMP_FILE"
  done
done

# Now aggregate by repo and detect mismatches
# We use awk (supports associative arrays) to gather versions per repo and track files.
awk -v strict=$STRICT '
  {
    repo=$1; ver=$2; file=$3;
    key=repo "\t" ver
    versions[repo][ver] = 1
    files[key] = (files[key] ? files[key] "," file : file)
  }
  END {
    inconsistencies=0
    for (repo in versions) {
      # Count distinct versions for this repo
      count=0
      vers_list=""
      for (v in versions[repo]) {
        count++
        vers_list = (vers_list ? vers_list "," v : v)
      }
      if (count > 1) {
        inconsistencies++
        printf("Repo: %s\n", repo)
        split(vers_list, arr, ",")
        for (i in arr) {
          ver=arr[i]
          key=repo "\t" ver
          printf("  - %s\n    files: %s\n", ver, files[key])
        }
        printf("\n")
      }
    }
    if (inconsistencies==0) {
      printf("All action versions are consistent across workflows.\n")
    } else if (strict==1) {
      exit 2
    }
  }
' "$TMP_FILE"
