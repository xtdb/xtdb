#!/usr/bin/env bash

set -euo pipefail

usage() {
  echo "Usage: $0 <version> <release-notes-file> [<ref>]" >&2
  echo "  e.g. $0 2.2.0 ~/notes/v2.2.0.md" >&2
  echo "       $0 2.1.1 ~/notes/v2.1.1.md upstream/2.1.x" >&2
  echo "  <ref> defaults to the xtdb/xtdb remote's main." >&2
  exit 1
}

step() {
  echo
  echo "==> $*"
}

await_enter() {
  read -r -p "$* - press Enter to continue (Ctrl-C to stop) " _
}

[[ $# -ge 2 && $# -le 3 ]] || usage

version=${1#v}
tag="v$version"
notes=$(realpath -m "$2")
[[ -f $notes ]] || { echo "No release notes at $notes" >&2; exit 1; }

prerelease=
[[ $version == *-* ]] && prerelease=1

remote=$(git remote -v | awk '$2 ~ /[:\/]xtdb\/xtdb(\.git)?$/ && $3 == "(push)" { print $1; exit }')
[[ -n $remote ]] || { echo "No remote pointing at xtdb/xtdb" >&2; exit 1; }

git fetch --quiet --tags "$remote"
ref=${3:-$remote/main}

if git rev-parse --quiet --verify "refs/tags/$tag" >/dev/null; then
  echo "Tag $tag already exists" >&2
  exit 1
fi

step "Creating a worktree for $tag at $ref"
worktree=$(mktemp -d -t "xtdb-release-$tag.XXXXXX")
git worktree add --quiet --detach "$worktree" "$ref"
stopped() {
  echo
  echo "Stopped - the worktree is still at $worktree (git worktree remove --force $worktree)" >&2
}
trap stopped ERR
trap 'stopped; exit 130' INT
cd "$worktree"
git log -1 --format='    %h %s'

step "Tagging $tag"
git tag --annotate --sign --cleanup=verbatim -F "$notes" "$tag"

unsynced_docs=$(git log --oneline "$remote/docs-live..$tag" -- docs/)
if [[ -n $unsynced_docs ]]; then
  echo
  echo "Heads up - these docs changes aren't on $remote/docs-live yet:"
  echo "$unsynced_docs"
fi

step "Pushing $tag to $remote - this starts the Docker image builds"
await_enter "Ready to push $tag"
git push "$remote" "$tag"
echo "    https://github.com/xtdb/xtdb/actions/workflows/tag.yml"

step "Deploying to Maven..."
sleep 2
XTDB_VERSION=$version ./gradlew clean jreleaserDeploy

step "Publish the deployment on Sonatype"
echo "    https://central.sonatype.com/publishing"
echo "    Check api, core, main, aws, azure, google-cloud, kafka, postgres-source and datasets are all there at $version."
await_enter "Published on Sonatype"

step "Drafting the GitHub release"
gh release create "$tag" --repo xtdb/xtdb --draft --verify-tag --title "$tag" --notes-file "$notes" ${prerelease:+--prerelease}
echo "    $(gh release view "$tag" --repo xtdb/xtdb --json url --jq .url)"
await_enter "Published on GitHub"

trap - ERR INT
cd - >/dev/null
git worktree remove "$worktree"

echo
if [[ -z $prerelease ]]; then
  echo "Close the release's milestone, and create the next one."
  echo
fi
echo "Now go spread the word!"
echo " - Slack"
echo " - LinkedIn"
echo " - Discord"
echo
echo "🚀"
