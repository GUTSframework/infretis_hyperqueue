#!/usr/bin/env bash
# Sync this repo with a git clone of it on a cluster login node.
#
# Usage: scripts/sync.sh <host> [push|pull|both]    (default: both)
#
#   push  this repo's HEAD -> the branch checked out in <host>:$REMOTE_DIR,
#         with git (forced, so rewritten commits go too). The clone's files
#         follow, and runs there record the commit. Only commits travel:
#         uncommitted changes stay here (push says so). Refused if the clone
#         has commits made there, which the forced push would drop.
#   pull  <host>:$REMOTE_DIR/runs/$SITE/ -> runs/$SITE/ with rsync (adds and
#         updates, never deletes). runs/ is not tracked by git.
#
# REMOTE_DIR defaults to infretis_hyperqueue (relative to the remote home),
# SITE to das6.
#
# Once, on the login node, in the clone (git 2.4 or later):
#
#   git config receive.denyCurrentBranch updateInstead
#
# It lets a push update the checked-out branch and its files, and refuses
# the push if files there were changed, instead of overwriting them.
set -euo pipefail

host="${1:?usage: scripts/sync.sh <host> [push|pull|both]}"
mode="${2:-both}"
remote_dir="${REMOTE_DIR:-infretis_hyperqueue}"
site="${SITE:-das6}"
root="$(cd "$(dirname "$0")/.." && pwd)"

push() {
    local branch head
    { read -r branch; read -r head; } < <(ssh -q "$host" \
        "git -C '$remote_dir' symbolic-ref --short HEAD && git -C '$remote_dir' rev-parse HEAD")
    # The push is forced, so it would drop commits made in the clone; refuse
    # unless the clone's head is a commit this repo has (pushed from here).
    if ! git -C "$root" cat-file -e "$head^{commit}" 2>/dev/null; then
        echo "$host:$remote_dir has commits made there (its $branch is at ${head:0:7})." >&2
        echo "Fetch them first: git fetch $host:$remote_dir $branch" >&2
        exit 1
    fi
    if [ -n "$(git -C "$root" status --porcelain --untracked-files=no)" ]; then
        echo "note: uncommitted changes stay here; only $(git -C "$root" rev-parse --short HEAD) is pushed" >&2
    fi
    git -C "$root" push --force "$host:$remote_dir" "HEAD:refs/heads/$branch"
}

pull() {
    mkdir -p "$root/runs/$site"
    rsync -av "$host:$remote_dir/runs/$site/" "$root/runs/$site/"
}

case "$mode" in
    push) push ;;
    pull) pull ;;
    both) push; pull ;;
    *) echo "unknown mode: $mode" >&2; exit 2 ;;
esac
