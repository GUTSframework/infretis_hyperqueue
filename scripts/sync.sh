#!/usr/bin/env bash
# Sync this repo with a cluster login node: rsync, so the cluster needs no
# git remote and the laptop's uncommitted changes can be tried.
#
# Usage: scripts/sync.sh <host> [push|pull|both]    (default: both)
#
#   push  code -> <host>:$REMOTE_DIR. Deletes remote files removed here,
#         except the excluded paths, which are never touched: the remote's
#         own .venv/, bin/ and runs/ stay as they are. Writes REVISION there
#         (`git describe --always --dirty`), since .git is not copied;
#         bench/bare_run.py records it as the run's commit.
#   pull  <host>:$REMOTE_DIR/runs/$SITE/ -> runs/$SITE/ (adds and updates,
#         never deletes).
#
# REMOTE_DIR defaults to infretis_hyperqueue (relative to the remote home),
# SITE to das6.
set -euo pipefail

host="${1:?usage: scripts/sync.sh <host> [push|pull|both]}"
mode="${2:-both}"
remote_dir="${REMOTE_DIR:-infretis_hyperqueue}"
site="${SITE:-das6}"
root="$(cd "$(dirname "$0")/.." && pwd)"

push() {
    rsync -av --delete \
        --exclude .git/ --exclude .venv/ --exclude bin/ --exclude runs/ \
        --exclude work/ --exclude result --exclude __pycache__/ \
        --exclude .pytest_cache/ --exclude .claude/ --exclude .idea/ --exclude .vscode/ \
        --exclude /REVISION \
        "$root/" "$host:$remote_dir/"
    git -C "$root" describe --always --dirty --abbrev=7 \
        | ssh -q "$host" "cat > '$remote_dir/REVISION'"
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
