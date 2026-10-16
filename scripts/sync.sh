#!/usr/bin/env bash
# Sync this repo with a cluster login node (plan decision 12: rsync, no remote).
#
# Usage: scripts/sync.sh <host> [push|pull|both]    (default: both)
#
#   push  code -> <host>:$REMOTE_DIR. Deletes remote files removed here,
#         except the excluded paths, which are never touched: the remote's
#         own .venv/, bin/ and runs/ stay as they are.
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
        "$root/" "$host:$remote_dir/"
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
