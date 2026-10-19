#!/usr/bin/env bash
# Wait on the laptop until Slurm jobs, or a chain, on a login node have finished.
#
# Usage: scripts/wait-jobs.sh [--pull] <host> [jobid ...]
#        scripts/wait-jobs.sh [--pull] <host> --run <rundir>
#
# Polls <host> over ssh every $INTERVAL s (default 30), printing what it sees
# whenever that changes:
#
# - With job ids, or none: `squeue`, for those jobs or all of yours there.
#   When none is left it prints the jobs' final state (sacct, if the site
#   keeps accounting).
# - With --run: `gutsrun-infretis status <rundir>` for the chain in <rundir>
#   (relative to $REMOTE_DIR, default infretis_hyperqueue, or absolute),
#   until none of the chain's jobs is queued or running. A chain submits new
#   jobs as it goes, so its job ids are not known in advance.
#
# With --pull it then runs `scripts/sync.sh <host> pull`. Gives up after
# $MAX_WAIT s (default 1800; 43200 with --run) or 5 failed ssh calls in a
# row. Read-only on the remote side.
set -euo pipefail

usage="usage: scripts/wait-jobs.sh [--pull] <host> [jobid ... | --run <rundir>]"
pull=0
if [ "${1:-}" = --pull ]; then pull=1; shift; fi
host="${1:?$usage}"
shift
rundir=""
if [ "${1:-}" = --run ]; then
    rundir="${2:?$usage}"
    shift 2
fi
jobs="$(IFS=,; echo "$*")"
interval="${INTERVAL:-30}"
remote_dir="${REMOTE_DIR:-infretis_hyperqueue}"
root="$(cd "$(dirname "$0")/.." && pwd)"

# -q hides ssh's own warnings; the remote $USER is expanded on the remote side.
if [ -n "$rundir" ]; then
    max_wait="${MAX_WAIT:-43200}"
    # gutsrun-infretis status exits 0 once no job of the chain is left, 10 while
    # some are (or squeue does not answer).
    query="cd $(printf %q "$remote_dir") && .venv/bin/gutsrun-infretis status $(printf %q "$rundir")"
    what="chain in $rundir"
elif [ -n "$jobs" ]; then
    max_wait="${MAX_WAIT:-1800}"
    query="squeue -h -j $jobs -o '%i %j %T %M %R' 2>/dev/null || true"
    what="jobs"
else
    max_wait="${MAX_WAIT:-1800}"
    query="squeue -h -u \$USER -o '%i %j %T %M %R'"
    what="jobs"
fi

start=$SECONDS
last="" failures=0
while :; do
    rc=0
    out="$(ssh -q -o BatchMode=yes -o ConnectTimeout=15 "$host" "$query")" || rc=$?
    if [ "$rc" = 255 ]; then
        failures=$((failures + 1))
        echo "$(date +%H:%M:%S)  ssh to $host failed ($failures/5)" >&2
        [ "$failures" -ge 5 ] && exit 1
    else
        failures=0
        if [ -n "$rundir" ] && [ "$rc" != 0 ] && [ "$rc" != 10 ]; then
            [ -n "$out" ] && echo "$out" >&2
            echo "gutsrun-infretis status failed (exit $rc)" >&2
            exit 1
        fi
        if [ "$out" != "$last" ]; then
            if [ -n "$rundir" ]; then
                echo "$(date +%H:%M:%S)"
                sed 's/^/    /' <<<"$out"
            elif [ -n "$out" ]; then
                echo "$(date +%H:%M:%S)  jobs left:"
                sed 's/^/    /' <<<"$out"
            fi
            last="$out"
        fi
        if [ -n "$rundir" ]; then
            [ "$rc" = 0 ] && break
        else
            [ -z "$out" ] && break
        fi
    fi
    if [ $((SECONDS - start)) -ge "$max_wait" ]; then
        echo "still running after ${max_wait}s; giving up (jobs keep running)" >&2
        exit 1
    fi
    sleep "$interval"
done
echo "$(date +%H:%M:%S)  $what: nothing left in the queue after $((SECONDS - start))s"

if [ -n "$jobs" ]; then
    ssh -q -o BatchMode=yes "$host" \
        "sacct -X -n -j $jobs -o JobID,JobName%24,State,ExitCode,Elapsed 2>/dev/null" || true
fi
if [ "$pull" = 1 ]; then
    "$root/scripts/sync.sh" "$host" pull
fi
command -v notify-send >/dev/null && notify-send "Slurm $what on $host finished" || true
