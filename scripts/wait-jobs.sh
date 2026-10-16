#!/usr/bin/env bash
# Wait on the laptop until Slurm jobs on a login node have finished.
#
# Usage: scripts/wait-jobs.sh [--pull] <host> [jobid ...]
#
# Polls `squeue` on <host> over ssh every $INTERVAL s (default 30), printing
# the queue whenever it changes. With job ids it waits for those, otherwise
# for all of your jobs there. When none is left it prints the jobs' final
# state (sacct, if the site keeps accounting), and with --pull runs
# `scripts/sync.sh <host> pull`. Gives up after $MAX_WAIT s (default 1800) or
# 5 failed ssh calls in a row. Read-only on the remote side.
set -euo pipefail

pull=0
if [ "${1:-}" = --pull ]; then pull=1; shift; fi
host="${1:?usage: scripts/wait-jobs.sh [--pull] <host> [jobid ...]}"
shift
jobs="$(IFS=,; echo "$*")"
interval="${INTERVAL:-30}"
max_wait="${MAX_WAIT:-1800}"
root="$(cd "$(dirname "$0")/.." && pwd)"

# -q hides ssh's own warnings; the remote $USER is expanded on the remote side.
if [ -n "$jobs" ]; then
    query="squeue -h -j $jobs -o '%i %j %T %M %R' 2>/dev/null || true"
else
    query="squeue -h -u \$USER -o '%i %j %T %M %R'"
fi

start=$SECONDS
last="" failures=0
while :; do
    if out="$(ssh -q -o BatchMode=yes -o ConnectTimeout=15 "$host" "$query")"; then
        failures=0
        if [ "$out" != "$last" ]; then
            if [ -n "$out" ]; then
                echo "$(date +%H:%M:%S)  jobs left:"
                sed 's/^/    /' <<<"$out"
            fi
            last="$out"
        fi
        [ -z "$out" ] && break
    else
        failures=$((failures + 1))
        echo "$(date +%H:%M:%S)  ssh to $host failed ($failures/5)" >&2
        [ "$failures" -ge 5 ] && exit 1
    fi
    if [ $((SECONDS - start)) -ge "$max_wait" ]; then
        echo "still running after ${max_wait}s; giving up (jobs keep running)" >&2
        exit 1
    fi
    sleep "$interval"
done
echo "$(date +%H:%M:%S)  no jobs left after $((SECONDS - start))s"

if [ -n "$jobs" ]; then
    ssh -q -o BatchMode=yes "$host" \
        "sacct -X -n -j $jobs -o JobID,JobName%24,State,ExitCode,Elapsed 2>/dev/null" || true
fi
if [ "$pull" = 1 ]; then
    "$root/scripts/sync.sh" "$host" pull
fi
command -v notify-send >/dev/null && notify-send "Slurm jobs on $host finished" || true
