#!/usr/bin/env bash
# When does Slurm's warning signal arrive, and when is a job killed?
#
# Submits <count> small jobs that each record their planned start and end,
# every TERM they receive, and a heartbeat every half second; the last
# heartbeat is when the job was killed. The jobs only sleep. Run from the
# repo root on a login node:
#
#   bash scripts/probe-signal-timing.sh <outdir> <count> <time> [sbatch options...]
#
# e.g. on DAS-6:
#
#   bash scripts/probe-signal-timing.sh runs/das6/probe-signal 6 00:04:00 -p defq
#   bash scripts/probe-signal-timing.sh runs/das6/probe-signal 2 00:15:00 -p defq
#
# Every job asks for --signal=B:TERM@90, as a link does with the default
# margin. Then, once all have ended (the second TERM is the walltime's; the
# KILL follows KillWait later):
#
#   python3 scripts/probe-signal-timing-report.py <outdir>
set -euo pipefail

if [ "${1:-}" = job ]; then
    # Inside the job: the batch shell, which `B:` sends the signal to.
    out="$2/$SLURM_JOB_ID"
    now() { date +%s.%N; }
    echo "start $(now)" >> "$out.log"
    scontrol show job -o "$SLURM_JOB_ID" | tr ' ' '\n' \
        | grep -E '^(StartTime|EndTime|TimeLimit|NodeList)=' >> "$out.log"
    trap 'echo "TERM $(now)" >> "$out.log"' TERM
    while true; do
        now > "$out.hb"
        # In the background and waited for, so a TERM is handled at once.
        sleep 0.5 & wait $! || true
    done
fi

outdir="${1:?usage: probe-signal-timing.sh <outdir> <count> <time> [sbatch options...]}"
count="${2:?count}"
time="${3:?time, e.g. 00:04:00}"
shift 3
mkdir -p "$outdir"
outdir="$(cd "$outdir" && pwd)"
script="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
for _ in $(seq "$count"); do
    sbatch --parsable --job-name=probe-signal --time="$time" --signal=B:TERM@90 \
        --nodes=1 --ntasks=1 --cpus-per-task=1 --output="$outdir/%j.out" "$@" \
        --wrap="bash '$script' job '$outdir'"
done
