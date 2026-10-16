#!/usr/bin/env bash
# Two-node /home file-visibility probe (Fast Track step 6). Submit from the
# repo root on fs0, after the venv exists:
#
#   sbatch scripts/das6/probe-2nodes.sbatch
#   MODES=replace sbatch scripts/das6/probe-2nodes.sbatch    # one mode only
#
# bench/fs_latency.py plays ping-pong through a fresh directory on /home, one
# rank per node: once polling with os.listdir (as the executor's watcher does),
# once retrying open() on the known name, once rereading one file that is
# replaced in place (like spool/CURRENT). Each mode is capped at 4 min so that
# a slow NFS cache cannot run the job past its limit.
# Results: runs/das6/probe-2nodes-<jobid>/ and the .out file next to it.
#SBATCH --job-name=gutsrun-probe-2nodes
#SBATCH --partition=defq
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --exclusive
#SBATCH --time=00:10:00
#SBATCH --output=runs/das6/probe-2nodes-%j.out
set -u
repo="$SLURM_SUBMIT_DIR"
out="$repo/runs/das6/probe-2nodes-$SLURM_JOB_ID"
mkdir -p "$out"
echo "nodes: $SLURM_JOB_NODELIST"; date -Is
grep -E ' /home ' /proc/mounts

for mode in ${MODES:-listdir open replace}; do
    echo "== $mode"
    timeout 240 srun --nodes=2 --ntasks=2 --ntasks-per-node=1 \
        "$repo/.venv/bin/python" "$repo/bench/fs_latency.py" "$out/$mode" \
        --mode "$mode" --rounds 20 --timeout 20 \
        | tee "$out/$mode.json"
    echo "exit ${PIPESTATUS[0]}"
done
date -Is
