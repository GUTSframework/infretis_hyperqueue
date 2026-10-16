#!/usr/bin/env bash
# One-node compute probe (Fast Track step 6). Submit from the repo root on fs0,
# after the venv exists (scripts/setup-venv.sh):
#
#   sbatch scripts/das6/probe-node.sbatch
#
# Records what a compute node has (CPUs, Python, modules, Apptainer, local
# disk, /home mount options), then times the infretis cold start
# (bench/cold_start.py on 2d_potential) with 1, 24 and 48 children at once:
#   home   the venv on /home (NFS), as built;
#   local  the venv unpacked from bin/venv.tar to node-local disk.
# Results: runs/das6/probe-node-<jobid>/ and the .out file next to it.
#SBATCH --job-name=gutsrun-probe-node
#SBATCH --partition=defq
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --time=00:10:00
#SBATCH --output=runs/das6/probe-node-%j.out
set -u
section() { printf '\n== %s\n' "$1"; }
elapsed() { awk -v a="$1" -v b="$(date +%s.%N)" 'BEGIN { printf "%.3f\n", b - a }'; }
repo="$SLURM_SUBMIT_DIR"
out="$repo/runs/das6/probe-node-$SLURM_JOB_ID"
mkdir -p "$out"

section host;    hostname; date -Is; nproc
section cpu;     lscpu | grep -E '^(Model name|Socket|Core|Thread|CPU\(s\)|NUMA node\(s\)|Flags)' | cut -c1-200
section memory;  free -g
section python;  for p in python3 python3.12; do printf '%s: ' "$p"; command -v "$p" >/dev/null && { command -v "$p"; "$p" -V; } || echo missing; done
section modules
type module >/dev/null 2>&1 || { [ -f /etc/profile.d/lmod.sh ] && . /etc/profile.d/lmod.sh; }
if type module >/dev/null 2>&1; then
    module avail 2>&1 | grep -i -E 'gromacs|gmx|lammps|python|apptainer|singularity' | head -30 || echo "no matching modules"
else
    echo "no module command"
fi
section engines; for c in gmx gmx_mpi lmp lmp_serial apptainer; do printf '%s: ' "$c"; command -v "$c" || echo missing; done; apptainer --version 2>&1
section gpus;    nvidia-smi -L 2>&1 | head -8
section limits;  ulimit -a | grep -E 'processes|open files|cpu time|virtual'
section home-mount; grep -E ' /home ' /proc/mounts
section local-disk
echo "TMPDIR=${TMPDIR:-unset}"
for d in /local-ssd /local /tmp /var/tmp "${TMPDIR:-}"; do
    [ -n "$d" ] || continue
    printf '%s: ' "$d"
    if [ -d "$d" ]; then
        printf '%s ' "$( [ -w "$d" ] && echo writable || echo read-only)"; df -h "$d" | tail -1
    else
        echo missing
    fi
done

# Node-local disk for the venv copy: first writable candidate.
local_base=""
for d in /local-ssd /local "${TMPDIR:-}" /tmp; do
    [ -n "$d" ] && [ -d "$d" ] && [ -w "$d" ] && { local_base="$d"; break; }
done
[ -n "$local_base" ] || { echo "no writable local disk"; exit 1; }
local="$local_base/$USER-gutsrun-$SLURM_JOB_ID"
trap 'rm -rf "$local"' EXIT
mkdir -p "$local"
echo "using $local"

[ -x "$repo/.venv/bin/python" ] || { echo "no venv at $repo/.venv; run scripts/setup-venv.sh first"; exit 1; }

cold_start() {  # where python
    for n in 1 24 48; do
        repeat=5; [ "$n" -gt 1 ] && repeat=3
        echo "-- $1 concurrent=$n repeat=$repeat"
        "$2" "$repo/bench/cold_start.py" "$rundir" --repeat "$repeat" --concurrent "$n" \
            > "$out/cold-$1-$n.json" 2> "$out/cold-$1-$n.err" \
            && grep -E '"(concurrent|max_total_s|median_total_s|median_imports_s|median_init_s)"' "$out/cold-$1-$n.json" \
            || { echo "failed:"; tail -5 "$out/cold-$1-$n.err"; }
    done
}
rundir="$out/2d"
cp -r "$repo/examples/2d_potential" "$rundir"

# /home first: packing reads the whole venv and would warm this node's cache.
section cold-start-home
cold_start home "$repo/.venv/bin/python"

section venv-pack
# Packing is a one-off on /home; unpacking happens at every job start.
mkdir -p "$repo/bin"
if [ ! -f "$repo/bin/venv.tar" ] || [ "$repo/.venv/pyvenv.cfg" -nt "$repo/bin/venv.tar" ] \
        || [ "$repo/requirements-lock.txt" -nt "$repo/bin/venv.tar" ]; then
    t0=$(date +%s.%N)
    tar -C "$repo" -cf "$repo/bin/venv.tar" .venv
    echo "pack_s $(elapsed "$t0")"
fi
ls -l "$repo/bin/venv.tar"
t0=$(date +%s.%N)
tar -C "$local" -xf "$repo/bin/venv.tar"
echo "unpack_s $(elapsed "$t0")"
"$local/.venv/bin/python" -c 'import sys, infretis; print(sys.prefix, infretis.__file__)'


section cold-start-local
cold_start local "$local/.venv/bin/python"
section done; date -Is
