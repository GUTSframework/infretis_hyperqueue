#!/usr/bin/env bash
# Time what an infretis LAMMPS move costs on a cluster's compute node, in the
# environment a site's `[environment] setup` leaves: Python imports from the
# venv, `lmp` start-up, and a fixed-length MD run of the water_dissociation
# example. Moves end where their path crosses an interface, so their own
# lengths cannot be compared between clusters; a fixed nsteps can.
#
# Submit from the repo root, with the site's partition and a whole node, as
# a link would run:
#
#   sbatch -p rome -N 1 --exclusive -t 00:05:00 -o runs/snellius-lammps/%j-time-lammps-move.out \
#       bench/time-lammps-move.sh snellius-lammps
#   sbatch -t 00:05:00 -o runs/das6/%j-time-lammps-move.out bench/time-lammps-move.sh das6
#
# NSTEPS (default 1000) sets the MD run's length; REPEAT (default 3) how often
# each step runs. The LAMMPS logs stay in runs/<site>/<job>-time-lammps-move/.
set -euo pipefail

site="${1:?usage: bench/time-lammps-move.sh <site>}"
nsteps="${NSTEPS:-1000}"
repeat="${REPEAT:-3}"
# sbatch runs a copy of this script, so the repo is where it was submitted.
root="${SLURM_SUBMIT_DIR:-$PWD}"
cd "$root"
[ -f "sites/$site.toml" ] || { echo "no sites/$site.toml in $root (submit from the repo root)" >&2; exit 2; }
python="$root/.venv/bin/python"
example="$root/examples/water_dissociation_lammps"

setup="$("$python" -c 'import sys, tomllib
print(tomllib.load(open(sys.argv[1], "rb")).get("environment", {}).get("setup", ""))' "sites/$site.toml")"
eval "$setup"
# As in an HQ task with one core (see examples/README.md).
export OMP_NUM_THREADS=1

work="$root/runs/$site/${SLURM_JOB_ID:-local}-time-lammps-move"
mkdir -p "$work/run"
cp -r "$example/lammps_input" "$work/"
cd "$work/run"   # lammps.input reads ../lammps_input/qeq_ff.water

echo "== $(hostname) job ${SLURM_JOB_ID:-none} site $site code $(git -C "$root" rev-parse --short HEAD) $(date -Is)"
echo "lmp: $(command -v lmp)"
TIMEFORMAT='%R s'

echo "== python -c 'import infretis.scheduler' (venv, what the launcher imports)"
for i in $(seq "$repeat"); do time "$python" -c 'import infretis.scheduler'; done

lmp_run() {   # <steps> <label>
    lmp -i ../lammps_input/lammps.input -log "log.$2.lammps" -screen none \
        -var subcycles 1 -var timestep 0.5 -var nsteps "$1" \
        -var initconf "$work/lammps_input/initial.lammpstrj" -var name "traj.$2" \
        -var lammpsdata "$work/lammps_input/lammps.data" \
        -var temperature 300 -var seed 1
}

echo "== lmp, 0 steps (start-up: MPI, reading the inputs, QEq set-up)"
for i in $(seq "$repeat"); do time lmp_run 0 "zero$i"; done

echo "== lmp, $nsteps steps (wall time, then LAMMPS's own loop time)"
for i in $(seq "$repeat"); do
    time lmp_run "$nsteps" "run$i"
    grep 'Loop time' "log.run$i.lammps"
done
