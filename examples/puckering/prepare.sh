#!/usr/bin/env bash
# Generate initial paths for infentory `puckering`, following its README:
# solvate oxane, EM / NVT / NPT equilibration, 1 ns MD, then `inft infinit`
# with state A at lambda = 10 and state B at lambda = 90.
#
# Run inside `nix develop` with .venv set up:
#     examples/puckering/prepare.sh [OUTDIR]     (default: runs/prepared/puckering)
#
# OUTDIR ends up with gromacs_input/ (solvated), eq/ (equilibration + MD),
# and infinit/ whose load/ and final .toml are the initial paths for
# bench/bare_run.py (see examples/README.md). Generated data is not tracked.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
out="${1:-$root/runs/prepared/puckering}"
threads="${THREADS:-4}"
# gmx mdrun aborts if OMP_NUM_THREADS differs from -ntomp.
unset OMP_NUM_THREADS
gmx="gmx -quiet"
py="$root/.venv/bin/python"
inft="$root/.venv/bin/inft"

if [ -e "$out" ]; then
    echo "$out exists; remove it or pick another directory" >&2
    exit 1
fi
mkdir -p "$out"
out="$(cd "$out" && pwd)"
cp -r "$here/gromacs_input" "$out/"

echo "== solvate"
cd "$out/gromacs_input"
$gmx solvate -cs spc216.gro -cp mol.gro -p topol.top -o conf.g96

echo "== equilibrate + MD"
mkdir -p "$out/eq"
cd "$out/eq"
top="../gromacs_input/topol.top"
$gmx grompp -f "$here/prep/em.mdp" -p "$top" -c ../gromacs_input/conf.g96 -o em.tpr
$gmx mdrun -deffnm em -ntomp "$threads" -ntmpi 1
$gmx grompp -f "$here/prep/nvt.mdp" -p "$top" -c em.gro -o nvt.tpr
$gmx mdrun -deffnm nvt -ntomp "$threads" -ntmpi 1
$gmx grompp -f "$here/prep/npt.mdp" -p "$top" -c nvt.gro -t nvt.cpt -o npt.tpr
$gmx mdrun -deffnm npt -ntomp "$threads" -ntmpi 1
$gmx grompp -f "$here/prep/md.mdp" -p "$top" -c npt.gro -t npt.cpt -o md_run.tpr
$gmx mdrun -deffnm md_run -ntomp "$threads" -ntmpi 1 -c confout.g96

echo "== infinit"
mkdir -p "$out/infinit"
cd "$out/infinit"
"$py" - "$here/infretis0.toml" "$threads" <<'EOF'
import sys
import tomli, tomli_w
cfg = tomli.loads(open(sys.argv[1]).read())
workers = int(sys.argv[2])
cfg["simulation"]["interfaces"] = [10.0, 90.0]
cfg["infinit"]["initial_conf"] = "../eq/confout.g96"
cfg["engine"]["input_path"] = "../gromacs_input"
cfg["runner"]["workers"] = workers
cfg["runner"]["wmdrun"] = ["gmx mdrun -ntomp 1 -ntmpi 1"] * workers
open("infretis0.toml", "w").write(tomli_w.dumps(cfg))
EOF
"$inft" infinit -toml infretis0.toml
echo "== done: $out/infinit"
