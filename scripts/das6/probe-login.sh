#!/usr/bin/env bash
# Read-only checks on the DAS-6 login node (Fast Track step 6). Changes
# nothing. Run from the repo root on fs0:
#
#   bash scripts/das6/probe-login.sh 2>&1 | tee runs/das6/probe-login.txt
set -u
section() { printf '\n== %s\n' "$1"; }

section host;    hostname; date -Is; uname -r; nproc
section python;  for p in python3 python3.12 python3.13; do printf '%s: ' "$p"; command -v "$p" >/dev/null && { command -v "$p"; "$p" -V; } || echo missing; done
section modules
if command -v module >/dev/null 2>&1 || type module >/dev/null 2>&1; then
    module avail 2>&1 | grep -i -E 'gromacs|lammps|python|cuda|apptainer|singularity|gcc|openmpi|prun|slurm' | head -60
else
    echo "no module command"
fi
section containers; for c in apptainer singularity; do printf '%s: ' "$c"; command -v "$c" || echo missing; done
section slurm;   sinfo -s 2>&1 | head -20; echo; sinfo -o '%P %a %l %D %c %m %G' 2>&1 | head -20
section my-jobs; squeue -u "$USER" 2>&1 | head -20
section storage
df -h "$HOME" 2>&1; df -i "$HOME" 2>&1
quota -s 2>&1 | head -10
for d in "/var/scratch/$USER" /var/scratch /local /scratch; do printf '%s: ' "$d"; [ -d "$d" ] && df -h "$d" | tail -1 || echo missing; done
section leftovers
ls -d ~/hq-pilot-work ~/hyperqueue_test ~/infretis_hyperqueue 2>&1
pgrep -u "$USER" -a hq 2>&1 || echo "no hq processes"
for d in ~/hq-pilot-work/*/; do [ -e "$d/hq-current" ] && echo "server dir: $d"; done 2>/dev/null
section internet
for u in https://pypi.org/simple/ https://github.com/; do printf '%s: ' "$u"; curl -s -o /dev/null -m 10 -w '%{http_code}\n' "$u" || echo unreachable; done
