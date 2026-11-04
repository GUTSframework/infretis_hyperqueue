#!/usr/bin/env bash
# Install conda-forge's CUDA build of GROMACS on DAS-6, next to the CPU
# engines of install-engines.sh (run that first: it installs miniforge), and
# check that it starts on a GPU node. Run on fs0 from the repo root:
#
#   bash scripts/das6/install-gromacs-gpu.sh 2>&1 | tee runs/das6/install-gromacs-gpu.txt
#
# Lays out, under $ROOT (default /var/scratch/$USER/miniforge3):
#   envs/md-gpu/   GROMACS only, CUDA, without MPI
# A run uses it by naming $ROOT/envs/md-gpu/bin/gmx in its infretis.toml
# (`gmx` and `wmdrun`); the site's PATH keeps the CPU build.
set -euo pipefail

ROOT="${ROOT:-/var/scratch/$USER/miniforge3}"
# The CPU build's version (install-engines.sh). The build needs a driver for
# CUDA 12.9 or newer and declares glibc 2.17; DAS-6 has 2.28.
GROMACS_SPEC="gromacs=2026.3=nompi_cuda_h39c90b0_0"
# fs0 has no GPU, so conda sees no CUDA driver; the node check below tests
# the real one.
export CONDA_OVERRIDE_CUDA=12.9
export CONDA_PKGS_DIRS="$ROOT/pkgs"
section() { printf '\n== %s\n' "$1"; }
conda() { "$ROOT/bin/python" -m conda "$@"; }
[ -x "$ROOT/bin/python" ] || { echo "no miniforge in $ROOT: run install-engines.sh" >&2; exit 1; }

section host; hostname; date -Is
if ! conda list -n md-gpu --explicit 2>/dev/null | grep -q "/gromacs-2026.3-nompi_cuda_h39c90b0_0"; then
    section "env md-gpu"
    if [ -d "$ROOT/envs/md-gpu" ]; then conda remove --yes --quiet -n md-gpu --all; fi
    conda create --yes --quiet -n md-gpu -c conda-forge --override-channels "$GROMACS_SPEC"
    conda clean --all --yes
fi
gmx="$ROOT/envs/md-gpu/bin/gmx"
conda list -n md-gpu --explicit | grep -E '/(gromacs|cuda-version|cuda-cudart)-'
du -sh "$ROOT/envs/md-gpu"

section "gmx on fs0 (no GPU)"
"$gmx" --version 2>&1 | grep -E '^GROMACS version|^SIMD instructions|^GPU support'

section "gmx on a GPU node"
srun -p defq -N1 -t 2 --exclusive bash -c "
hostname
cat /proc/driver/nvidia/version
'$gmx' --version 2>&1 | grep -E '^GROMACS version|^GPU support|^CUDA (driver|runtime)'
"
