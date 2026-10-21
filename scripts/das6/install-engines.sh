#!/usr/bin/env bash
# Install GROMACS and LAMMPS from conda-forge into /var/scratch on DAS-6,
# the way DAS-6 users get MD engines (the site provides none). Run on fs0
# from the repo root:
#
#   bash scripts/das6/install-engines.sh 2>&1 | tee runs/das6/install-engines.txt
#
# Lays out, under $ROOT (default /var/scratch/$USER/miniforge3):
#   envs/md/   one env: GROMACS and LAMMPS, CPU builds without MPI
#   md-bin/    symlinks to gmx and lmp only. sites/das6.toml puts this on
#              PATH, not envs/md/bin, so the env's own python3 never shadows
#              anything a task calls.
# Nothing outside $ROOT is written: no `conda init`, no ~/.condarc, the
# package cache stays in $ROOT/pkgs (on /home it would count against the
# 5 GB quota). Removing $ROOT undoes it all.
#
# Also records what the install costs in files and bytes, and the quotas
# (`quota`, `df -i`) before and after. Run again to repeat the checks, or
# after changing a pin: an env without the pinned builds is replaced.
set -euo pipefail

ROOT="${ROOT:-/var/scratch/$USER/miniforge3}"
MINIFORGE_VERSION=26.7.2-0
MINIFORGE_SHA256=281b0ac7d550802efc81af633225a5e6116d29ae72f3ab4eae7168c3931a4c05
# Build strings pick the CPU, non-MPI variants: infretis starts one engine
# process per move. GROMACS is the laptop's version (ASSUMPTIONS.md). LAMMPS
# is the stable 29Aug2024: conda-forge's 2025.07.22 builds need glibc 2.34
# or 2.38 although they declare 2.17, and DAS-6's glibc is older, so their
# lmp does not start there.
GROMACS_SPEC="gromacs=2026.3=nompi_*"
LAMMPS_SPEC="lammps=2024.08.29=cpu_py312_h5afe51e_nompi_9"

export CONDA_PKGS_DIRS="$ROOT/pkgs"
section() { printf '\n== %s\n' "$1"; }
# Inodes, which file quotas count: the env hard-links from the package
# cache, so counting names would count most files twice.
files() { find "$@" -xdev -printf '%i\n' | sort -u | wc -l; }
usage() {
    # DAS-6 sets file limits through `quota` (0 = none), if at all.
    quota -s 2>&1 | head -10 || true
    df -h "$(dirname "$ROOT")"; df -i "$(dirname "$ROOT")"
}

mkdir -p "$(dirname "$ROOT")"
section host; hostname; date -Is
section "quota before"; usage

if [ ! -x "$ROOT/bin/python" ]; then
    section "miniforge $MINIFORGE_VERSION"
    installer="$(mktemp -d)/Miniforge3.sh"
    curl -fsSL -o "$installer" \
        "https://github.com/conda-forge/miniforge/releases/download/$MINIFORGE_VERSION/Miniforge3-$MINIFORGE_VERSION-Linux-x86_64.sh"
    echo "$MINIFORGE_SHA256  $installer" | sha256sum -c -
    bash "$installer" -b -p "$ROOT"
    rm -r "$(dirname "$installer")"
    printf 'inodes in the base install: %s\n' "$(files "$ROOT")"
fi
# Not bin/conda: its #! line falls back to `/usr/bin/env python` when the
# prefix is too long for one.
conda() { "$ROOT/bin/python" -m conda "$@"; }
conda --version

# A spec as a pattern for its URL in `conda list --explicit`, e.g.
# gromacs=2026.3=nompi_* -> */gromacs-2026.3-nompi_*.conda*
url_pattern() {
    local name="${1%%=*}" rest="${1#*=}"
    echo "*/$name-${rest%%=*}-${rest#*=}.conda*"
}
has_pins() {
    local installed gromacs lammps
    installed="$(conda list -n md --explicit)" || return 1
    gromacs="$(url_pattern "$GROMACS_SPEC")" lammps="$(url_pattern "$LAMMPS_SPEC")"
    # shellcheck disable=SC2053  # the right-hand sides are patterns
    [[ "$installed" == $gromacs && "$installed" == $lammps ]]
}
if [ ! -d "$ROOT/envs/md" ] || ! has_pins; then
    section "env md"
    # Replaced, not updated: a pin can change the env's Python, which
    # `conda install` keeps.
    if [ -d "$ROOT/envs/md" ]; then conda remove --yes --quiet -n md --all; fi
    conda create --yes --quiet -n md -c conda-forge --override-channels \
        "$GROMACS_SPEC" "$LAMMPS_SPEC"
    section "files before clean"
    printf 'inodes: env %s, package cache %s, all of %s %s\n' \
        "$(files "$ROOT/envs/md")" "$(files "$ROOT/pkgs")" "$ROOT" "$(files "$ROOT")"
    conda clean --all --yes
fi
conda list -n md --explicit > "$ROOT/md-explicit.txt"
grep -E '/(gromacs|lammps)-' "$ROOT/md-explicit.txt"

mkdir -p "$ROOT/md-bin"
ln -sfn "$ROOT/envs/md/bin/gmx" "$ROOT/md-bin/gmx"
ln -sfn "$ROOT/envs/md/bin/lmp" "$ROOT/md-bin/lmp"

section "checks"
export PATH="$ROOT/md-bin:$PATH"
command -v gmx lmp
# conda-forge's gmx is a wrapper that picks SSE2, AVX_256 or AVX2_256 from
# /proc/cpuinfo; the line records which, so DAS-6 move times are not read
# as production speed.
gmx --version 2>&1 | grep -E '^GROMACS version|^SIMD instructions|^Precision|^MPI library'
# water_dissociation needs pair_style reaxff and fix qeq/reaxff (REAXFF).
lmp_help="$(lmp -h 2>&1)" || { echo "lmp does not run:" >&2; echo "$lmp_help" >&2; exit 1; }
sed -n '/^Installed packages/,/^List of individual/p' <<<"$lmp_help" | grep -ow REAXFF \
    || { echo "LAMMPS lacks REAXFF" >&2; exit 1; }
grep -E -m2 '^Large-scale|^LAMMPS version' <<<"$lmp_help" || true

section "files and size"
printf 'env md: %s inodes\npackage cache: %s inodes, %s not shared with the env\nall of %s: %s inodes\n' \
    "$(files "$ROOT/envs/md")" "$(files "$ROOT/pkgs")" \
    "$(( $(files "$ROOT/envs/md" "$ROOT/pkgs") - $(files "$ROOT/envs/md") ))" \
    "$ROOT" "$(files "$ROOT")"
du -sh "$ROOT/envs/md" "$ROOT/pkgs" "$ROOT"
section "quota after"; usage
