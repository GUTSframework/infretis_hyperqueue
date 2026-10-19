#!/usr/bin/env bash
# Start a chain of one-node infretis runs on DAS-6 (Fast Track step 7, gate B).
# Run from the repo root on fs0, after the venv exists:
#
#   scripts/das6/chain.sh <case> <runner>
#   STEPS=2000 TIME=00:05:00 scripts/das6/chain.sh 2d gutsrun
#   scripts/das6/chain.sh --pack          (only repack bin/venv.tar)
#
# case: 2d (examples/2d_potential) or lammps (examples/water_dissociation_lammps;
# needs an `lmp` on the nodes). runner: bare (infretis's own
# ProcessPoolExecutor, the baseline row) or gutsrun (the launcher with the HQ
# backend).
#
# Each link is one Slurm job running scripts/das6/link.sbatch. Link 1 creates
# runs/<site>/<NAME>/; later links continue from its restart.toml. Each link
# pre-submits the next one with afterany, so a link that is killed or
# cancelled is still followed; the link cancels it again once all steps are
# done or the run failed. Slurm options come from sites/<site>.toml.
#
# Environment (default):
#   STEPS    MC steps (the example's)       WORKERS  infretis workers (4)
#   TIME     walltime per link (00:14:00)   MARGIN   s before walltime that SIGTERM comes (90)
#   LINKS    most links in the chain (3, the DAS-6 daytime cap)
#   NAME     run name (<date>-<case>-<runner>)   SITE  (das6)
#   NIGHT=1  allow TIME beyond the site's daytime limit
#
# Before the first job it packs .venv into bin/venv.tar if the tarball is
# missing or older than the venv (plan decision 15; ~8 s on fs0). Prints the
# first job's id, for scripts/wait-jobs.sh.
set -euo pipefail

cd "$(dirname "$0")/../.."
export SITE="${SITE:-das6}"
. scripts/das6/common.sh

seconds() { local h m s; IFS=: read -r h m s <<< "$1"; echo $((10#$h * 3600 + 10#$m * 60 + 10#$s)); }

# submit <link> [<job to follow>]: submit one link, print its job id.
submit() {
    local exclusive=() dep=()
    [ "$(site_get slurm.exclusive)" = true ] && exclusive=(--exclusive)
    [ -n "${2:-}" ] && dep=(--dependency="afterany:$2")
    LINK="$1" sbatch --parsable --export=ALL \
        --job-name="gutsrun-$NAME-$1" --partition="$(site_get slurm.partition)" \
        "${exclusive[@]}" --nodes=1 --time="$TIME" --signal="B:TERM@$MARGIN" \
        --output="runs/$SITE/$NAME-link$1-%j.out" "${dep[@]}" \
        scripts/das6/link.sbatch
}

case "${1:-}" in
    --pack) pack_venv; ls -l bin/venv.tar; exit 0 ;;
    # Called by link.sbatch, with the chain's environment already exported.
    --next) submit "${2:?link}" "${3:?job to follow}"; exit 0 ;;
esac

case="${1:?usage: scripts/das6/chain.sh <case> <runner>}"
runner="${2:?usage: scripts/das6/chain.sh <case> <runner>}"
case "$case" in
    2d) example=examples/2d_potential ;;
    lammps) example=examples/water_dissociation_lammps ;;
    *) echo "unknown case: $case (2d, lammps)" >&2; exit 2 ;;
esac
case "$runner" in
    bare|gutsrun) ;;
    *) echo "unknown runner: $runner (bare, gutsrun)" >&2; exit 2 ;;
esac

export CASE="$case" RUNNER="$runner" EXAMPLE="$example"
export STEPS="${STEPS:-}" WORKERS="${WORKERS:-4}" TIME="${TIME:-00:14:00}"
export MARGIN="${MARGIN:-90}" LINKS="${LINKS:-3}"
export NAME="${NAME:-$(date +%F)-$case-$runner}"

limit="$(site_get slurm.max_walltime_day)"
if [ "${NIGHT:-0}" != 1 ] && [ "$(seconds "$TIME")" -ge "$(seconds "$limit")" ]; then
    echo "TIME=$TIME is not below the daytime limit $limit (NIGHT=1 to override)" >&2
    exit 2
fi
if [ "$(seconds "$TIME")" -le $((MARGIN + 60)) ]; then
    echo "TIME=$TIME leaves no time to run before SIGTERM at MARGIN=$MARGIN s" >&2
    exit 2
fi
[ -e "runs/$SITE/$NAME" ] && { echo "runs/$SITE/$NAME exists; pick another NAME" >&2; exit 2; }
[ -x .venv/bin/python ] || { echo "no .venv; run scripts/setup-venv.sh first" >&2; exit 2; }
if [ "$(site_get python.local_venv)" = true ] && venv_stale; then
    echo "packing .venv into bin/venv.tar" >&2
    pack_venv
fi
if [ "$runner" = gutsrun ] && ! { [ -x bin/hq ] || command -v hq > /dev/null; }; then
    echo "no hq in bin/ or on PATH; run scripts/get-hq.sh" >&2
    exit 2
fi

mkdir -p "runs/$SITE"
submit 1
