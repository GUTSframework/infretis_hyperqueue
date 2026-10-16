#!/usr/bin/env bash
# Fast Track step 5: each case through bare infretis, gutsrun (local) and
# gutsrun (HQ), back to back, same seed and steps. Run inside `nix develop`.
#
#   bench/step5.sh runs/laptop/2026-10-16-step5 [cases...]
#
# Cases: 2d, lammps, puckering (needs runs/prepared/puckering/ready from
# examples/puckering/prepare.sh). The HQ runs get a fresh server and one
# worker with as many CPUs as workers.
set -euo pipefail

out="${1:?output directory}"; shift
cases=("${@:-2d lammps puckering}")
read -r -a cases <<< "${cases[*]}"
workers="${WORKERS:-4}"
mkdir -p "$out"

example() {
  case "$1" in
    2d) echo "examples/2d_potential --steps ${STEPS_2D:-400}" ;;
    lammps) echo "examples/water_dissociation_lammps --steps ${STEPS_LAMMPS:-200}" ;;
    puckering) echo "runs/prepared/puckering/ready --steps ${STEPS_PUCK:-200}" ;;
  esac
}

for c in "${cases[@]}"; do
  read -r -a ex <<< "$(example "$c")"
  for r in bare local hq; do
    rd="$out/$c-$r"
    [ -e "$rd" ] && { echo "skip $rd (exists)"; continue; }
    args=(--workers "$workers")
    case "$c" in
      puckering) args+=(--wmdrun "gmx mdrun -ntomp 1 -ntmpi 1") ;;
    esac
    case "$r" in
      bare) runner=(--runner bare) ;;
      local) runner=(--runner gutsrun --backend local) ;;
      hq) runner=(--runner gutsrun --backend hq) ;;
    esac
    echo "== $c / $r  $(date +%T)"
    if [ "$r" = hq ]; then
      server="$(realpath -m "$out")/hq-server-$c"; rm -rf "$server"
      hq server start --server-dir "$server" > "$out/hq-server-$c.log" 2>&1 &
      spid=$!
      until [ -e "$server/hq-current" ]; do sleep 0.1; done
      hq worker start --server-dir "$server" --cpus "$workers" > "$out/hq-worker-$c.log" 2>&1 &
      sleep 1
      HQ_SERVER_DIR="$server" .venv/bin/python bench/bare_run.py "${ex[@]}" "$rd" "${args[@]}" "${runner[@]}" \
        > "$out/$c-$r.json" 2> "$out/$c-$r.err" || echo "  rc=$?"
      hq server stop --server-dir "$server" > /dev/null 2>&1 || true
      wait "$spid" 2>/dev/null || true
    else
      .venv/bin/python bench/bare_run.py "${ex[@]}" "$rd" "${args[@]}" "${runner[@]}" \
        > "$out/$c-$r.json" 2> "$out/$c-$r.err" || echo "  rc=$?"
    fi
    grep -h '"wall_s"' "$out/$c-$r.json" || true
  done
done
