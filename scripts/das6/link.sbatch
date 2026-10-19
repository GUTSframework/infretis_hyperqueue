#!/usr/bin/env bash
# One link of a chained one-node infretis run (Fast Track step 7). Submitted by
# scripts/das6/chain.sh from the repo root, which sets the Slurm options and
# the environment (CASE, EXAMPLE, RUNNER, STEPS, WORKERS, LINK, LINKS, NAME,
# SITE, TIME, MARGIN).
#
# 1. Link > 1: stop if restart.toml does not parse or no steps remain.
# 2. Pre-submit the next link (afterany), unless this is the last one.
# 3. Copy the venv from bin/venv.tar to node-local disk and run that copy
#    (plan decision 15). Copies left by jobs that are gone (a walltime kill
#    skips the EXIT trap) are removed first.
# 4. gutsrun: an HQ server and one worker inside the job, a fresh server dir,
#    no journal (plan decision 3).
# 5. bench/bare_run.py runs the case. Slurm's SIGTERM (--signal=B:TERM@MARGIN)
#    is passed on; infretis stops between moves (gutsrun) or is killed (bare).
# 6. Cancel the next link if all steps are done or the run failed.
#
# Outputs, in runs/<site>/: <NAME>/ (the run), <NAME>-link<N>-<job>.out (this
# log), .json (bare_run.py's record), -hq-server.log, -hq-worker.log.
#
# Without Slurm (laptop test of everything but chaining), from the repo root:
#
#   CASE=2d EXAMPLE=examples/2d_potential RUNNER=gutsrun STEPS=40 WORKERS=4 \
#   LINK=1 LINKS=1 NAME=test SITE=das6 LOCAL_DISK=$TMPDIR CORES=4 \
#       bash scripts/das6/link.sbatch
#
# (LOCAL_DISK and CORES override the site file.)
set -u

repo="$(pwd)"
. scripts/das6/common.sh
job="${SLURM_JOB_ID:-nojob-$$}"
runs="$repo/runs/$SITE"
rundir="$runs/$NAME"
prefix="$runs/$NAME-link$LINK-$job"
log() { echo "[$(date +%T)] $*"; }

next=""
venv_dir=""
server=""
cancel_next() {
    if [ -n "$next" ]; then scancel "$next" && log "cancelled next link $next ($1)"; next=""; fi
}
fail() { log "$*"; cancel_next failed; exit 1; }
cleanup() {
    if [ -n "$server" ]; then
        hq server stop --server-dir "$server" > /dev/null 2>&1
        wait 2>/dev/null
        rm -rf "$server"
    fi
    [ -n "$venv_dir" ] && rm -rf "$venv_dir"
}
trap cleanup EXIT

log "link $LINK/$LINKS of $NAME ($CASE, $RUNNER) on $(hostname), job $job," \
    "repo $(cat REVISION 2>/dev/null || git describe --always --dirty 2>/dev/null)"

# --- 1. continue only if there is something to continue -----------------------
restart=()
if [ "$LINK" -gt 1 ]; then
    state="$(restart_state "$rundir")" \
        || { log "restart.toml in $rundir is missing or does not parse; chain stops" \
                 "(backups: restart.toml.bak.*, restart.toml.link*)"; exit 1; }
    read -r cstep steps <<< "$state"
    if [ "$cstep" -ge "$steps" ]; then log "all $steps steps done"; exit 0; fi
    log "continuing at step $cstep of $steps"
    cp -p "$rundir/restart.toml" "$rundir/restart.toml.link$LINK"
    restart=(--restart)
fi

# --- 2. the next link -----------------------------------------------------------
if [ "$LINK" -lt "$LINKS" ] && command -v sbatch > /dev/null; then
    if next="$(scripts/das6/chain.sh --next "$((LINK + 1))" "$job")"; then
        log "next link: job $next (afterany:$job)"
    else
        next=""
        log "could not submit the next link; this one runs anyway"
    fi
fi

ulimit -n "$(ulimit -Hn)" 2>/dev/null
log "open files: soft $(ulimit -Sn), hard $(ulimit -Hn)"

# --- 3. venv --------------------------------------------------------------------
py="$repo/.venv/bin/python"
if [ "$(site_get python.local_venv)" = true ]; then
    venv_stale && fail "bin/venv.tar is missing or older than .venv; run scripts/das6/chain.sh --pack"
    base="${LOCAL_DISK:-$(site_get paths.local_disk)}"
    if command -v squeue > /dev/null && mine="$(squeue -h -u "$USER" -o %i)"; then
        for d in "$base/$USER-gutsrun-"*; do
            [ -d "$d" ] || continue
            grep -qx "${d##*-gutsrun-}" <<< "$mine" || { rm -rf "$d"; log "removed stale $d"; }
        done
    fi
    venv_dir="$base/$USER-gutsrun-$job"
    t0=$SECONDS
    mkdir -p "$venv_dir" && tar -C "$venv_dir" -xf bin/venv.tar || fail "unpacking the venv failed"
    py="$venv_dir/.venv/bin/python"
    log "venv unpacked to $venv_dir in $((SECONDS - t0)) s"
fi
"$py" -c 'import infretis' || fail "$py cannot import infretis"

# --- 4. HQ ----------------------------------------------------------------------
if [ "$RUNNER" = gutsrun ]; then
    [ -x "$repo/bin/hq" ] && PATH="$repo/bin:$PATH"
    hq --version | grep -q ' v0\.26\.2$' || fail "need hq 0.26.2, have: $(hq --version 2>&1)"
    server="$runs/$NAME-hq-$job"
    rm -rf "$server"
    # Created here so that waiting below lists an existing directory: retrying a
    # name that does not exist yet can miss it for 30 s on NFS (ASSUMPTIONS.md).
    mkdir -p "$server"
    hq server start --server-dir "$server" > "$prefix-hq-server.log" 2>&1 &
    for _ in $(seq 300); do
        ls "$server" | grep -qx hq-current && break
        sleep 0.2
    done
    ls "$server" | grep -qx hq-current || fail "hq server did not start; see $prefix-hq-server.log"
    hq worker start --server-dir "$server" --cpus "${CORES:-$(site_get slurm.cores_per_node)}" \
        > "$prefix-hq-worker.log" 2>&1 &
    export HQ_SERVER_DIR="$server"
    log "hq server and worker started in $server"
fi

# --- 5. run ---------------------------------------------------------------------
args=("$EXAMPLE" "$rundir" --workers "$WORKERS" --runner "$RUNNER")
[ -n "${STEPS:-}" ] && args+=(--steps "$STEPS")
[ "$RUNNER" = gutsrun ] && args+=(--backend hq)
log "running: bench/bare_run.py ${args[*]} ${restart[*]}"
"$py" bench/bare_run.py "${args[@]}" "${restart[@]}" > "$prefix.json" &
run_pid=$!
stop() { log "SIGTERM: stopping infretis"; kill -TERM "$run_pid" 2>/dev/null; }
trap stop TERM
# wait returns early when the trap runs; wait again until the run has ended.
while :; do
    wait "$run_pid"; rc=$?
    [ "$rc" -gt 128 ] && kill -0 "$run_pid" 2>/dev/null && continue
    break
done
trap - TERM
log "bare_run.py exit $rc"

# --- 6. done, stopped or failed -------------------------------------------------
state="$(restart_state "$rundir")" || fail "restart.toml does not parse after the run; chain stops"
read -r cstep steps <<< "$state"
if [ "$cstep" -ge "$steps" ]; then
    log "all $steps steps done"
    cancel_next done
elif [ "$rc" -eq 3 ]; then
    if [ -n "$next" ]; then
        log "stopped at step $cstep of $steps; link $((LINK + 1)) continues"
    else
        log "stopped at step $cstep of $steps; no further link (LINKS=$LINKS)"
    fi
else
    fail "run failed (exit $rc) at step $cstep of $steps; chain stops"
fi
