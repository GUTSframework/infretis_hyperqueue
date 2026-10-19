# Helpers for scripts/das6/chain.sh and link.sbatch; source it from the repo
# root. Needs python3.12 on PATH (/usr/bin on DAS-6, the dev shell on NixOS).

# site_get <dotted.key>: a value from sites/$SITE.toml (booleans as true/false).
site_get() {
    python3.12 - "sites/${SITE:?}.toml" "$1" <<'EOF'
import sys, tomllib
v = tomllib.load(open(sys.argv[1], "rb"))
for k in sys.argv[2].split("."):
    v = v[k]
print(str(v).lower() if isinstance(v, bool) else v)
EOF
}

# restart_state <rundir>: prints "<cstep> <steps>"; fails if restart.toml is
# missing or does not parse.
restart_state() {
    python3.12 - "$1/restart.toml" <<'EOF'
import sys, tomllib
c = tomllib.load(open(sys.argv[1], "rb"))
print(c["current"]["cstep"], c["simulation"]["steps"])
EOF
}

# venv_stale: true if bin/venv.tar is missing or older than the venv. pyvenv.cfg
# changes when the venv is created, site-packages when a package is installed
# or removed.
venv_stale() {
    local f
    [ -f bin/venv.tar ] || return 0
    for f in .venv/pyvenv.cfg .venv/lib/python3.12/site-packages requirements-lock.txt; do
        [ "$f" -nt bin/venv.tar ] && return 0
    done
    return 1
}

# pack_venv: .venv -> bin/venv.tar, via a temp file so a running job never
# reads a half-written tarball.
pack_venv() {
    mkdir -p bin
    tar -cf bin/.venv.tar.tmp .venv && mv bin/.venv.tar.tmp bin/venv.tar
}
