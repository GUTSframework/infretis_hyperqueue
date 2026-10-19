#!/usr/bin/env bash
# Create .venv with Python 3.12 and the exact versions in requirements-lock.txt.
# On the laptop run it inside `nix develop`. On a cluster, PYTHON must name a
# Python 3.12 (DAS-6: /usr/bin/python3.12; the default python3 is 3.6.8), and
# wheels only, without pip's cache in $HOME:
#
#   PIP_ONLY_BINARY=:all: PIP_NO_CACHE_DIR=1 bash scripts/setup-venv.sh
#
# (infretis and inftools still install from git; their build backends are wheels.)
#
# This checkout is installed editable, which gives .venv/bin/gutsrun-infretis.
#
# To change a pin: edit requirements.txt, run with REQS=requirements.txt, then
# regenerate the lock with
# `.venv/bin/python -m pip freeze --exclude-editable > requirements-lock.txt`.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
python="${PYTHON:-python3.12}"
reqs="${REQS:-requirements-lock.txt}"

"$python" -m venv "$root/.venv"
"$root/.venv/bin/python" -m pip install --upgrade pip
"$root/.venv/bin/python" -m pip install -r "$root/$reqs"
"$root/.venv/bin/python" -m pip install --no-deps -e "$root"
"$root/.venv/bin/python" -c 'import infretis, hyperqueue, gutsrun_infretis; print("ok:", infretis.__file__)'
