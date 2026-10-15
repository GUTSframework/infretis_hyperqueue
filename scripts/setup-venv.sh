#!/usr/bin/env bash
# Create .venv with Python 3.12 and the exact versions in requirements-lock.txt.
# On the laptop run it inside `nix develop`; on a cluster, load a Python 3.12
# module first (DAS-6's default python3 is 3.6.8).
#
# To change a pin: edit requirements.txt, run with REQS=requirements.txt, then
# regenerate the lock with `.venv/bin/python -m pip freeze > requirements-lock.txt`.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
python="${PYTHON:-python3.12}"
reqs="${REQS:-requirements-lock.txt}"

"$python" -m venv "$root/.venv"
"$root/.venv/bin/python" -m pip install --upgrade pip
"$root/.venv/bin/python" -m pip install -r "$root/$reqs"
"$root/.venv/bin/python" -c 'import infretis, hyperqueue; print("ok:", infretis.__file__)'
