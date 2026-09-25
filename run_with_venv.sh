#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# noibu-traffic-gen runner
# Creates/uses venv .venv, installs dependencies and Playwright browsers,
# then runs the ab-test traffic generator with .env support.
# -----------------------------------------------------------------------------

PYBIN="${PYTHON:-python3}"
if ! command -v "$PYBIN" >/dev/null 2>&1; then
  if command -v python3 >/dev/null 2>&1; then PYBIN="python3"
  elif command -v python >/dev/null 2>&1; then PYBIN="python"
  else echo "No python found on PATH"; exit 1; fi
fi

echo ">> Using Python: $PYBIN"
echo ">> Creating/using venv .venv"
"$PYBIN" -m venv .venv
# shellcheck disable=SC1090
source .venv/bin/activate

echo ">> Upgrading pip and installing deps…"
python -m pip install --upgrade pip
python -m pip install playwright==1.48.0 python-dotenv==1.0.1

echo ">> Installing Playwright Chromium browser…"
python -m playwright install chromium firefox webkit

echo ">> Running noibu-traffic-gen.py …"
export PYTHONUNBUFFERED=1
python -u noibu-traffic-gen.py
