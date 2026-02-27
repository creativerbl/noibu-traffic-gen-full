#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# noibu-traffic-gen runner
# Creates/uses venv .venv, installs dependencies and Playwright browsers,
# then runs the new Chromium-only traffic generator with .env support.
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
python -m pip install playwright==1.48.0 typer==0.12.5 pydantic==2.8.2 PyYAML==6.0.2 \
    python-dotenv==1.0.1 tenacity==8.5.0

echo ">> Installing Playwright Chromium browser…"
python -m playwright install chromium

echo ">> Running noibu-traffic-gen.py …"
export PYTHONUNBUFFERED=1
python -u noibu-traffic-gen.py
