#!/usr/bin/env bash
set -euo pipefail

# Direct-run script:
# - Creates/uses venv .venv
# - Installs deps + Playwright browsers
# - Runs noisy_runner_with_testcard.py with unbuffered logs
#
# Env overrides supported:
#   ORIGIN (default: https://noibu.mybigcommerce.com)
#   SESSIONS_PER_MINUTE (default: 12)
#   AVG_SESSION_MINUTES (default: 1)
#   EXTRA_FIXED_WAIT_SEC (default: 5)
#   CHECKOUT_COMPLETE_RATE (default: 0.3)
#
# Usage:
#   ./run_noisy_noibu.sh
#   SESSIONS_PER_MINUTE=6 AVG_SESSION_MINUTES=2 ./run_noisy_noibu.sh

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
# minimal deps (trafficgen is your local package)
python -m pip install playwright==1.48.0 typer==0.12.5 pydantic==2.8.2 PyYAML==6.0.2 python-dotenv==1.0.1 tenacity==8.5.0

echo ">> Installing Playwright browsers…"
python -m playwright install

echo ">> Running noisy_runner_with_testcard.py …"
export PYTHONUNBUFFERED=1
python -u noisy_runner_with_testcard.py
