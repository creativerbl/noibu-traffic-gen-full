#!/bin/bash
set -e

# Detect Python 3
PYTHON=""
for cmd in python3 python; do
    if command -v "$cmd" &>/dev/null; then
        version=$("$cmd" --version 2>&1 | grep -oP '\d+\.\d+')
        major=$(echo "$version" | cut -d. -f1)
        if [ "$major" = "3" ]; then
            PYTHON="$cmd"
            break
        fi
    fi
done

if [ -z "$PYTHON" ]; then
    echo "ERROR: Python 3 not found. Please install Python 3.9+"
    exit 1
fi

echo "Using: $PYTHON ($($PYTHON --version 2>&1))"

# Create venv if needed
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    $PYTHON -m venv .venv
fi

source .venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --quiet --upgrade pip
pip install --quiet playwright python-dotenv

# Install Chromium browser
echo "Installing Chromium browser..."
python -m playwright install chromium

# Run the traffic generator
echo ""
echo "Starting traffic generator..."
python noibu-traffic-gen.py
