#!/bin/bash
# run_fetcher.sh
# Loads the FMP API key from .env and runs the hybrid price fetcher.

SCRIPT_DIR="/home/daniel/Mac-D-Alert/scripts"
ENV_FILE="$SCRIPT_DIR/.env"

if [ -f "$ENV_FILE" ]; then
    export $(grep -v '^#' "$ENV_FILE" | xargs)
else
    echo "Warning: .env file not found at $ENV_FILE"
fi

python3 "$SCRIPT_DIR/fetch_prices_hybrid.py" "$@"
