#!/usr/bin/env python3
"""
run_daily.py
Sovson Analytics - Master Runner

Runs the full daily pipeline in order:
  1. fetch_prices.py    - pull latest price data from Yahoo Finance
  2. calculate_macd.py  - compute 5-day rolling MACD
  3. fetch_earnings.py  - pull analyst ratings and earnings dates
  4. signal_detector.py - detect signals and write to DB

This script mirrors what the two n8n workflows will eventually do.
Run it manually for testing, or call it from cron / n8n.

Usage:
    python3 run_daily.py              # run full pipeline
    python3 run_daily.py --signals-only  # skip price/earnings fetch, just detect signals
"""

import sys
import time
import logging
import subprocess
from pathlib import Path
from datetime import datetime

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR    = Path("/home/daniel/sovson-analytics")
SCRIPTS_DIR = Path("/home/daniel/Mac-D-Alert/scripts")
LOG_DIR     = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────
log_file = LOG_DIR / f"run_daily_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file),
    ],
)
log = logging.getLogger(__name__)

# ── Pipeline definition ───────────────────────────────────────────────────────
# Each step: (label, script_name, required)
# required=True means pipeline aborts if this step fails
PIPELINE = [
    ("Fetch Prices",     "fetch_prices.py",    True),
    ("Calculate MACD",   "calculate_macd.py",  True),
    ("Fetch Earnings",   "fetch_earnings.py",  False),  # non-fatal if Yahoo throttles
    ("Signal Detector",  "signal_detector.py", True),
]

SIGNALS_ONLY_PIPELINE = [
    ("Signal Detector",  "signal_detector.py", True),
]


# ── Runner ────────────────────────────────────────────────────────────────────

def run_script(label: str, script_name: str) -> bool:
    """
    Runs a single script as a subprocess.
    Streams output in real time and returns True on success, False on failure.
    """
    script_path = SCRIPTS_DIR / script_name

    if not script_path.exists():
        log.error(f"{label}: script not found at {script_path}")
        return False

    log.info(f"{'=' * 50}")
    log.info(f"STARTING: {label}")
    log.info(f"{'=' * 50}")

    start = time.time()

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=False,   # let output stream to terminal/log
            text=True,
        )
        elapsed = round(time.time() - start, 1)

        if result.returncode == 0:
            log.info(f"COMPLETED: {label} ({elapsed}s)")
            return True
        else:
            log.error(f"FAILED: {label} — exit code {result.returncode} ({elapsed}s)")
            return False

    except Exception as exc:
        log.error(f"EXCEPTION in {label}: {exc}")
        return False


def run_pipeline(steps: list) -> None:
    """Runs a list of pipeline steps, stopping on any required failure."""
    total_start = time.time()
    results     = []

    for label, script, required in steps:
        success = run_script(label, script)
        results.append((label, success, required))

        if not success and required:
            log.error(f"Required step '{label}' failed — aborting pipeline")
            break

        if not success and not required:
            log.warning(f"Optional step '{label}' failed — continuing anyway")

        # Brief pause between steps
        time.sleep(1)

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed = round(time.time() - total_start, 1)
    log.info(f"{'=' * 50}")
    log.info(f"PIPELINE SUMMARY ({elapsed}s total)")
    log.info(f"{'=' * 50}")

    all_ok = True
    for label, success, required in results:
        status = "✅ OK" if success else ("❌ FAILED (required)" if required else "⚠️  FAILED (optional)")
        log.info(f"  {label}: {status}")
        if not success and required:
            all_ok = False

    if all_ok:
        log.info("Pipeline completed successfully.")
    else:
        log.error("Pipeline completed with errors.")
        sys.exit(1)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    signals_only = "--signals-only" in sys.argv

    log.info(f"{'=' * 50}")
    log.info(f"Sovson Analytics — Daily Run")
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S PST')}")
    log.info(f"Mode: {'signals only' if signals_only else 'full pipeline'}")
    log.info(f"{'=' * 50}")

    steps = SIGNALS_ONLY_PIPELINE if signals_only else PIPELINE
    run_pipeline(steps)


if __name__ == "__main__":
    main()
