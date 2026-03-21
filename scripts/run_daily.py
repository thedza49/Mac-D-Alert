#!/usr/bin/env python3
"""
run_daily.py
Sovson Analytics - Master Runner

Runs the full daily pipeline in order:
  1. fetch_prices.py    - pull latest price data from Yahoo Finance
  2. calculate_macd.py  - compute 5-day rolling MACD
  3. fetch_earnings.py  - pull analyst ratings and earnings dates
  4. signal_detector.py - detect signals and write to DB

This script ensures orchestration logic lives in code rather than
complex n8n workflows, allowing for better error handling and sequence integrity.

Usage:
    python3 run_daily.py              # run full pipeline
    python3 run_daily.py --signals-only  # skip price/earnings fetch, just detect signals
"""

import sys
import os
import time
import logging
import subprocess
import fcntl
from pathlib import Path
from datetime import datetime

# ── Paths (Relative to script for repo-agnostic execution) ──────────────────
SCRIPTS_DIR = Path(__file__).resolve().parent
BASE_DIR    = SCRIPTS_DIR.parent
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

# ── Lock file to prevent concurrent runs ──────────────────────────────────────
LOCK_FILE = BASE_DIR / ".run_daily.lock"

# ── Pipeline definition ───────────────────────────────────────────────────────
# Each step: (label, script_name, required)
PIPELINE = [
    ("Fetch Prices",     "fetch_prices.py",    True),
    ("Calculate MACD",   "calculate_macd.py",  True),
    ("Fetch Earnings",   "fetch_earnings.py",  False),
    ("Signal Detector",  "signal_detector.py", True),
]

SIGNALS_ONLY_PIPELINE = [
    ("Signal Detector",  "signal_detector.py", True),
]


# ── Runner ────────────────────────────────────────────────────────────────────

def run_script(label: str, script_name: str) -> bool:
    """Runs a single script as a subprocess."""
    script_path = SCRIPTS_DIR / script_name

    if not script_path.exists():
        log.error(f"{label}: script not found at {script_path}")
        return False

    log.info(f"{'=' * 50}")
    log.info(f"STARTING: {label}")
    log.info(f"{'=' * 50}")

    start = time.time()

    try:
        # Avoid shell=True for security and reliability
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=False,
            text=True,
            cwd=BASE_DIR
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

        time.sleep(1)

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

    # ── Concurrency Check ─────────────────────────────────────────────────────
    # Open the lock file in append mode to avoid truncating if we can't lock it
    lock_file = open(LOCK_FILE, "a")
    try:
        fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        log.error("Another instance of run_daily.py is already running. Exiting.")
        sys.exit(0)

    log.info(f"{'=' * 50}")
    log.info(f"Sovson Analytics — Daily Run")
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S PST')}")
    log.info(f"Mode: {'signals only' if signals_only else 'full pipeline'}")
    log.info(f"{'=' * 50}")

    try:
        # Write PID to lock file for debugging/info
        lock_file.truncate(0)
        lock_file.write(str(os.getpid()))
        lock_file.flush()

        steps = SIGNALS_ONLY_PIPELINE if signals_only else PIPELINE
        run_pipeline(steps)
    finally:
        # Don't delete the lock file, just release the lock.
        # This makes is_task_running in dashboard.py more reliable (always uses fcntl).
        lock_file.close()


if __name__ == "__main__":
    main()
