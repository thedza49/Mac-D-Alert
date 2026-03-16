#!/usr/bin/env python3
"""
data_auditor.py
Sovson Analytics - Data Integrity & Healing

Scans the database for missing values (gaps) in historical data 
and triggers targeted backfills.
"""

import sqlite3
import logging
import sys
import subprocess
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "sovson_analytics.db"
SCRIPTS_DIR = BASE_DIR / "scripts"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

def check_gaps():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    # 1. Check for signals without backtest data
    signals_missing_backtest = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE peak_gain_pct IS NULL AND signal_type = 'BUY'"
    ).fetchone()[0]
    
    if signals_missing_backtest > 0:
        log.info(f"🔍 Found {signals_missing_backtest} BUY signals missing backtest returns.")
        # We can't easily backfill this without re-running the detector logic
        # but we already have run_backtest_only in signal_detector.py
    
    # 2. Check for missing HA candles or Indicators in daily_prices
    prices_missing_data = conn.execute(
        "SELECT ticker, COUNT(*) as gaps FROM daily_prices WHERE ha_open IS NULL OR ma_50d IS NULL GROUP BY ticker"
    ).fetchall()
    
    for row in prices_missing_data:
        log.warning(f"⚠️ {row['ticker']}: {row['gaps']} days missing HA or Indicators.")

    # 3. Check for tickers missing signals entirely (Provisioning)
    provision_needed = conn.execute(
        "SELECT ticker FROM tickers WHERE active = 1 AND ticker NOT IN (SELECT DISTINCT ticker FROM signals)"
    ).fetchall()
    
    for row in provision_needed:
        log.warning(f"🚨 {row['ticker']}: Active but has 0 signals recorded.")

    conn.close()

def run_repairs():
    """Triggers existing scripts with repair flags."""
    log.info("🛠 Starting auto-repairs...")
    
    # Repair missing signals/backtests for all active tickers
    try:
        log.info("Reparing signal backtests...")
        subprocess.run([sys.executable, str(SCRIPTS_DIR / "signal_detector.py"), "--backtest-only"], check=True)
    except Exception as e:
        log.error(f"Signal repair failed: {e}")

    log.info("✅ Repair cycle complete.")

if __name__ == "__main__":
    check_gaps()
    if "--repair" in sys.argv:
        run_repairs()
