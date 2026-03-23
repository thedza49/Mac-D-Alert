#!/usr/bin/env python3
"""
data_auditor.py
Sovson Analytics - Data Integrity & Healing

Scans the database for missing values (gaps) in historical data 
and triggers targeted backfills. Includes Sync Validation for graphs and lifecycles.
"""

import sqlite3
import logging
import sys
import subprocess
import os
from pathlib import Path
from datetime import datetime, timezone

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "sovson_analytics.db"
SCRIPTS_DIR = BASE_DIR / "scripts"
STATIC_DIR = SCRIPTS_DIR / "static"

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
    
    # 2. Check for missing HA candles in daily_prices
    prices_missing_data = conn.execute(
        "SELECT ticker, COUNT(*) as gaps FROM daily_prices WHERE ha_open IS NULL GROUP BY ticker"
    ).fetchall()
    
    for row in prices_missing_data:
        log.warning(f"⚠️ {row['ticker']}: {row['gaps']} days missing HA candles in daily_prices.")

    # 3. Check for missing indicators in macd_5d_data
    macd_missing_data = conn.execute(
        "SELECT ticker, COUNT(*) as gaps FROM macd_5d_data WHERE macd_line IS NULL OR ma_50d IS NULL GROUP BY ticker"
    ).fetchall()
    
    for row in macd_missing_data:
        log.warning(f"⚠️ {row['ticker']}: {row['gaps']} periods missing indicators in macd_5d_data.")

    # 4. Check for tickers missing signals entirely (Provisioning)
    provision_needed = conn.execute(
        "SELECT ticker FROM tickers WHERE active = 1 AND ticker NOT IN (SELECT DISTINCT ticker FROM signals)"
    ).fetchall()
    
    for row in provision_needed:
        log.warning(f"🚨 {row['ticker']}: Active but has 0 signals recorded.")

    # 5. Sync Validation
    out_of_sync_tickers = check_sync_validation(conn)

    conn.close()
    return out_of_sync_tickers

def check_sync_validation(conn):
    """
    Performs Graph Sync and Lifecycle Sync checks.
    Returns a list of tickers that need graph regeneration.
    """
    log.info("Checking Sync Validation...")
    out_of_sync_graphs = []
    
    active_tickers = [row['ticker'] for row in conn.execute("SELECT ticker FROM tickers WHERE active = 1")]
    
    for ticker in active_tickers:
        # --- 1. Graph Sync Check ---
        graph_path = STATIC_DIR / f"graph_{ticker}.png"
        
        # Get most recent signal timestamp (created_at)
        res = conn.execute(
            "SELECT MAX(created_at) as last_signal FROM signals WHERE ticker = ?", (ticker,)
        ).fetchone()
        last_signal_str = res['last_signal']
        
        if not graph_path.exists():
            log.warning(f"📈 {ticker}: Graph PNG missing.")
            out_of_sync_graphs.append(ticker)
        elif last_signal_str:
            # Convert SQLite timestamp (UTC) to aware datetime
            try:
                last_signal_dt = datetime.strptime(last_signal_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                graph_mtime = datetime.fromtimestamp(graph_path.stat().st_mtime, tz=timezone.utc)
                
                if graph_mtime < last_signal_dt:
                    log.warning(f"📈 {ticker}: Graph is out of sync (Signal: {last_signal_str}, Graph: {graph_mtime.strftime('%Y-%m-%d %H:%M:%S')})")
                    out_of_sync_graphs.append(ticker)
            except Exception as e:
                log.error(f"Error parsing timestamp for {ticker}: {e}")

        # --- 2. Lifecycle Sync Check ---
        # Logic matches analyze_lifecycles.py: pair all pending BUYs with the FIRST subsequent SELL
        cursor = conn.execute("""
            SELECT signal_date, signal_type
            FROM signals
            WHERE ticker = ?
            ORDER BY signal_date ASC, id ASC
        """, (ticker,))
        signals = cursor.fetchall()
        
        expected_lifecycles = 0
        pending_buys = 0
        for sig in signals:
            if sig['signal_type'] == 'BUY':
                pending_buys += 1
            elif sig['signal_type'] == 'SELL' and pending_buys > 0:
                expected_lifecycles += pending_buys
                pending_buys = 0
        
        actual_lifecycles = conn.execute(
            "SELECT COUNT(*) FROM trade_lifecycles WHERE ticker = ?", (ticker,)
        ).fetchone()[0]
        
        if expected_lifecycles != actual_lifecycles:
            log.warning(f"🔄 {ticker}: Lifecycle mismatch! Expected {expected_lifecycles}, found {actual_lifecycles} in trade_lifecycles.")

    return out_of_sync_graphs

def run_repairs(out_of_sync_graphs=None):
    """Triggers existing scripts with repair flags."""
    log.info("🛠 Starting auto-repairs...")
    
    # 1. Repair missing signals/backtests for all active tickers
    try:
        log.info("Repairing signal backtests...")
        subprocess.run([sys.executable, str(SCRIPTS_DIR / "signal_detector.py"), "--backtest-only"], check=True)
    except Exception as e:
        log.error(f"Signal repair failed: {e}")

    # 2. Repair out-of-sync graphs
    if out_of_sync_graphs:
        log.info(f"Regenerating graphs for: {', '.join(out_of_sync_graphs)}")
        try:
            subprocess.run([sys.executable, str(SCRIPTS_DIR / "generate_static.py")] + out_of_sync_graphs, check=True)
        except Exception as e:
            log.error(f"Graph regeneration failed: {e}")

    log.info("✅ Repair cycle complete.")

if __name__ == "__main__":
    out_of_sync = check_gaps()
    if "--repair" in sys.argv:
        run_repairs(out_of_sync)
