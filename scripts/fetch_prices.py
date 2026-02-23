#!/usr/bin/env python3
"""
fetch_prices.py
Sovson Analytics - Phase 2 (Updated 2026-02-23)

Fetches 3 years of daily OHLC price data using yahooquery.
This library is used to bypass Yahoo Finance rate limiting (429) 
that affects direct requests/yfinance.
Calculates Heikin Ashi candle values and stores everything in SQLite.

Usage:
    python3 fetch_prices.py              # fetch all active tickers
    python3 fetch_prices.py AAPL META    # fetch specific tickers only
"""

import sys
import time
import sqlite3
import logging
import random
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
from yahooquery import Ticker

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path("/home/daniel/sovson-analytics")
DB_PATH  = BASE_DIR / "data" / "sovson_analytics.db"
LOG_DIR  = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "fetch_prices.log"),
    ],
)
log = logging.getLogger(__name__)

def fetch_yahooquery(ticker: str) -> pd.DataFrame | None:
    """
    Fetches ~3 years of daily OHLC using yahooquery.
    """
    try:
        t = Ticker(ticker)
        df = t.history(period="3y", interval="1d")
        if df is None or df.empty:
            log.warning(f"{ticker}: No data returned from yahooquery")
            return None
            
        if isinstance(df, pd.DataFrame):
            # yahooquery returns a multi-index (symbol, date)
            if isinstance(df.index, pd.MultiIndex):
                df = df.xs(ticker)
            
            df = df.rename(columns={
                "open": "Open", 
                "high": "High", 
                "low": "Low", 
                "close": "Close", 
                "volume": "Volume"
            })
            
            # Ensure we have the required columns
            required = ["Open", "High", "Low", "Close", "Volume"]
            if not all(col in df.columns for col in required):
                log.error(f"{ticker}: Missing columns in response")
                return None
                
            df = df[required].dropna()
            
            if len(df) < 30:
                log.warning(f"{ticker}: only {len(df)} rows — skipping")
                return None
                
            return df
            
    except Exception as e:
        log.error(f"{ticker}: yahooquery error: {e}")
    return None

def calculate_heikin_ashi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds ha_open, ha_high, ha_low, ha_close columns.
    """
    ha_close = (df["Open"] + df["High"] + df["Low"] + df["Close"]) / 4

    ha_open = pd.Series(index=df.index, dtype=float)
    ha_open.iloc[0] = (df["Open"].iloc[0] + df["Close"].iloc[0]) / 2
    for i in range(1, len(df)):
        ha_open.iloc[i] = (ha_open.iloc[i - 1] + ha_close.iloc[i - 1]) / 2

    ha_high = pd.concat([df["High"], ha_open, ha_close], axis=1).max(axis=1)
    ha_low  = pd.concat([df["Low"],  ha_open, ha_close], axis=1).min(axis=1)

    df = df.copy()
    df["ha_open"]  = ha_open
    df["ha_high"]  = ha_high
    df["ha_low"]   = ha_low
    df["ha_close"] = ha_close
    return df

def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def get_active_tickers(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT ticker FROM tickers WHERE active = 1 ORDER BY ticker"
    ).fetchall()
    return [r["ticker"] for r in rows]

def upsert_prices(conn: sqlite3.Connection, ticker: str, df: pd.DataFrame) -> int:
    cur = conn.cursor()
    rows_written = 0
    for date, row in df.iterrows():
        # Handle date index which might be datetime or string
        date_str = date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date)
        cur.execute(
            """
            INSERT INTO daily_prices
                (ticker, date, open, high, low, close, volume,
                 ha_open, ha_high, ha_low, ha_close)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker, date) DO UPDATE SET
                open=excluded.open, high=excluded.high, low=excluded.low,
                close=excluded.close, volume=excluded.volume,
                ha_open=excluded.ha_open, ha_high=excluded.ha_high,
                ha_low=excluded.ha_low, ha_close=excluded.ha_close
            """,
            (ticker, date_str, float(row["Open"]), float(row["High"]),
             float(row["Low"]), float(row["Close"]), int(row["Volume"]),
             float(row["ha_open"]), float(row["ha_high"]), float(row["ha_low"]), float(row["ha_close"]))
        )
        rows_written += 1
    conn.commit()
    return rows_written

def main() -> None:
    if not DB_PATH.exists():
        log.error(f"Database not found at {DB_PATH}. Run setup_database.py first.")
        sys.exit(1)

    conn = get_connection()

    if len(sys.argv) > 1:
        tickers = [t.strip().upper() for t in sys.argv[1:]]
        log.info(f"Mode: manual — tickers: {tickers}")
    else:
        tickers = get_active_tickers(conn)
        log.info(f"Mode: all active — found {len(tickers)}: {tickers}")

    if not tickers:
        log.warning("No tickers found.")
        conn.close()
        return

    success_count = 0
    fail_count    = 0

    for i, ticker in enumerate(tickers):
        log.info(f"── {ticker} ──────────────────────────────")
        
        # Jittered delay
        if i > 0:
            time.sleep(random.uniform(2, 5))

        df = fetch_yahooquery(ticker)
        if df is None:
            fail_count += 1
            continue

        df    = calculate_heikin_ashi(df)
        rows  = upsert_prices(conn, ticker, df)
        start = df.index[0].strftime("%Y-%m-%d") if hasattr(df.index[0], "strftime") else str(df.index[0])
        end   = df.index[-1].strftime("%Y-%m-%d") if hasattr(df.index[-1], "strftime") else str(df.index[-1])

        log.info(f"{ticker}: Success via yahooquery ({rows} rows, {start} -> {end})")
        success_count += 1

    conn.close()
    log.info("=" * 50)
    log.info(f"Done.  Success: {success_count}   Failed: {fail_count}")

if __name__ == "__main__":
    main()
