#!/usr/bin/env python3
"""
fetch_prices_hybrid.py
Sovson Analytics - Phase 2 (Refactored)

Hybrid fetching strategy:
1. Primary: Yahoo Finance (Direct API)
2. Fallback: Financial Modeling Prep (FMP) if Yahoo fails or rate limits (429).

Calculates Heikin Ashi values and stores in SQLite.
"""

import os
import sys
import time
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timedelta

import requests
import pandas as pd

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
        logging.FileHandler(LOG_DIR / "fetch_prices_hybrid.log"),
    ],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
YAHOO_URL = "https://query2.finance.yahoo.com/v8/finance/chart/{ticker}"
FMP_URL   = "https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}"
FMP_API_KEY = os.environ.get("FMP_API_KEY")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# ── Yahoo Fetcher ─────────────────────────────────────────────────────────────

def fetch_yahoo(ticker: str) -> pd.DataFrame | None:
    end_ts   = int(datetime.now().timestamp())
    start_ts = int((datetime.now() - timedelta(days=3 * 365 + 60)).timestamp())

    url    = YAHOO_URL.format(ticker=ticker)
    params = {
        "period1":  start_ts,
        "period2":  end_ts,
        "interval": "1d",
        "events":   "history",
    }

    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            result = data["chart"]["result"]
            if not result: return None
            
            chart = result[0]
            timestamps = chart["timestamp"]
            quotes = chart["indicators"]["quote"][0]
            
            df = pd.DataFrame({
                "Open": quotes["open"],
                "High": quotes["high"],
                "Low": quotes["low"],
                "Close": quotes["close"],
                "Volume": quotes["volume"],
            }, index=pd.to_datetime(timestamps, unit="s", utc=True)
                               .tz_convert("America/Los_Angeles")
                               .normalize())
            df.index.name = "Date"
            return df.dropna()
        elif resp.status_code == 429:
            log.warning(f"{ticker}: Yahoo rate limited (429)")
        else:
            log.warning(f"{ticker}: Yahoo HTTP {resp.status_code}")
    except Exception as e:
        log.error(f"{ticker}: Yahoo fetch error: {e}")
    
    return None

# ── FMP Fetcher ───────────────────────────────────────────────────────────────

def fetch_fmp(ticker: str) -> pd.DataFrame | None:
    if not FMP_API_KEY:
        log.warning(f"{ticker}: FMP_API_KEY not set, skipping fallback")
        return None

    log.info(f"{ticker}: Attempting FMP fallback...")
    
    # FMP free tier usually gives 5 years of history
    url = FMP_URL.format(ticker=ticker)
    params = {"apikey": FMP_API_KEY}
    
    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            if "historical" not in data:
                log.warning(f"{ticker}: FMP returned no historical data")
                return None
            
            df = pd.DataFrame(data["historical"])
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date").sort_index()
            
            # Rename columns to match Yahoo format
            df = df.rename(columns={
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume"
            })
            
            # Filter to last 3 years + buffer
            cutoff = datetime.now() - timedelta(days=3 * 365 + 60)
            df = df[df.index >= cutoff]
            
            df.index = df.index.tz_localize("America/Los_Angeles").normalize()
            df.index.name = "Date"
            
            return df[["Open", "High", "Low", "Close", "Volume"]]
        else:
            log.error(f"{ticker}: FMP HTTP {resp.status_code}")
    except Exception as e:
        log.error(f"{ticker}: FMP fetch error: {e}")
        
    return None

# ── Heikin Ashi ───────────────────────────────────────────────────────────────

def calculate_heikin_ashi(df: pd.DataFrame) -> pd.DataFrame:
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

# ── Database ──────────────────────────────────────────────────────────────────

def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def get_active_tickers(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT ticker FROM tickers WHERE active = 1 ORDER BY ticker").fetchall()
    return [r["ticker"] for r in rows]

def upsert_prices(conn: sqlite3.Connection, ticker: str, df: pd.DataFrame) -> int:
    cur = conn.cursor()
    rows_written = 0
    for date, row in df.iterrows():
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
            (ticker, date.strftime("%Y-%m-%d"), float(row["Open"]), float(row["High"]),
             float(row["Low"]), float(row["Close"]), int(row["Volume"]),
             float(row["ha_open"]), float(row["ha_high"]), float(row["ha_low"]), float(row["ha_close"]))
        )
        rows_written += 1
    conn.commit()
    return rows_written

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if not DB_PATH.exists():
        log.error(f"Database not found at {DB_PATH}")
        sys.exit(1)

    conn = get_connection()
    tickers = [t.strip().upper() for t in sys.argv[1:]] if len(sys.argv) > 1 else get_active_tickers(conn)

    if not tickers:
        log.warning("No tickers to process")
        conn.close()
        return

    for i, ticker in enumerate(tickers):
        log.info(f"── {ticker} ──────────────────────────────")
        if i > 0: time.sleep(2)

        source = "Yahoo"
        df = fetch_yahoo(ticker)
        
        if df is None:
            source = "FMP"
            df = fetch_fmp(ticker)

        if df is not None:
            df = calculate_heikin_ashi(df)
            rows = upsert_prices(conn, ticker, df)
            log.info(f"{ticker}: Success via {source} ({rows} rows)")
        else:
            log.error(f"{ticker}: All fetch sources failed")

    conn.close()
    log.info("Done.")

if __name__ == "__main__":
    main()
