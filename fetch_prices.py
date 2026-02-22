#!/usr/bin/env python3
"""
fetch_prices.py
Sovson Analytics - Phase 2

Fetches 3 years of daily OHLC price data from yFinance for all active tickers,
calculates Heikin Ashi candle values, and stores everything in the SQLite database.

Usage:
    python3 fetch_prices.py              # fetch all active tickers
    python3 fetch_prices.py AAPL META    # fetch specific tickers only
"""

import sys
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timedelta

import yfinance as yf
import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR  = Path("/home/daniel/sovson-analytics")
DB_PATH   = BASE_DIR / "data" / "sovson_analytics.db"
LOG_DIR   = BASE_DIR / "logs"
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


# ── Database helpers ──────────────────────────────────────────────────────────

def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # safe concurrent access
    return conn


def get_active_tickers(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT ticker FROM tickers WHERE active = 1 ORDER BY ticker"
    ).fetchall()
    return [r["ticker"] for r in rows]


def upsert_prices(conn: sqlite3.Connection, ticker: str, df: pd.DataFrame) -> int:
    """Insert or replace price rows. Returns number of rows written."""
    rows_written = 0
    cur = conn.cursor()

    for date, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO daily_prices
                (ticker, date, open, high, low, close, volume,
                 ha_open, ha_high, ha_low, ha_close)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker, date) DO UPDATE SET
                open    = excluded.open,
                high    = excluded.high,
                low     = excluded.low,
                close   = excluded.close,
                volume  = excluded.volume,
                ha_open  = excluded.ha_open,
                ha_high  = excluded.ha_high,
                ha_low   = excluded.ha_low,
                ha_close = excluded.ha_close
            """,
            (
                ticker,
                date.strftime("%Y-%m-%d"),
                float(row["Open"]),
                float(row["High"]),
                float(row["Low"]),
                float(row["Close"]),
                int(row["Volume"]),
                float(row["ha_open"]),
                float(row["ha_high"]),
                float(row["ha_low"]),
                float(row["ha_close"]),
            ),
        )
        rows_written += 1

    conn.commit()
    return rows_written


# ── Heikin Ashi calculation ───────────────────────────────────────────────────

def calculate_heikin_ashi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds ha_open, ha_high, ha_low, ha_close columns to the dataframe.

    Formulas:
        HA Close = (Open + High + Low + Close) / 4
        HA Open  = (prev_HA_Open + prev_HA_Close) / 2   [first bar = (Open+Close)/2]
        HA High  = max(High, HA_Open, HA_Close)
        HA Low   = min(Low,  HA_Open, HA_Close)
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


# ── Core fetch logic ──────────────────────────────────────────────────────────

def fetch_ticker(ticker: str) -> pd.DataFrame | None:
    """
    Downloads 3 years + 60 days of daily OHLC from yFinance.
    Extra 60 days gives us enough history to seed the 26-period EMA
    used in MACD calculations without cold-start distortion.
    Returns cleaned DataFrame or None on failure.
    """
    end_date   = datetime.today()
    start_date = end_date - timedelta(days=3 * 365 + 60)

    try:
        raw = yf.download(
            ticker,
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            auto_adjust=True,     # adjusts for splits/dividends automatically
            progress=False,
            threads=False,
        )
    except Exception as exc:
        log.error(f"{ticker}: yFinance download failed — {exc}")
        return None

    if raw.empty:
        log.warning(f"{ticker}: no data returned from yFinance")
        return None

    # yFinance sometimes returns multi-level columns — flatten
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    # Keep only the columns we need
    needed = ["Open", "High", "Low", "Close", "Volume"]
    missing = [c for c in needed if c not in raw.columns]
    if missing:
        log.error(f"{ticker}: missing columns {missing}")
        return None

    df = raw[needed].dropna()

    if len(df) < 30:
        log.warning(f"{ticker}: only {len(df)} rows — skipping (too little history)")
        return None

    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if not DB_PATH.exists():
        log.error(f"Database not found at {DB_PATH}. Run setup_database.py first.")
        sys.exit(1)

    conn = get_connection()

    # Decide which tickers to process
    if len(sys.argv) > 1:
        tickers = [t.strip().upper() for t in sys.argv[1:]]
        log.info(f"Mode: manual override — tickers: {tickers}")
    else:
        tickers = get_active_tickers(conn)
        log.info(f"Mode: all active tickers — found {len(tickers)}: {tickers}")

    if not tickers:
        log.warning("No tickers to process. Add tickers to the database first.")
        log.warning("  python3 -c \"import sys; sys.path.insert(0,'scripts'); "
                    "from database_helper import add_ticker; add_ticker('AAPL','Apple Inc')\"")
        conn.close()
        return

    # Process each ticker
    success_count = 0
    fail_count    = 0

    for ticker in tickers:
        log.info(f"── {ticker} ──────────────────────────────")

        df = fetch_ticker(ticker)
        if df is None:
            fail_count += 1
            continue

        df = calculate_heikin_ashi(df)
        rows = upsert_prices(conn, ticker, df)

        log.info(f"{ticker}: wrote {rows} rows  "
                 f"({df.index[0].date()} → {df.index[-1].date()})")
        success_count += 1

    conn.close()

    log.info("=" * 50)
    log.info(f"Done. Success: {success_count}  Failed: {fail_count}")

    if fail_count > 0:
        log.warning("Check logs above for error details on failed tickers.")


if __name__ == "__main__":
    main()
