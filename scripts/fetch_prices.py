#!/usr/bin/env python3
"""
fetch_prices.py
Sovson Analytics - Phase 2

Fetches 3 years of daily OHLC price data directly from Yahoo Finance's
chart API (no yfinance library needed, no fc.yahoo.com auth required).
Calculates Heikin Ashi candle values and stores everything in SQLite.

Usage:
    python3 fetch_prices.py              # fetch all active tickers
    python3 fetch_prices.py AAPL META    # fetch specific tickers only
"""

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
        logging.FileHandler(LOG_DIR / "fetch_prices.log"),
    ],
)
log = logging.getLogger(__name__)

# ── Yahoo Finance direct API ──────────────────────────────────────────────────
YAHOO_URL = "https://query2.finance.yahoo.com/v8/finance/chart/{ticker}"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def fetch_yahoo(ticker: str) -> pd.DataFrame | None:
    """
    Fetches ~3 years + 60 days of daily OHLC directly from Yahoo Finance
    chart API. Returns a DataFrame with Open/High/Low/Close/Volume columns,
    or None on failure.
    """
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
    except requests.exceptions.RequestException as exc:
        log.error(f"{ticker}: request failed — {exc}")
        return None

    if resp.status_code == 429:
        log.warning(f"{ticker}: rate limited (429) — waiting 10s and retrying")
        time.sleep(10)
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
        except requests.exceptions.RequestException as exc:
            log.error(f"{ticker}: retry failed — {exc}")
            return None

    if resp.status_code != 200:
        log.error(f"{ticker}: HTTP {resp.status_code}")
        return None

    try:
        data   = resp.json()
        result = data["chart"]["result"]
        if not result:
            log.warning(f"{ticker}: empty result from Yahoo")
            return None

        chart      = result[0]
        timestamps = chart["timestamp"]
        quotes     = chart["indicators"]["quote"][0]

        df = pd.DataFrame({
            "Open":   quotes["open"],
            "High":   quotes["high"],
            "Low":    quotes["low"],
            "Close":  quotes["close"],
            "Volume": quotes["volume"],
        }, index=pd.to_datetime(timestamps, unit="s", utc=True)
                           .tz_convert("America/Los_Angeles")
                           .normalize())

        df.index.name = "Date"
        df = df.dropna()

        if len(df) < 30:
            log.warning(f"{ticker}: only {len(df)} rows — skipping")
            return None

        return df

    except (KeyError, IndexError, TypeError) as exc:
        log.error(f"{ticker}: failed to parse response — {exc}")
        return None


# ── Heikin Ashi ───────────────────────────────────────────────────────────────

def calculate_heikin_ashi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds ha_open, ha_high, ha_low, ha_close columns.

    HA Close = (Open + High + Low + Close) / 4
    HA Open  = (prev_HA_Open + prev_HA_Close) / 2  [seed: (Open+Close)/2]
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


# ── Database ──────────────────────────────────────────────────────────────────

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
        cur.execute(
            """
            INSERT INTO daily_prices
                (ticker, date, open, high, low, close, volume,
                 ha_open, ha_high, ha_low, ha_close)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker, date) DO UPDATE SET
                open     = excluded.open,
                high     = excluded.high,
                low      = excluded.low,
                close    = excluded.close,
                volume   = excluded.volume,
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
                int(row["Volume"]) if row["Volume"] else 0,
                float(row["ha_open"]),
                float(row["ha_high"]),
                float(row["ha_low"]),
                float(row["ha_close"]),
            ),
        )
        rows_written += 1
    conn.commit()
    return rows_written


# ── Main ──────────────────────────────────────────────────────────────────────

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
        log.warning("No tickers found. Add some first with database_helper.add_ticker()")
        conn.close()
        return

    success_count = 0
    fail_count    = 0

    for i, ticker in enumerate(tickers):
        log.info(f"── {ticker} ──────────────────────────────")

        # polite delay between requests to avoid rate limiting
        if i > 0:
            time.sleep(2)

        df = fetch_yahoo(ticker)
        if df is None:
            fail_count += 1
            continue

        df    = calculate_heikin_ashi(df)
        rows  = upsert_prices(conn, ticker, df)
        start = df.index[0].strftime("%Y-%m-%d")
        end   = df.index[-1].strftime("%Y-%m-%d")

        log.info(f"{ticker}: wrote {rows} rows  ({start} -> {end})")
        success_count += 1

    conn.close()
    log.info("=" * 50)
    log.info(f"Done.  Success: {success_count}   Failed: {fail_count}")


if __name__ == "__main__":
    main()
