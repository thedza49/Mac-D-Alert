#!/usr/bin/env python3
"""
calculate_macd.py
Sovson Analytics - Phase 3

Reads daily price data from SQLite, calculates 5-day rolling MACD (12, 26, 9)
plus supporting indicators (50-day MA, volume averages), and stores results
in the macd_5d_data table.

Also determines the current signal phase for each ticker:
  - APPROACHING_BUY  : MACD line rising toward signal line from below
  - BUY              : MACD line crosses above signal line
  - APPROACHING_SELL : MACD line falling toward signal line from above
  - SELL             : MACD line crosses below signal line
  - NEUTRAL          : no notable condition

Usage:
    python3 calculate_macd.py              # all active tickers
    python3 calculate_macd.py AAPL META    # specific tickers
"""

import sys
import sqlite3
import logging
from pathlib import Path
from datetime import date

import pandas as pd
import numpy as np

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
        logging.FileHandler(LOG_DIR / "calculate_macd.log"),
    ],
)
log = logging.getLogger(__name__)

# ── MACD parameters ───────────────────────────────────────────────────────────
MACD_FAST   = 12
MACD_SLOW   = 26
MACD_SIGNAL = 9

# How close (as fraction of price) MACD and signal must be to trigger APPROACHING
APPROACHING_THRESHOLD = 0.003   # 0.3% of current price


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


def load_prices(conn: sqlite3.Connection, ticker: str) -> pd.DataFrame:
    """Load all daily prices for a ticker, sorted oldest → newest."""
    rows = conn.execute(
        """
        SELECT date, close, volume
        FROM daily_prices
        WHERE ticker = ?
        ORDER BY date ASC
        """,
        (ticker,),
    ).fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])
    df["date"]   = pd.to_datetime(df["date"])
    df["close"]  = df["close"].astype(float)
    df["volume"] = df["volume"].astype(float)
    df.set_index("date", inplace=True)
    return df


def upsert_macd_row(conn: sqlite3.Connection, ticker: str, row: dict) -> None:
    conn.execute(
        """
        INSERT INTO macd_5d_data (
            ticker, calculation_date, period_start_date, period_end_date,
            macd_line, signal_line, histogram,
            volume_5d_avg, ma_50d, current_phase
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker, calculation_date) DO UPDATE SET
            period_start_date = excluded.period_start_date,
            period_end_date   = excluded.period_end_date,
            macd_line         = excluded.macd_line,
            signal_line       = excluded.signal_line,
            histogram         = excluded.histogram,
            volume_5d_avg     = excluded.volume_5d_avg,
            ma_50d            = excluded.ma_50d,
            current_phase     = excluded.current_phase
        """,
        (
            ticker,
            row["calculation_date"],
            row["period_start_date"],
            row["period_end_date"],
            row["macd_line"],
            row["signal_line"],
            row["histogram"],
            row["volume_5d_avg"],
            row["ma_50d"],
            row["current_phase"],
        ),
    )


# ── MACD calculation ──────────────────────────────────────────────────────────

def compute_macd(prices: pd.Series) -> pd.DataFrame:
    """
    Computes MACD line, signal line, and histogram for a price series.
    Uses standard EMA-based MACD (12, 26, 9).
    Returns DataFrame with columns: macd_line, signal_line, histogram.
    """
    ema_fast   = prices.ewm(span=MACD_FAST,   adjust=False).mean()
    ema_slow   = prices.ewm(span=MACD_SLOW,   adjust=False).mean()
    macd_line  = ema_fast - ema_slow
    signal     = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
    histogram  = macd_line - signal

    return pd.DataFrame({
        "macd_line":   macd_line,
        "signal_line": signal,
        "histogram":   histogram,
    })


def determine_phase(
    macd_today:   float,
    signal_today: float,
    macd_prev:    float,
    signal_prev:  float,
    current_price: float,
) -> str:
    """
    Determines the current signal phase based on MACD position and movement.

    BUY:              MACD crossed above signal today (was below yesterday)
    APPROACHING_BUY:  MACD below signal but gap is closing and within threshold
    SELL:             MACD crossed below signal today (was above yesterday)
    APPROACHING_SELL: MACD above signal but gap is closing and within threshold
    NEUTRAL:          everything else
    """
    gap_today = macd_today - signal_today
    gap_prev  = macd_prev  - signal_prev
    threshold = current_price * APPROACHING_THRESHOLD

    # Crossovers (highest priority)
    if gap_prev < 0 and gap_today >= 0:
        return "BUY"
    if gap_prev > 0 and gap_today <= 0:
        return "SELL"

    # Approaching — gap narrowing and within threshold
    if gap_today < 0 and gap_today > gap_prev and abs(gap_today) < threshold:
        return "APPROACHING_BUY"
    if gap_today > 0 and gap_today < gap_prev and abs(gap_today) < threshold:
        return "APPROACHING_SELL"

    return "NEUTRAL"


# ── Rolling 5-day window processing ──────────────────────────────────────────

def process_ticker(conn: sqlite3.Connection, ticker: str) -> int:
    """
    Computes MACD for all available price history and writes one row
    per trading day to macd_5d_data. Returns number of rows written.
    """
    df = load_prices(conn, ticker)

    if df.empty:
        log.warning(f"{ticker}: no price data found — run fetch_prices.py first")
        return 0

    if len(df) < MACD_SLOW + MACD_SIGNAL + 5:
        log.warning(f"{ticker}: not enough data ({len(df)} rows) for reliable MACD")
        return 0

    # Compute full MACD series
    macd_df = compute_macd(df["close"])

    # 50-day moving average and 5-day volume average
    df["ma_50d"]       = df["close"].rolling(50).mean()
    df["volume_5d_avg"] = df["volume"].rolling(5).mean()

    # Merge
    combined = df.join(macd_df)

    rows_written = 0
    today_str    = date.today().isoformat()

    # We need at least 2 rows to determine phase (need previous day)
    indices = combined.index.tolist()
    for i in range(1, len(indices)):
        idx      = indices[i]
        idx_prev = indices[i - 1]

        row_today = combined.loc[idx]
        row_prev  = combined.loc[idx_prev]

        # Skip rows where MACD isn't fully warmed up yet
        if pd.isna(row_today["macd_line"]) or pd.isna(row_today["signal_line"]):
            continue

        phase = determine_phase(
            macd_today    = float(row_today["macd_line"]),
            signal_today  = float(row_today["signal_line"]),
            macd_prev     = float(row_prev["macd_line"])   if not pd.isna(row_prev["macd_line"])   else 0,
            signal_prev   = float(row_prev["signal_line"]) if not pd.isna(row_prev["signal_line"]) else 0,
            current_price = float(row_today["close"]),
        )

        # period_start = 5 trading days back (or start of data)
        start_idx = max(0, i - 4)
        period_start = indices[start_idx].strftime("%Y-%m-%d")
        period_end   = idx.strftime("%Y-%m-%d")

        upsert_macd_row(conn, ticker, {
            "calculation_date": today_str,
            "period_start_date": period_start,
            "period_end_date":   period_end,
            "macd_line":         round(float(row_today["macd_line"]),   6),
            "signal_line":       round(float(row_today["signal_line"]), 6),
            "histogram":         round(float(row_today["histogram"]),   6),
            "volume_5d_avg":     round(float(row_today["volume_5d_avg"]), 2) if not pd.isna(row_today["volume_5d_avg"]) else None,
            "ma_50d":            round(float(row_today["ma_50d"]), 4)        if not pd.isna(row_today["ma_50d"])        else None,
            "current_phase":     phase,
        })
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
        log.warning("No tickers found. Run fetch_prices.py first.")
        conn.close()
        return

    success_count = 0
    fail_count    = 0

    for ticker in tickers:
        log.info(f"── {ticker} ──────────────────────────────")
        rows = process_ticker(conn, ticker)
        if rows > 0:
            log.info(f"{ticker}: wrote {rows} MACD rows")
            success_count += 1
        else:
            fail_count += 1

    conn.close()
    log.info("=" * 50)
    log.info(f"Done.  Success: {success_count}   Failed: {fail_count}")


if __name__ == "__main__":
    main()
