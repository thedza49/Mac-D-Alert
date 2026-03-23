#!/usr/bin/env python3
"""
signal_detector.py
Sovson Analytics - Phase 5

Reads MACD and earnings data from SQLite, detects 4-phase signals,
scores confidence, and writes qualifying signals to the signals table.

Signal phases:
  BUY              - MACD crossed above signal line today
  APPROACHING_BUY  - at current convergence speed, crossover within 3 days
  SELL             - MACD crossed below signal line today
  APPROACHING_SELL - at current convergence speed, crossover within 3 days

Approaching logic uses 3-day average closing speed to filter noise.
A shrinking gap alone does NOT trigger approaching — the crossover must
be genuinely imminent at the current rate of convergence.

Duplicate prevention: will not write the same phase for the same ticker
if that phase was already recorded within the last 3 days.

Usage:
    python3 signal_detector.py              # all active tickers
    python3 signal_detector.py AAPL META    # specific tickers
"""

import sys
import sqlite3
import logging
import argparse
import subprocess
from pathlib import Path
from datetime import date, timedelta, datetime

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "sovson_analytics.db"
LOG_DIR  = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
SCRIPTS_DIR = BASE_DIR / "scripts"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "signal_detector.log"),
    ],
)
log = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
APPROACHING_DAYS_THRESHOLD = 3    # crossover must be within this many days
APPROACHING_PRICE_GAP      = 0.005  # gap must be < 0.5% of price
CONVERGENCE_LOOKBACK       = 3    # days to average for closing speed
DUPLICATE_LOOKBACK_DAYS    = 3    # suppress same phase within this window
BACKTEST_YEARS             = 3    # history scan depth

# ── Confidence scoring weights ────────────────────────────────────────────────
BASE_SCORE = {
    "BUY":              50,
    "APPROACHING_BUY":  30,
    "SELL":             50,
    "APPROACHING_SELL": 30,
}
SCORE_BUY_RATIO_STRONG    = 15   # buy_ratio > 0.70
SCORE_UPSIDE_STRONG       = 10   # upside_to_target_pct > 15%
SCORE_VOLUME_ABOVE_AVG    = 10   # volume > 20-day average
SCORE_EARNINGS_SAFE       = 10   # earnings > 14 days away
SCORE_ABOVE_50MA          = 5    # price above 50-day MA
SCORE_HEIKIN_CONFIRM      = 15   # Heikin Ashi candle is strong
PENALTY_EARNINGS_IMMINENT = -15  # earnings within 7 days


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


def get_recent_macd(conn: sqlite3.Connection, ticker: str, days: int = 5, end_date: str = None) -> list[dict]:
    """
    Returns rows from macd_5d_data for a ticker leading up to end_date.
    """
    if not end_date:
        end_date = date.today().isoformat()
    
    rows = conn.execute(
        """
        SELECT period_end_date, macd_line, signal_line, histogram,
               volume_5d_avg, ma_50d, current_phase
        FROM macd_5d_data
        WHERE ticker = ? AND period_end_date <= ?
        ORDER BY period_end_date DESC
        LIMIT ?
        """,
        (ticker, end_date, days),
    ).fetchall()
    return [dict(r) for r in reversed(rows)]


def get_latest_price(conn: sqlite3.Connection, ticker: str, date_str: str = None) -> dict | None:
    if not date_str:
        date_str = date.today().isoformat()
    
    row = conn.execute(
        """
        SELECT close, volume, 
               ha_open, ha_high, ha_low, ha_close
        FROM daily_prices
        WHERE ticker = ? AND date <= ?
        ORDER BY date DESC
        LIMIT 1
        """,
        (ticker, date_str),
    ).fetchone()
    return dict(row) if row else None


def already_signaled(conn: sqlite3.Connection, ticker: str, phase: str, signal_date: str = None) -> bool:
    """Returns True if this phase was already recorded within the lookback window."""
    if not signal_date:
        signal_date = date.today().isoformat()
    
    dt = datetime.strptime(signal_date, "%Y-%m-%d").date()
    cutoff = (dt - timedelta(days=DUPLICATE_LOOKBACK_DAYS)).isoformat()
    
    row = conn.execute(
        """
        SELECT id FROM signals
        WHERE ticker = ? AND signal_type = ? AND signal_date >= ? AND signal_date <= ?
        LIMIT 1
        """,
        (ticker, phase, cutoff, signal_date),
    ).fetchone()
    return row is not None


def insert_signal(conn: sqlite3.Connection, signal: dict) -> None:
    conn.execute(
        """
        INSERT INTO signals (
            ticker, signal_date, signal_type, price_at_signal,
            macd_line, signal_line, histogram,
            volume_vs_avg_pct, earnings_days_out, buy_ratio,
            confidence_score, performance_history
        ) VALUES (
            :ticker, :signal_date, :signal_type, :price_at_signal,
            :macd_line, :signal_line, :histogram,
            :volume_vs_avg_pct, :earnings_days_out, :buy_ratio,
            :confidence_score, :performance_history
        )
        ON CONFLICT(ticker, signal_date, signal_type) DO UPDATE SET
            performance_history = excluded.performance_history
        """,
        signal,
    )
    conn.commit()


def get_latest_earnings(conn: sqlite3.Connection, ticker: str) -> dict | None:
    row = conn.execute(
        """
        SELECT days_until_earnings, buy_ratio, upside_to_target_pct,
               current_price, avg_price_target
        FROM earnings_data
        WHERE ticker = ?
        ORDER BY fetched_date DESC
        LIMIT 1
        """,
        (ticker,),
    ).fetchone()
    return dict(row) if row else None


# ── Signal detection logic ────────────────────────────────────────────────────

def get_performance_history_string(conn: sqlite3.Connection, ticker: str, signal_type: str, signal_date: str) -> str:
    """Fetches the last 3 historical signals of the same type and formats the history string."""
    rows = conn.execute(
        """
        SELECT signal_date, gain_1w_pct, gain_2w_pct, peak_gain_pct, days_to_peak
        FROM signals
        WHERE ticker = ? AND signal_type = ? AND signal_date < ?
        ORDER BY signal_date DESC
        LIMIT 3
        """,
        (ticker, signal_type, signal_date)
    ).fetchall()
    
    if not rows:
        return "No recent signals of this type."
    
    entries = []
    for r in rows:
        gain_1w = f"+{r['gain_1w_pct']}%" if (r['gain_1w_pct'] is not None and r['gain_1w_pct'] >= 0) else f"{r['gain_1w_pct']}%" if r['gain_1w_pct'] is not None else "N/A"
        gain_3w = f"+{r['gain_2w_pct']}%" if (r['gain_2w_pct'] is not None and r['gain_2w_pct'] >= 0) else f"{r['gain_2w_pct']}%" if r['gain_2w_pct'] is not None else "N/A"
        peak = f"+{r['peak_gain_pct']}%" if (r['peak_gain_pct'] is not None and r['peak_gain_pct'] >= 0) else f"{r['peak_gain_pct']}%" if r['peak_gain_pct'] is not None else "N/A"
        days = f"({r['days_to_peak']}d)" if r['days_to_peak'] is not None else ""
        entries.append(f"• **{r['signal_date']}**: 1w: {gain_1w} | 3w: {gain_3w} | Peak: {peak} {days}")
    
    return "\n".join(entries)


def detect_phase(macd_rows: list[dict], price_data: dict | None) -> str:
    """
    Determines the current signal phase using rate-of-convergence logic.
    Optionally confirms with Heikin Ashi trend.

    Requires at least 2 rows. Uses up to last 3 rows for convergence speed.
    Returns one of: BUY, APPROACHING_BUY, SELL, APPROACHING_SELL, NEUTRAL.
    """
    if len(macd_rows) < 2:
        return "NEUTRAL"

    today = macd_rows[-1]
    prev  = macd_rows[-2]

    macd_today   = today["macd_line"]
    signal_today = today["signal_line"]
    macd_prev    = prev["macd_line"]
    signal_prev  = prev["signal_line"]

    gap_today = macd_today - signal_today   # positive = MACD above signal
    gap_prev  = macd_prev  - signal_prev

    # ── Heikin Ashi trend detection ───────────────────────────────────────────
    is_ha_bullish = False
    is_ha_bearish = False
    if price_data and "ha_open" in price_data and "ha_close" in price_data:
        ha_open  = price_data["ha_open"]
        ha_close = price_data["ha_close"]
        if ha_close > ha_open:
            is_ha_bullish = True
        elif ha_close < ha_open:
            is_ha_bearish = True

    # ── Crossover detection (highest priority) ────────────────────────────────
    if gap_prev < 0 and gap_today >= 0:
        # Require bullish Heikin Ashi confirmation for BUY
        return "BUY" if is_ha_bullish else "NEUTRAL"
    if gap_prev > 0 and gap_today <= 0:
        # Require bearish Heikin Ashi confirmation for SELL
        return "SELL" if is_ha_bearish else "NEUTRAL"

    # ── Rate-of-convergence for APPROACHING ───────────────────────────────────
    # Only consider APPROACHING if the gap is closing and Heikin Ashi confirms
    if gap_today < 0 and not is_ha_bullish:
        return "NEUTRAL"
    if gap_today > 0 and not is_ha_bearish:
        return "NEUTRAL"

    # Build list of historical gaps (oldest to newest, up to lookback)
    gaps = []
    for row in macd_rows:
        gaps.append(row["macd_line"] - row["signal_line"])

    # Daily closing speeds over the available lookback window
    closing_speeds = []
    for i in range(1, len(gaps)):
        speed = abs(gaps[i - 1]) - abs(gaps[i])   # positive = converging
        closing_speeds.append(speed)

    if not closing_speeds:
        return "NEUTRAL"

    # Use average of available closing speeds (up to last 3)
    avg_closing_speed = sum(closing_speeds[-CONVERGENCE_LOOKBACK:]) / len(closing_speeds[-CONVERGENCE_LOOKBACK:])

    # Gap must be actively converging (positive average closing speed)
    if avg_closing_speed <= 0:
        return "NEUTRAL"

    current_gap_abs = abs(gap_today)
    days_to_cross   = current_gap_abs / avg_closing_speed

    # Additional threshold: Gap must be < 0.5% of price
    price = price_data["close"] if price_data else 1e9
    gap_pct = current_gap_abs / price

    if days_to_cross <= APPROACHING_DAYS_THRESHOLD and gap_pct <= APPROACHING_PRICE_GAP:
        if gap_today < 0:
            return "APPROACHING_BUY"
        if gap_today > 0:
            return "APPROACHING_SELL"

    return "NEUTRAL"


def score_signal(phase: str, earnings: dict | None, price_data: dict | None) -> int:
    """Calculates confidence score 0-100 based on supporting factors."""
    score = BASE_SCORE.get(phase, 0)

    if earnings:
        days_out = earnings.get("days_until_earnings")
        buy_ratio = earnings.get("buy_ratio")
        upside    = earnings.get("upside_to_target_pct")

        if buy_ratio and buy_ratio > 0.70:
            score += SCORE_BUY_RATIO_STRONG
        if upside and upside > 15:
            score += SCORE_UPSIDE_STRONG
        if days_out is not None:
            if days_out > 14:
                score += SCORE_EARNINGS_SAFE
            elif days_out <= 7:
                score += PENALTY_EARNINGS_IMMINENT

    if price_data:
        close         = price_data.get("close")
        ma_50d        = price_data.get("ma_50d")
        volume        = price_data.get("volume")
        volume_5d_avg = price_data.get("volume_5d_avg")

        # ma_50d and volume_5d_avg from price_data or latest_macd
        if close and ma_50d and close > ma_50d:
            score += SCORE_ABOVE_50MA
        if volume and volume_5d_avg and volume_5d_avg > 0:
            if volume > volume_5d_avg * 1.0:
                score += SCORE_VOLUME_ABOVE_AVG
        
        # Heikin Ashi strength (confirmation score)
        ha_open  = price_data.get("ha_open")
        ha_close = price_data.get("ha_close")
        ha_low   = price_data.get("ha_low")
        ha_high  = price_data.get("ha_high")
        
        if ha_open and ha_close:
            # Strong Bullish: green + no lower wick
            if phase in ["BUY", "APPROACHING_BUY"]:
                if ha_close > ha_open and ha_low == ha_open:
                    score += SCORE_HEIKIN_CONFIRM
            # Strong Bearish: red + no upper wick
            elif phase in ["SELL", "APPROACHING_SELL"]:
                if ha_close < ha_open and ha_high == ha_open:
                    score += SCORE_HEIKIN_CONFIRM

    return max(0, min(100, score))   # clamp 0-100


# ── Per-ticker processing ─────────────────────────────────────────────────────

def trigger_fmp_fetch(ticker: str):
    """Triggers the earnings fetcher which now includes FMP analyst data."""
    log.info(f"{ticker}: Triggering FMP analyst enrichment...")
    try:
        subprocess.run([sys.executable, str(SCRIPTS_DIR / "fetch_earnings.py"), ticker], check=True)
    except Exception as e:
        log.error(f"{ticker}: Failed to trigger FMP fetch: {e}")


def process_ticker(conn: sqlite3.Connection, ticker: str, signal_date: str = None) -> str | None:
    """
    Runs signal detection for a single ticker on a specific date.
    Returns the signal phase if one was recorded, else None.
    """
    if not signal_date:
        signal_date = date.today().isoformat()
        
    macd_rows = get_recent_macd(conn, ticker, days=CONVERGENCE_LOOKBACK + 2, end_date=signal_date)

    if len(macd_rows) < 2:
        return None

    # Check if the last row's date matches the signal_date we are evaluating
    if macd_rows[-1]["period_end_date"] != signal_date:
        return None

    # Fetch supporting data for detection and scoring
    price_data = get_latest_price(conn, ticker, signal_date)
    
    phase = detect_phase(macd_rows, price_data)

    if phase == "NEUTRAL":
        return None

    # Duplicate suppression
    if already_signaled(conn, ticker, phase, signal_date):
        return None

    earnings   = get_latest_earnings(conn, ticker)
    
    latest_macd = macd_rows[-1]

    # Ensure price_data has indicators from MACD rows if missing
    if price_data:
        if not price_data.get("ma_50d"):
            price_data["ma_50d"] = latest_macd.get("ma_50d")
        if not price_data.get("volume_5d_avg"):
            price_data["volume_5d_avg"] = latest_macd.get("volume_5d_avg")

    confidence = score_signal(phase, earnings, price_data)

    # Volume vs average
    vol_vs_avg = None
    if price_data:
        v    = price_data.get("volume")
        vavg = price_data.get("volume_5d_avg")
        if v and vavg and vavg > 0:
            vol_vs_avg = round(((v - vavg) / vavg) * 100, 2)

    perf_history = get_performance_history_string(conn, ticker, phase, signal_date)

    signal = {
        "ticker":           ticker,
        "signal_date":      signal_date,
        "signal_type":      phase,
        "price_at_signal":  price_data["close"] if price_data else 0,
        "macd_line":        latest_macd["macd_line"],
        "signal_line":      latest_macd["signal_line"],
        "histogram":        latest_macd["histogram"],
        "volume_vs_avg_pct": vol_vs_avg,
        "earnings_days_out": earnings["days_until_earnings"] if earnings else None,
        "buy_ratio":         earnings["buy_ratio"] if earnings else None,
        "confidence_score":  confidence,
        "performance_history": perf_history,
    }

    insert_signal(conn, signal)

    log.info(f"{ticker}: {phase} signal recorded for {signal_date} - score {confidence}")
    
    # Trigger FMP enrichment only for real-time BUY/APPROACHING_BUY
    if signal_date == date.today().isoformat() and phase in ["BUY", "APPROACHING_BUY"]:
        trigger_fmp_fetch(ticker)
        
    return phase


def calculate_backtest_returns(conn: sqlite3.Connection, ticker: str, signal_date: str, price_at_signal: float):
    """Calculates returns at 1w, 3w, peak, and exit for a signal."""
    # 1. Peak within first 5 trading days (1 week)
    row_1w = conn.execute(
        "SELECT MAX(high) as peak_high FROM (SELECT high FROM daily_prices WHERE ticker = ? AND date > ? ORDER BY date ASC LIMIT 5)",
        (ticker, signal_date)
    ).fetchone()
    
    # 2. Peak within first 15 trading days (3 weeks)
    row_3w = conn.execute(
        "SELECT MAX(high) as peak_high FROM (SELECT high FROM daily_prices WHERE ticker = ? AND date > ? ORDER BY date ASC LIMIT 15)",
        (ticker, signal_date)
    ).fetchone()
    
    # 3. Peak within first 30 trading days (Absolute Signal Peak)
    peak_row = conn.execute(
        "SELECT MAX(high) as peak, date FROM (SELECT high, date FROM daily_prices WHERE ticker = ? AND date > ? ORDER BY date ASC LIMIT 30)",
        (ticker, signal_date)
    ).fetchone()
    
    # Exit return (next SELL signal)
    exit_row = conn.execute(
        "SELECT signal_date, price_at_signal FROM signals WHERE ticker = ? AND signal_date > ? AND signal_type = 'SELL' ORDER BY signal_date ASC LIMIT 1",
        (ticker, signal_date)
    ).fetchone()

    updates = {}
    if row_1w and row_1w["peak_high"] is not None:
        p1 = row_1w["peak_high"]
        updates["price_1w_later"] = p1
        updates["gain_1w_pct"] = round(((p1 - price_at_signal) / price_at_signal) * 100, 2)
        
    if row_3w and row_3w["peak_high"] is not None:
        p3 = row_3w["peak_high"]
        updates["price_2w_later"] = p3 
        updates["gain_2w_pct"] = round(((p3 - price_at_signal) / price_at_signal) * 100, 2)

    if peak_row and peak_row["peak"] is not None:
        peak = peak_row["peak"]
        updates["peak_price"] = peak
        updates["peak_gain_pct"] = round(((peak - price_at_signal) / price_at_signal) * 100, 2)
        
        # Days to peak
        d1 = datetime.strptime(signal_date, "%Y-%m-%d")
        d2 = datetime.strptime(peak_row["date"], "%Y-%m-%d")
        updates["days_to_peak"] = (d2 - d1).days

    if exit_row:
        updates["exit_signal_date"] = exit_row["signal_date"]
        updates["exit_price"] = exit_row["price_at_signal"]
        updates["exit_gain_pct"] = round(((exit_row["price_at_signal"] - price_at_signal) / price_at_signal) * 100, 2)
        
        d1 = datetime.strptime(signal_date, "%Y-%m-%d")
        d2 = datetime.strptime(exit_row["signal_date"], "%Y-%m-%d")
        updates["days_to_exit"] = (d2 - d1).days

    if updates:
        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        params = list(updates.values()) + [ticker, signal_date]
        conn.execute(f"UPDATE signals SET {set_clause} WHERE ticker = ? AND signal_date = ?", params)
        conn.commit()

def run_backtest_only(conn: sqlite3.Connection, ticker: str):
    """Calculates backtest returns for all existing BUY signals for a ticker."""
    log.info(f"{ticker}: Recalculating all backtest returns...")
    sig_rows = conn.execute("SELECT signal_date, price_at_signal FROM signals WHERE ticker = ? AND signal_type = 'BUY'", (ticker,)).fetchall()
    for s in sig_rows:
        calculate_backtest_returns(conn, ticker, s["signal_date"], s["price_at_signal"])
    log.info(f"{ticker}: Backtest calculation complete.")

def run_history_scan(conn: sqlite3.Connection, ticker: str):
    """Scans last 3 years of backfilled data for signals."""
    start_date = (date.today() - timedelta(days=BACKTEST_YEARS * 365)).isoformat()
    log.info(f"{ticker}: Scanning history since {start_date}...")
    
    rows = conn.execute(
        "SELECT period_end_date FROM macd_5d_data WHERE ticker = ? AND period_end_date >= ? ORDER BY period_end_date ASC",
        (ticker, start_date)
    ).fetchall()
    
    found_count = 0
    for r in rows:
        d = r["period_end_date"]
        phase = process_ticker(conn, ticker, signal_date=d)
        if phase:
            found_count += 1
    
    # After finding all signals, calculate backtest returns
    log.info(f"{ticker}: Calculating backtest returns...")
    sig_rows = conn.execute("SELECT signal_date, price_at_signal FROM signals WHERE ticker = ? AND signal_type = 'BUY' AND signal_date >= ?", (ticker, start_date)).fetchall()
    for s in sig_rows:
        calculate_backtest_returns(conn, ticker, s["signal_date"], s["price_at_signal"])
        
    log.info(f"{ticker}: History scan complete. Found {found_count} signals.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("tickers", nargs="*", help="Specific tickers to process")
    parser.add_argument("--history", action="store_true", help="Scan last 3 years of backfilled data")
    parser.add_argument("--backtest-only", action="store_true", help="Recalculate returns for existing signals")
    args = parser.parse_args()

    if not DB_PATH.exists():
        log.error(f"Database not found at {DB_PATH}. Run setup_database.py first.")
        sys.exit(1)

    conn = get_connection()
    # Add unique constraint to signals table if not exists to support ON CONFLICT
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_signals_uniq ON signals(ticker, signal_date, signal_type);")
    except:
        pass

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers]
    else:
        tickers = get_active_tickers(conn)

    for ticker in tickers:
        if args.history:
            run_history_scan(conn, ticker)
        elif args.backtest_only:
            run_backtest_only(conn, ticker)
        else:
            # ROBUST DAILY RUN:
            # Instead of just today, we scan the last 7 available days of MACD data.
            # This ensures that if the Pi was off for a weekend, we still catch
            # the crossovers that happened on Friday or Monday.
            # Duplicate prevention (already_signaled) keeps the signals table clean.
            
            # 1. Auto-backfill check
            sig_count = conn.execute("SELECT COUNT(*) FROM signals WHERE ticker = ?", (ticker,)).fetchone()[0]
            if sig_count == 0:
                log.info(f"{ticker}: No signals found. Running initial backfill...")
                run_history_scan(conn, ticker)
            
            # 2. Process recent window (7 days)
            recent_dates = conn.execute(
                "SELECT period_end_date FROM macd_5d_data WHERE ticker = ? ORDER BY period_end_date DESC LIMIT 7",
                (ticker,)
            ).fetchall()
            
            for r in reversed(recent_dates):
                process_ticker(conn, ticker, signal_date=r["period_end_date"])

    conn.close()


if __name__ == "__main__":
    main()
