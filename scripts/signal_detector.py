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
from pathlib import Path
from datetime import date, timedelta

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
        logging.FileHandler(LOG_DIR / "signal_detector.log"),
    ],
)
log = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
APPROACHING_DAYS_THRESHOLD = 3    # crossover must be within this many days
CONVERGENCE_LOOKBACK       = 3    # days to average for closing speed
DUPLICATE_LOOKBACK_DAYS    = 3    # suppress same phase within this window

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


def get_recent_macd(conn: sqlite3.Connection, ticker: str, days: int = 5) -> list[dict]:
    """
    Returns the most recent N rows from macd_5d_data for a ticker,
    ordered oldest to newest.
    """
    rows = conn.execute(
        """
        SELECT period_end_date, macd_line, signal_line, histogram,
               volume_5d_avg, ma_50d
        FROM macd_5d_data
        WHERE ticker = ?
        ORDER BY period_end_date DESC
        LIMIT ?
        """,
        (ticker, days),
    ).fetchall()
    return [dict(r) for r in reversed(rows)]   # oldest first


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


def get_latest_price(conn: sqlite3.Connection, ticker: str) -> dict | None:
    row = conn.execute(
        """
        SELECT close, volume, ma_50d, volume_5d_avg
        FROM daily_prices dp
        LEFT JOIN (
            SELECT ticker, AVG(volume) as vol_20d_avg
            FROM (
                SELECT ticker, volume FROM daily_prices
                WHERE ticker = ?
                ORDER BY date DESC LIMIT 20
            )
        ) va ON va.ticker = dp.ticker
        WHERE dp.ticker = ?
        ORDER BY dp.date DESC
        LIMIT 1
        """,
        (ticker, ticker),
    ).fetchone()
    return dict(row) if row else None


def already_signaled(conn: sqlite3.Connection, ticker: str, phase: str) -> bool:
    """Returns True if this phase was already recorded within the lookback window."""
    cutoff = (date.today() - timedelta(days=DUPLICATE_LOOKBACK_DAYS)).isoformat()
    row = conn.execute(
        """
        SELECT id FROM signals
        WHERE ticker = ? AND signal_type = ? AND signal_date >= ?
        LIMIT 1
        """,
        (ticker, phase, cutoff),
    ).fetchone()
    return row is not None


def insert_signal(conn: sqlite3.Connection, signal: dict) -> None:
    conn.execute(
        """
        INSERT INTO signals (
            ticker, signal_date, signal_type, price_at_signal,
            macd_line, signal_line, histogram,
            volume_vs_avg_pct, earnings_days_out, buy_ratio,
            confidence_score
        ) VALUES (
            :ticker, :signal_date, :signal_type, :price_at_signal,
            :macd_line, :signal_line, :histogram,
            :volume_vs_avg_pct, :earnings_days_out, :buy_ratio,
            :confidence_score
        )
        """,
        signal,
    )
    conn.commit()


# ── Signal detection logic ────────────────────────────────────────────────────

def detect_phase(macd_rows: list[dict]) -> str:
    """
    Determines the current signal phase using rate-of-convergence logic.

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

    # ── Crossover detection (highest priority) ────────────────────────────────
    if gap_prev < 0 and gap_today >= 0:
        return "BUY"
    if gap_prev > 0 and gap_today <= 0:
        return "SELL"

    # ── Rate-of-convergence for APPROACHING ───────────────────────────────────
    # Build list of historical gaps (oldest to newest, up to lookback)
    gaps = []
    for row in macd_rows:
        gaps.append(row["macd_line"] - row["signal_line"])

    # Daily closing speeds over the available lookback window
    # closing speed = how much the absolute gap shrank each day
    # positive closing speed = gap shrinking (converging)
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

    if days_to_cross <= APPROACHING_DAYS_THRESHOLD:
        if gap_today < 0:
            return "APPROACHING_BUY"    # MACD below signal, closing fast
        if gap_today > 0:
            return "APPROACHING_SELL"   # MACD above signal, closing fast

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

        if close and ma_50d and close > ma_50d:
            score += SCORE_ABOVE_50MA
        if volume and volume_5d_avg and volume_5d_avg > 0:
            if volume > volume_5d_avg * 1.0:   # any above-average volume
                score += SCORE_VOLUME_ABOVE_AVG

    return max(0, min(100, score))   # clamp 0-100


# ── Per-ticker processing ─────────────────────────────────────────────────────

def process_ticker(conn: sqlite3.Connection, ticker: str) -> str | None:
    """
    Runs signal detection for a single ticker.
    Returns the signal phase if one was recorded, else None.
    """
    macd_rows = get_recent_macd(conn, ticker, days=CONVERGENCE_LOOKBACK + 2)

    if len(macd_rows) < 2:
        log.warning(f"{ticker}: not enough MACD rows — run calculate_macd.py first")
        return None

    phase = detect_phase(macd_rows)

    if phase == "NEUTRAL":
        log.info(f"{ticker}: NEUTRAL — no signal")
        return None

    # Duplicate suppression
    if already_signaled(conn, ticker, phase):
        log.info(f"{ticker}: {phase} already recorded within {DUPLICATE_LOOKBACK_DAYS} days — skipping")
        return None

    # Pull supporting data for scoring
    earnings   = get_latest_earnings(conn, ticker)
    price_data = get_latest_price(conn, ticker)
    confidence = score_signal(phase, earnings, price_data)

    latest_macd = macd_rows[-1]

    # Volume vs average
    vol_vs_avg = None
    if price_data:
        v    = price_data.get("volume")
        vavg = price_data.get("volume_5d_avg")
        if v and vavg and vavg > 0:
            vol_vs_avg = round(((v - vavg) / vavg) * 100, 2)

    signal = {
        "ticker":           ticker,
        "signal_date":      date.today().isoformat(),
        "signal_type":      phase,
        "price_at_signal":  price_data["close"] if price_data else 0,
        "macd_line":        latest_macd["macd_line"],
        "signal_line":      latest_macd["signal_line"],
        "histogram":        latest_macd["histogram"],
        "volume_vs_avg_pct": vol_vs_avg,
        "earnings_days_out": earnings["days_until_earnings"] if earnings else None,
        "buy_ratio":         earnings["buy_ratio"] if earnings else None,
        "confidence_score":  confidence,
    }

    insert_signal(conn, signal)

    log.info(
        f"{ticker}: {phase} signal recorded — "
        f"confidence: {confidence}/100, "
        f"earnings in: {signal['earnings_days_out']}d"
    )
    return phase


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
        log.warning("No tickers found.")
        conn.close()
        return

    signals_fired = []

    for ticker in tickers:
        log.info(f"── {ticker} ──────────────────────────────")
        phase = process_ticker(conn, ticker)
        if phase:
            signals_fired.append((ticker, phase))

    conn.close()
    log.info("=" * 50)
    log.info(f"Done.  Signals fired: {len(signals_fired)}")
    for ticker, phase in signals_fired:
        log.info(f"  {ticker}: {phase}")


if __name__ == "__main__":
    main()
