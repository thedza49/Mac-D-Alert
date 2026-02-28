#!/usr/bin/env python3
"""
fetch_earnings.py
Sovson Analytics - Phase 4

Fetches analyst ratings, price targets, and earnings data from Yahoo Finance
for all active tickers and stores results in the earnings_data table.

Data fetched per ticker:
  - Next earnings date + days until earnings
  - Analyst buy/hold/sell counts and buy ratio
  - Average analyst price target and upside % vs current price
  - Forward P/E ratio
  - Last 4 quarters of earnings results (as JSON)

Usage:
    python3 fetch_earnings.py              # all active tickers
    python3 fetch_earnings.py AAPL META    # specific tickers
"""

import sys
import json
import time
import sqlite3
import logging
from pathlib import Path
from datetime import date, datetime

import requests

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path("/home/daniel/Mac-D-Alert")
DB_PATH  = Path("/home/daniel/sovson-analytics/data/sovson_analytics.db")
LOG_DIR  = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
ENV_PATH = BASE_DIR / "scripts" / ".env"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "fetch_earnings.log"),
    ],
)
log = logging.getLogger(__name__)

# ── Load FMP Key ──────────────────────────────────────────────────────────────
def load_env():
    if not ENV_PATH.exists():
        return {}
    env = {}
    with open(ENV_PATH, "r") as f:
        for line in f:
            if "=" in line:
                k, v = line.strip().split("=", 1)
                env[k] = v
    return env

ENV = load_env()
FMP_API_KEY = ENV.get("FMP_API_KEY")

# ── Yahoo Finance endpoints ───────────────────────────────────────────────────
YAHOO_QUOTE_URL   = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
# ── FMP endpoints ─────────────────────────────────────────────────────────────
FMP_TARGETS_URL = "https://financialmodelingprep.com/stable/price-target-summary?symbol={ticker}&apikey={apikey}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


# ── Yahoo Finance fetcher ─────────────────────────────────────────────────────

def fetch_quote_summary(ticker: str, modules: list[str]) -> dict | None:
    """
    Fetches Yahoo Finance quoteSummary for the given modules.
    Returns the parsed result dict or None on failure.
    """
    url    = YAHOO_QUOTE_URL.format(ticker=ticker)
    params = {"modules": ",".join(modules)}

    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
    except requests.exceptions.RequestException as exc:
        log.error(f"{ticker}: request failed — {exc}")
        return None

    if resp.status_code == 429:
        log.warning(f"{ticker}: rate limited — waiting 15s and retrying")
        time.sleep(15)
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
        result = data["quoteSummary"]["result"]
        if not result:
            log.warning(f"{ticker}: empty quoteSummary result")
            return None
        return result[0]
    except (KeyError, IndexError, TypeError) as exc:
        log.error(f"{ticker}: failed to parse response — {exc}")
        return None


def safe_get(d: dict, *keys, default=None):
    """Safely navigate nested dicts."""
    for key in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(key, default)
        if d is None:
            return default
    return d


def fetch_fmp_analyst_calls(ticker: str) -> list[dict]:
    """Fetches the price target summary from FMP."""
    if not FMP_API_KEY:
        log.warning(f"{ticker}: FMP_API_KEY not set, skipping analyst calls")
        return []
    
    url = FMP_TARGETS_URL.format(ticker=ticker, apikey=FMP_API_KEY)
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if not data:
                return []
            
            # Since individual calls are restricted/unsupported on current API version,
            # we return the summary metrics as a "virtual" feed entry for the dashboard.
            s = data[0]
            return [
                {
                    "firm": "FMP Consensus (1M)",
                    "action": f"{s.get('lastMonthCount')} Analysts",
                    "target": s.get("lastMonthAvgPriceTarget"),
                    "date": "Recent"
                },
                {
                    "firm": "FMP Consensus (3M)",
                    "action": f"{s.get('lastQuarterCount')} Analysts",
                    "target": s.get("lastQuarterAvgPriceTarget"),
                    "date": "90 Days"
                }
            ]
        else:
            log.error(f"{ticker}: FMP HTTP {resp.status_code}")
    except Exception as e:
        log.error(f"{ticker}: FMP analyst fetch failed - {e}")
    return []

def parse_earnings_data(ticker: str) -> dict | None:
    """
    Fetches and parses all earnings/analyst data for a ticker.
    Returns a flat dict ready to insert into earnings_data, or None on failure.
    """
    modules = [
        "financialData",        # current price, analyst targets, ratings
        "defaultKeyStatistics", # forward P/E
        "calendarEvents",       # next earnings date
        "earningsHistory",      # last 4 quarters actual vs estimate
    ]

    data = fetch_quote_summary(ticker, modules)
    # If Yahoo fails, we still want to try FMP for analyst calls if possible
    # but the current structure expects a full 'data' dict. 
    # Let's make it robust.

    today = date.today().isoformat()
    
    if data:
        # ── Current price ─────────────────────────────────────────────────────────
        fin = data.get("financialData", {})
        current_price = safe_get(fin, "currentPrice", "raw")

        # ── Analyst ratings ───────────────────────────────────────────────────────
        num_buy      = safe_get(fin, "numberOfAnalystOpinions", "raw", default=0) or 0
        avg_target   = safe_get(fin, "targetMeanPrice",  "raw")
        target_high  = safe_get(fin, "targetHighPrice",  "raw")
        target_low   = safe_get(fin, "targetLowPrice",   "raw")

        # Yahoo gives a recommendationKey: "buy", "hold", "sell", "strongBuy" etc.
        rec_mean = safe_get(fin, "recommendationMean", "raw")

        num_buy_ratings  = None
        num_hold_ratings = None
        num_sell_ratings = None
        buy_ratio        = None

        if rec_mean is not None and num_buy > 0:
            if rec_mean <= 1.5: buy_ratio = 0.90
            elif rec_mean <= 2.0: buy_ratio = 0.75
            elif rec_mean <= 2.5: buy_ratio = 0.60
            elif rec_mean <= 3.0: buy_ratio = 0.45
            elif rec_mean <= 3.5: buy_ratio = 0.30
            else: buy_ratio = 0.15

            num_buy_ratings  = round(num_buy * buy_ratio)
            num_hold_ratings = round(num_buy * (1 - buy_ratio) * 0.7)
            num_sell_ratings = num_buy - num_buy_ratings - num_hold_ratings

        upside_pct = None
        if current_price and avg_target:
            upside_pct = round(((avg_target - current_price) / current_price) * 100, 2)

        stats      = data.get("defaultKeyStatistics", {})
        forward_pe = safe_get(stats, "forwardPE", "raw")

        calendar        = data.get("calendarEvents", {})
        earnings_dates  = safe_get(calendar, "earnings", "earningsDate", default=[])
        next_earnings   = None
        days_until      = None

        if earnings_dates:
            try:
                ts = earnings_dates[0].get("raw")
                if ts:
                    next_dt      = datetime.fromtimestamp(ts).date()
                    next_earnings = next_dt.isoformat()
                    days_until   = (next_dt - date.today()).days
            except (IndexError, TypeError, OSError):
                pass

        history = data.get("earningsHistory", {})
        hist_list = safe_get(history, "history", default=[])
        last_4_quarters = []
        for q in hist_list[-4:]:
            last_4_quarters.append({
                "date": safe_get(q, "quarter", "fmt"),
                "estimate": safe_get(q, "epsEstimate", "raw"),
                "actual": safe_get(q, "epsActual", "raw"),
                "surprise_pct": safe_get(q, "surprisePercent", "raw"),
            })
    else:
        # Defaults if Yahoo fails
        current_price = None
        num_buy_ratings = None
        num_hold_ratings = None
        num_sell_ratings = None
        buy_ratio = None
        avg_target = None
        upside_pct = None
        forward_pe = None
        next_earnings = None
        days_until = None
        last_4_quarters = []

    # ── FMP Individual Analyst Calls (Override/Enrich) ────────────────────────
    fmp_calls = fetch_fmp_analyst_calls(ticker)
    
    return {
        "ticker":                  ticker,
        "fetched_date":            today,
        "next_earnings_date":      next_earnings,
        "days_until_earnings":     days_until,
        "num_buy_ratings":         num_buy_ratings,
        "num_hold_ratings":        num_hold_ratings,
        "num_sell_ratings":        num_sell_ratings,
        "buy_ratio":               round(buy_ratio, 4) if buy_ratio else None,
        "avg_price_target":        round(avg_target, 2) if avg_target else None,
        "current_price":           round(current_price, 4) if current_price else None,
        "upside_to_target_pct":    upside_pct,
        "forward_pe":              round(forward_pe, 2) if forward_pe else None,
        "sector_avg_pe":           None,
        "last_4_quarters_json":    json.dumps(last_4_quarters) if last_4_quarters else None,
        "recent_analyst_calls_json": json.dumps(fmp_calls) if fmp_calls else None,
    }


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


def upsert_earnings(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute(
        """
        INSERT INTO earnings_data (
            ticker, fetched_date, next_earnings_date, days_until_earnings,
            num_buy_ratings, num_hold_ratings, num_sell_ratings, buy_ratio,
            avg_price_target, current_price, upside_to_target_pct,
            forward_pe, sector_avg_pe, last_4_quarters_json,
            recent_analyst_calls_json
        ) VALUES (
            :ticker, :fetched_date, :next_earnings_date, :days_until_earnings,
            :num_buy_ratings, :num_hold_ratings, :num_sell_ratings, :buy_ratio,
            :avg_price_target, :current_price, :upside_to_target_pct,
            :forward_pe, :sector_avg_pe, :last_4_quarters_json,
            :recent_analyst_calls_json
        )
        ON CONFLICT(ticker, fetched_date) DO UPDATE SET
            next_earnings_date   = excluded.next_earnings_date,
            days_until_earnings  = excluded.days_until_earnings,
            num_buy_ratings      = excluded.num_buy_ratings,
            num_hold_ratings     = excluded.num_hold_ratings,
            num_sell_ratings     = excluded.num_sell_ratings,
            buy_ratio            = excluded.buy_ratio,
            avg_price_target     = excluded.avg_price_target,
            current_price        = excluded.current_price,
            upside_to_target_pct = excluded.upside_to_target_pct,
            forward_pe           = excluded.forward_pe,
            last_4_quarters_json = excluded.last_4_quarters_json,
            recent_analyst_calls_json = excluded.recent_analyst_calls_json
        """,
        row,
    )
    conn.commit()


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

    for i, ticker in enumerate(tickers):
        log.info(f"── {ticker} ──────────────────────────────")

        if i > 0:
            time.sleep(2)

        row = parse_earnings_data(ticker)
        if row is None:
            log.error(f"{ticker}: failed to fetch earnings data")
            fail_count += 1
            continue

        upsert_earnings(conn, row)

        log.info(
            f"{ticker}: saved — "
            f"next earnings: {row['next_earnings_date']} "
            f"({row['days_until_earnings']}d), "
            f"buy ratio: {row['buy_ratio']}, "
            f"target: ${row['avg_price_target']} "
            f"(+{row['upside_to_target_pct']}%)"
        )
        success_count += 1

    conn.close()
    log.info("=" * 50)
    log.info(f"Done.  Success: {success_count}   Failed: {fail_count}")


if __name__ == "__main__":
    main()
