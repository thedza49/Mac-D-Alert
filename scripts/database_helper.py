#!/usr/bin/env python3
# database_helper.py
# Small SQLite helper functions for Sovson Analytics.

import sqlite3
from pathlib import Path
from typing import List, Dict, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "sovson_analytics.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_active_tickers() -> List[Dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT ticker, name, active, added_date, notes
            FROM tickers
            WHERE active = 1
            ORDER BY ticker ASC
            """
        ).fetchall()
    return [dict(r) for r in rows]


def add_ticker(ticker: str, name: Optional[str] = None, notes: Optional[str] = None) -> None:
    ticker = ticker.strip().upper()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO tickers (ticker, name, active, added_date, notes)
            VALUES (?, ?, 1, date('now'), ?)
            ON CONFLICT(ticker) DO UPDATE SET
                name = excluded.name,
                active = 1,
                notes = excluded.notes
            """,
            (ticker, name, notes),
        )
        conn.commit()


def disable_ticker(ticker: str) -> None:
    ticker = ticker.strip().upper()
    with get_connection() as conn:
        conn.execute("UPDATE tickers SET active = 0 WHERE ticker = ?", (ticker,))
        conn.commit()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Manage stock tickers for Sovson Analytics.")
    parser.add_index = parser.add_subparsers(dest="command", help="Command to run")

    # List
    parser_list = parser.add_index.add_parser("list", help="List all active tickers")

    # Add
    parser_add = parser.add_index.add_parser("add", help="Add a new ticker")
    parser_add.add_argument("ticker", help="Stock ticker (e.g. TSLA)")
    parser_add.add_argument("--name", help="Company name")
    parser_add.add_argument("--notes", help="Notes about this ticker")

    # Disable
    parser_disable = parser.add_index.add_parser("disable", help="Disable a ticker")
    parser_disable.add_argument("ticker", help="Stock ticker to disable")

    args = parser.parse_args()

    if args.command == "list" or not args.command:
        tickers = get_active_tickers()
        print(f"\n--- Active Tickers ({len(tickers)}) ---")
        for t in tickers:
            print(f"{t['ticker']}: {t['name'] or 'N/A'}")
        print("")
    elif args.command == "add":
        add_ticker(args.ticker, args.name, args.notes)
        print(f"Ticker {args.ticker.upper()} added/enabled.")
    elif args.command == "disable":
        disable_ticker(args.ticker)
        print(f"Ticker {args.ticker.upper()} disabled.")
