#!/usr/bin/env python3
# database_helper.py
# Small SQLite helper functions for Sovson Analytics.

import sqlite3
from pathlib import Path
from typing import List, Dict, Optional

DB_PATH = Path("/home/daniel/sovson-analytics/data/sovson_analytics.db")


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
    print("Active tickers:", get_active_tickers())
