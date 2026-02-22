

#!/usr/bin/env python3
import sqlite3
from pathlib import Path

DB_PATH = Path('/home/daniel/sovson-analytics/data/sovson_analytics.db')


def create_database() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.executescript('''
    CREATE TABLE IF NOT EXISTS tickers (
        ticker TEXT PRIMARY KEY,
        name TEXT,
        active INTEGER DEFAULT 1,
        added_date DATE,
        notes TEXT
    );

    CREATE TABLE IF NOT EXISTS daily_prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL,
        date DATE NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume INTEGER NOT NULL,
        ha_open REAL,
        ha_high REAL,
        ha_low REAL,
        ha_close REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(ticker, date)
    );

    CREATE INDEX IF NOT EXISTS idx_daily_ticker_date ON daily_prices(ticker, date);
    CREATE INDEX IF NOT EXISTS idx_daily_ticker ON daily_prices(ticker);

    CREATE TABLE IF NOT EXISTS macd_5d_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL,
        calculation_date DATE NOT NULL,
        period_start_date DATE NOT NULL,
        period_end_date DATE NOT NULL,
        macd_line REAL,
        signal_line REAL,
        histogram REAL,
        volume_5d_avg REAL,
        ma_50d REAL,
        current_phase TEXT,
        calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(ticker, calculation_date)
    );

    CREATE INDEX IF NOT EXISTS idx_macd_ticker_date ON macd_5d_data(ticker, calculation_date);
    CREATE INDEX IF NOT EXISTS idx_macd_phase ON macd_5d_data(current_phase);

    CREATE TABLE IF NOT EXISTS earnings_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL,
        fetched_date DATE NOT NULL,
        next_earnings_date DATE,
        days_until_earnings INTEGER,
        num_buy_ratings INTEGER,
        num_hold_ratings INTEGER,
        num_sell_ratings INTEGER,
        buy_ratio REAL,
        avg_price_target REAL,
        current_price REAL,
        upside_to_target_pct REAL,
        forward_pe REAL,
        sector_avg_pe REAL,
        last_4_quarters_json TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(ticker, fetched_date)
    );

    CREATE INDEX IF NOT EXISTS idx_earnings_ticker ON earnings_data(ticker);

    CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL,
        signal_date DATE NOT NULL,
        signal_type TEXT NOT NULL,
        price_at_signal REAL NOT NULL,
        macd_line REAL NOT NULL,
        signal_line REAL NOT NULL,
        histogram REAL,
        volume_vs_avg_pct REAL,
        earnings_days_out INTEGER,
        buy_ratio REAL,
        num_analyst_upgrades_6mo INTEGER,
        num_analyst_downgrades_6mo INTEGER,
        confidence_score INTEGER,
        price_1w_later REAL,
        price_2w_later REAL,
        price_4w_later REAL,
        gain_1w_pct REAL,
        gain_2w_pct REAL,
        gain_4w_pct REAL,
        peak_price REAL,
        peak_gain_pct REAL,
        days_to_peak INTEGER,
        exit_signal_date DATE,
        exit_price REAL,
        exit_gain_pct REAL,
        days_to_exit INTEGER,
        discord_message_id TEXT,
        alert_markdown_path TEXT,
        chart_image_path TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_signals_ticker ON signals(ticker);
    CREATE INDEX IF NOT EXISTS idx_signals_date ON signals(signal_date);
    CREATE INDEX IF NOT EXISTS idx_signals_type ON signals(signal_type);
    CREATE INDEX IF NOT EXISTS idx_signals_ticker_type ON signals(ticker, signal_type);
    ''')

    conn.commit()
    conn.close()


if __name__ == '__main__':
    create_database()
    print(f'Database initialized at: {DB_PATH}')
