#!/usr/bin/env python3
"""
scripts/analyze_lifecycles.py
Signal Lifecycle Engine (v1.6.0)
Momo (Lead Builder)
"""

import sqlite3
import os
import sys
from datetime import datetime
from typing import List, Dict, Optional

# Add the root directory to path so we can import database_helper
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.database_helper import get_connection

def pair_signals(ticker: str) -> List[Dict]:
    """
    Fetch all BUY and SELL signals for a ticker and pair BUY signals with
    the FIRST subsequent SELL signal.
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        # Fetch all signals for this ticker, ordered by date
        cursor.execute("""
            SELECT id, signal_date, signal_type, price_at_signal
            FROM signals
            WHERE ticker = ?
            ORDER BY signal_date ASC, id ASC
        """, (ticker,))
        signals = [dict(row) for row in cursor.fetchall()]

    pairs = []
    pending_buys = []

    for signal in signals:
        if signal['signal_type'] == 'BUY':
            pending_buys.append(signal)
        elif signal['signal_type'] == 'SELL' and pending_buys:
            for buy in pending_buys:
                pairs.append({
                    'buy': buy,
                    'sell': signal
                })
            pending_buys = [] # Reset after closing all pending buys

    return pairs

def discover_apex(ticker: str, buy_date: str, sell_date: str) -> float:
    """
    Query daily_prices to find the maximum HIGH price between buy_date and sell_date (inclusive).
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT MAX(high) as apex
            FROM daily_prices
            WHERE ticker = ? AND date >= ? AND date <= ?
        """, (ticker, buy_date, sell_date))
        result = cursor.fetchone()
        return result['apex'] if result and result['apex'] is not None else 0.0

def calculate_metrics(buy_price: float, apex_price: float, sell_price: float) -> Dict:
    """
    Calculate Captured Profit, Peak Profit, and Capture Efficiency.
    """
    # 1. Captured Profit: ((Sell Price - Buy Price) / Buy Price)
    captured_profit = (sell_price - buy_price) / buy_price if buy_price > 0 else 0.0
    
    # 2. Peak Profit: ((Apex Price - Buy Price) / Buy Price)
    peak_profit = (apex_price - buy_price) / buy_price if buy_price > 0 else 0.0
    
    # 3. Capture Efficiency: (Captured Profit / Peak Profit)
    if peak_profit > 0:
        capture_efficiency = captured_profit / peak_profit
    elif peak_profit < 0:
        # If the peak was a loss (Apex < Buy), efficiency is tricky.
        # But if captured is better than peak (less loss), efficiency is... 
        # Standard calculation as per spec: (Captured / Peak)
        capture_efficiency = captured_profit / peak_profit
    else:
        capture_efficiency = 1.0 if captured_profit == 0 else 0.0
        
    return {
        'captured_profit': captured_profit,
        'peak_profit': peak_profit,
        'capture_efficiency': capture_efficiency,
        'is_win': 1 if captured_profit > 0 else 0
    }

def persist_lifecycle(ticker: str, pair: Dict, apex: float, metrics: Dict) -> None:
    """
    Save the lifecycle record to the database, avoiding duplicates.
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO trade_lifecycles (
                ticker, buy_signal_id, sell_signal_id, 
                buy_price, sell_price, apex_price,
                captured_profit, peak_profit, capture_efficiency, is_win
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(buy_signal_id, sell_signal_id) DO UPDATE SET
                apex_price = excluded.apex_price,
                captured_profit = excluded.captured_profit,
                peak_profit = excluded.peak_profit,
                capture_efficiency = excluded.capture_efficiency,
                is_win = excluded.is_win
        """, (
            ticker, pair['buy']['id'], pair['sell']['id'],
            pair['buy']['price_at_signal'], pair['sell']['price_at_signal'], apex,
            metrics['captured_profit'], metrics['peak_profit'], 
            metrics['capture_efficiency'], metrics['is_win']
        ))
        conn.commit()

def run_analysis():
    """Main loop through tickers."""
    from scripts.database_helper import get_active_tickers
    tickers = get_active_tickers()
    
    for t in tickers:
        ticker = t['ticker']
        pairs = pair_signals(ticker)
        for pair in pairs:
            apex = discover_apex(ticker, pair['buy']['signal_date'], pair['sell']['signal_date'])
            # If Apex discovery in daily_prices failed, use the max of signal prices as fallback
            if apex == 0.0:
                apex = max(pair['buy']['price_at_signal'], pair['sell']['price_at_signal'])
                
            metrics = calculate_metrics(
                pair['buy']['price_at_signal'],
                apex,
                pair['sell']['price_at_signal']
            )
            persist_lifecycle(ticker, pair, apex, metrics)

if __name__ == "__main__":
    run_analysis()
    print("Signal Lifecycle analysis complete.")
