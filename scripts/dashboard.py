#!/usr/bin/env python3
"""
dashboard.py
Sovson Analytics - Web Dashboard (MACD Focus)

Clean, simplified dashboard to view ticker status and project releases.
"""

import sqlite3
import json
from pathlib import Path
from datetime import date, datetime
from flask import Flask, render_template, send_from_directory, jsonify, request, redirect, url_for

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "sovson_analytics.db"
STATIC_DIR = BASE_DIR / "scripts" / "static"

app = Flask(__name__, template_folder=str(BASE_DIR / "templates"))

@app.route("/static/<path:filename>")
def serve_static(filename):
    return send_from_directory(STATIC_DIR, filename)

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_signal_evolution(conn, ticker, count=3):
    """
    Fetch the last `count` completed trades for a ticker.
    """
    trades = conn.execute("""
        SELECT 
            s.signal_date, 
            tl.buy_price, 
            tl.peak_profit, 
            s.days_to_peak, 
            tl.captured_profit, 
            s.days_to_exit
        FROM trade_lifecycles tl
        JOIN signals s ON tl.buy_signal_id = s.id
        WHERE tl.ticker = ?
        ORDER BY s.signal_date DESC
        LIMIT ?
    """, (ticker, count)).fetchall()
    
    return [dict(t) for t in trades]

def get_ansi_lifecycle_text(conn, ticker, count=3):
    """
    Generate a formatted string with ANSI colors for Discord.
    """
    trades = get_signal_evolution(conn, ticker, count)

    if not trades:
        return "```ansi\n\u001b[31mNo trade history available.\u001b[0m\n```"

    lines = ["```ansi"]
    for i, t in enumerate(trades):
        # Format date: Jan 10
        try:
            dt = datetime.strptime(t["signal_date"], "%Y-%m-%d")
            date_str = dt.strftime("%b %d")
        except:
            date_str = t["signal_date"]

        lines.append(f"Trade {i+1} ({date_str}) | BUY ${t['buy_price']:.2f}")
        
        # Peak
        peak_val = t['peak_profit'] if t['peak_profit'] is not None else 0
        peak_days = t['days_to_peak'] if t['days_to_peak'] is not None else 0
        peak_color = "\u001b[32m" if peak_val >= 0 else "\u001b[31m"
        lines.append(f"🚀 Peak: {peak_color}{peak_val*100:+.1f}%\u001b[0m | {peak_days:+d}d")
        
        # Final
        final_val = t['captured_profit'] if t['captured_profit'] is not None else 0
        final_days = t['days_to_exit'] if t['days_to_exit'] is not None else 0
        final_color = "\u001b[32m" if final_val >= 0 else "\u001b[31m"
        lines.append(f"🏁 Final: {final_color}{final_val*100:+.1f}%\u001b[0m | {final_days:+d}d")
        
        if i < len(trades) - 1:
            lines.append("") # Spacer
            
    lines.append("```")
    return "\n".join(lines)

def get_ticker_lifecycle_summary(conn, ticker):
    """
    Aggregate metrics from trade_lifecycles for a specific ticker.
    """
    row = conn.execute("""
        SELECT 
            COUNT(*) as total_trades,
            AVG(is_win) * 100 as win_rate,
            AVG(capture_efficiency) * 100 as avg_efficiency,
            AVG(peak_profit) * 100 as avg_apex,
            AVG(captured_profit) * 100 as avg_final
        FROM trade_lifecycles
        WHERE ticker = ?
    """, (ticker,)).fetchone()
    
    if not row or row["total_trades"] == 0:
        return "📊 No trade history available."
    
    # Format: 📊 WR: 75% | Eff: 82% | Apex: +12.5% | Final: +8.2% (4 trades)
    summary = (
        f"📊 WR: {row['win_rate']:.0f}% | "
        f"Eff: {row['avg_efficiency']:.0f}% | "
        f"Apex: {row['avg_apex']:+.1f}% | "
        f"Final: {row['avg_final']:+.1f}% "
        f"({row['total_trades']} trades)"
    )
    return summary

@app.route("/")
def index():
    conn = get_connection()
    all_tickers = conn.execute("SELECT ticker FROM tickers WHERE active = 1 ORDER BY ticker").fetchall()
    
    status_data = conn.execute("""
        SELECT m.ticker, m.current_phase, m.period_end_date,
               (SELECT MAX(date) FROM daily_prices WHERE ticker = m.ticker) as last_data_date,
               p.close as current_price,
               ((p.close - prev.close) / prev.close) * 100 as pct_change,
               s.peak_gain_pct,
               e.recent_analyst_calls_json
        FROM macd_5d_data m
        INNER JOIN (SELECT ticker, MAX(period_end_date) as latest FROM macd_5d_data GROUP BY ticker) latest ON m.ticker = latest.ticker AND m.period_end_date = latest.latest
        INNER JOIN tickers t ON t.ticker = m.ticker AND t.active = 1
        LEFT JOIN daily_prices p ON p.ticker = m.ticker AND p.date = m.period_end_date
        LEFT JOIN daily_prices prev ON prev.ticker = m.ticker AND prev.date = (SELECT MAX(date) FROM daily_prices WHERE ticker = m.ticker AND date < m.period_end_date)
        LEFT JOIN signals s ON s.ticker = m.ticker AND s.signal_type = 'BUY' AND s.signal_date = (SELECT MAX(signal_date) FROM signals WHERE ticker = s.ticker AND signal_type = 'BUY')
        LEFT JOIN (SELECT ticker, recent_analyst_calls_json FROM earnings_data GROUP BY ticker HAVING MAX(fetched_date)) e ON e.ticker = m.ticker
        ORDER BY m.ticker
    """).fetchall()

    processed_data = []
    for r in status_data:
        d = dict(r)
        d["analyst_calls"] = json.loads(d["recent_analyst_calls_json"]) if d.get("recent_analyst_calls_json") else []
        d["evolution"] = get_signal_evolution(conn, d["ticker"])
        processed_data.append(d)

    conn.close()
        
    return render_template('dashboard.html', 
                                  status_data=processed_data, 
                                  all_tickers=all_tickers,
                                  now=datetime.now().strftime("%B %d, %Y %I:%M %p"))

@app.route("/api/tickers/add", methods=["POST"])
def add_ticker():
    ticker = request.form.get("ticker", "").upper().strip()
    if ticker:
        conn = get_connection()
        conn.execute("INSERT OR REPLACE INTO tickers (ticker, active, added_date) VALUES (?, 1, ?)", (ticker, date.today().isoformat()))
        conn.commit()
        conn.close()
    return redirect(url_for("index"))

@app.route("/api/tickers/remove/<ticker>", methods=["POST"])
def remove_ticker(ticker):
    conn = get_connection()
    conn.execute("UPDATE tickers SET active = 0 WHERE ticker = ?", (ticker,))
    conn.commit()
    conn.close()
    return redirect(url_for("index"))

@app.route("/api/signals/unsent")
def get_unsent_signals():
    conn = get_connection()
    signals = conn.execute("""
        SELECT s.id, s.ticker, s.signal_date, s.signal_type, s.price_at_signal, s.performance_history,
               (SELECT (SUM(CASE WHEN peak_gain_pct > 3 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) FROM signals WHERE ticker = s.ticker AND signal_type = 'BUY') as win_rate,
               e.recent_analyst_calls_json,
               (SELECT avg_price_target FROM earnings_data WHERE ticker = s.ticker ORDER BY fetched_date DESC LIMIT 1) as consensus_target
        FROM signals s
        LEFT JOIN earnings_data e ON e.ticker = s.ticker AND e.fetched_date = (SELECT MAX(fetched_date) FROM earnings_data WHERE ticker = s.ticker)
        WHERE s.discord_message_id IS NULL
        ORDER BY s.signal_date DESC
    """).fetchall()
    
    results = []
    for s in signals:
        d = dict(s)
        d["analysts"] = json.loads(d["recent_analyst_calls_json"]) if d.get("recent_analyst_calls_json") else []
        d["lifecycle_text"] = get_ansi_lifecycle_text(conn, s["ticker"])
        results.append(d)
        
    conn.close()
    return jsonify(results)

@app.route("/api/signals/mark-sent/<int:signal_id>", methods=["POST"])
def mark_signal_sent(signal_id):
    conn = get_connection()
    conn.execute("UPDATE signals SET discord_message_id = 'SENT' WHERE id = ?", (signal_id,))
    conn.commit()
    conn.close()
    return jsonify({"status": "success"})

import subprocess
import sys
import fcntl

LOCK_FILE = BASE_DIR / ".run_daily.lock"

def is_task_running():
    try:
        f = open(LOCK_FILE, "r")
        try:
            fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.lockf(f, fcntl.LOCK_UN)
            return False
        except IOError:
            return True
        finally:
            f.close()
    except FileNotFoundError:
        return False

@app.route("/api/run/daily", methods=["POST"])
def run_daily():
    if is_task_running():
        return jsonify({"status": "error", "message": "A background task is already running."}), 429
    try:
        subprocess.Popen([sys.executable, str(BASE_DIR / "scripts" / "run_daily.py")], cwd=BASE_DIR)
        return jsonify({"status": "started", "message": "Daily pipeline triggered in background."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/run/update-releases", methods=["POST"])
def run_update_releases():
    if is_task_running():
        return jsonify({"status": "error", "message": "A background task is already running."}), 429
    try:
        subprocess.Popen([sys.executable, str(BASE_DIR / "scripts" / "update_releases.py")], cwd=BASE_DIR)
        return jsonify({"status": "started", "message": "Release update triggered in background."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
