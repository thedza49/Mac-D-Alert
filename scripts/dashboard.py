#!/usr/bin/env python3
"""
dashboard.py
Sovson Analytics - Web Dashboard

Simplified Flask dashboard to view ticker status and graphs.
"""

import sqlite3
from pathlib import Path
from datetime import date, timedelta
from flask import Flask, render_template_string, send_from_directory

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path("/home/daniel/sovson-analytics")
DB_PATH  = BASE_DIR / "data" / "sovson_analytics.db"
STATIC_DIR = Path("/home/daniel/Mac-D-Alert/scripts/static")

app = Flask(__name__)

@app.route("/static/<path:filename>")
def serve_static(filename):
    return send_from_directory(STATIC_DIR, filename)

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sovson Analytics</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            background: #0f1117;
            color: #e0e0e0;
            padding: 24px;
        }
        h1 { font-size: 22px; font-weight: 600; color: #ffffff; margin-bottom: 4px; }
        .subtitle { font-size: 13px; color: #666; margin-bottom: 28px; }
        h2 { font-size: 14px; font-weight: 600; color: #aaa; text-transform: uppercase; letter-spacing: 0.08em; margin: 28px 0 12px; }
        table { width: 100%; border-collapse: collapse; font-size: 13px; margin-bottom: 8px; }
        thead th {
            text-align: left; padding: 8px 12px; background: #1a1d27; color: #888;
            font-weight: 500; font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em;
            border-bottom: 1px solid #2a2d3a;
        }
        tbody tr { border-bottom: 1px solid #1e2130; }
        tbody td { padding: 10px 12px; vertical-align: middle; }
        .badge { display: inline-block; padding: 3px 9px; border-radius: 4px; font-size: 11px; font-weight: 600; text-transform: uppercase; }
        .badge-BUY { background: #0d3320; color: #2ecc71; }
        .badge-APPROACHING_BUY { background: #1a3520; color: #7dcea0; }
        .badge-SELL { background: #3d0d0d; color: #e74c3c; }
        .badge-NEUTRAL { background: #1e2130; color: #666; }
        .macd-pos { color: #2ecc71; }
        .macd-neg { color: #e74c3c; }
        .backtest-score { font-size: 11px; color: #888; }
        .analyst-feed { font-size: 11px; color: #aaa; margin-top: 4px; }
        .analyst-item { border-left: 2px solid #5dade2; padding-left: 6px; margin-bottom: 4px; }
        .refresh { font-size: 12px; color: #555; margin-top: 24px; }
        .refresh a { color: #5dade2; text-decoration: none; }
    </style>
</head>
<body>
<h1>Sovson Analytics</h1>
<p class="subtitle">Updated: {{ now }}</p>

<h2>Market Status & Backtesting</h2>
<table>
    <thead>
        <tr>
            <th>Ticker</th>
            <th>Price</th>
            <th>Backtesting (1w / 3w / Peak / Exit)</th>
            <th>Recent Analyst Calls</th>
            <th>Phase</th>
            <th>Graph</th>
        </tr>
    </thead>
    <tbody>
        {% if status_data %}
            {% for m in status_data %}
            <tr>
                <td><strong>{{ m.ticker }}</strong><br><span style="font-size: 10px; color: #555;">{{ m.period_end_date }}</span></td>
                <td>{{ "$%.2f"|format(m.current_price) if m.current_price else '—' }}<br>
                    <span class="{{ 'macd-pos' if m.pct_change and m.pct_change > 0 else 'macd-neg' }}" style="font-size: 11px;">
                        {{ "%+.2f%%"|format(m.pct_change) if m.pct_change else '—' }}
                    </span>
                </td>
                <td class="backtest-score">
                    1w: <span class="{{ 'macd-pos' if m.gain_1w_pct and m.gain_1w_pct > 0 else 'macd-neg' }}">{{ "%+.1f%%"|format(m.gain_1w_pct) if m.gain_1w_pct else '—' }}</span> |
                    3w: <span class="{{ 'macd-pos' if m.gain_3w_pct and m.gain_3w_pct > 0 else 'macd-neg' }}">{{ "%+.1f%%"|format(m.gain_3w_pct) if m.gain_3w_pct else '—' }}</span><br>
                    Peak: <span class="macd-pos">{{ "%+.1f%%"|format(m.peak_gain_pct) if m.peak_gain_pct else '—' }}</span> ({{ m.days_to_peak }}d) |
                    Exit: <span class="{{ 'macd-pos' if m.exit_gain_pct and m.exit_gain_pct > 0 else 'macd-neg' }}">{{ "%+.1f%%"|format(m.exit_gain_pct) if m.exit_gain_pct else '—' }}</span>
                </td>
                <td>
                    <div class="analyst-feed">
                    {% for c in m.analyst_calls %}
                        <div class="analyst-item">
                            <strong>{{ c.firm }}</strong>: {{ c.action }} ({{ "$%.0f"|format(c.target) if c.target else 'N/A' }})
                        </div>
                    {% endfor %}
                    </div>
                </td>
                <td><span class="badge badge-{{ m.current_phase or 'NEUTRAL' }}">
                    {{ (m.current_phase or 'NEUTRAL').replace('_', ' ') }}
                </span></td>
                <td><a href="/static/graph_{{ m.ticker }}.png" target="_blank" style="color: #5dade2;">View Graph</a></td>
            </tr>
            {% endfor %}
        {% else %}
            <tr><td colspan="6">No data found.</td></tr>
        {% endif %}
    </tbody>
</table>

<p class="refresh">Auto-refreshes every 5 min · <a href="/">Refresh now</a></p>
<script>setTimeout(() => location.reload(), 300000);</script>
</body>
</html>
"""

@app.route("/")
def index():
    conn = get_connection()
    # Market Status with Backtesting Scorecard
    status_data = conn.execute("""
        SELECT m.ticker, m.current_phase, m.period_end_date,
               p.close as current_price,
               p.volume as current_volume,
               ((p.close - prev.close) / prev.close) * 100 as pct_change,
               s.gain_1w_pct, s.gain_2w_pct as gain_3w_pct, s.peak_gain_pct, s.days_to_peak, s.exit_gain_pct,
               e.recent_analyst_calls_json
        FROM macd_5d_data m
        INNER JOIN (
            SELECT ticker, MAX(period_end_date) as latest
            FROM macd_5d_data
            GROUP BY ticker
        ) latest ON m.ticker = latest.ticker AND m.period_end_date = latest.latest
        INNER JOIN tickers t ON t.ticker = m.ticker AND t.active = 1
        LEFT JOIN daily_prices p ON p.ticker = m.ticker AND p.date = m.period_end_date
        LEFT JOIN daily_prices prev ON prev.ticker = m.ticker AND prev.date = (
            SELECT MAX(date) FROM daily_prices WHERE ticker = m.ticker AND date < m.period_end_date
        )
        LEFT JOIN (
            SELECT ticker, gain_1w_pct, gain_2w_pct, peak_gain_pct, days_to_peak, exit_gain_pct
            FROM signals
            WHERE signal_type = 'BUY'
            GROUP BY ticker
            HAVING MAX(signal_date)
        ) s ON s.ticker = m.ticker
        LEFT JOIN (
            SELECT ticker, recent_analyst_calls_json
            FROM earnings_data
            GROUP BY ticker
            HAVING MAX(fetched_date)
        ) e ON e.ticker = m.ticker
        ORDER BY m.ticker
    """).fetchall()
    conn.close()
    import json
    # Convert analyst json to list for template
    processed_data = []
    for r in status_data:
        d = dict(r)
        d["analyst_calls"] = json.loads(d["recent_analyst_calls_json"]) if d.get("recent_analyst_calls_json") else []
        processed_data.append(d)
        
    return render_template_string(TEMPLATE, status_data=processed_data, now=date.today().strftime("%B %d, %Y"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
