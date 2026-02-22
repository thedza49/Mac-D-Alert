#!/usr/bin/env python3
"""
dashboard.py
Sovson Analytics - Web Dashboard

Simple Flask dashboard to view signals, MACD status, and earnings data.
Access on your home network at: http://raspberrypi.local:5000

Usage:
    python3 dashboard.py

Install Flask first if needed:
    pip3 install flask --break-system-packages
"""

import sqlite3
import json
from pathlib import Path
from datetime import date, timedelta
from flask import Flask, render_template_string

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path("/home/daniel/sovson-analytics")
DB_PATH  = BASE_DIR / "data" / "sovson_analytics.db"

app = Flask(__name__)

# ── Database ──────────────────────────────────────────────────────────────────

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── HTML Template ─────────────────────────────────────────────────────────────

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

        h1 {
            font-size: 22px;
            font-weight: 600;
            color: #ffffff;
            margin-bottom: 4px;
        }

        .subtitle {
            font-size: 13px;
            color: #666;
            margin-bottom: 28px;
        }

        h2 {
            font-size: 14px;
            font-weight: 600;
            color: #aaa;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin: 28px 0 12px;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
            margin-bottom: 8px;
        }

        thead th {
            text-align: left;
            padding: 8px 12px;
            background: #1a1d27;
            color: #888;
            font-weight: 500;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            border-bottom: 1px solid #2a2d3a;
        }

        tbody tr {
            border-bottom: 1px solid #1e2130;
        }

        tbody tr:hover {
            background: #161925;
        }

        tbody td {
            padding: 10px 12px;
            vertical-align: middle;
        }

        .badge {
            display: inline-block;
            padding: 3px 9px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .badge-BUY              { background: #0d3320; color: #2ecc71; }
        .badge-APPROACHING_BUY  { background: #1a3520; color: #7dcea0; }
        .badge-SELL             { background: #3d0d0d; color: #e74c3c; }
        .badge-APPROACHING_SELL { background: #3a1a1a; color: #cd6155; }
        .badge-NEUTRAL          { background: #1e2130; color: #666; }

        .confidence {
            display: inline-block;
            width: 36px;
            text-align: center;
            font-weight: 600;
        }
        .conf-high   { color: #2ecc71; }
        .conf-mid    { color: #f39c12; }
        .conf-low    { color: #e74c3c; }

        .macd-pos { color: #2ecc71; }
        .macd-neg { color: #e74c3c; }

        .earnings-warn { color: #f39c12; font-weight: 600; }

        .no-data {
            color: #444;
            font-style: italic;
            padding: 20px 12px;
        }

        .refresh {
            font-size: 12px;
            color: #555;
            margin-top: 24px;
        }

        .refresh a {
            color: #5dade2;
            text-decoration: none;
        }
    </style>
</head>
<body>

<h1>Sovson Analytics</h1>
<p class="subtitle">Updated: {{ now }}  ·  DB: {{ db_path }}</p>

<!-- ── RECENT SIGNALS ───────────────────────────────────────────────── -->
<h2>Recent Signals (last 14 days)</h2>
<table>
    <thead>
        <tr>
            <th>Date</th>
            <th>Ticker</th>
            <th>Signal</th>
            <th>Confidence</th>
            <th>Price</th>
            <th>MACD</th>
            <th>Signal Line</th>
            <th>Histogram</th>
            <th>Buy Ratio</th>
            <th>Earnings In</th>
            <th>Vol vs Avg</th>
        </tr>
    </thead>
    <tbody>
        {% if signals %}
            {% for s in signals %}
            <tr>
                <td>{{ s.signal_date }}</td>
                <td><strong>{{ s.ticker }}</strong></td>
                <td><span class="badge badge-{{ s.signal_type }}">{{ s.signal_type.replace('_', ' ') }}</span></td>
                <td>
                    <span class="confidence
                        {% if s.confidence_score >= 70 %}conf-high
                        {% elif s.confidence_score >= 50 %}conf-mid
                        {% else %}conf-low{% endif %}">
                        {{ s.confidence_score }}
                    </span>
                </td>
                <td>${{ "%.2f"|format(s.price_at_signal) }}</td>
                <td class="{{ 'macd-pos' if s.macd_line > 0 else 'macd-neg' }}">{{ "%.4f"|format(s.macd_line) }}</td>
                <td>{{ "%.4f"|format(s.signal_line) }}</td>
                <td class="{{ 'macd-pos' if s.histogram > 0 else 'macd-neg' }}">{{ "%.4f"|format(s.histogram) }}</td>
                <td>{{ "%.0f%%"|format(s.buy_ratio * 100) if s.buy_ratio else '—' }}</td>
                <td class="{{ 'earnings-warn' if s.earnings_days_out and s.earnings_days_out <= 14 else '' }}">
                    {{ s.earnings_days_out ~ 'd' if s.earnings_days_out else '—' }}
                </td>
                <td>{{ "+%.1f%%"|format(s.volume_vs_avg_pct) if s.volume_vs_avg_pct else '—' }}</td>
            </tr>
            {% endfor %}
        {% else %}
            <tr><td colspan="11" class="no-data">No signals in the last 14 days. Run signal_detector.py to check.</td></tr>
        {% endif %}
    </tbody>
</table>

<!-- ── MACD STATUS ───────────────────────────────────────────────────── -->
<h2>Current MACD Status (all active tickers)</h2>
<table>
    <thead>
        <tr>
            <th>Ticker</th>
            <th>Phase</th>
            <th>MACD Line</th>
            <th>Signal Line</th>
            <th>Histogram</th>
            <th>Gap</th>
            <th>50d MA</th>
            <th>Vol 5d Avg</th>
            <th>As Of</th>
        </tr>
    </thead>
    <tbody>
        {% if macd_status %}
            {% for m in macd_status %}
            <tr>
                <td><strong>{{ m.ticker }}</strong></td>
                <td><span class="badge badge-{{ m.current_phase or 'NEUTRAL' }}">
                    {{ (m.current_phase or 'NEUTRAL').replace('_', ' ') }}
                </span></td>
                <td class="{{ 'macd-pos' if m.macd_line and m.macd_line > 0 else 'macd-neg' }}">
                    {{ "%.4f"|format(m.macd_line) if m.macd_line else '—' }}
                </td>
                <td>{{ "%.4f"|format(m.signal_line) if m.signal_line else '—' }}</td>
                <td class="{{ 'macd-pos' if m.histogram and m.histogram > 0 else 'macd-neg' }}">
                    {{ "%.4f"|format(m.histogram) if m.histogram else '—' }}
                </td>
                <td>{{ "%.4f"|format(m.macd_line - m.signal_line) if m.macd_line and m.signal_line else '—' }}</td>
                <td>{{ "%.2f"|format(m.ma_50d) if m.ma_50d else '—' }}</td>
                <td>{{ "{:,.0f}".format(m.volume_5d_avg) if m.volume_5d_avg else '—' }}</td>
                <td>{{ m.period_end_date or '—' }}</td>
            </tr>
            {% endfor %}
        {% else %}
            <tr><td colspan="9" class="no-data">No MACD data found. Run calculate_macd.py first.</td></tr>
        {% endif %}
    </tbody>
</table>

<!-- ── EARNINGS / ANALYST ────────────────────────────────────────────── -->
<h2>Analyst & Earnings Data (latest per ticker)</h2>
<table>
    <thead>
        <tr>
            <th>Ticker</th>
            <th>Current Price</th>
            <th>Avg Target</th>
            <th>Upside</th>
            <th>Buy Ratio</th>
            <th>Forward P/E</th>
            <th>Next Earnings</th>
            <th>Days Out</th>
            <th>Last Updated</th>
        </tr>
    </thead>
    <tbody>
        {% if earnings %}
            {% for e in earnings %}
            <tr>
                <td><strong>{{ e.ticker }}</strong></td>
                <td>{{ "$%.2f"|format(e.current_price) if e.current_price else '—' }}</td>
                <td>{{ "$%.2f"|format(e.avg_price_target) if e.avg_price_target else '—' }}</td>
                <td class="{{ 'macd-pos' if e.upside_to_target_pct and e.upside_to_target_pct > 0 else 'macd-neg' }}">
                    {{ "+%.1f%%"|format(e.upside_to_target_pct) if e.upside_to_target_pct else '—' }}
                </td>
                <td>{{ "%.0f%%"|format(e.buy_ratio * 100) if e.buy_ratio else '—' }}</td>
                <td>{{ "%.1f"|format(e.forward_pe) if e.forward_pe else '—' }}</td>
                <td class="{{ 'earnings-warn' if e.days_until_earnings and e.days_until_earnings <= 14 else '' }}">
                    {{ e.next_earnings_date or '—' }}
                </td>
                <td class="{{ 'earnings-warn' if e.days_until_earnings and e.days_until_earnings <= 14 else '' }}">
                    {{ e.days_until_earnings ~ 'd' if e.days_until_earnings else '—' }}
                </td>
                <td>{{ e.fetched_date }}</td>
            </tr>
            {% endfor %}
        {% else %}
            <tr><td colspan="9" class="no-data">No earnings data found. Run fetch_earnings.py first.</td></tr>
        {% endif %}
    </tbody>
</table>

<p class="refresh">Auto-refreshes every 5 minutes · <a href="/">Refresh now</a></p>

<script>setTimeout(() => location.reload(), 300000);</script>
</body>
</html>
"""


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    conn = get_connection()

    cutoff = (date.today() - timedelta(days=14)).isoformat()

    # Recent signals
    signals = conn.execute(
        """
        SELECT signal_date, ticker, signal_type, confidence_score,
               price_at_signal, macd_line, signal_line, histogram,
               buy_ratio, earnings_days_out, volume_vs_avg_pct
        FROM signals
        WHERE signal_date >= ?
        ORDER BY signal_date DESC, confidence_score DESC
        """,
        (cutoff,),
    ).fetchall()

    # Latest MACD per ticker
    macd_status = conn.execute(
        """
        SELECT m.ticker, m.current_phase, m.macd_line, m.signal_line,
               m.histogram, m.volume_5d_avg, m.ma_50d, m.period_end_date
        FROM macd_5d_data m
        INNER JOIN (
            SELECT ticker, MAX(period_end_date) as latest
            FROM macd_5d_data
            GROUP BY ticker
        ) latest ON m.ticker = latest.ticker AND m.period_end_date = latest.latest
        INNER JOIN tickers t ON t.ticker = m.ticker AND t.active = 1
        ORDER BY m.ticker
        """,
    ).fetchall()

    # Latest earnings per ticker
    earnings = conn.execute(
        """
        SELECT e.ticker, e.current_price, e.avg_price_target,
               e.upside_to_target_pct, e.buy_ratio, e.forward_pe,
               e.next_earnings_date, e.days_until_earnings, e.fetched_date
        FROM earnings_data e
        INNER JOIN (
            SELECT ticker, MAX(fetched_date) as latest
            FROM earnings_data
            GROUP BY ticker
        ) latest ON e.ticker = latest.ticker AND e.fetched_date = latest.latest
        INNER JOIN tickers t ON t.ticker = e.ticker AND t.active = 1
        ORDER BY e.ticker
        """,
    ).fetchall()

    conn.close()

    return render_template_string(
        TEMPLATE,
        signals=signals,
        macd_status=macd_status,
        earnings=earnings,
        now=date.today().strftime("%B %d, %Y"),
        db_path=str(DB_PATH),
    )


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("Sovson Analytics Dashboard")
    print("Open in browser: http://raspberrypi.local:5000")
    print("Or use your Pi's IP: http://<pi-ip>:5000")
    print("Ctrl+C to stop")
    print("=" * 50)
    app.run(host="0.0.0.0", port=5000, debug=False)
