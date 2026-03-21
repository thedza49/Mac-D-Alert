#!/usr/bin/env python3
"""
dashboard.py
Sovson Analytics - Web Dashboard

Tabbed Flask dashboard to view ticker status, Pi health, and System Memory.
"""

import sqlite3
import json
from pathlib import Path
from datetime import date, datetime, timedelta
from flask import Flask, render_template_string, send_from_directory, jsonify, request, redirect, url_for

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "sovson_analytics.db"
STATIC_DIR = BASE_DIR / "scripts" / "static"

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
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            background: #0f1117;
            color: #e0e0e0;
            padding: 24px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        
        h1 { font-size: 22px; font-weight: 600; color: #ffffff; margin-bottom: 4px; }
        .subtitle { font-size: 13px; color: #666; margin-bottom: 28px; }
        h2 { font-size: 14px; font-weight: 600; color: #aaa; text-transform: uppercase; letter-spacing: 0.08em; margin: 28px 0 12px; }
        
        /* Tabs */
        .tabs {
            display: flex; gap: 4px; border-bottom: 1px solid #2a2d3a; margin-bottom: 24px;
        }
        .tab {
            padding: 10px 20px; cursor: pointer; border-radius: 8px 8px 0 0;
            font-size: 14px; font-weight: 500; color: #888; transition: 0.2s;
            text-decoration: none;
        }
        .tab:hover { color: #fff; background: #1a1d27; }
        .tab.active { color: #5dade2; background: #1a1d27; border-bottom: 2px solid #5dade2; }
        
        .tab-content { display: none; }
        .tab-content.active { display: block; }
        
        /* Health Grid */
        .health-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin-bottom: 32px; }
        .health-card { background: #1a1d27; padding: 16px; border-radius: 8px; border: 1px solid #2a2d3a; }
        .health-card p:first-child { font-size: 11px; color: #666; text-transform: uppercase; margin-bottom: 8px; }
        .health-card p:last-child { font-size: 20px; font-weight: 600; }
        
        /* Charts */
        .chart-container { background: #1a1d27; padding: 20px; border-radius: 8px; border: 1px solid #2a2d3a; margin-bottom: 24px; }
        
        /* Tables */
        table { width: 100%; border-collapse: collapse; font-size: 13px; margin-bottom: 24px; }
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
        
        .management-box { background: #1a1d27; padding: 20px; border-radius: 8px; margin-bottom: 32px; border: 1px solid #2a2d3a; }
        .form-group { display: flex; gap: 10px; margin-bottom: 15px; }
        input[type="text"] { background: #0f1117; border: 1px solid #3d4255; color: #fff; padding: 8px 12px; border-radius: 4px; font-size: 13px; outline: none; }
        button { background: #5dade2; color: #fff; border: none; padding: 8px 16px; border-radius: 4px; font-size: 13px; font-weight: 600; cursor: pointer; }
        .btn-remove { background: #3d0d0d; color: #e74c3c; padding: 4px 8px; font-size: 11px; margin-left: 10px; }
        .ticker-pill { display: inline-flex; align-items: center; background: #2a2d3a; padding: 4px 10px; border-radius: 100px; margin-right: 8px; margin-bottom: 8px; font-size: 12px; font-weight: 500; }
        
        /* File Viewer */
        .file-list { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 16px; margin-bottom: 32px; }
        .file-card { background: #1a1d27; padding: 16px; border-radius: 8px; border: 1px solid #2a2d3a; cursor: pointer; transition: 0.2s; }
        .file-card:hover { border-color: #5dade2; background: #212532; }
        .file-card h3 { font-size: 14px; margin-bottom: 6px; color: #fff; }
        .file-card p { font-size: 12px; color: #666; line-height: 1.4; }
        .file-type { font-size: 10px; text-transform: uppercase; color: #5dade2; margin-top: 8px; display: block; }
        
        .modal { display: none; position: fixed; z-index: 100; left: 0; top: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.8); }
        .modal-content { background: #1a1d27; margin: 5% auto; padding: 20px; border-radius: 8px; width: 80%; max-width: 900px; height: 80%; display: flex; flex-direction: column; }
        .modal-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; }
        .modal-body { flex-grow: 1; overflow-y: auto; background: #0f1117; padding: 15px; border-radius: 4px; font-family: monospace; white-space: pre-wrap; font-size: 13px; color: #ccc; }
        
        .refresh { font-size: 12px; color: #555; margin-top: 24px; }
        .refresh a { color: #5dade2; text-decoration: none; }
    </style>
</head>
<body>
<div class="container">
    <h1>Sovson Analytics</h1>
    <p class="subtitle">Updated: {{ now }}</p>

    <div class="tabs">
        <div class="tab active" onclick="showTab('market')">Market Analysis</div>
        <div class="tab" onclick="showTab('health')">Pi Health</div>
        <div class="tab" onclick="showTab('system')">System Memory</div>
        <div class="tab" onclick="showTab('usage')">Nia Usage</div>
        <div class="tab" onclick="showTab('releases')">Releases</div>
    </div>

    <!-- Market Analysis Tab -->
    <div id="market" class="tab-content active">
        <h2>Market Status & Signals</h2>
        <table>
            <thead>
                <tr>
                    <th>Ticker</th>
                    <th>Price</th>
                    <th>Signals Date</th>
                    <th>Last Data</th>
                    <th>Backtesting</th>
                    <th>Analyst Sentiment</th>
                    <th>Phase</th>
                </tr>
            </thead>
            <tbody>
                {% for m in status_data %}
                <tr>
                    <td><strong>{{ m.ticker }}</strong></td>
                    <td>{{ "$%.2f"|format(m.current_price) if m.current_price else '—' }}<br>
                        <span class="{{ 'macd-pos' if m.pct_change and m.pct_change > 0 else 'macd-neg' }}" style="font-size: 11px;">
                            {{ "%+.2f%%"|format(m.pct_change) if m.pct_change else '—' }}
                        </span>
                    </td>
                    <td style="font-size: 11px; color: #888;">{{ m.period_end_date }}</td>
                    <td style="font-size: 11px; font-weight: 600; color: {{ '#2ecc71' if m.last_data_date == m.period_end_date else '#e67e22' }};">{{ m.last_data_date }}</td>
                    <td style="font-size: 11px; color: #888;">
                        Peak: <span class="macd-pos">+{{ "%.1f%%"|format(m.peak_gain_pct) if m.peak_gain_pct else '0%' }}</span>
                    </td>
                    <td>
                        {% for c in m.analyst_calls[:1] %}
                            <span style="font-size:11px; color:#aaa;">{{ c.firm }}: {{ c.action }}</span>
                        {% endfor %}
                    </td>
                    <td><span class="badge badge-{{ m.current_phase or 'NEUTRAL' }}">{{ (m.current_phase or 'NEUTRAL').replace('_', ' ') }}</span></td>
                </tr>
                <tr>
                    <td colspan="6" style="padding: 0; border-bottom: none;">
                        <div style="background: #111; padding: 10px; border-radius: 4px; margin: 5px 12px 15px;">
                            <img src="/static/graph_{{ m.ticker }}.png" alt="MACD Graph for {{ m.ticker }}" style="width: 100%; max-width: 800px; border-radius: 4px; border: 1px solid #2a2d3a;">
                        </div>
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>

        <h2>Ticker Management</h2>
        <div class="management-box">
            <form action="/api/tickers/add" method="POST" class="form-group">
                <input type="text" name="ticker" placeholder="Enter Ticker" required style="text-transform: uppercase;">
                <button type="submit">Add</button>
            </form>
            {% for t in all_tickers %}
                <div class="ticker-pill">
                    {{ t.ticker }}
                    <form action="/api/tickers/remove/{{ t.ticker }}" method="POST" style="display:inline;">
                        <button type="submit" class="btn-remove">×</button>
                    </form>
                </div>
            {% endfor %}
        </div>
    </div>

    <!-- Pi Health Tab -->
    <div id="health" class="tab-content">
        <h2>Live Vitals</h2>
        <div class="health-grid">
            <div class="health-card">
                <p>CPU</p>
                <p style="color: #2ecc71">{{ "%.1f%%"|format(hw.cpu_usage) }}</p>
            </div>
            <div class="health-card">
                <p>RAM</p>
                <p style="color: #5dade2">{{ "%.1f%%"|format(hw.mem_usage) }}</p>
            </div>
            <div class="health-card">
                <p>Temp</p>
                <p style="color: #e74c3c">{{ "%.1f°F"|format((hw.temp * 9/5) + 32) }}</p>
            </div>
            <div class="health-card">
                <p>Load (1m)</p>
                <p>{{ "%.2f"|format(hw.load_1m) }}</p>
            </div>
        </div>

        <h2>Historical Trends (24h)</h2>
        <div class="chart-container">
            <canvas id="healthChart"></canvas>
        </div>
    </div>

    <!-- System Memory Tab -->
    <div id="system" class="tab-content">
        <h2>Core Markdown Files</h2>
        <p class="subtitle" style="margin-bottom: 20px;">These files define who I am, what I know about you, and what I've done.</p>
        <div class="file-list">
            {% for f in system_files %}
            <div class="file-card" onclick="viewFile('{{ f.path }}', '{{ f.name }}')">
                <h3>{{ f.name }}</h3>
                <p>{{ f.description }}</p>
                <span class="file-type">{{ f.type }}</span>
            </div>
            {% endfor %}
        </div>
    </div>

    <!-- Usage Tab -->
    <div id="usage" class="tab-content">
        <h2>Nia Token Usage (Last 30 Days)</h2>
        <p class="subtitle" style="margin-bottom: 20px;">Daily breakdown of API costs and agent activity.</p>
        <table>
            <thead>
                <tr>
                    <th>Date</th>
                    <th>Agent / Model</th>
                    <th>Tokens In</th>
                    <th>Tokens Out</th>
                    <th>Estimated Cost</th>
                </tr>
            </thead>
            <tbody>
                {% for row in claw_stats %}
                <tr>
                    <td><strong>{{ row.date_display }}</strong></td>
                    <td><strong>{{ row.agent_label or 'Nia' }}</strong><br><span style="font-size: 11px; color: #666;">{{ row.model }}</span></td>
                    <td>{{ "{:,}".format(row.t_in) }}</td>
                    <td>{{ "{:,}".format(row.t_out) }}</td>
                    <td style="color: #5dade2; font-weight: 600;">
                        ${{ "%.2f"|format(row.est_cost) }}
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>

    # Usage Tab content ...
    
    <!-- Releases Tab -->
    <div id="releases" class="tab-content">
        <h2>Project Releases</h2>
        <p class="subtitle" style="margin-bottom: 20px;">Latest official versions on GitHub.</p>
        <div class="file-list">
            {% for r in releases %}
            <div class="file-card" onclick="window.open('{{ r.url }}', '_blank')">
                <h3>{{ r.name }}</h3>
                <p style="font-size: 18px; color: #2ecc71; margin: 10px 0;">{{ r.version }}</p>
                <p>Released: {{ r.date }}</p>
                <span class="file-type">GITHUB RELEASE</span>
            </div>
            {% endfor %}
        </div>
        
        <div style="margin-top: 30px;">
            <button onclick="triggerUpdateReleases()" id="updateBtn">Check for Updates</button>
        </div>
    </div>
    <div id="fileModal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2 id="modalTitle">File Content</h2>
                <button onclick="closeModal()" style="background:transparent; border:none; color:#666; font-size:24px; cursor:pointer;">&times;</button>
            </div>
            <div id="modalBody" class="modal-body">Loading...</div>
        </div>
    </div>

    <p class="refresh">Auto-refreshes every 5 min · <a href="/">Refresh now</a></p>
</div>

<script>
    function showTab(tabId) {
        document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        document.getElementById(tabId).classList.add('active');
        const activeTabElement = Array.from(document.querySelectorAll('.tab')).find(t => t.innerText.toLowerCase().includes(tabId.replace('market', 'analysis').replace('system', 'memory')));
        if (activeTabElement) activeTabElement.classList.add('active');
        localStorage.setItem('activeTab', tabId);
    }

    function triggerUpdateReleases() {
        const btn = document.getElementById('updateBtn');
        btn.innerText = "Checking...";
        btn.disabled = true;
        fetch('/api/run/update-releases', { method: 'POST' })
            .then(res => res.json())
            .then(data => {
                setTimeout(() => {
                    location.reload();
                }, 3000);
            });
    }

    // Restore active tab
    const savedTab = localStorage.getItem('activeTab') || 'market';
    showTab(savedTab);

    // File View Logic
    function viewFile(path, name) {
        document.getElementById('modalTitle').innerText = name;
        document.getElementById('modalBody').innerText = "Loading...";
        document.getElementById('fileModal').style.display = "block";
        
        fetch(`/api/file?path=${encodeURIComponent(path)}`)
            .then(res => res.json())
            .then(data => {
                document.getElementById('modalBody').innerText = data.content;
            });
    }

    function closeModal() {
        document.getElementById('fileModal').style.display = "none";
    }

    window.onclick = function(event) {
        if (event.target == document.getElementById('fileModal')) closeModal();
    }

    // Health Chart
    const ctx = document.getElementById('healthChart').getContext('2d');
    const chartData = {{ history_json | safe }};
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: chartData.map(d => d.time),
            datasets: [
                { label: 'CPU %', data: chartData.map(d => d.cpu), borderColor: '#2ecc71', tension: 0.3, pointRadius: 0, yAxisID: 'y' },
                { label: 'Temp °F', data: chartData.map(d => (d.temp * 9/5) + 32), borderColor: '#e74c3c', tension: 0.3, pointRadius: 0, yAxisID: 'y1' },
                { label: 'RAM %', data: chartData.map(d => d.mem), borderColor: '#5dade2', tension: 0.3, pointRadius: 0, yAxisID: 'y' }
            ]
        },
        options: {
            responsive: true,
            scales: {
                x: { ticks: { color: '#666', maxTicksLimit: 12 }, grid: { color: '#1e2130' } },
                y: { 
                    type: 'linear', display: true, position: 'left',
                    ticks: { color: '#2ecc71' },
                    grid: { color: '#1e2130' },
                    title: { display: true, text: 'Percentage (%)', color: '#888' },
                    min: 0, max: 100
                },
                y1: {
                    type: 'linear', display: true, position: 'right',
                    ticks: { color: '#e74c3c' },
                    grid: { drawOnChartArea: false },
                    title: { display: true, text: 'Temperature (°F)', color: '#888' }
                }
            },
            plugins: { legend: { labels: { color: '#aaa' } } }
        }
    });

    setTimeout(() => location.reload(), 300000);
</script>
</body>
</html>
"""

@app.route("/")
def index():
    conn = get_connection()
    all_tickers = conn.execute("SELECT ticker FROM tickers WHERE active = 1 ORDER BY ticker").fetchall()
    system_files = conn.execute("SELECT * FROM system_config_files").fetchall()
    
    hardware_data = conn.execute("SELECT * FROM hardware_stats ORDER BY timestamp DESC LIMIT 1").fetchone()
    if not hardware_data: hardware_data = {"cpu_usage": 0, "mem_usage": 0, "disk_usage": 0, "temp": 0, "load_1m": 0}

    history = conn.execute("SELECT strftime('%H:%M', timestamp) as time, cpu_usage, mem_usage, temp FROM hardware_stats WHERE timestamp > datetime('now', '-24 hours') ORDER BY timestamp ASC").fetchall()
    history_json = [{"time": h["time"], "cpu": h["cpu_usage"], "mem": h["mem_usage"], "temp": h["temp"]} for h in history]

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

    # Usage Tab content
    claw_stats_raw = conn.execute("""
        SELECT day, agent_label, model, sum(tokens_in) as t_in, sum(tokens_out) as t_out 
        FROM claw_usage_v2 
        GROUP BY day, agent_label, model 
        ORDER BY day DESC LIMIT 50
    """).fetchall()

    # Process Usage Data for display
    usage_list = []
    for row in claw_stats_raw:
        d = dict(row)
        # Format the date nicely: "Wednesday, Mar 18"
        try:
            dt = datetime.strptime(d["day"], "%Y-%m-%d")
            d["date_display"] = dt.strftime("%A, %b %d")
        except:
            d["date_display"] = d["day"]
        
        # Simple dynamic cost calculation based on model
        # Rates per 1M tokens
        model_name = d["model"].lower()
        if "flash" in model_name:
            in_rate, out_rate = 0.075, 0.30
        elif "qwen" in model_name or "coder" in model_name:
            in_rate, out_rate = 0.50, 1.50
        elif "plus" in model_name: # Researcher
            in_rate, out_rate = 0.15, 0.60
        else:
            in_rate, out_rate = 0.20, 0.80
            
        d["est_cost"] = (d["t_in"] * in_rate / 1000000) + (d["t_out"] * out_rate / 1000000)
        usage_list.append(d)
    
    releases = conn.execute("SELECT repo_name as name, version, title, datetime(release_date) as date, url FROM project_releases ORDER BY release_date DESC").fetchall()
    
    conn.close()

    processed_data = []
    for r in status_data:
        d = dict(r)
        d["analyst_calls"] = json.loads(d["recent_analyst_calls_json"]) if d.get("recent_analyst_calls_json") else []
        processed_data.append(d)
        
    return render_template_string(TEMPLATE, 
                                  status_data=processed_data, 
                                  claw_stats=usage_list,
                                  releases=releases,
                                  all_tickers=all_tickers,
                                  system_files=system_files,
                                  hw=hardware_data,
                                  history_json=json.dumps(history_json),
                                  now=datetime.now().strftime("%B %d, %Y %I:%M %p"))

@app.route("/api/file")
def get_file():
    path = request.args.get("path")
    try:
        with open(path, "r") as f:
            return jsonify({"content": f.read()})
    except Exception as e:
        return jsonify({"content": f"Error reading file: {e}"})

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
    # Get signals that haven't been sent to Discord
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
    conn.close()
    
    results = []
    for s in signals:
        d = dict(s)
        d["analysts"] = json.loads(d["recent_analyst_calls_json"]) if d.get("recent_analyst_calls_json") else []
        results.append(d)
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

# Use the same lock file as run_daily.py for consistency
LOCK_FILE = BASE_DIR / ".run_daily.lock"

def is_task_running():
    """Checks if the daily pipeline is already running using file locking."""
    try:
        f = open(LOCK_FILE, "r")
        try:
            fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # If we reached here, we got the lock, so it's NOT running
            fcntl.lockf(f, fcntl.LOCK_UN)
            return False
        except IOError:
            # Could not acquire lock, so it IS running
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
        # run_daily.py handles its own fcntl lock internally
        subprocess.Popen([sys.executable, str(BASE_DIR / "scripts" / "run_daily.py")], cwd=BASE_DIR)
        return jsonify({"status": "started", "message": "Daily pipeline triggered in background."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/run/collector", methods=["POST"])
def run_collector():
    if is_task_running():
        return jsonify({"status": "error", "message": "A background task is already running."}), 429
    # Runs the data collection scripts sequentially
    try:
        cmd = f"{sys.executable} scripts/fetch_prices.py && {sys.executable} scripts/fetch_earnings.py"
        subprocess.Popen(cmd, shell=True, cwd=BASE_DIR)
        return jsonify({"status": "started", "message": "Collector scripts triggered in background."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/run/engine", methods=["POST"])
def run_engine():
    if is_task_running():
        return jsonify({"status": "error", "message": "A background task is already running."}), 429
    # Runs the calculation scripts sequentially
    try:
        cmd = f"{sys.executable} scripts/calculate_macd.py && {sys.executable} scripts/signal_detector.py"
        subprocess.Popen(cmd, shell=True, cwd=BASE_DIR)
        return jsonify({"status": "started", "message": "Engine scripts triggered in background."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/run/auditor", methods=["POST"])
def run_auditor():
    if is_task_running():
        return jsonify({"status": "error", "message": "A background task is already running."}), 429
    # Runs the data integrity auditor with repair enabled
    try:
        subprocess.Popen([sys.executable, str(BASE_DIR / "scripts" / "data_auditor.py"), "--repair"], cwd=BASE_DIR)
        return jsonify({"status": "started", "message": "Data Auditor triggered in background."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/run/update-releases", methods=["POST"])
def run_update_releases():
    if is_task_running():
        return jsonify({"status": "error", "message": "A background task is already running."}), 429
    # Updates the release info from GitHub
    try:
        subprocess.Popen([sys.executable, str(BASE_DIR / "scripts" / "update_releases.py")], cwd=BASE_DIR)
        return jsonify({"status": "started", "message": "Release update triggered in background."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
