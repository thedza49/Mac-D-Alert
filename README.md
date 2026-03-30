# Mac-D-Alert (Sovson Analytics)

Automated stock market technical analysis engine designed to identify high-probability "Buy" and "Approaching Buy" signals. Running locally on Raspberry Pi, optimized for speed, reliability, and zero-cost AI orchestration.

## 🎯 Overview
Mac-D-Alert monitors a basket of high-growth stocks (Magnificent 7 base) and identifies trend reversals using a dual-confirmation strategy:
1.  **5-Day Rolling MACD**: Detects momentum shifts and crossover acceleration.
2.  **Heikin Ashi Price Action**: Filters out market noise by requiring trend confirmation before issuing a signal.

## 🏗️ Architecture
The system is built to be "token-free" and reboot-resilient:
-   **Language**: Python 3.11+
-   **Database**: SQLite (`/home/daniel/Mac-D-Alert/data/sovson_analytics.db`)
    -   `signals` (Raw crossovers and history)
    -   `trade_lifecycles` (Signal pairings, apex discovery, and efficiency metrics)
-   **Web UI**: Flask Dashboard ([http://raspberrypi.local:5000](http://raspberrypi.local:5000))
    -   **Scorecard**: Aggregated win rates and average performance per ticker.
    -   **Evolution View**: Visual history of the last 3 completed trades.
-   **Persistence**: Managed via `systemd` (`macd-dashboard.service`)
-   **Orchestration**: native **n8n** workflows manage the daily schedule and inter-script dependencies.

## 📊 Data Pipeline & Logic
1.  **Collector (`fetch_prices.py`, `fetch_earnings.py`)**: Runs daily at 1:30 PM PST to pull OHLCV data (via `yahooquery`) and analyst ratings/earnings dates (via FMP v4).
2.  **Engine (`calculate_macd.py`, `signal_detector.py`)**: Computes 5-day MACD/Signal/Histogram and Heikin Ashi candles. Scans for signal criteria (Histogram crossover + HA confirmation).
3.  **Lifecycle Engine (`analyze_lifecycles.py`)**: Pairs BUY signals with subsequent SELL signals to calculate "Captured Profit" vs "Peak Profit" (Apex).
4.  **Audit (`data_auditor.py`)**: Daily database health checks to ensure data integrity and backtest completeness.
5.  **Notifier (`n8n Workflow C`)**: Polls the internal API for unsent signals and pushes rich embeds to Discord.

### 📉 Confidence Scoring & Filters
Every signal is scored (0-100) based on:
-   **MACD Gap Convergence**: Speed and distance to crossover.
-   **HA Trend Strength**: Confirmation of bullish/bearish candle bodies.
-   **Analyst Sentiment**: Integration with firm-specific price targets and upside calculations.
-   **Earnings Safety**: Penalizes signals occurring immediately before earnings calls.

## 🔔 Enhanced Notifications (v1.6.0)
Automated Discord alerts include:
-   **ANSI Colored Evolution**: A minimalist text block showing the "Signal Evolution" for the last 3 trades (Green for gains, Red for losses).
-   **Peak Performance (Apex)**: Displays how high a trade went (and how many days it took) before the signal eventually closed.
-   **Native Charts**: `graph_[TICKER].png` attached directly to the message.
-   **Scorecard Link**: Direct access to the live performance metrics on the local portal.

## 🚀 Simple Setup Guide (Non-Coders)

### 1. Install the "Brain"
```bash
cd ~/Mac-D-Alert
pip install -r requirements.txt
python3 scripts/setup_database.py
python3 scripts/analyze_lifecycles.py # Initialize trade history
```

### 2. Set Up the Dashboard
```bash
sudo cp macd-dashboard.service /etc/systemd/system/
sudo systemctl enable --now macd-dashboard.service
```

### 3. Install n8n Workflows
Import the following into n8n:
- `n8n_workflow_orchestrator.json` (Daily 1:30 PM - Master Logic)
- `n8n_workflow_notifier.json` (Discord Messaging v1.6.0)
- `n8n_workflow_backup.json` (Daily Database Maintenance)

## 🗺️ Mini Roadmap

- [x] **Stability & Reliability Patch**: Monitor sequential workflow firing and ensure no n8n trigger failures.
- [x] **Discord Notification Redesign**: Implemented v1.6.0 ANSI-colored Signal Evolution format.
- [x] **Backtesting Expectations**: Implemented Signal Lifecycle Engine and Apex tracking.
- [ ] **Prediction Market Integration**: Research sentiment analysis from Polymarket/Kalshi as a new scoring indicator.

---
*Developed for Daniel (Sovson Analytics) by Nia @ OpenClaw.*
