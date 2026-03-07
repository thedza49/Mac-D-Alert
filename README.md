# Mac-D-Alert (Sovson Analytics)

A automated stock market technical analysis engine designed to identify high-probability "Buy" and "Approaching Buy" signals. Running locally on Raspberry Pi, optimized for speed, reliability, and zero-cost AI orchestration.

## 🎯 Overview

Mac-D-Alert monitors a basket of high-growth stocks (Magnificent 7 base) and identifies trend reversals using a dual-confirmation strategy:

1.  **5-Day Rolling MACD**: Detects momentum shifts and crossover acceleration.
2.  **Heikin Ashi Price Action**: Filters out market noise by requiring trend confirmation before issuing a signal.

## 🏗️ Architecture

The system is built to be "token-free" and reboot-resilient:

-   **Data Pipeline**: Python scripts handle OHLC data fetching (Yahoo Finance), MACD calculation, and signal detection.
-   **Orchestration**: native **n8n** workflows manage the daily schedule and inter-script dependencies.
-   **Storage**: Lightweight **SQLite** database for price history, backtesting logs, and signal tracking.
-   **Dashboard**: A local Flask-based web UI for visualizing stock graphs and historical win rates.
-   **Notifications**: Automated Discord alerts with deep-linked charts and confidence scores.

## 📊 Logic & Confidence Scoring

Every signal is scored (0-100) based on:
-   **MACD Gap Convergence**: Speed and distance to crossover.
-   **HA Trend Strength**: Confirmation of bullish/bearish candle bodies.
-   **Analyst Sentiment**: Real-time integration with buy/sell ratios and price targets.
-   **Earnings Safety**: Penalizes signals occurring immediately before earnings calls.

## 🚀 Setup & Installation

### Prerequisites
- Python 3.11+
- n8n (installed on host)
- SQLite3

### 1. Clone & Install Dependencies
```bash
git clone https://github.com/thedza49/Mac-D-Alert.git
cd Mac-D-Alert
pip install -r requirements.txt
```

### 2. Database Initialization
```bash
python3 scripts/setup_database.py
```

### 3. n8n Integration
Import the provided workflow templates into your n8n instance:
- `n8n_workflow_collector.json`: Daily data fetching.
- `n8n_workflow_engine.json`: Signal processing & math.
- `n8n_workflow_notifier.json`: Discord alerts & API polling.

### 4. Dashboard Service
Enable the dashboard as a system service (recommended for Pi):
```bash
sudo cp macd-dashboard.service /etc/systemd/system/
sudo systemctl enable --now macd-dashboard.service
```

## 📈 Public API & Webhook
The dashboard exposes a simple API at `/api/signals/unsent` for custom integrations with other notification services.

---
*Developed for Daniel (Sovson Analytics) by Nia @ OpenClaw.*
