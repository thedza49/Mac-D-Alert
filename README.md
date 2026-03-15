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
-   **Enhanced Notifications**: Automated Discord alerts with native chart attachments, 3-signal backtesting history, and real-time analyst sentiment "freshness" (e.g., "4 days ago").

## 📊 Logic & Confidence Scoring

Every signal is scored (0-100) based on:
-   **MACD Gap Convergence**: Speed and distance to crossover.
-   **HA Trend Strength**: Confirmation of bullish/bearish candle bodies.
-   **Analyst Sentiment (FMP v4)**: Real-time integration with firm-specific price targets and upside calculations.
-   **Performance Backtesting**: Automatic calculation of 1-week, 3-week, and peak returns for every historical signal.
-   **Earnings Safety**: Penalizes signals occurring immediately before earnings calls.

## 🚀 Simple Setup Guide (Non-Coders)

### 1. Install the "Brain"
If you are on your Raspberry Pi, run these commands to install dependencies and initialize your database:
```bash
cd ~/Mac-D-Alert
pip install -r requirements.txt
python3 scripts/setup_database.py
```

### 2. Set Up the Dashboard
This lets you see your graphs at `http://raspberrypi.local:5000`.
```bash
sudo cp macd-dashboard.service /etc/systemd/system/
sudo systemctl enable --now macd-dashboard.service
```

### 3. Install n8n Workflows (Automated Tasks)
You don't need to write any code here. Just import the "Workflows" into your n8n dashboard:

1.  **Download the latest workflows**: Go to the [Releases page](https://github.com/thedza49/Mac-D-Alert/releases) and download the `n8n_workflows.zip`.
2.  **Open n8n** in your browser.
2.  Click **Workflows** > **Add Workflow**.
3.  Click the **three dots (⋮)** in the top right and select **Import from File**.
4.  Upload the three files found in the `Mac-D-Alert` folder:
    *   `n8n_workflow_collector.json` (Grabs the daily data)
    *   `n8n_workflow_engine.json` (Calculates the signals)
    *   `n8n_workflow_notifier.json` (Sends Discord alerts)
5.  **Important**: Set an environment variable in n8n named `DISCORD_WEBHOOK_URL` with your Discord Webhook URL. The "Discord Notifier" workflow is pre-configured to use this variable.
6.  Click **Save** and turn the toggle to **Active**.

---
*Developed for Daniel (Sovson Analytics) by Nia @ OpenClaw.*
