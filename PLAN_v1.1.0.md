# PLAN: Mac-D-Alert (Sovson Analytics) - v1.1.0

## 🎯 Goal
Upgrade the system architecture for better reliability, lower token costs, and rock-solid math.

## 🏗️ Phase 1: Architecture Split (n8n)
Move automation from OpenClaw/Agents to native n8n workflows to save tokens and improve stability.

- **Workflow A: The Collector**
  - **Schedule**: 1:30 PM PST (30 min after market close).
  - **Action**: Execute `python3 scripts/fetch_prices.py` and `python3 scripts/fetch_earnings.py`.
  - **Output**: Fresh raw data in SQLite.

- **Workflow B: The Engine**
  - **Trigger**: On completion of Workflow A.
  - **Action**: Execute `python3 scripts/calculate_macd.py` and `python3 scripts/signal_detector.py`.
  - **Output**: Updated MACD calculations and fresh Buy/Sell signals.

## 🧠 Phase 2: Logic Audit (Coder)
Using the newly upgraded **Qwen 2.5 Coder 32B** model (paid/reliable tier), Coder will perform a deep dive into `scripts/signal_detector.py`.

- **Checklist**:
  - Verify 5-day rolling MACD math.
  - Audit Heikin Ashi trend detection for edge case failures.
  - Optimize database query patterns.
  - Ensure signal thresholding is robust against "dirty" data.

## 🚀 Phase 3: Discord Integration (n8n)
Instead of me coding a Discord bot, we'll use n8n to poll our Dashboard API and send the alerts. This keeps it 100% serverless and token-free.

- **Workflow C: Discord Notifier**
  - **Schedule**: Every 30 minutes between 1:30 PM and 3:00 PM PST.
  - **Action**: 
    1. HTTP Request to `http://localhost:5000/api/signals/unsent`.
    2. For each signal found, send a Discord Webhook message.
    3. HTTP POST to `http://localhost:5000/api/signals/mark-sent/{{id}}` to avoid duplicates.
  - **Message Format**:
    ```text
    🚨 [TICKER] [SIGNAL_TYPE] Detected!
    💰 Price: $[PRICE]
    📊 Historical Win Rate: [WIN_RATE]%
    📈 Avg. Peak Gain: +[AVG_GAIN]%
    📝 Analyst Sentiment: [SENTIMENT]
    🔗 View Graph: http://raspberrypi.local:5000/static/graph_[TICKER].png
    ```

## 📦 Phase 4: GitHub & Versioning
- **Commit**: Push all script refinements and the new n8n architecture documentation.
- **Release**: Tag as `v1.1.0` on GitHub.

---
**Status**: Awaiting Daniel's greenlight to proceed.
