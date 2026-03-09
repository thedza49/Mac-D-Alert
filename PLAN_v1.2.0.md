# PLAN: Mac-D-Alert (Sovson Analytics) - v1.2.0

## 🎯 Goal
Upgrade the notification engine for better decision-making data and a cleaner Discord experience.

## 🏗️ Phase 1: Signal History (Backtesting)
Enhance `signal_detector.py` to provide the "Proof" for every signal.

- **Tasks**:
  - Fetch the last 3 historical signals of the same type for the ticker.
  - Calculate:
    - **1-Week Return** (5 trading days).
    - **3-Week Return** (15 trading days).
    - **Peak Return** + **Days to Peak**.
  - Format into a compact string: `1w: +X% | 3w: +Y% | Peak: +Z% (Dd)`.
  - Pass this to the `signals` table for the notifier to pick up.

## 🧠 Phase 2: Analyst Sentiment Freshness (FMP v4)
Integrate individual analyst call history to gauge momentum.

- **Tasks**:
  - Update `fetch_earnings.py` to use the **FMP v4 Price Target** endpoint.
  - Capture:
    - Firm Name (e.g., Morgan Stanley).
    - Date of the call.
    - Rating (Buy, Neutral, etc.).
    - Price Target.
  - Calculate **Upside %** for each individual call vs. current price.
  - Compute a **Relative Date String** (e.g., "4 days ago") for Discord display.

## 🚀 Phase 3: Native Discord Attachments (n8n)
Move away from deep links and render the proof directly in the chat.

- **Workflow C Update**:
  - Add a **Read Binary File** node to grab `graph_[TICKER].png` from the local Pi storage.
  - Configure the **Discord Webhook** node to attach the image file natively.
  - Update the **Embed Layout**:
    - **Green Embeds** for BUYS/APPROACHING BUYS.
    - **Red Embeds** for SELLS/APPROACHING SELLS.
    - **Backtesting Section** (BUY signals only).
    - **Analyst Tape** (3 most recent calls).

## 📦 Phase 4: GitHub & Deployment
- **Commit**: Push script updates, n8n JSON refactor, and documentation.
- **Deploy**: Restart `macd-dashboard.service` and activate the new n8n workflow.

---
**Status**: Executing (Coder Agent active).
