# Mac-D-Alert (Sovson Analytics)

Automated stock market technical analysis engine designed to identify high-probability Buy and Approaching Buy signals. Running locally on Raspberry Pi, optimized for speed, reliability, and minimal dependencies.

## 🎯 Overview
Mac-D-Alert monitors a focused basket of high-growth stocks and identifies trend reversals using a dual-confirmation strategy:
1. **5-Day Rolling MACD**: Detects momentum shifts and crossover acceleration.
2. **Heikin Ashi Price Action**: Filters out market noise by requiring trend confirmation before issuing a signal.

## 🏗️ Architecture
- **Language**: Python 3.11+
- **Database**: SQLite (`/home/daniel/Mac-D-Alert/data/sovson_analytics.db`)
  - `signals` — raw crossovers and performance history
  - `trade_lifecycles` — BUY/SELL pairings, apex discovery, and capture efficiency
- **Web UI**: Flask Dashboard (`http://raspberrypi.local:5000`)
  - Evolution View: visual history of the last 3 completed trades
- **Persistence**: managed via `systemd` (`macd-dashboard.service`)
- **Orchestration**: native `cron` jobs — no Docker, no n8n, no external tools

## 📊 Data Pipeline

Daily pipeline runs at 1:45 PM PST (Mon–Fri) via cron:

1. **`fetch_prices.py`** — pulls OHLCV data via `yahooquery`, calculates Heikin Ashi candles
2. **`fetch_earnings.py`** — pulls analyst ratings and earnings dates via FMP v4
3. **`calculate_macd.py`** — computes 5-day rolling MACD / Signal / Histogram + 50-day MA
4. **`signal_detector.py`** — detects 4-phase signals (BUY, APPROACHING_BUY, SELL, APPROACHING_SELL), writes to DB
5. **`analyze_lifecycles.py`** — pairs BUY→SELL signals, calculates captured profit vs peak profit (apex)
6. **`generate_static.py`** — regenerates chart images for the dashboard

Notifications run every 30 minutes (Mon–Fri) via cron:

7. **`notify_telegram.py`** — polls Flask API for unsent signals, delivers to Telegram

## 🔔 Telegram Notifications

Alerts are delivered to a private Telegram channel and include:
- Signal type and ticker with color-coded emoji
- Price at signal
- Signal evolution — last 3 completed trade outcomes
- Link to the live dashboard

## 🚀 Setup

### 1. Install dependencies
```bash
cd ~/Mac-D-Alert
pip install -r requirements.txt
python3 scripts/setup_database.py
```

### 2. Set up the dashboard
```bash
sudo cp macd-dashboard.service /etc/systemd/system/
sudo systemctl enable --now macd-dashboard.service
```

### 3. Set up cron jobs
```bash
crontab -e
```

Add the following two entries:
```
# Daily pipeline — 1:45 PM PST (Mon–Fri)
45 21 * * 1-5 cd /home/daniel/Mac-D-Alert && python3 scripts/run_daily.py >> logs/cron_run_daily.log 2>&1

# Telegram notifier — every 30 mins (Mon–Fri)
*/30 14-23 * * 1-5 cd /home/daniel/Mac-D-Alert && TELEGRAM_BOT_TOKEN="your_token_here" python3 scripts/notify_telegram.py >> logs/cron_notifier.log 2>&1
```

## 🗺️ Roadmap

- [x] ~~Replace n8n with native cron + Python orchestration~~
- [x] Signal Lifecycle Engine — apex tracking and capture efficiency
- [x] ~~Confidence score in alerts~~
- [x] Migrate notifications from Discord to Telegram
- [ ] Signal quality validation — confirm MACD BUY signals predict positive forward returns
- [ ] Betting market signals — surface prediction market data for a ticker when a signal fires
- [ ] Multi-indicator platform — pluggable pattern library, MACD as Indicator #1

---

*Sovson Analytics*
