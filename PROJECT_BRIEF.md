# PROJECT_BRIEF: Mac-D-Alert (Sovson Analytics)

**Date**: February 26, 2026
**Status**: Active & Automated

## 🎯 Overview
Mac-D-Alert is a stock market technical analysis tool designed to identify "Buy" and "Approaching Buy" signals using a 5-day rolling MACD strategy combined with Heikin Ashi price action. It runs on a Raspberry Pi 4.

## 🏗️ Architecture
- **Language**: Python 3.11+
- **Database**: SQLite (`/home/daniel/sovson-analytics/data/sovson_analytics.db`)
- **Web UI**: Flask Dashboard ([http://192.168.1.152:5000](http://192.168.1.152:5000))
- **Persistence**: Managed via `systemd` (`macd-dashboard.service`)
- **Automation**: Cron job runs daily at 6:00 AM PST.

## 📊 Data Pipeline
1.  **fetch_prices.py**: Pulls OHLCV data via `yahooquery`.
2.  **calculate_macd.py**: Computes 5-day MACD, Signal line, and Histogram.
3.  **fetch_earnings.py**: Gathers analyst ratings and upcoming earnings dates.
4.  **signal_detector.py**: Scans for signal criteria (MACD Histogram crossover + Heikin Ashi confirmation).

## 🚀 Transferable Context for Other AIs
- **DB Schema**: Tables include `daily_prices`, `macd_5d_data`, `signals`, `tickers`, and `earnings_data`.
- **Key Logic**: Signals are generated when the MACD Histogram turns positive and price action shows upward momentum.
- **Active Tickers**: AAPL, META, MSFT, NVDA (Magnificent 7 base).

## 📅 Recent Updates (Feb 25-26)
- **Database Repair**: Added missing `ma_50d` and `volume_5d_avg` columns.
- **Service Hardening**: Converted the dashboard to a `systemd` service to ensure it survives Pi reboots.
- **Network Profile**: Running on a Static IP (192.168.1.152) within a Calix Mesh network.
