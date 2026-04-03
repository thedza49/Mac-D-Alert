#!/usr/bin/env python3
"""
notify_discord.py
Sovson Analytics — Discord Notifier

Polls the Flask API for unsent signals and delivers them to Discord
via webhook. Replaces the n8n Workflow C (Discord Notifier).

Designed to be run by cron every 30 minutes on weekdays:
 */30 9-18 * * 1-5 cd ~/Mac-D-Alert && python3 scripts/notify_discord.py

Environment variable required:
 DISCORD_WEBHOOK_URL — full Discord webhook URL
"""

import os
import sys
import logging
import requests
from pathlib import Path
from datetime import datetime

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
 level=logging.INFO,
 format="%(asctime)s %(levelname)-8s %(message)s",
 handlers=[
 logging.StreamHandler(sys.stdout),
 logging.FileHandler(LOG_DIR / "notify_discord.log"),
 ],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
FLASK_BASE_URL = "http://127.0.0.1:5000"
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL", "")

SIGNAL_COLORS = {
 "BUY": 0x00C853, # green
 "APPROACHING_BUY": 0x69F0AE, # light green
 "SELL": 0xD50000, # red
 "APPROACHING_SELL": 0xFF6D00, # orange
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_unsent_signals() -> list:
 """Pull unsent signals from the Flask API."""
 try:
  r = requests.get(f"{FLASK_BASE_URL}/api/signals/unsent", timeout=10)
  r.raise_for_status()
  return r.json()
 except Exception as exc:
  log.error(f"Failed to fetch unsent signals: {exc}")
  return []


def mark_signal_sent(signal_id: int) -> bool:
 """Tell the Flask API a signal has been delivered."""
 try:
  r = requests.post(
  f"{FLASK_BASE_URL}/api/signals/mark-sent/{signal_id}",
  timeout=10,
  )
  r.raise_for_status()
  return True
 except Exception as exc:
  log.error(f"Failed to mark signal {signal_id} as sent: {exc}")
  return False


def build_embed(signal: dict) -> dict:
 """Build a Discord embed payload for a single signal."""
 ticker = signal.get("ticker", "???")
 signal_type = signal.get("signal_type", "???")
 price = signal.get("price_at_signal", 0)
 win_rate = signal.get("win_rate") or 0
 lifecycle = signal.get("lifecycle_text", "No history available.")
 score = signal.get("confidence_score")

 color = SIGNAL_COLORS.get(signal_type, 0x9E9E9E)

 lines = []
 lines.append(f"💰 **Price:** ${price:,.2f}")
 if score is not None:
  lines.append(f"🎯 **Confidence:** {int(score)}/100")
 lines.append(f"📊 **Win Rate:** {int(win_rate)}%")
 lines.append("")
 lines.append("**Signal Evolution:**")
 lines.append(f"```\n{lifecycle}\n```")
 lines.append(f"🔗 [View Scorecard](http://raspberrypi.local:5000)")

 return {
 "embeds": [
 {
 "title": f"🚨 {ticker} — {signal_type.replace('_', ' ')}",
 "description": "\n".join(lines),
 "color": color,
 "footer": {"text": f"Sovson Analytics • {datetime.now().strftime('%Y-%m-%d %H:%M PST')}"},
 }
 ]
 }


def send_to_discord(payload: dict) -> bool:
 """POST a payload to the Discord webhook."""
 if not DISCORD_WEBHOOK:
  log.error("DISCORD_WEBHOOK_URL is not set — cannot send notification")
  return False
 try:
  r = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
  r.raise_for_status()
  return True
 except Exception as exc:
  log.error(f"Discord POST failed: {exc}")
  return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
 if not DISCORD_WEBHOOK:
  log.error("DISCORD_WEBHOOK_URL environment variable not set. Exiting.")
  sys.exit(1)

 signals = fetch_unsent_signals()

 if not signals:
  log.info("No unsent signals found.")
  return

 log.info(f"Found {len(signals)} unsent signal(s).")

 for signal in signals:
  signal_id = signal.get("id")
  ticker = signal.get("ticker", "???")
  signal_type = signal.get("signal_type", "???")

  log.info(f"Sending: {ticker} {signal_type} (id={signal_id})")

  payload = build_embed(signal)
  if send_to_discord(payload):
   if mark_signal_sent(signal_id):
    log.info(f" ✅ Delivered and marked sent: {ticker} {signal_type}")
   else:
    log.warning(f" ⚠️ Delivered but failed to mark sent: {ticker} {signal_type}")
  else:
   log.error(f" ❌ Failed to deliver: {ticker} {signal_type}")


if __name__ == "__main__":
 main()
