#!/usr/bin/env python3
"""
notify_telegram.py
Sovson Analytics — Telegram Notifier

Polls the Flask API for unsent signals and delivers them to a Telegram
channel topic via bot. Replaces notify_discord.py.

Designed to be run by cron every 30 minutes on weekdays:
  */30 14-23 * * 1-5 cd ~/Mac-D-Alert && TELEGRAM_BOT_TOKEN="..." python3 scripts/notify_telegram.py

Environment variable required:
  TELEGRAM_BOT_TOKEN — token from BotFather
"""

import os
import sys
import logging
import requests
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR  = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "notify_telegram.log"),
    ],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
FLASK_BASE_URL   = "http://127.0.0.1:5000"
BOT_TOKEN        = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID          = os.environ.get("TELEGRAM_CHAT_ID", "-1003850036083")
TOPIC_ID         = os.environ.get("TELEGRAM_TOPIC_ID", "2")

TELEGRAM_API     = f"https://api.telegram.org/bot{BOT_TOKEN}"

# Signal type labels and emoji
SIGNAL_EMOJI = {
    "BUY":              "🟢",
    "APPROACHING_BUY":  "🟡",
    "SELL":             "🔴",
    "APPROACHING_SELL": "🟠",
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


def escape_html(text: str) -> str:
    """Escape special characters for Telegram HTML mode."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def build_message(signal: dict) -> str:
    """Build an HTML formatted message for a single signal."""
    ticker      = signal.get("ticker", "???")
    signal_type = signal.get("signal_type", "???")
    price       = signal.get("price_at_signal", 0)
    win_rate    = signal.get("win_rate") or 0
    lifecycle   = signal.get("lifecycle_text", "No history available.")
    score       = signal.get("confidence_score")

    emoji = SIGNAL_EMOJI.get(signal_type, "⚪")
    label = signal_type.replace("_", " ")

    lines = []
    lines.append(f"{emoji} <b>{escape_html(ticker)} — {escape_html(label)}</b>")
    lines.append("")
    lines.append(f"💰 <b>Price:</b> ${escape_html(f'{price:,.2f}')}")

    if score is not None:
        lines.append(f"🎯 <b>Confidence:</b> {int(score)}/100")

    lines.append(f"📊 <b>Win Rate:</b> {int(win_rate)}%")
    lines.append("")
    lines.append("<b>Signal Evolution:</b>")
    lines.append(f"<pre>{escape_html(lifecycle)}</pre>")
    lines.append(f"🔗 <a href='http://raspberrypi.local:5000'>View Dashboard</a>")
    lines.append(f"<i>{escape_html(datetime.now().strftime('%Y-%m-%d %H:%M PST'))}</i>")

    return "\n".join(lines)


def send_to_telegram(text: str) -> bool:
    """POST a message to the Telegram bot API."""
    if not BOT_TOKEN:
        log.error("TELEGRAM_BOT_TOKEN is not set — cannot send notification")
        return False
    try:
        payload = {
            "chat_id":              CHAT_ID,
            "message_thread_id":    TOPIC_ID,
            "text":                 text,
            "parse_mode":           "HTML",
            "disable_web_page_preview": True,
        }
        r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
        r.raise_for_status()
        return True
    except Exception as exc:
        log.error(f"Telegram POST failed: {exc}")
        # Log response body for debugging
        try:
            log.error(f"Response: {exc.response.text}")
        except Exception:
            pass
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if not BOT_TOKEN:
        log.error("TELEGRAM_BOT_TOKEN environment variable not set. Exiting.")
        sys.exit(1)

    signals = fetch_unsent_signals()

    if not signals:
        log.info("No unsent signals found.")
        return

    log.info(f"Found {len(signals)} unsent signal(s).")

    for signal in signals:
        signal_id   = signal.get("id")
        ticker      = signal.get("ticker", "???")
        signal_type = signal.get("signal_type", "???")

        log.info(f"Sending: {ticker} {signal_type} (id={signal_id})")

        message = build_message(signal)
        if send_to_telegram(message):
            if mark_signal_sent(signal_id):
                log.info(f"  ✅ Delivered and marked sent: {ticker} {signal_type}")
            else:
                log.warning(f"  ⚠️  Delivered but failed to mark sent: {ticker} {signal_type}")
        else:
            log.error(f"  ❌ Failed to deliver: {ticker} {signal_type}")


if __name__ == "__main__":
    main()
