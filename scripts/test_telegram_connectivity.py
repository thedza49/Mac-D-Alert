import os
import requests
import sys

def test_telegram():
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "-1003850036083")
    topic_id = os.environ.get("TELEGRAM_TOPIC_ID", "2")

    if not token:
        print("Error: TELEGRAM_BOT_TOKEN not found in environment.")
        return

    print(f"Using Bot Token: {token[:5]}...{token[-5:]}")
    print(f"Target Chat ID: {chat_id}")
    print(f"Target Topic ID: {topic_id}")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "message_thread_id": topic_id,
        "text": "🛠 <b>Mac-D-Alert Diagnostic</b>\nThis is a test message to verify Telegram connectivity.",
        "parse_mode": "HTML"
    }

    try:
        r = requests.post(url, json=payload, timeout=10)
        print(f"Status Code: {r.status_code}")
        print(f"Response: {r.text}")
        if r.status_code == 200:
            print("\n✅ SUCCESS: Message sent successfully!")
        else:
            print("\n❌ FAILED: Check the error message above.")
    except Exception as e:
        print(f"\n❌ EXCEPTION: {e}")

if __name__ == "__main__":
    test_telegram()
