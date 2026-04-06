import os
from datetime import datetime, timedelta

LOG_FILE = "/home/daniel/Mac-D-Alert/logs/cron_run_daily.log"
DB_FILE = "/home/daniel/Mac-D-Alert/data/sovson_analytics.db"

def check():
    now = datetime.now()
    # 1. Check DB size (should be small now)
    db_size = os.path.getsize(DB_FILE) / (1024 * 1024)
    if db_size > 500:
        print(f"CRITICAL: Database is too large ({db_size:.1f}MB)")
    
    # 2. Check if cron ran in the last 24h
    if os.path.exists(LOG_FILE):
        mtime = datetime.fromtimestamp(os.path.getmtime(LOG_FILE))
        if now - mtime > timedelta(hours=26):
            print("WARNING: Daily cron hasn't updated the log in 26 hours.")
    else:
        print("ERROR: Daily cron log file missing.")

if __name__ == "__main__":
    check()
