#!/usr/bin/env python3
import os
import time
from pathlib import Path

# Configuration
TARGET_DIR = Path("/home/daniel/.openclaw/media/inbound/")
MAX_AGE_DAYS = 7
MAX_AGE_SECONDS = MAX_AGE_DAYS * 24 * 60 * 60

def cleanup():
    now = time.time()
    if not TARGET_DIR.exists():
        print(f"Directory {TARGET_DIR} does not exist.")
        return

    count = 0
    for file_path in TARGET_DIR.iterdir():
        if file_path.is_file():
            file_age = now - file_path.stat().st_mtime
            if file_age > MAX_AGE_SECONDS:
                try:
                    file_path.unlink()
                    # print(f"Deleted: {file_path.name}")
                    count += 1
                except Exception as e:
                    print(f"Error deleting {file_path.name}: {e}")
    
    print(f"Cleanup complete. Deleted {count} files older than {MAX_AGE_DAYS} days.")

if __name__ == "__main__":
    cleanup()
