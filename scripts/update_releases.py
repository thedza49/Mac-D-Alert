#!/usr/bin/env python3
import subprocess
import sqlite3
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "sovson_analytics.db"

REPOS = [
    "thedza49/Mac-D-Alert",
    "thedza49/pets-plus",
    "thedza49/ValkyrieOrigins"
]

def fetch_latest_releases():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    for repo in REPOS:
        try:
            # Use gh api since the installed version of gh is too old for --json on release list
            result = subprocess.run(
                ["gh", "api", f"repos/{repo}/releases/latest", "--jq", "{tagName: .tag_name, name: .name, publishedAt: .published_at, url: .html_url}"],
                capture_output=True, text=True, check=True
            )
            rel = json.loads(result.stdout)
            
            if rel:
                cursor.execute("""
                    INSERT INTO project_releases (repo_name, version, title, release_date, url)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(repo_name) DO UPDATE SET
                        version = excluded.version,
                        title = excluded.title,
                        release_date = excluded.release_date,
                        url = excluded.url
                """, (repo.split('/')[-1], rel['tagName'], rel['name'], rel['publishedAt'], rel['url']))
                
        except Exception as e:
            print(f"Error fetching release for {repo}: {e}")
            
    conn.commit()
    conn.close()
    print("✅ Release info updated.")

if __name__ == "__main__":
    fetch_latest_releases()
