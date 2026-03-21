#!/usr/bin/env python3
import json
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "sovson_analytics.db"

def get_claw_sessions():
    """Runs the OpenClaw CLI to get session data as JSON."""
    try:
        # Check if openclaw is in PATH, otherwise fallback to common location
        import shutil
        openclaw_path = shutil.which("openclaw") or "/home/daniel/.npm-global/bin/openclaw"
        # We use --all-agents to catch Coder, Researcher, and Main Nia
        result = subprocess.run([openclaw_path, 'sessions', '--all-agents', '--json'], 
                               capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except Exception as e:
        print(f"Error fetching sessions: {e}")
        return None

def update_usage():
    data = get_claw_sessions()
    if not data or 'sessions' not in data:
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    updated_count = 0
    for sess in data['sessions']:
        session_id = sess.get('sessionId')
        # We only care about sessions with tracked tokens
        t_in = sess.get('inputTokens')
        t_out = sess.get('outputTokens')
        updated_at = sess.get('updatedAt')
        
        if session_id and t_in is not None and t_out is not None and updated_at:
            agent = sess.get('agentId', 'unknown')
            model = sess.get('model', 'unknown')
            
            # Use the actual update date of the session rather than 'today'
            # to handle missed runs or sessions spanning midnight correctly.
            sess_date = datetime.fromtimestamp(updated_at / 1000).date().isoformat()
            
            # Upsert into the v2 table
            cursor.execute("""
                INSERT INTO claw_usage_v2 (day, session_id, agent_label, model, tokens_in, tokens_out)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(day, session_id) DO UPDATE SET
                    tokens_in = excluded.tokens_in,
                    tokens_out = excluded.tokens_out
            """, (sess_date, session_id, agent, model, t_in, t_out))
            updated_count += 1

    conn.commit()
    conn.close()
    print(f"✅ Usage updated. Processed {updated_count} active sessions.")

if __name__ == "__main__":
    update_usage()
