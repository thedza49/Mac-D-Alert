#!/usr/bin/env python3
import json
import sqlite3
import subprocess
from datetime import date
from pathlib import Path

# Paths
DB_PATH = "/home/daniel/Mac-D-Alert/data/sovson_analytics.db"

def get_claw_sessions():
    """Runs the OpenClaw CLI to get session data as JSON."""
    try:
        # We use --all-agents to catch Coder, Researcher, and Main Nia
        result = subprocess.run(['openclaw', 'sessions', '--all-agents', '--json'], 
                               capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except Exception as e:
        print(f"Error fetching sessions: {e}")
        return None

def update_usage():
    data = get_claw_sessions()
    if not data or 'sessions' not in data:
        return

    today = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for sess in data['sessions']:
        session_id = sess.get('sessionId')
        # We only care about sessions with tracked tokens
        t_in = sess.get('inputTokens')
        t_out = sess.get('outputTokens')
        
        if session_id and t_in is not None and t_out is not None:
            agent = sess.get('agentId', 'unknown')
            model = sess.get('model', 'unknown')
            
            # Upsert into the v2 table
            cursor.execute("""
                INSERT INTO claw_usage_v2 (day, session_id, agent_label, model, tokens_in, tokens_out)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(day, session_id) DO UPDATE SET
                    tokens_in = excluded.tokens_in,
                    tokens_out = excluded.tokens_out
            """, (today, session_id, agent, model, t_in, t_out))

    conn.commit()
    conn.close()
    print(f"✅ Usage updated for {today}")

if __name__ == "__main__":
    update_usage()
