#!/usr/bin/env python3
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / 'data' / 'sovson_analytics.db'

def fix_database():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.executescript('''
    CREATE TABLE IF NOT EXISTS hardware_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        cpu_usage REAL,
        mem_usage REAL,
        disk_usage REAL,
        temp REAL,
        load_1m REAL,
        load_5m REAL,
        load_15m REAL
    );

    CREATE TABLE IF NOT EXISTS claw_usage_v2 (
        day DATE,
        session_id TEXT,
        agent_label TEXT,
        model TEXT,
        tokens_in INTEGER,
        tokens_out INTEGER,
        PRIMARY KEY (day, session_id)
    );

    CREATE TABLE IF NOT EXISTS project_releases (
        repo_name TEXT PRIMARY KEY,
        version TEXT,
        title TEXT,
        release_date TIMESTAMP,
        url TEXT
    );

    CREATE TABLE IF NOT EXISTS system_config_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        description TEXT,
        type TEXT,
        path TEXT
    );

    -- Seed system files if table is empty
    INSERT INTO system_config_files (name, description, type, path)
    SELECT 'SOUL.md', 'Nia''s personality, role, and core instructions.', 'MARKDOWN', '/home/daniel/.openclaw/workspace/SOUL.md'
    WHERE NOT EXISTS (SELECT 1 FROM system_config_files WHERE name = 'SOUL.md');

    INSERT INTO system_config_files (name, description, type, path)
    SELECT 'USER.md', 'Information about Daniel, preferences, and goals.', 'MARKDOWN', '/home/daniel/.openclaw/workspace/USER.md'
    WHERE NOT EXISTS (SELECT 1 FROM system_config_files WHERE name = 'USER.md');

    INSERT INTO system_config_files (name, description, type, path)
    SELECT 'MEMORY.md', 'Long-term project memory and decisions.', 'MARKDOWN', '/home/daniel/.openclaw/workspace/MEMORY.md'
    WHERE NOT EXISTS (SELECT 1 FROM system_config_files WHERE name = 'MEMORY.md');

    INSERT INTO system_config_files (name, description, type, path)
    SELECT 'AGENTS.md', 'Workspace rules and team coordination.', 'MARKDOWN', '/home/daniel/.openclaw/workspace/AGENTS.md'
    WHERE NOT EXISTS (SELECT 1 FROM system_config_files WHERE name = 'AGENTS.md');
    ''')

    conn.commit()
    conn.close()
    print("✅ Database tables ensured.")

if __name__ == '__main__':
    fix_database()
