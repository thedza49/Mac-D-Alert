#!/usr/bin/env python3
"""
pi_monitor.py
Background task to log Raspberry Pi hardware stats to the database.
"""

import sqlite3
import subprocess
import os
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "sovson_analytics.db"

def get_stats():
    # CPU Usage (%)
    cpu = subprocess.check_output("top -bn1 | grep 'Cpu(s)' | awk '{print $2 + $4}'", shell=True).decode().strip()
    
    # Memory Usage (%)
    mem = subprocess.check_output("free | grep Mem | awk '{print $3/$2 * 100.0}'", shell=True).decode().strip()
    
    # Disk Usage (%) on /
    disk = subprocess.check_output("df -h / | awk 'NR==2 {print $5}' | sed 's/%//'", shell=True).decode().strip()
    
    # Temperature (C)
    temp = subprocess.check_output("vcgencmd measure_temp | sed \"s/temp=//;s/'C//\"", shell=True).decode().strip()
    
    # Load Averages (1m, 5m, 15m)
    load = subprocess.check_output("cat /proc/loadavg | awk '{print $1, $2, $3}'", shell=True).decode().strip().split()
    
    return {
        "cpu": float(cpu),
        "mem": float(mem),
        "disk": float(disk),
        "temp": float(temp),
        "load_1m": float(load[0]),
        "load_5m": float(load[1]),
        "load_15m": float(load[2])
    }

def log_stats():
    stats = get_stats()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Insert current stats
    cursor.execute("""
        INSERT INTO hardware_stats (cpu_usage, mem_usage, disk_usage, temp, load_1m, load_5m, load_15m)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (stats["cpu"], stats["mem"], stats["disk"], stats["temp"], stats["load_1m"], stats["load_5m"], stats["load_15m"]))
    
    # Cleanup records older than 30 days
    cursor.execute("DELETE FROM hardware_stats WHERE timestamp < datetime('now', '-30 days')")
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    log_stats()
