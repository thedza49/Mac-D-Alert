#!/bin/bash
# docker-entrypoint.sh
# Starts cron daemon in background, then runs Flask dashboard in foreground.

echo "Starting Mac-D-Alert..."

# Export environment variables for cron
printenv | grep -v "no_proxy" >> /etc/environment

# Start cron daemon
service cron start

# Run Flask dashboard (foreground — keeps container alive)
exec python3 /app/scripts/dashboard.py
