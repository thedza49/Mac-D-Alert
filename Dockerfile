# Mac-D-Alert — Dockerfile
# Raspberry Pi 4 (linux/arm64)

FROM python:3.11-slim

# Install cron and dependencies
RUN apt-get update && apt-get install -y \
    cron \
    curl \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files
COPY . .

# Copy crontab into container and install it
COPY crontab.docker /etc/cron.d/macd-alert
RUN chmod 0644 /etc/cron.d/macd-alert
RUN crontab /etc/cron.d/macd-alert

# Create log directory
RUN mkdir -p /app/logs

# Expose Flask dashboard port
EXPOSE 5000

# Startup script — runs cron in background, then starts Flask dashboard
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

CMD ["/docker-entrypoint.sh"]
