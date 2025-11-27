#!/bin/bash
# Entrypoint script for workflow container with cron

# Create log file
touch /var/log/cron.log

# Export environment variables for cron jobs
printenv | grep -v "no_proxy" >> /etc/environment

# Load crontab
crontab /app/infra/workflow.cron

# Log cron startup
echo "$(date) - Cron service starting" >> /var/log/cron.log
crontab -l >> /var/log/cron.log

# Start cron in foreground (blocks, keeping container alive)
cron -f
