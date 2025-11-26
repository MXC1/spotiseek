#!/bin/bash
# Entrypoint script for workflow container with cron

# Create log file
touch /var/log/cron.log

# Load crontab
crontab /app/workflow.cron

# Start cron in foreground and tail the log
cron && tail -f /var/log/cron.log
