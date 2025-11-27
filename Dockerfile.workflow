# Dockerfile for scheduled workflow execution
FROM python:3.11-slim

# Install cron
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Copy cron file and entrypoint
COPY workflow.cron /app/workflow.cron
COPY entrypoint-workflow.sh /app/entrypoint-workflow.sh

# Convert Windows line endings to Unix and make entrypoint executable
RUN sed -i 's/\r$//' /app/workflow.cron && \
    sed -i 's/\r$//' /app/entrypoint-workflow.sh && \
    chmod +x /app/entrypoint-workflow.sh

# Run entrypoint script
CMD ["/app/entrypoint-workflow.sh"]
