# Dockerfile for scheduled workflow execution
FROM python:3.11-slim

# Install cron
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*
COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .


# Copy cron file and entrypoint from infra
COPY infra/workflow.cron /app/infra/workflow.cron
COPY infra/entrypoint-workflow.sh /app/infra/entrypoint-workflow.sh

# Convert Windows line endings to Unix and make entrypoint executable
RUN sed -i 's/\r$//' /app/infra/workflow.cron && \
	sed -i 's/\r$//' /app/infra/entrypoint-workflow.sh && \
	chmod +x /app/infra/entrypoint-workflow.sh

# Run entrypoint script
CMD ["/app/infra/entrypoint-workflow.sh"]
