# Dockerfile for scheduled workflow execution (task-based scheduler)
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install ffmpeg for audio processing
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set PYTHONPATH so 'scripts' is importable as a package
ENV PYTHONPATH="/app"

# Run the task scheduler in daemon mode
CMD ["python", "-m", "scripts.task_scheduler", "--daemon"]
