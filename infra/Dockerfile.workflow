# Dockerfile for scheduled workflow execution (task-based scheduler)
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install ffmpeg for audio processing, curl for fetching Essentia model files
RUN apt-get update && apt-get install -y ffmpeg curl && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt requirements-audio-features.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir -r requirements-audio-features.txt

# Download pretrained Essentia models for local audio-feature analysis. Two
# embedding pipelines are needed:
# - discogs-effnet, feeding the approachability and mood_happy classifier heads
# - MusiCNN, feeding the DEAM arousal-valence regression head (energy = arousal)
# -f makes curl fail the build on a bad download rather than silently leaving a
# missing/partial model file.
RUN mkdir -p /app/models/essentia \
    && curl -f -L -o /app/models/essentia/discogs-effnet-bs64-1.pb \
        https://essentia.upf.edu/models/feature-extractors/discogs-effnet/discogs-effnet-bs64-1.pb \
    && curl -f -L -o /app/models/essentia/discogs-effnet-bs64-1.json \
        https://essentia.upf.edu/models/feature-extractors/discogs-effnet/discogs-effnet-bs64-1.json \
    && curl -f -L -o /app/models/essentia/approachability_2c-discogs-effnet-1.pb \
        https://essentia.upf.edu/models/classification-heads/approachability/approachability_2c-discogs-effnet-1.pb \
    && curl -f -L -o /app/models/essentia/approachability_2c-discogs-effnet-1.json \
        https://essentia.upf.edu/models/classification-heads/approachability/approachability_2c-discogs-effnet-1.json \
    && curl -f -L -o /app/models/essentia/mood_happy-discogs-effnet-1.pb \
        https://essentia.upf.edu/models/classification-heads/mood_happy/mood_happy-discogs-effnet-1.pb \
    && curl -f -L -o /app/models/essentia/mood_happy-discogs-effnet-1.json \
        https://essentia.upf.edu/models/classification-heads/mood_happy/mood_happy-discogs-effnet-1.json \
    && curl -f -L -o /app/models/essentia/msd-musicnn-1.pb \
        https://essentia.upf.edu/models/feature-extractors/musicnn/msd-musicnn-1.pb \
    && curl -f -L -o /app/models/essentia/msd-musicnn-1.json \
        https://essentia.upf.edu/models/feature-extractors/musicnn/msd-musicnn-1.json \
    && curl -f -L -o /app/models/essentia/deam-msd-musicnn-2.pb \
        https://essentia.upf.edu/models/classification-heads/deam/deam-msd-musicnn-2.pb \
    && curl -f -L -o /app/models/essentia/deam-msd-musicnn-2.json \
        https://essentia.upf.edu/models/classification-heads/deam/deam-msd-musicnn-2.json

# Copy application code
COPY . .

# Set PYTHONPATH so 'scripts' is importable as a package
ENV PYTHONPATH="/app"

# Disable Python output buffering to ensure logs appear immediately in Docker
ENV PYTHONUNBUFFERED=1

# Run the task scheduler in daemon mode
CMD ["python", "-m", "scripts.task_scheduler", "--daemon"]
