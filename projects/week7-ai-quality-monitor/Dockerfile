# Dockerfile
# ----------------------------------------------------------
# Purpose: Package the AI quality monitor pipeline into a
#          container that Cloud Run Jobs can execute.
#
# Think of this file as the recipe for building your lunchbox.
# It tells Docker: start with Python, add our code,
# install libraries, then set the default command to run.
# ----------------------------------------------------------

# Start with official Python 3.11 slim image
# "slim" means minimal OS — smaller container, faster deploys
FROM python:3.11-slim

# Set working directory inside the container
# Think of this as "cd /app" inside the container
WORKDIR /app

# Copy requirements first (before code)
# Why? Docker caches layers. If requirements don't change,
# Docker skips reinstalling them on rebuild — much faster.
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# Set environment variable placeholder
# The actual value is injected at runtime by Cloud Run
# Never bake secrets into the image itself
ENV GEMINI_API_KEY=""

# Default command: run the pipeline
CMD ["python", "pipeline.py"]