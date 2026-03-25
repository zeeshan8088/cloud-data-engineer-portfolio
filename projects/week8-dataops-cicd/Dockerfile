# =============================================================================
# Stage 1: Builder
# Purpose: install all dependencies including build tools
# This stage is DISCARDED after building — it never ships to production
# =============================================================================
FROM python:3.11-slim AS builder

# Set working directory inside the builder container
WORKDIR /build

# Install system-level build dependencies
# These are needed to compile some Python packages but NOT needed at runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first — this is a Docker cache optimisation trick.
# Docker caches each layer. If requirements.txt hasn't changed, this layer
# is reused from cache and pip install is skipped. Massive time saver.
COPY etl/requirements.txt .

# Install dependencies into a specific folder we can copy later
RUN pip install --upgrade pip \
    && pip install --prefix=/install --no-cache-dir -r requirements.txt


# =============================================================================
# Stage 2: Runtime
# Purpose: the actual image that runs in production
# Only contains Python + our installed packages + our code — nothing else
# =============================================================================
FROM python:3.11-slim AS runtime

# Security best practice: never run as root inside a container
# Create a non-root user called 'appuser'
RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

# Copy ONLY the installed packages from the builder stage
# This is the key multi-stage move — we get the packages without the build tools
COPY --from=builder /install /usr/local

# Copy our application code
COPY etl/ .

# Switch to non-root user
USER appuser

# Environment variables with sensible defaults
ENV ENVIRONMENT=production
ENV HIGH_VALUE_THRESHOLD=1000.0
ENV PYTHONUNBUFFERED=1

# This is what runs when the container starts
CMD ["python", "etl_pipeline.py"]
