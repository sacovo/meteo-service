# Use Python 3.11 slim image as base
FROM python:3.12-slim AS base

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1 \
  DEBIAN_FRONTEND=noninteractive \
  PIP_NO_CACHE_DIR=1 \
  PIP_DISABLE_PIP_VERSION_CHECK=1

# Set work directory
WORKDIR /app

# Create cache mount points and install system dependencies
RUN --mount=type=cache,target=/var/cache/apt \
  --mount=type=cache,target=/var/lib/apt \
  apt-get update && apt-get install -y \
  build-essential \
  libgdal-dev \
  gdal-bin \
  libproj-dev \
  proj-data \
  proj-bin \
  libgeos-dev \
  libeccodes-dev \
  libeccodes-tools \
  libnetcdf-dev \
  libhdf5-dev \
  pkg-config \
  curl \
  wget \
  && apt-get clean


# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies with cache mount
RUN --mount=type=cache,target=/root/.cache/pip \
  pip install --no-cache-dir -r requirements.txt


# Create non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Copy application code
COPY . .

# Create necessary directories and set permissions
RUN mkdir -p /app/logs /app/cache /home/appuser && \
  chown -R appuser:appuser /app /home/appuser

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# Command to run the application
CMD ["python", "main.py"]
