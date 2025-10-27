# Multi-stage Dockerfile for Bliq production deployment
# Builds frontend (React) and backend (FastAPI) in a single image

FROM node:20-slim AS frontend-build

WORKDIR /frontend

# Copy frontend package files
COPY frontend/package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy frontend source
COPY frontend/ ./

# Build frontend for production
RUN npm run build

# ============================================
# Backend build stage
# ============================================
FROM python:3.11-slim AS backend-build

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

# Copy backend files
COPY pyproject.toml uv.lock ./
COPY src/ ./src/

# Create venv and install dependencies
RUN uv venv /venv && \
    uv pip install --no-cache -r pyproject.toml

# ============================================
# Production runtime stage
# ============================================
FROM python:3.11-slim

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 bliq

WORKDIR /app

# Copy Python virtual environment from build stage
COPY --from=backend-build /venv /venv

# Copy backend source
COPY --from=backend-build /app/src ./src

# Copy built frontend from frontend-build stage into src directory
# (main.py expects it at ../frontend/dist relative to src/bliq/main.py)
COPY --from=frontend-build /frontend/dist ./frontend/dist

# Set up directories for data storage
RUN mkdir -p /data/metastore /data/datastore && \
    chown -R bliq:bliq /app /data

# Switch to non-root user
USER bliq

# Set environment variables
# These are defaults that can be overridden at runtime with -e or --env-file
ENV PATH="/venv/bin:${PATH}" \
    PYTHONPATH=/app \
    VIRTUAL_ENV=/venv

# Default storage locations (can be overridden)
ENV METASTORE_URL="sqlite:////data/metastore/bliq.db" \
    DATASTORE_URL="file:///data/datastore"

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run migrations and start server
# Migrations use METASTORE_URL from environment
CMD ["sh", "-c", "bliq migrate && uvicorn bliq.main:app --host 0.0.0.0 --port 8000"]
