# Multi-target Dockerfile for Bliq
# Build targets: backend, frontend

# ============================================
# Backend target
# ============================================
FROM python:3.11-slim AS backend

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv --break-system-packages

WORKDIR /app

# Copy backend files and install dependencies
COPY pyproject.toml uv.lock* README.md ./
COPY src/ ./src/

RUN uv venv /venv && \
    . /venv/bin/activate && \
    uv pip install -e ".[all]"

# Set up data directories
RUN mkdir -p /data/metastore /data/datastore

# Set environment variables
ENV PATH="/venv/bin:${PATH}" \
    PYTHONPATH=/app \
    VIRTUAL_ENV=/venv \
    METASTORE_URL="sqlite:////data/metastore/bliq.db" \
    DATASTORE_URL="file:///data/datastore"

# Expose backend port
EXPOSE 8000

# Run backend
CMD ["sh", "-c", "bliq migrate && uvicorn bliq.main:app --host 0.0.0.0 --port 8000"]

# ============================================
# Frontend target
# ============================================
FROM node:20-slim AS frontend

WORKDIR /app/frontend

# Copy frontend files and install dependencies
COPY frontend/package*.json ./
RUN npm install

# Copy frontend source
COPY frontend/ ./

# Expose frontend port
EXPOSE 5173

# Run frontend dev server
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "5173"]
