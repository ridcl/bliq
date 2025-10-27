# Bliq

Bliq is a lightweight dataset catalog that provides versioning, efficient storage, and easy querying.

Bliq supports local filesystem, S3 and Azure Blob Storage for data storage, as well as SQLite and PostgreSQL for metadata.

Bliq consists of three principal components:

* client - lightweight CLI and a Python library
* API - a backend server in Python
* UI - a frontend in Node.js

## Run in Docker

The simplest way to run API and UI is using Docker image.

-- TODO: publish image, add example

For detaild instructions and examples, see [Docker.md].

## Local installation

### Client Only (Default)

For users who want to connect to existing Bliq servers:

```bash
pip install bliq
```

This installs only the client library with minimal dependencies (requests, pandas, pyarrow).

### With Server

To run your own Bliq server:

```bash
pip install bliq[server]
```

This includes FastAPI, uvicorn, and database management tools.

### With Storage Backends

```bash
# PostgreSQL support
pip install bliq[server,postgresql]

# S3 support
pip install bliq[server,s3]

# Azure Blob Storage support
pip install bliq[server,azure]

# Everything
pip install bliq[all]
```

## Quick Start

### Using the Client

```python
from bliq import BliqClient
import pandas as pd

# Connect to server
client = BliqClient("http://localhost:8000")

# Create a dataset
df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35]
})

result = client.create("team/users", "User data", df)
print(f"Created: {result}")  # team/users/v1

# Load the dataset
df = client.load("team/users/v1")
print(df)

# Add more data (creates new version)
new_data = pd.DataFrame({
    "id": [4],
    "name": ["David"],
    "age": [40]
})

result = client.extend("team/users/v1", new_data)
print(f"Extended: {result}")  # team/users/v2

# Query with filtering
df = client.load("team/users/v2",
                 columns=["name", "age"],
                 filter="age > 30",
                 limit=10)

# List all datasets
datasets = client.list(namespace="team")
for ds in datasets:
    print(f"{ds["name"]}: {ds["row_count"]} rows")

# Get detailed info
info = client.describe("team/users/v2")
print(info)

# Delete when done
client.erase("team/users/v1")  # Delete specific version
client.erase("team/users")     # Delete all versions
```

### Running the API

```bash
# Start server (runs migrations automatically)
bliq serve

# Custom port
bliq serve --port 9000

# Development mode with auto-reload
bliq serve --reload
```

The server will be available at http://localhost:8000 with:
- REST API: http://localhost:8000/api/v1/*
- API Docs: http://localhost:8000/docs
- Health Check: http://localhost:8000/health


Configure via environment variables:

```bash
# Metadata database (default: SQLite)
export METASTORE_URL="postgresql://user:pass@localhost:5432/bliq"

# Dataset storage (default: local filesystem)
export DATASTORE_URL="s3://my-bucket/datasets"

# For S3
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."

# For Azure
export AZURE_STORAGE_CONNECTION_STRING="..."

# Start server
bliq serve
```

### Running the UI

```bash
cd frontend
npm run dev
```

The UI will be available at http://localhost:5173

### CLI Commands

```bash
# Run database migrations
bliq migrate

# Check migration status
bliq migration-status

# List datasets
bliq list-datasets
bliq list-datasets --namespace team

# Show dataset details
bliq show-dataset team/users/v1

# Start server
bliq serve
```

## Docker Deployment

For production deployments with web UI:

```bash
# Build image
docker build -f Docker -t bliq:latest .

# Run with default settings (SQLite + local storage)
docker run -d \
  --name bliq \
  -p 8000:8000 \
  -v bliq-data:/data \
  bliq:latest

# Run with PostgreSQL + S3
docker run -d \
  --name bliq \
  -p 8000:8000 \
  -e METASTORE_URL="postgresql://user:pass@postgres:5432/bliq" \
  -e DATASTORE_URL="s3://bucket/path" \
  -e AWS_ACCESS_KEY_ID="..." \
  -e AWS_SECRET_ACCESS_KEY="..." \
  bliq:latest
```