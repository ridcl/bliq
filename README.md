# Bliq - Dataset Catalog with Versioned Storage

Bliq is a lightweight dataset catalog that provides versioning, efficient storage, and easy querying of tabular data using Apache Arrow.

## Features

- **Versioned Datasets**: Track changes with automatic versioning (v1, v2, v3...)
- **Efficient Storage**: Block-level deduplication using Apache Arrow format
- **Flexible Querying**: SQL-like filtering with column selection
- **Multiple Storage Backends**: Local filesystem, S3, Azure Blob Storage
- **REST API**: Access datasets over HTTP
- **Python Client**: Simple, intuitive API for data scientists
- **Optional Server**: Self-host your own catalog or use as a library

## Installation

Bliq can be installed in different modes depending on your needs:

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
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})

result = client.create('team/users', 'User data', df)
print(f"Created: {result}")  # team/users/v1

# Load the dataset
df = client.load('team/users/v1')
print(df)

# Add more data (creates new version)
new_data = pd.DataFrame({
    'id': [4],
    'name': ['David'],
    'age': [40]
})

result = client.extend('team/users/v1', new_data)
print(f"Extended: {result}")  # team/users/v2

# Query with filtering
df = client.load('team/users/v2',
                 columns=['name', 'age'],
                 filter='age > 30',
                 limit=10)

# List all datasets
datasets = client.list(namespace='team')
for ds in datasets:
    print(f"{ds['name']}: {ds['row_count']} rows")

# Get detailed info
info = client.describe('team/users/v2')
print(info)

# Delete when done
client.erase('team/users/v1')  # Delete specific version
client.erase('team/users')     # Delete all versions
```

### Running the Server

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

### Server Configuration

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
docker build -f Dockerfile.prod -t bliq:latest .

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

See [DOCKER.md](DOCKER.md) for complete Docker documentation.

## Architecture

```
┌─────────────────────────────────────┐
│  Python Client (bliq)               │
│  - BliqClient                       │
│  - Pandas integration               │
└──────────────┬──────────────────────┘
               │ HTTP + Arrow IPC
               ▼
┌─────────────────────────────────────┐
│  Bliq Server (bliq[server])         │
│  - REST API (FastAPI)               │
│  - Dataset Manager                  │
│  - Migrations                       │
└──────────────┬──────────────────────┘
               │
               ├─────────────┬─────────────────┐
               ▼             ▼                 ▼
         ┌─────────┐   ┌──────────┐    ┌──────────┐
         │MetaStore│   │DataStore │    │DataStore │
         │(SQLite/ │   │(Local FS)│    │(S3/Azure)│
         │Postgres)│   └──────────┘    └──────────┘
         └─────────┘
```

## API Reference

### BliqClient

#### `create(name: str, description: str, data: DataFrame) -> str`
Create a new dataset.

**Parameters:**
- `name`: Dataset name without version (e.g., "team/users")
- `description`: Human-readable description
- `data`: pandas DataFrame with the data

**Returns:** Full versioned name (e.g., "team/users/v1")

#### `extend(name: str, data: DataFrame, create_new_version: bool = True) -> str`
Add data to an existing dataset.

**Parameters:**
- `name`: Versioned dataset name (e.g., "team/users/v1")
- `data`: pandas DataFrame to append
- `create_new_version`: If True, creates v2, v3, etc. If False, appends to existing version

**Returns:** Versioned name (may be new version)

#### `load(name: str, columns: List[str] = None, filter: str = None, limit: int = None) -> DataFrame`
Load a dataset.

**Parameters:**
- `name`: Versioned dataset name
- `columns`: Optional list of columns to select
- `filter`: Optional SQL WHERE clause (without WHERE keyword)
- `limit`: Optional row limit

**Returns:** pandas DataFrame

#### `describe(name: str) -> str`
Get detailed information about a dataset.

**Returns:** Human-readable description with schema and statistics

#### `list(namespace: str = None) -> List[dict]`
List all datasets, optionally filtered by namespace.

**Returns:** List of dataset metadata

#### `erase(name: str) -> None`
Delete a dataset or specific version.

**Parameters:**
- `name`: Either "namespace/dataset" (deletes all) or "namespace/dataset/v1" (deletes version)

## Storage Backends

### MetaStore (Metadata Database)

**SQLite** (default, development):
```bash
export METASTORE_URL="sqlite:////data/bliq/metastore.db"
```

**PostgreSQL** (recommended, production):
```bash
pip install bliq[server,postgresql]
export METASTORE_URL="postgresql://user:pass@host:5432/database"
```

### DataStore (Dataset Storage)

**Local Filesystem** (default):
```bash
export DATASTORE_URL="file:///data/datasets"
```

**S3** (AWS, MinIO, etc.):
```bash
pip install bliq[server,s3]
export DATASTORE_URL="s3://bucket-name/path"
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
```

**Azure Blob Storage**:
```bash
pip install bliq[server,azure]
export DATASTORE_URL="azure://container-name/path"
export AZURE_STORAGE_CONNECTION_STRING="..."
```

## Use Cases

### Data Science Teams
- Share datasets across team members
- Version control for data (like Git for datasets)
- Reproducible analysis with pinned versions

### Machine Learning Pipelines
- Store training/validation/test splits with versions
- Track data lineage and transformations
- Roll back to previous data versions

### Data Engineering
- Catalog data assets across organization
- Query datasets without moving data
- Incremental data updates with deduplication

### Self-Service Analytics
- Empower analysts to discover and access datasets
- Provide SQL-like filtering without database complexity
- Central catalog for all tabular data

## Documentation

- [DOCKER.md](DOCKER.md) - Complete Docker deployment guide
- [PUBLISHING.md](PUBLISHING.md) - How to publish to PyPI