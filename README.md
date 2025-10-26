# Bliq - Dataset Catalog with Metadata Storage

A dataset catalog system with efficient metadata storage, versioning, and querying capabilities. Bliq provides both a web interface and Python API for managing versioned datasets with support for multiple storage backends (local disk, Azure Blob, AWS S3).

## Features

- **Versioned Dataset Management**: Create and manage multiple versions of datasets with copy-on-write semantics
- **Flexible Storage**: Support for local disk, Azure Blob Storage, and AWS S3
- **Efficient Querying**: DuckDB-powered queries with predicate pushdown and column pruning
- **Metadata Store**: SQLite or PostgreSQL metadata storage for tracking datasets, versions, and blocks
- **RESTful API**: FastAPI-based backend with Arrow IPC format support
- **Web Interface**: React-based frontend for browsing and managing datasets

## Project Structure

```
bliq/
├── src/bliq/
│   ├── main.py           # FastAPI backend server
│   ├── manager.py        # DatasetManager - high-level API
│   ├── metastore.py      # Metadata storage (SQLite/PostgreSQL)
│   ├── datastore.py      # Data storage backends (Local/Azure/S3)
│   └── migrations/       # Database migrations
├── frontend/             # React + Vite frontend
├── tests/               # Test suite
└── pyproject.toml       # Python dependencies
```

## Quick Start

### Prerequisites

- Python 3.11+
- Node.js 18+ (for frontend)
- uv (Python package manager) - [Install uv](https://github.com/astral-sh/uv)

### 1. Backend Setup

#### Install Dependencies

```bash
# Using uv (recommended)
uv sync

# Or using pip
pip install -e .
```

#### Configure Environment

Create a `.env` file (or copy from `.env.example`):

```bash
# Metadata Database (SQLite for development)
METADATA_DB_URL=sqlite:///./data/metadata.db

# Local Storage Path
STORAGE_BASE_PATH=/data/catalogue

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
```

#### Launch Backend Server

```bash
# Using uv
uv run uvicorn bliq.main:app --reload --host 0.0.0.0 --port 8000

# Or using uvicorn directly
uvicorn bliq.main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at: http://localhost:8000

API documentation: http://localhost:8000/docs

### 2. Frontend Setup

#### Install Dependencies

```bash
cd frontend
npm install
```

#### Launch Development Server

```bash
npm run dev
```

The frontend will be available at: http://localhost:5173

#### Build for Production

```bash
npm run build
```

### 3. Using DatasetManager Directly

The `DatasetManager` provides a high-level Python API for working with datasets directly, without needing the web server.

#### Basic Setup with SQLite + Local Disk

```python
import pandas as pd
import pyarrow as pa
from bliq.metastore import create_metadata_store
from bliq.datastore import create_local_storage
from bliq.manager import DatasetManager

# Initialize metadata store (SQLite)
metastore = create_metadata_store("sqlite:///data/metadata.db")

# Initialize data store (local disk)
datastore = create_local_storage("/data/catalogue", metadata_store=metastore)

# Create DatasetManager
manager = DatasetManager(metastore, datastore)
```

#### Create a Dataset

```python
# Prepare some data
df = pd.DataFrame({
    'user_id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
    'age': [25, 30, 35, 28, 32],
    'city': ['New York', 'San Francisco', 'Chicago', 'Boston', 'Seattle']
})

# Convert to Arrow Table
table = pa.Table.from_pandas(df)

# Create dataset (version v1)
version_name = manager.create(
    name="analytics/users",
    description="User demographics dataset",
    data=table
)
print(f"Created: {version_name}")  # Output: analytics/users/v1
```

#### Load a Dataset

```python
# Load entire dataset
table = manager.load("analytics/users/v1")
df = table.to_pandas()
print(df)

# Load with filters
table = manager.load(
    "analytics/users/v1",
    columns=["name", "age"],
    filter="age > 25",
    limit=10
)

# Load specific columns
table = manager.load(
    "analytics/users/v1",
    columns=["user_id", "name"]
)
```

#### Extend a Dataset (Add Data)

```python
# New data to add
new_df = pd.DataFrame({
    'user_id': [6, 7, 8],
    'name': ['Frank', 'Grace', 'Henry'],
    'age': [29, 31, 27],
    'city': ['Austin', 'Denver', 'Miami']
})
new_table = pa.Table.from_pandas(new_df)

# Option 1: Create new version (immutable, copy-on-write)
new_version = manager.extend(
    "analytics/users/v1",
    new_table,
    create_new_version=True
)
print(f"Created: {new_version}")  # Output: analytics/users/v2

# Option 2: Add to existing version (mutable)
manager.extend(
    "analytics/users/v1",
    new_table,
    create_new_version=False
)
```

#### Describe a Dataset

```python
info = manager.describe("analytics/users/v1")
print(info)
```

Output:
```
Dataset: analytics/users
Version: v1
Description: User demographics dataset

Statistics:
  Rows: 5
  Blocks: 1
  Size: 0.00 MB
  Created at: 2025-01-15 10:30:00

Schema:
  user_id: int64
  name: object
  age: int64
  city: object

Blocks:
  [0] block-a1b2c3d4e5f6.parquet (5 rows)
```

#### Delete a Dataset or Version

```python
# Delete specific version
manager.erase("analytics/users/v1")

# Delete entire dataset (all versions)
manager.erase("analytics/users")
```

### Complete Example Script

```python
#!/usr/bin/env python3
"""
Example script demonstrating DatasetManager usage with SQLite and local storage.
"""

import pandas as pd
import pyarrow as pa
from bliq.metastore import create_metadata_store
from bliq.datastore import create_local_storage
from bliq.manager import DatasetManager

def main():
    # Setup
    print("Initializing DatasetManager...")
    metastore = create_metadata_store("sqlite:///./data/metadata.db")
    datastore = create_local_storage("/data/catalogue", metadata_store=metastore)
    manager = DatasetManager(metastore, datastore)

    # Create sample dataset
    print("\n1. Creating dataset...")
    df = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=100),
        'revenue': range(1000, 1100),
        'region': ['US'] * 50 + ['EU'] * 50
    })
    table = pa.Table.from_pandas(df)

    version = manager.create(
        name="sales/daily",
        description="Daily sales data",
        data=table
    )
    print(f"Created: {version}")

    # Describe dataset
    print("\n2. Dataset information:")
    print(manager.describe(version))

    # Load with filter
    print("\n3. Loading filtered data...")
    filtered = manager.load(
        version,
        columns=["date", "revenue"],
        filter="region = 'US'",
        limit=5
    )
    print(filtered.to_pandas())

    # Extend dataset
    print("\n4. Extending dataset with new data...")
    new_df = pd.DataFrame({
        'date': pd.date_range('2024-04-11', periods=10),
        'revenue': range(1100, 1110),
        'region': ['APAC'] * 10
    })
    new_table = pa.Table.from_pandas(new_df)

    v2 = manager.extend(version, new_table, create_new_version=True)
    print(f"Created new version: {v2}")

    # Compare versions
    print("\n5. Comparing versions:")
    print(f"v1 info:\n{manager.describe(version)}")
    print(f"\nv2 info:\n{manager.describe(v2)}")

    print("\nDone!")

if __name__ == "__main__":
    main()
```

Save this as `example.py` and run:
```bash
uv run python example.py
```

## Storage Backends

### Local Filesystem

```python
from bliq.datastore import create_local_storage

datastore = create_local_storage("/path/to/data")
```

### Azure Blob Storage

```python
from bliq.datastore import create_azure_storage

# Using connection string
datastore = create_azure_storage(
    connection_string="DefaultEndpointsProtocol=https;AccountName=...",
    container_name="datasets"
)

# Using account name (with DefaultAzureCredential)
datastore = create_azure_storage(
    account_name="mystorageaccount",
    container_name="datasets"
)
```

### AWS S3

```python
from bliq.datastore import create_s3_storage

datastore = create_s3_storage(
    bucket_name="my-datasets",
    region="us-east-1",
    access_key_id="...",
    secret_access_key="..."
)
```

## Metadata Store Options

### SQLite (Development)

```python
from bliq.metastore import create_metadata_store

metastore = create_metadata_store("sqlite:///./data/metadata.db")
```

### PostgreSQL (Production)

```python
metastore = create_metadata_store(
    "postgresql://user:password@localhost:5432/bliq_metadata"
)
```

## API Endpoints

### Dataset Operations

- `POST /api/catalog/datasets/save` - Save a dataset (Arrow IPC format)
- `GET /api/catalog/datasets/load` - Load a dataset with filtering
- `GET /api/catalog/datasets/schema` - Get dataset schema
- `GET /api/catalog/datasets/statistics` - Get dataset statistics

### Metadata Operations

- `GET /api/catalog/namespaces/{namespace}/datasets` - List datasets in namespace
- `GET /api/catalog/namespaces/{namespace}/datasets/{name}` - Get dataset info
- `GET /api/catalog/namespaces/{namespace}/datasets/{name}/versions` - List versions
- `GET /api/catalog/namespaces/{namespace}/datasets/{name}/versions/{version}` - Get version info

## Development

### Run Tests

```bash
uv run pytest
```

### Code Formatting

```bash
uv run black src/
uv run isort src/
```

### Type Checking

```bash
uv run mypy src/
```

## Architecture

### Dataset Versioning

Bliq uses copy-on-write semantics for dataset versions:

- **Blocks**: Data is stored in parquet blocks (files) with unique IDs
- **Versions**: Each version references a list of blocks
- **Sharing**: Blocks can be shared between versions (efficient storage)
- **Immutability**: Creating a new version doesn't copy data, only metadata

```
namespace/dataset_name/
  ├── block-a1b2c3d4.parquet  (shared by v1, v2)
  ├── block-e5f6g7h8.parquet  (only in v2)
  └── block-i9j0k1l2.parquet  (only in v2)
```

### Query Optimization

DuckDB provides efficient querying through:

- **Predicate pushdown**: Filters applied at parquet row group level
- **Column pruning**: Only requested columns are read
- **Metadata scanning**: Fast statistics without reading data
- **Streaming**: Results streamed without loading entire dataset

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]
