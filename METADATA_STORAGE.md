# Metadata Storage Implementation

This document describes the metadata storage system for the Bliq dataset catalog.

## Overview

The metadata storage layer tracks datasets, versions, and data blocks (parquet files) in either SQLite or PostgreSQL. It provides a rich metadata API for listing datasets, managing versions, and tracking block locations.

## Architecture

### Components

1. **MetaStore**: Abstract interface defining metadata operations
2. **UnifiedMetaStore**: Unified implementation supporting both SQLite and PostgreSQL
3. **MigrationRunner**: Handles database schema migrations
4. **CLI Tool**: Command-line interface for migrations and management

### Database Schema

```sql
datasets
├── id (PRIMARY KEY)
├── namespace (e.g., "team-ml", "project-alpha")
├── name
├── description
├── created_at
└── updated_at

dataset_versions
├── id (PRIMARY KEY)
├── dataset_id (FOREIGN KEY → datasets.id)
├── version
├── description
├── row_count
├── file_count
├── size_bytes
├── schema_json
├── created_by
└── created_at

dataset_blocks
├── id (PRIMARY KEY)
├── version_id (FOREIGN KEY → dataset_versions.id)
├── block_number
├── relative_url (path relative to storage base URL)
├── size_bytes
├── row_count
└── created_at
```

## Configuration

### Environment Variables

```bash
# Use SQLite (default)
METADATA_DB_URL=sqlite:///./data/metadata.db

# Or use PostgreSQL
METADATA_DB_URL=postgresql://user:password@localhost:5432/bliq_metadata
```

### Python Code

```python
from bliq.metastore import create_metadata_store
from bliq.datastore import create_local_storage

# Create metadata store (uses METADATA_DB_URL env var)
metadata_store = create_metadata_store()

# Or explicitly specify connection string
metadata_store = create_metadata_store("sqlite:///./my_metadata.db")

# Create storage with metadata
storage = create_local_storage("/data/catalogue", metadata_store=metadata_store)

# Save dataset (automatically updates metadata)
storage.save(df, "my-dataset", "v1", namespace="analytics", description="Initial version")
```

## Database Migrations

### Running Migrations

```bash
# Run all pending migrations
bliq migrate

# Or with explicit connection string
bliq migrate --connection-string postgresql://user:pass@localhost/bliq

# Check migration status
bliq migration-status
```

### Creating New Migrations

1. Create a new SQL file in `src/bliq/migrations/versions/`:
   ```
   src/bliq/migrations/versions/002_add_dataset_owner.sql
   ```

2. Write the migration SQL:
   ```sql
   -- Add owner field to datasets table
   ALTER TABLE datasets ADD COLUMN owner VARCHAR(255);

   -- Add index for faster lookups
   CREATE INDEX idx_datasets_owner ON datasets(owner);
   ```

3. Run migrations:
   ```bash
   bliq migrate
   ```

### Migration Naming Convention

- Format: `{version}_{description}.sql`
- Version: 3-digit zero-padded number (001, 002, 003, ...)
- Description: Snake_case description
- Examples:
  - `001_initial_schema.sql`
  - `002_add_dataset_owner.sql`
  - `003_add_dataset_tags.sql`

### Dialect-Specific SQL

The migration runner automatically adapts SQL for the target dialect:

- `AUTOINCREMENT` (SQLite) ↔ `SERIAL` (PostgreSQL)
- `INTEGER PRIMARY KEY AUTOINCREMENT` → `SERIAL PRIMARY KEY`

Write migrations in SQLite syntax, and they'll be automatically adapted for PostgreSQL.

## API Endpoints

### Dataset Management

```http
# List datasets in a namespace
GET /api/catalog/namespaces/{namespace}/datasets?limit=100&offset=0

# Get dataset info
GET /api/catalog/namespaces/{namespace}/datasets/{dataset_name}

# List versions for a dataset
GET /api/catalog/namespaces/{namespace}/datasets/{dataset_name}/versions

# Get version details including blocks
GET /api/catalog/namespaces/{namespace}/datasets/{dataset_name}/versions/{version}
```

### Save Dataset with Metadata

```http
POST /api/catalog/datasets/save?dataset_name=users&version=v1&namespace=analytics&description=Initial+version
Content-Type: application/vnd.apache.arrow.stream

[Arrow IPC stream data]
```

## CLI Commands

```bash
# Run migrations
bliq migrate

# Check migration status
bliq migration-status

# List all datasets
bliq list-datasets

# List datasets in a namespace
bliq list-datasets --namespace analytics

# Show dataset details
bliq show-dataset analytics my-dataset
```

## Python API

### Basic Usage

```python
from bliq.metastore import create_metadata_store

# Create store
store = create_metadata_store()

# Create dataset
dataset = store.create_dataset(
    namespace="analytics",
    name="user-events",
    description="User event logs"
)

# Create version
version = store.create_version(
    dataset_id=dataset.id,
    version="v1",
    description="Initial load",
    row_count=1000000,
    file_count=10
)

# Add blocks
for i in range(10):
    store.add_block(
        version_id=version.id,
        block_number=i,
        relative_url=f"user-events/v1/part-{i:04d}.parquet",
        row_count=100000
    )

# Query metadata
datasets = store.list_datasets(namespace="analytics")
versions = store.list_versions(dataset.id)
blocks = store.get_blocks(version.id)
```

### Integration with Storage

The storage layer automatically updates metadata when saving datasets:

```python
from bliq.datastore import create_local_storage
from bliq.metastore import create_metadata_store

# Setup
metadata_store = create_metadata_store()
storage = create_local_storage("/data/catalogue", metadata_store=metadata_store)

# Save dataset (metadata is automatically updated)
import pandas as pd
df = pd.DataFrame({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})
storage.save(
    df,
    dataset_name="test-data",
    version="v1",
    namespace="experiments",
    description="Test dataset"
)

# Metadata is now populated:
# - datasets table has entry for "experiments/test-data"
# - dataset_versions table has entry for "v1"
# - dataset_blocks table has entry for the parquet file
```

## Production Deployment

### PostgreSQL Setup

1. Create database:
   ```sql
   CREATE DATABASE bliq_metadata;
   CREATE USER bliq_user WITH PASSWORD 'secure_password';
   GRANT ALL PRIVILEGES ON DATABASE bliq_metadata TO bliq_user;
   ```

2. Set connection string:
   ```bash
   export METADATA_DB_URL="postgresql://bliq_user:secure_password@localhost:5432/bliq_metadata"
   ```

3. Run migrations:
   ```bash
   pip install bliq[postgresql]  # Installs psycopg2
   bliq migrate
   ```

4. Start application:
   ```bash
   uvicorn bliq.main:app --host 0.0.0.0 --port 8000
   ```

### Docker Deployment

```dockerfile
FROM python:3.11

WORKDIR /app
COPY . /app

RUN pip install -e .[postgresql]

# Run migrations on startup
CMD bliq migrate && uvicorn bliq.main:app --host 0.0.0.0 --port 8000
```

### Migration Best Practices

1. **Always test migrations on a copy of production data first**
2. **Migrations are forward-only** (no automatic rollback)
3. **Keep migrations small and focused**
4. **Add indexes for frequently queried columns**
5. **Use transactions** (automatically handled by runner)
6. **Backup database before running migrations in production**

## Troubleshooting

### Migration Fails Midway

The migration runner uses transactions, so partial migrations should roll back. If a migration fails:

1. Check the error message
2. Fix the SQL file
3. The migration will retry on next run

### SQLite Database Locked

If you see "database is locked" errors:
- Ensure no other processes are accessing the database
- Use PostgreSQL for multi-process scenarios
- Close connections properly in your code

### Connection String Format

```bash
# SQLite
sqlite:///./data/metadata.db           # Relative path
sqlite:////absolute/path/metadata.db   # Absolute path (4 slashes!)

# PostgreSQL
postgresql://user:password@host:5432/database
postgresql://user:password@host/database  # Default port 5432
```

## Future Enhancements

Potential improvements for the metadata system:

1. **Dataset tags/labels** for categorization
2. **Access control** (user permissions per namespace)
3. **Audit logging** (track all changes)
4. **Data lineage** (track dataset provenance)
5. **Statistics tracking** (query counts, popular datasets)
6. **Soft deletes** (mark datasets as deleted instead of removing)
7. **Rollback migrations** (down migrations)
8. **Data quality metrics** (NULL counts, value distributions)
