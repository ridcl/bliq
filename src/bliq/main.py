"""
FastAPI REST API for Bliq Dataset Catalog.

Exposes DatasetManager functionality via HTTP endpoints.
"""

import os
import io
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, PlainTextResponse
from pydantic import BaseModel
import pyarrow as pa
import pyarrow.ipc

from bliq.manager import DatasetManager
from bliq.metastore import MetaStore
from bliq.datastore import LocalDataStore

# Initialize app
app = FastAPI(
    title="Bliq Dataset Catalog API",
    version="1.0",
    description="Dataset versioning and management with block-level storage",
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize MetaStore and DataStore
METASTORE_URL = os.getenv("METADATA_DB_URL", "sqlite:///./data/metadata.db")
DATASTORE_URL = os.getenv("STORAGE_BASE_PATH", "/data/catalogue")

metastore = MetaStore(METASTORE_URL)
datastore = LocalDataStore(DATASTORE_URL)  # TODO: make fully configurable via URL
manager = DatasetManager(metastore, datastore)


# ============================================================================
# Pydantic Models
# ============================================================================


class CreateRequest(BaseModel):
    """Request body for creating a dataset."""

    name: str
    description: str


class ExtendRequest(BaseModel):
    """Request body for extending a dataset."""

    name: str
    create_new_version: bool = True


class LoadRequest(BaseModel):
    """Query parameters for loading a dataset."""

    name: str
    columns: Optional[List[str]] = None
    filter: Optional[str] = None
    limit: Optional[int] = None


class EraseRequest(BaseModel):
    """Request body for erasing a dataset."""

    name: str


class DescribeRequest(BaseModel):
    """Request body for describing a dataset."""

    name: str


class ResponseModel(BaseModel):
    """Generic success response."""

    status: str
    message: str
    data: Optional[dict] = None


# ============================================================================
# Health Check
# ============================================================================


@app.get("/")
def root():
    """API root endpoint."""
    return {
        "service": "Bliq Dataset Catalog API",
        "version": "2.0",
        "endpoints": {
            "create": "/api/v1/datasets/create",
            "extend": "/api/v1/datasets/extend",
            "load": "/api/v1/datasets/load",
            "erase": "/api/v1/datasets/erase",
            "describe": "/api/v1/datasets/describe",
        },
    }


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "healthy"}


# ============================================================================
# Dataset Operations
# ============================================================================


@app.post("/api/v1/datasets/create")
async def create_dataset(request: Request, name: str, description: str):
    """
    Create a new dataset.

    Expects request body to be Apache Arrow IPC stream format.

    Args:
        name: Qualified name without version (e.g., "analytics/users")
        description: Dataset description

    Returns:
        Created dataset name with version (e.g., "analytics/users/v1")

    Example:
        POST /api/v1/datasets/create?name=analytics/users&description=User%20data
        Body: Arrow IPC stream
    """
    try:
        # Read Arrow IPC data from request body
        arrow_bytes = await request.body()
        if not arrow_bytes:
            raise HTTPException(status_code=400, detail="Request body is empty")

        buffer = io.BytesIO(arrow_bytes)

        # Deserialize Arrow IPC format
        with pa.ipc.open_stream(buffer) as reader:
            table = reader.read_all()

        # Create dataset via manager
        result_name = manager.create(name=name, description=description, data=table)

        return {
            "status": "success",
            "message": f"Dataset created: {result_name}",
            "data": {
                "name": result_name,
                "rows": len(table),
                "columns": len(table.schema),
            },
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to create dataset: {str(e)}"
        )


@app.post("/api/v1/datasets/extend")
async def extend_dataset(request: Request, name: str, create_new_version: bool = True):
    """
    Extend an existing dataset with new data.

    Expects request body to be Apache Arrow IPC stream format.

    Args:
        name: Qualified name with version (e.g., "analytics/users/v1")
        create_new_version: If True, creates new version (default). If False, extends existing version.

    Returns:
        Dataset name with version (may be new version if create_new_version=True)

    Example:
        POST /api/v1/datasets/extend?name=analytics/users/v1&create_new_version=true
        Body: Arrow IPC stream
    """
    try:
        # Read Arrow IPC data from request body
        arrow_bytes = await request.body()
        if not arrow_bytes:
            raise HTTPException(status_code=400, detail="Request body is empty")

        buffer = io.BytesIO(arrow_bytes)

        # Deserialize Arrow IPC format
        with pa.ipc.open_stream(buffer) as reader:
            table = reader.read_all()

        # Extend dataset via manager
        result_name = manager.extend(
            name=name, data=table, create_new_version=create_new_version
        )

        return {
            "status": "success",
            "message": f"Dataset extended: {result_name}",
            "data": {
                "name": result_name,
                "rows_added": len(table),
                "new_version_created": create_new_version,
            },
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to extend dataset: {str(e)}"
        )


@app.get("/api/v1/datasets/load")
async def load_dataset(
    name: str,
    columns: Optional[str] = Query(None, description="Comma-separated list of columns"),
    filter: Optional[str] = Query(
        None, description="SQL WHERE clause (without WHERE keyword)"
    ),
    limit: Optional[int] = Query(None, description="Maximum number of rows"),
):
    """
    Load a dataset.

    Returns data as Apache Arrow IPC stream format.

    Args:
        name: Qualified name with version (e.g., "analytics/users/v1")
        columns: Optional comma-separated list of columns to select
        filter: Optional SQL WHERE clause (without WHERE keyword)
        limit: Optional row limit

    Returns:
        Arrow IPC stream

    Example:
        GET /api/v1/datasets/load?name=analytics/users/v1&columns=id,name&limit=100
    """
    try:
        # Parse columns if provided
        column_list = columns.split(",") if columns else None

        # Load dataset via manager
        table = manager.load(name=name, columns=column_list, filter=filter, limit=limit)

        # Serialize to Arrow IPC format
        buffer = io.BytesIO()
        with pa.ipc.new_stream(buffer, table.schema) as writer:
            writer.write_table(table)

        arrow_bytes = buffer.getvalue()

        # Return as Arrow IPC response
        return Response(
            content=arrow_bytes,
            media_type="application/vnd.apache.arrow.stream",
            headers={
                "X-Row-Count": str(len(table)),
                "X-Column-Count": str(len(table.schema)),
            },
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load dataset: {str(e)}")


@app.delete("/api/v1/datasets/erase")
async def erase_dataset(name: str):
    """
    Erase a dataset or version.

    Args:
        name: Qualified name, either:
              - "namespace/dataset_name" - erases entire dataset
              - "namespace/dataset_name/version" - erases specific version

    Returns:
        Success message

    Example:
        DELETE /api/v1/datasets/erase?name=analytics/users
        DELETE /api/v1/datasets/erase?name=analytics/users/v1
    """
    try:
        # Erase dataset via manager
        manager.erase(name=name)

        return {
            "status": "success",
            "message": f"Dataset erased: {name}",
        }

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to erase dataset: {str(e)}"
        )


@app.get("/api/v1/datasets/describe", response_class=PlainTextResponse)
async def describe_dataset(name: str):
    """
    Describe a dataset.

    Returns human-readable description with schema and statistics.

    Args:
        name: Qualified name with version (e.g., "analytics/users/v1")

    Returns:
        Plain text description

    Example:
        GET /api/v1/datasets/describe?name=analytics/users/v1
    """
    try:
        # Describe dataset via manager
        description = manager.describe(name=name)

        return description

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to describe dataset: {str(e)}"
        )


# ============================================================================
# Utility Endpoints
# ============================================================================


@app.get("/api/v1/datasets/list")
async def list_datasets(namespace: Optional[str] = None):
    """
    List all datasets (optional: filtered by namespace).

    Args:
        namespace: Optional namespace filter

    Returns:
        List of dataset version information

    Example:
        GET /api/v1/datasets/list
        GET /api/v1/datasets/list?namespace=analytics
    """
    try:
        datasets = manager.list(namespace=namespace)
        return {
            "status": "success",
            "data": datasets,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to list datasets: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
