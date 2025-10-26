"""
Bliq Client

A client library for interacting with the Bliq Dataset Catalog server.
Uses Apache Arrow for efficient data serialization over HTTP.
"""

import io
from typing import Optional, List, Union
import pandas as pd
import pyarrow as pa
import requests


class BliqClient:
    """
    Client for interacting with the Bliq Dataset Catalog server.

    Uses Apache Arrow IPC format for efficient data transfer between
    client and server. Mirrors the DatasetManager API.

    Example:
        >>> client = BliqClient("http://localhost:8000")
        >>>
        >>> # Create a dataset
        >>> import pandas as pd
        >>> df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
        >>> result = client.create('test/users', 'User data', df)
        >>> print(result)  # 'test/users/v1'
        >>>
        >>> # Load dataset
        >>> df = client.load('test/users/v1', limit=10)
        >>>
        >>> # Describe dataset
        >>> info = client.describe('test/users/v1')
        >>> print(info)
    """

    def __init__(self, base_url: str = "http://localhost:8000"):
        """
        Initialize the Bliq client.

        Args:
            base_url: Base URL of the catalog server (default: http://localhost:8000)
        """
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def create(
        self,
        name: str,
        description: str,
        data: Union[pd.DataFrame, pa.Table],
    ) -> str:
        """
        Create a new dataset.

        Args:
            name: Qualified name without version (e.g., "analytics/users")
            description: Dataset description
            data: Data as pandas DataFrame or PyArrow Table

        Returns:
            Created dataset name with version (e.g., "analytics/users/v1")

        Raises:
            requests.HTTPError: If dataset already exists or other errors

        Example:
            >>> df = pd.DataFrame({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})
            >>> result = client.create('test/data', 'Test dataset', df)
            >>> print(result)
            'test/data/v1'
        """
        # Convert to Arrow Table if needed
        if isinstance(data, pd.DataFrame):
            arrow_table = pa.Table.from_pandas(data)
        else:
            arrow_table = data

        # Serialize to Arrow IPC format
        buffer = io.BytesIO()
        with pa.ipc.new_stream(buffer, arrow_table.schema) as writer:
            writer.write_table(arrow_table)

        arrow_bytes = buffer.getvalue()

        # Send to server
        url = f"{self.base_url}/api/v1/datasets/create"
        params = {
            "name": name,
            "description": description,
        }

        response = self.session.post(
            url,
            data=arrow_bytes,
            params=params,
            headers={
                "Content-Type": "application/vnd.apache.arrow.stream",
            },
        )

        response.raise_for_status()
        result = response.json()
        return result["data"]["name"]

    def extend(
        self,
        name: str,
        data: Union[pd.DataFrame, pa.Table],
        create_new_version: bool = True,
    ) -> str:
        """
        Extend an existing dataset with new data.

        Args:
            name: Qualified name with version (e.g., "analytics/users/v1")
            data: Data as pandas DataFrame or PyArrow Table
            create_new_version: If True, creates new version (default).
                               If False, extends existing version.

        Returns:
            Dataset name with version (may be new version if create_new_version=True)

        Raises:
            requests.HTTPError: If source version doesn't exist or other errors

        Example:
            >>> # Create new version
            >>> df = pd.DataFrame({'id': [4, 5], 'value': ['d', 'e']})
            >>> result = client.extend('test/data/v1', df, create_new_version=True)
            >>> print(result)
            'test/data/v2'

            >>> # Extend existing version
            >>> result = client.extend('test/data/v1', df, create_new_version=False)
            >>> print(result)
            'test/data/v1'
        """
        # Convert to Arrow Table if needed
        if isinstance(data, pd.DataFrame):
            arrow_table = pa.Table.from_pandas(data)
        else:
            arrow_table = data

        # Serialize to Arrow IPC format
        buffer = io.BytesIO()
        with pa.ipc.new_stream(buffer, arrow_table.schema) as writer:
            writer.write_table(arrow_table)

        arrow_bytes = buffer.getvalue()

        # Send to server
        url = f"{self.base_url}/api/v1/datasets/extend"
        params = {
            "name": name,
            "create_new_version": create_new_version,
        }

        response = self.session.post(
            url,
            data=arrow_bytes,
            params=params,
            headers={
                "Content-Type": "application/vnd.apache.arrow.stream",
            },
        )

        response.raise_for_status()
        result = response.json()
        return result["data"]["name"]

    def load(
        self,
        name: str,
        columns: Optional[List[str]] = None,
        filter: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Load a dataset.

        Args:
            name: Qualified name with version (e.g., "analytics/users/v1")
            columns: Optional list of columns to select
            filter: Optional SQL WHERE clause (without WHERE keyword)
            limit: Optional maximum number of rows

        Returns:
            pandas DataFrame containing the dataset

        Raises:
            requests.HTTPError: If dataset is not found or other errors

        Example:
            >>> # Load full dataset
            >>> df = client.load('test/data/v1')

            >>> # Load with filter and limit
            >>> df = client.load(
            ...     'test/data/v1',
            ...     columns=['id', 'name'],
            ...     filter='id > 10',
            ...     limit=100
            ... )
        """
        url = f"{self.base_url}/api/v1/datasets/load"

        params = {"name": name}

        if columns:
            params["columns"] = ",".join(columns)
        if filter:
            params["filter"] = filter
        if limit:
            params["limit"] = limit

        response = self.session.get(
            url,
            params=params,
            headers={
                "Accept": "application/vnd.apache.arrow.stream",
            },
        )

        response.raise_for_status()

        # Deserialize Arrow IPC format
        buffer = io.BytesIO(response.content)
        with pa.ipc.open_stream(buffer) as reader:
            arrow_table = reader.read_all()

        return arrow_table.to_pandas()

    def erase(self, name: str) -> None:
        """
        Completely erase a dataset or version, including data and metadata.

        Args:
            name: Qualified name, either:
                  - "namespace/dataset_name" - erases entire dataset
                  - "namespace/dataset_name/version" - erases specific version

        Raises:
            requests.HTTPError: If dataset or version doesn't exist

        Example:
            >>> # Erase entire dataset
            >>> client.erase('test/data')

            >>> # Erase only specific version
            >>> client.erase('test/data/v1')
        """
        url = f"{self.base_url}/api/v1/datasets/erase"
        params = {"name": name}

        response = self.session.delete(url, params=params)
        response.raise_for_status()

    def describe(self, name: str) -> str:
        """
        Describe a dataset.

        Returns human-readable description with schema and statistics.

        Args:
            name: Qualified name with version (e.g., "analytics/users/v1")

        Returns:
            Plain text description

        Raises:
            requests.HTTPError: If dataset version doesn't exist

        Example:
            >>> info = client.describe('test/data/v1')
            >>> print(info)
            Dataset: test/data
            Version: v1
            Description: Test dataset
            ...
        """
        url = f"{self.base_url}/api/v1/datasets/describe"
        params = {"name": name}

        response = self.session.get(url, params=params)
        response.raise_for_status()

        return response.text

    def list(self, namespace: Optional[str] = None) -> List[dict]:
        """
        List all dataset versions, optionally filtered by namespace.

        Args:
            namespace: Optional namespace filter (e.g., "test", "analytics")

        Returns:
            List of dicts with dataset information

        Raises:
            requests.HTTPError: If request fails

        Example:
            >>> datasets = client.list()
            >>> for ds in datasets:
            ...     print(f"{ds['name']}: {ds['row_count']} rows")

            >>> # Filter by namespace
            >>> test_datasets = client.list(namespace="test")
        """
        url = f"{self.base_url}/api/v1/datasets/list"
        params = {}

        if namespace:
            params["namespace"] = namespace

        response = self.session.get(url, params=params)
        response.raise_for_status()

        result = response.json()
        return result.get("data", [])

    def close(self):
        """Close the HTTP session."""
        self.session.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - closes session."""
        _ = exc_type, exc_val, exc_tb  # Unused but required by protocol
        self.close()
