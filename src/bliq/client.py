"""
Dataset Catalog Client

A client library for interacting with the ML Dataset Catalog server.
Uses Apache Arrow for efficient data serialization over HTTP.
"""

import io
from typing import Optional, List, Any
import pandas as pd
import pyarrow as pa
import requests


class DatasetCatalogClient:
    """
    Client for interacting with the Dataset Catalog server.

    Uses Apache Arrow IPC format for efficient data transfer between
    client and server. All dataset operations are performed through
    HTTP API calls.

    Example:
        >>> client = DatasetCatalogClient("http://localhost:8000")
        >>> df = pd.DataFrame({'id': [1, 2], 'value': ['a', 'b']})
        >>> client.save(df, "my-dataset", "v1")
        >>> loaded = client.load("my-dataset", "v1")
    """

    def __init__(self, base_url: str = "http://localhost:8000"):
        """
        Initialize the Dataset Catalog client.

        Args:
            base_url: Base URL of the catalog server (default: http://localhost:8000)
        """
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def save(
        self,
        df: pd.DataFrame,
        dataset_name: str,
        version: str,
        namespace: str = "default",
        description: Optional[str] = None,
    ) -> dict:
        """
        Save a pandas DataFrame to the catalog server.

        The DataFrame is serialized to Apache Arrow IPC format and sent
        to the server via HTTP POST. The server stores it as parquet files.

        Args:
            df: pandas DataFrame to save
            dataset_name: Name of the dataset
            version: Version identifier (e.g., "v1", "v2")
            namespace: Dataset namespace (default: "default")
            description: Optional description for this version

        Returns:
            Response dict with status and metadata

        Raises:
            requests.HTTPError: If the server returns an error (e.g., version exists)

        Example:
            >>> df = pd.DataFrame({
            ...     'user_id': [1, 2, 3],
            ...     'name': ['Alice', 'Bob', 'Charlie']
            ... })
            >>> client.save(df, "users", "v1", namespace="analytics")
            {'status': 'success', 'namespace': 'analytics', 'dataset': 'users', 'version': 'v1', 'rows': 3}
        """
        # Convert DataFrame to Arrow Table
        arrow_table = pa.Table.from_pandas(df)

        # Serialize to Arrow IPC format (aka Arrow Streaming format)
        buffer = io.BytesIO()
        with pa.ipc.new_stream(buffer, arrow_table.schema) as writer:
            writer.write_table(arrow_table)

        arrow_bytes = buffer.getvalue()

        # Send to server
        url = f"{self.base_url}/api/catalog/datasets/save"
        params = {
            "dataset_name": dataset_name,
            "version": version,
            "namespace": namespace,
        }
        if description:
            params["description"] = description

        response = self.session.post(
            url,
            data=arrow_bytes,
            params=params,
            headers={
                "Content-Type": "application/vnd.apache.arrow.stream",
            },
        )

        response.raise_for_status()
        return response.json()

    def load(
        self,
        dataset_name: str,
        version: str,
        filter_expression: Optional[str] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Load a dataset from the catalog server.

        The server returns the data in Apache Arrow IPC format for
        efficient deserialization.

        Args:
            dataset_name: Name of the dataset
            version: Version identifier
            filter_expression: Optional SQL WHERE clause (without WHERE keyword)
            columns: Optional list of columns to select
            limit: Optional row limit

        Returns:
            pandas DataFrame containing the dataset

        Raises:
            requests.HTTPError: If the dataset is not found or other errors

        Example:
            >>> # Load full dataset
            >>> df = client.load("users", "v1")

            >>> # Load with filter
            >>> df = client.load(
            ...     "users", "v1",
            ...     filter_expression="age > 25",
            ...     columns=["name", "email"]
            ... )
        """
        url = f"{self.base_url}/api/catalog/datasets/load"

        params = {
            "dataset_name": dataset_name,
            "version": version,
        }

        if filter_expression:
            params["filter_expression"] = filter_expression
        if columns:
            params["columns"] = ",".join(columns)
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

    def get_schema(self, dataset_name: str, version: str) -> dict:
        """
        Get the schema of a dataset without loading data.

        Args:
            dataset_name: Name of the dataset
            version: Version identifier

        Returns:
            Dict mapping column names to types

        Example:
            >>> schema = client.get_schema("users", "v1")
            >>> # {'user_id': 'BIGINT', 'name': 'VARCHAR', ...}
        """
        url = f"{self.base_url}/api/catalog/datasets/schema"
        params = {
            "dataset_name": dataset_name,
            "version": version,
        }

        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_statistics(self, dataset_name: str, version: str) -> dict:
        """
        Get statistics about a dataset (row count, file count, etc).

        Args:
            dataset_name: Name of the dataset
            version: Version identifier

        Returns:
            Dict with statistics including row_count, file_count, columns, schema

        Example:
            >>> stats = client.get_statistics("users", "v1")
            >>> print(f"Rows: {stats['row_count']}, Files: {stats['file_count']}")
        """
        url = f"{self.base_url}/api/catalog/datasets/statistics"
        params = {
            "dataset_name": dataset_name,
            "version": version,
        }

        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def list_datasets(self) -> List[dict]:
        """
        List all available datasets in the catalog.

        Returns:
            List of dicts with dataset information

        Example:
            >>> datasets = client.list_datasets()
            >>> for ds in datasets:
            ...     print(f"{ds['name']}: {ds['versions']}")
        """
        url = f"{self.base_url}/api/datasets"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def close(self):
        """Close the HTTP session."""
        self.session.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - closes session."""
        self.close()
