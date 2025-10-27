"""
Minimal DataStore interface focused on block-level operations.

This module provides a clean abstraction for reading/writing parquet blocks
to different storage backends (local disk, Azure Blob Storage, etc.).
"""

import io
import os
from abc import ABC, abstractmethod
from typing import List, Optional

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


def _build_duckdb_query(
    block_paths: List[str],
    columns: Optional[List[str]] = None,
    filter_expression: Optional[str] = None,
    limit: Optional[int] = None,
) -> str:
    # Build query
    if len(block_paths) == 1:
        from_clause = f"read_parquet('{block_paths[0]}')"
    else:
        # Multiple blocks - use list of files
        paths_str = "', '".join(block_paths)
        from_clause = f"read_parquet(['{paths_str}'])"

    # Build SELECT clause
    select_clause = ", ".join(columns) if columns else "*"
    query = f"SELECT {select_clause} FROM {from_clause}"

    # Add filter
    if filter_expression:
        query += f" WHERE {filter_expression}"

    # Add limit
    if limit:
        query += f" LIMIT {limit}"
    return query


class DataStore(ABC):
    """Abstract interface for block-level storage operations."""

    @abstractmethod
    def write_block(
        self,
        table: pa.Table,
        namespace: str,
        dataset_name: str,
        block_id: str,
    ) -> int:
        """
        Write a single parquet block.

        Args:
            table: PyArrow table to write
            namespace: Dataset namespace (e.g., "analytics")
            dataset_name: Dataset name (e.g., "users")
            block_id: Unique block identifier (e.g., "block-abc123")

        Returns:
            Size of written block in bytes
        """
        pass

    @abstractmethod
    def delete_block(self, namespace: str, dataset_name: str, block_id: str) -> None:
        """
        Delete a single block.

        Args:
            namespace: Dataset namespace
            dataset_name: Dataset name
            block_id: Block identifier to delete
        """
        pass

    @abstractmethod
    def read_blocks(
        self,
        namespace: str,
        dataset_name: str,
        block_ids: List[str],
        filter_expression: Optional[str] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> pa.Table:
        """
        Read data from one or more blocks using DuckDB.

        Args:
            namespace: Dataset namespace
            dataset_name: Dataset name
            block_ids: List of block IDs to read
            filter_expression: Optional SQL WHERE clause (without WHERE keyword)
            columns: Optional list of columns to select
            limit: Optional row limit

        Returns:
            PyArrow table with query results
        """
        pass


class LocalDataStore(DataStore):
    """Local filesystem storage backend."""

    def __init__(self, base_path: str):
        """
        Initialize local datastore.

        Args:
            base_path: Base directory for storing datasets
                      (e.g., "/data/bliq/datastore")
        """
        self.base_path = os.path.abspath(base_path)

    def _build_block_path(
        self, namespace: str, dataset_name: str, block_id: str
    ) -> str:
        """Build full path to a block file."""
        return (
            os.path.join(self.base_path, namespace, dataset_name, block_id) + ".parquet"
        )

    def write_block(
        self,
        table: pa.Table,
        namespace: str,
        dataset_name: str,
        block_id: str,
    ) -> int:
        """Write block to local filesystem."""
        block_path = self._build_block_path(namespace, dataset_name, block_id)

        # Create directory if needed
        os.makedirs(os.path.dirname(block_path), exist_ok=True)

        # Write parquet file
        pq.write_table(table, block_path, compression="snappy")

        # Return file size
        return os.path.getsize(block_path)

    def delete_block(self, namespace: str, dataset_name: str, block_id: str) -> None:
        """Delete block from local filesystem."""
        block_path = self._build_block_path(namespace, dataset_name, block_id)

        if os.path.exists(block_path):
            os.remove(block_path)

    def read_blocks(
        self,
        namespace: str,
        dataset_name: str,
        block_ids: List[str],
        filter_expression: Optional[str] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> pa.Table:
        """Read from blocks using DuckDB."""
        block_paths = [
            self._build_block_path(namespace, dataset_name, block_id)
            for block_id in block_ids
        ]
        conn = duckdb.connect(":memory:")
        try:
            query = _build_duckdb_query(
                block_paths,
                columns=columns,
                filter_expression=filter_expression,
                limit=limit,
            )
            return conn.execute(query).fetch_arrow_table()
        finally:
            conn.close()


class AzureDataStore(DataStore):
    """Azure Blob Storage backend."""

    def __init__(
        self,
        container_name: str,
        connection_string: Optional[str] = None,
        account_name: Optional[str] = None,
    ):
        """
        Initialize Azure datastore.

        Args:
            container_name: Azure Blob container name (e.g., "datasets")
            connection_string: Azure Storage connection string (optional)
            account_name: Azure Storage account name (optional, uses DefaultAzureCredential)

        Note:
            Provide either connection_string OR account_name.
            If neither is provided, will check AZURE_STORAGE_CONNECTION_STRING
            and AZURE_STORAGE_ACCOUNT_NAME environment variables.
        """
        self.container_name = container_name
        self.connection_string = connection_string or os.getenv(
            "AZURE_STORAGE_CONNECTION_STRING"
        )
        self.account_name = account_name or os.getenv("AZURE_STORAGE_ACCOUNT_NAME")

        if not self.connection_string and not self.account_name:
            raise ValueError(
                "Either connection_string or account_name must be provided, "
                "or set AZURE_STORAGE_CONNECTION_STRING/AZURE_STORAGE_ACCOUNT_NAME env variable"
            )

    def _build_blob_path(self, namespace: str, dataset_name: str, block_id: str) -> str:
        """Build blob path within container."""
        return f"{namespace}/{dataset_name}/{block_id}.parquet"

    def _get_blob_client(self, blob_path: str):
        """Get blob client for a specific blob."""
        from azure.storage.blob import BlobServiceClient

        if self.connection_string:
            blob_service_client = BlobServiceClient.from_connection_string(
                self.connection_string
            )
        else:
            from azure.identity import DefaultAzureCredential

            account_url = f"https://{self.account_name}.blob.core.windows.net"
            credential = DefaultAzureCredential()
            blob_service_client = BlobServiceClient(
                account_url=account_url, credential=credential
            )

        return blob_service_client.get_blob_client(
            container=self.container_name, blob=blob_path
        )

    def write_block(
        self,
        table: pa.Table,
        namespace: str,
        dataset_name: str,
        block_id: str,
    ) -> int:
        """Write block to Azure Blob Storage."""
        blob_path = self._build_blob_path(namespace, dataset_name, block_id)

        # Serialize table to parquet bytes
        parquet_buffer = io.BytesIO()
        pq.write_table(table, parquet_buffer, compression="snappy")
        parquet_bytes = parquet_buffer.getvalue()

        # Upload to Azure
        blob_client = self._get_blob_client(blob_path)
        blob_client.upload_blob(parquet_bytes, overwrite=False)

        return len(parquet_bytes)

    def delete_block(self, namespace: str, dataset_name: str, block_id: str) -> None:
        """Delete block from Azure Blob Storage."""
        blob_path = self._build_blob_path(namespace, dataset_name, block_id)

        blob_client = self._get_blob_client(blob_path)
        blob_client.delete_blob()

    def read_blocks(
        self,
        namespace: str,
        dataset_name: str,
        block_ids: List[str],
        filter_expression: Optional[str] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> pa.Table:
        """Read from blocks using DuckDB with Azure extension."""
        # Build blob paths
        blob_paths = [
            f"az://{self.container_name}/{self._build_blob_path(namespace, dataset_name, block_id)}"
            for block_id in block_ids
        ]

        # Create DuckDB connection with Azure extension
        conn = duckdb.connect(":memory:")

        try:
            # Install and load Azure extension
            conn.execute("INSTALL azure")
            conn.execute("LOAD azure")

            # Configure Azure credentials
            if self.connection_string:
                conn.execute(
                    f"SET azure_storage_connection_string='{self.connection_string}'"
                )
            elif self.account_name:
                conn.execute(f"SET azure_account_name='{self.account_name}'")

            query = _build_duckdb_query(
                blob_paths,
                columns=columns,
                filter_expression=filter_expression,
                limit=limit,
            )
            return conn.execute(query).fetch_arrow_table()

        finally:
            conn.close()
