"""
DatasetManager - High-level interface for dataset operations.

Coordinates between metadata store and data store to provide a unified API.
"""

import uuid
import json
import random
import string
from datetime import datetime, timedelta
from typing import Optional, Tuple
from pyarrow import Table
import pyarrow as pa

from bliq.metastore import MetaStore, BlockInfo
from bliq.datastore import DataStore


def create_test_table(num_rows: int = 100, seed: Optional[int] = None) -> Table:
    """
    Create a test Arrow Table with randomized data.

    Useful for testing and examples. Generates a table with various column types:
    - id: Sequential integers
    - name: Random strings
    - age: Random integers (18-80)
    - salary: Random floats (30000-150000)
    - is_active: Random booleans
    - department: Random categories (Engineering, Sales, Marketing, HR)
    - join_date: Random dates within the last 5 years
    - score: Random floats (0-100)

    Args:
        num_rows: Number of rows to generate (default: 100)
        seed: Random seed for reproducibility (optional)

    Returns:
        PyArrow Table with randomized data

    Example:
        >>> table = create_test_table(50)
        >>> print(f"Generated {len(table)} rows")
        Generated 50 rows
    """
    if seed is not None:
        random.seed(seed)

    # Generate data
    ids = list(range(1, num_rows + 1))

    names = [
        "".join(random.choices(string.ascii_uppercase, k=1))
        + "".join(random.choices(string.ascii_lowercase, k=random.randint(4, 9)))
        for _ in range(num_rows)
    ]

    ages = [random.randint(18, 80) for _ in range(num_rows)]

    salaries = [round(random.uniform(30000, 150000), 2) for _ in range(num_rows)]

    is_active = [random.choice([True, False]) for _ in range(num_rows)]

    departments = [
        random.choice(["Engineering", "Sales", "Marketing", "HR"])
        for _ in range(num_rows)
    ]

    # Generate random dates within last 5 years
    base_date = datetime.now() - timedelta(days=5 * 365)
    join_dates = [
        base_date + timedelta(days=random.randint(0, 5 * 365)) for _ in range(num_rows)
    ]

    scores = [round(random.uniform(0, 100), 2) for _ in range(num_rows)]

    # Create Arrow arrays
    data = {
        "id": pa.array(ids, type=pa.int64()),
        "name": pa.array(names, type=pa.string()),
        "age": pa.array(ages, type=pa.int32()),
        "salary": pa.array(salaries, type=pa.float64()),
        "is_active": pa.array(is_active, type=pa.bool_()),
        "department": pa.array(departments, type=pa.string()),
        "join_date": pa.array(join_dates, type=pa.timestamp("ms")),
        "score": pa.array(scores, type=pa.float64()),
    }

    # Create and return Arrow Table
    return pa.table(data)


class DatasetManager:
    """
    High-level dataset manager that coordinates metadata and data storage.

    Uses the new simplified DataStore and MetaStore designs with Peewee ORM.

    Storage Layout:
        namespace/dataset_name/block-{uuid}.parquet

    Blocks are shared between versions (copy-on-write semantics).
    """

    def __init__(self, metastore: MetaStore, datastore: DataStore):
        """
        Initialize DatasetManager.

        Args:
            metastore: MetaStore instance (from metastore2.py)
            datastore: DataStore instance (from datastore2.py)
        """
        self.metastore = metastore
        self.datastore = datastore

    def _parse_name(self, name: str) -> Tuple[str, str]:
        """
        Parse qualified name without version.

        Args:
            name: "namespace/dataset_name"

        Returns:
            (namespace, dataset_name)

        Raises:
            ValueError: If name format is invalid
        """
        parts = name.split("/")
        if len(parts) != 2:
            raise ValueError(
                f"Invalid name format: '{name}'. Expected 'namespace/dataset_name'"
            )
        return parts[0], parts[1]

    def _parse_name_with_version(self, name: str) -> Tuple[str, str, str]:
        """
        Parse qualified name with version.

        Args:
            name: "namespace/dataset_name/version"

        Returns:
            (namespace, dataset_name, version)

        Raises:
            ValueError: If name format is invalid
        """
        parts = name.split("/")
        if len(parts) != 3:
            raise ValueError(
                f"Invalid name format: '{name}'. Expected 'namespace/dataset_name/version'"
            )
        return parts[0], parts[1], parts[2]

    def _generate_block_id(self) -> str:
        """Generate unique block ID (without .parquet extension)."""
        return f"block-{uuid.uuid4().hex[:12]}"

    def _calculate_next_version(self, existing_versions: list[str]) -> str:
        """
        Calculate next version number.

        Args:
            existing_versions: List of version strings (e.g., ["v1", "v2"])

        Returns:
            Next version string (e.g., "v3")
        """
        if not existing_versions:
            return "v1"

        # Extract version numbers (assumes format "v1", "v2", etc.)
        version_numbers = []
        for v in existing_versions:
            try:
                num = int(v.replace("v", ""))
                version_numbers.append(num)
            except ValueError:
                pass

        if version_numbers:
            return f"v{max(version_numbers) + 1}"
        return "v1"

    def create(self, name: str, description: str, data: Table) -> str:
        """
        Create a new dataset.

        Args:
            name: Qualified name of the dataset w/o version, e.g. my-team/documents
            description: Dataset description
            data: Arrow table with actual data

        Returns:
            Qualified name with version, e.g. my-team/documents/v1

        Raises:
            ValueError: If dataset already exists

        Example:
            >>> manager.create("analytics/events", "User events", table)
            'analytics/events/v1'
        """
        # Parse name
        namespace, dataset_name = self._parse_name(name)

        # Generate block ID and write block to storage
        block_id = self._generate_block_id()
        size_bytes = self.datastore.write_block(
            table=data,
            namespace=namespace,
            dataset_name=dataset_name,
            block_id=block_id,
        )

        # Get schema from Arrow table
        schema = {field.name: str(field.type) for field in data.schema}
        schema_json = json.dumps(schema)

        # Create dataset with version in metastore
        version_info = self.metastore.create_dataset_with_version(
            namespace=namespace,
            name=dataset_name,
            version="v1",
            description=description,
            blocks=[
                BlockInfo(block_id=block_id, size_bytes=size_bytes, row_count=len(data))
            ],
            schema_json=schema_json,
        )

        return f"{namespace}/{dataset_name}/v1"

    def extend(self, name: str, data: Table, create_new_version: bool = True) -> str:
        """
        Extend existing dataset with new data.

        Args:
            name: Qualified name of the dataset, e.g. my-team/documents/v1
            data: Arrow table with actual data
            create_new_version: If True, creates a new version (default).
                Otherwise, adds data to the existing dataset version.

        Returns:
            Qualified name with version

        Raises:
            ValueError: If source version doesn't exist

        Example:
            >>> # Create new version with additional data
            >>> manager.extend("analytics/events/v1", new_data, create_new_version=True)
            'analytics/events/v2'

            >>> # Add data to existing version
            >>> manager.extend("analytics/events/v1", new_data, create_new_version=False)
            'analytics/events/v1'
        """
        # Parse name
        namespace, dataset_name, source_version = self._parse_name_with_version(name)

        # Generate block ID and write block to storage
        block_id = self._generate_block_id()
        size_bytes = self.datastore.write_block(
            table=data,
            namespace=namespace,
            dataset_name=dataset_name,
            block_id=block_id,
        )

        # Create BlockInfo
        new_block = BlockInfo(
            block_id=block_id, size_bytes=size_bytes, row_count=len(data)
        )

        if create_new_version:
            # Create new version (copy-on-write)
            all_versions = self.metastore.list_versions(namespace, dataset_name)
            next_version = self._calculate_next_version(all_versions)

            version_info = self.metastore.add_version(
                namespace=namespace,
                name=dataset_name,
                base_version=source_version,
                new_version=next_version,
                new_blocks=[new_block],
                description=f"Extended from {source_version}",
            )

            return f"{namespace}/{dataset_name}/{next_version}"
        else:
            # Extend existing version (mutable)
            version_info = self.metastore.extend_version(
                namespace=namespace,
                name=dataset_name,
                version=source_version,
                new_blocks=[new_block],
            )

            return f"{namespace}/{dataset_name}/{source_version}"

    def load(
        self,
        name: str,
        columns: Optional[list[str]] = None,
        filter: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Table:
        """
        Load dataset.

        Args:
            name: Qualified name of the dataset, e.g. my-team/documents/v1
            columns: List of columns to return
            filter: Filter expression (SQL WHERE clause without WHERE keyword)
            limit: Max number of rows to return

        Returns:
            Arrow table with actual data

        Raises:
            ValueError: If dataset version doesn't exist

        Example:
            >>> table = manager.load("analytics/events/v1", columns=["user_id", "event_type"])
            >>> table = manager.load("analytics/events/v1", filter="event_type = 'click'", limit=1000)
        """
        # Parse name
        namespace, dataset_name, version = self._parse_name_with_version(name)

        # Get version metadata
        version_info = self.metastore.get_version(namespace, dataset_name, version)
        if not version_info:
            raise ValueError(f"Dataset {name} not found")

        if not version_info.block_ids:
            raise ValueError(f"Dataset {name} has no data blocks")

        # Load data from blocks using DataStore
        table = self.datastore.read_blocks(
            namespace=namespace,
            dataset_name=dataset_name,
            block_ids=version_info.block_ids,
            filter_expression=filter,
            columns=columns,
            limit=limit,
        )

        return table

    def erase(self, name: str) -> None:
        """
        Completely erase a dataset or its version, including data and metadata.

        Args:
            name: Qualified name, either:
                  - "namespace/dataset_name" - erases entire dataset
                  - "namespace/dataset_name/version" - erases specific version

        Raises:
            ValueError: If dataset or version doesn't exist

        Example:
            >>> manager.erase("analytics/events")  # Erase entire dataset
            >>> manager.erase("analytics/events/v1")  # Erase only v1
        """
        parts = name.split("/")

        if len(parts) == 2:
            # Erase entire dataset
            namespace, dataset_name = parts

            # Get all unique block IDs across all versions
            all_block_ids = self.metastore.get_all_block_ids_for_dataset(
                namespace, dataset_name
            )

            # Delete all physical block files
            for block_id in all_block_ids:
                try:
                    self.datastore.delete_block(namespace, dataset_name, block_id)
                except Exception:
                    # Block might not exist physically, continue
                    pass

            # Delete metadata (CASCADE will delete versions and blocks)
            deleted = self.metastore.delete_dataset(namespace, dataset_name)
            if not deleted:
                raise ValueError(f"Dataset {name} not found")

        elif len(parts) == 3:
            # Erase specific version
            namespace, dataset_name, version = parts

            # Get version info
            version_info = self.metastore.get_version(namespace, dataset_name, version)
            if not version_info:
                raise ValueError(f"Version {name} not found")

            # Get blocks for this version
            version_block_ids = set(version_info.block_ids)

            # Get all block IDs across all versions
            all_block_ids = self.metastore.get_all_block_ids_for_dataset(
                namespace, dataset_name
            )
            all_block_ids_set = set(all_block_ids)

            # Delete version metadata first
            self.metastore.delete_version(namespace, dataset_name, version)

            # Get remaining block IDs after deletion
            remaining_block_ids = set(
                self.metastore.get_all_block_ids_for_dataset(namespace, dataset_name)
            )

            # Delete only non-shared blocks (blocks not used by other versions)
            blocks_to_delete = version_block_ids - remaining_block_ids

            for block_id in blocks_to_delete:
                try:
                    self.datastore.delete_block(namespace, dataset_name, block_id)
                except Exception:
                    # Block might not exist physically, continue
                    pass

        else:
            raise ValueError(
                f"Invalid name format: '{name}'. Expected 'namespace/dataset' or 'namespace/dataset/version'"
            )

    def describe(self, name: str) -> str:
        """
        Describe the dataset.

        The result consists of:
        * dataset description, as written in the metadata
        * column specs
        * dataset statistics, such as row number

        Args:
            name: Qualified name of the dataset, e.g. my-team/documents/v1

        Returns:
            Human-readable description

        Raises:
            ValueError: If dataset version doesn't exist

        Example:
            >>> print(manager.describe("analytics/events/v1"))
            Dataset: analytics/events
            Version: v1
            Description: User event logs
            ...
        """
        # Parse name
        namespace, dataset_name, version = self._parse_name_with_version(name)

        # Get version metadata
        version_info = self.metastore.get_version(namespace, dataset_name, version)
        if not version_info:
            raise ValueError(f"Version {name} not found")

        # Parse schema
        schema = {}
        if version_info.schema_json:
            try:
                schema = json.loads(version_info.schema_json)
            except json.JSONDecodeError:
                pass

        # Format output
        lines = [
            f"Dataset: {namespace}/{dataset_name}",
            f"Version: {version}",
            f"Description: {version_info.description or 'N/A'}",
            "",
            "Statistics:",
            f"  Rows: {version_info.row_count:,}",
            f"  Blocks: {version_info.file_count}",
        ]

        if version_info.size_bytes:
            size_mb = version_info.size_bytes / (1024 * 1024)
            lines.append(f"  Size: {size_mb:.2f} MB")

        if version_info.created_at:
            lines.append(f"  Created at: {version_info.created_at}")

        if schema:
            lines.extend(["", "Schema:"])
            for col_name, col_type in schema.items():
                lines.append(f"  {col_name}: {col_type}")

        lines.extend(["", "Blocks:"])
        for idx, block_id in enumerate(version_info.block_ids):
            lines.append(f"  [{idx}] {block_id}.parquet")

        return "\n".join(lines)
