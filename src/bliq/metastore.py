"""
Fresh MetaStore design using Peewee ORM.

This provides a clean, simple abstraction for tracking dataset metadata
without the complexity of manual SQL queries.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from peewee import (
    BigIntegerField,
    CharField,
    DateTimeField,
    ForeignKeyField,
    IntegerField,
    Model,
    PostgresqlDatabase,
    SqliteDatabase,
    TextField,
)

# ============================================================================
# Models
# ============================================================================


class BaseModel(Model):
    """Base model for all tables."""

    class Meta:
        pass  # database will be set dynamically


class Dataset(BaseModel):
    """Dataset - a named collection of versioned data."""

    namespace = CharField(index=True)  # e.g., "analytics"
    name = CharField(index=True)  # e.g., "users"
    description = TextField(null=True)
    created_at = DateTimeField(default=datetime.now)

    class Meta:
        indexes = ((("namespace", "name"), True),)  # Unique together


class Version(BaseModel):
    """Version - a specific snapshot of a dataset."""

    dataset = ForeignKeyField(Dataset, backref="versions", on_delete="CASCADE")
    version = CharField()  # e.g., "v1", "v2"
    description = TextField(null=True)
    row_count = IntegerField()
    file_count = IntegerField()
    size_bytes = BigIntegerField(null=True)
    schema_json = TextField(null=True)
    created_at = DateTimeField(default=datetime.now)

    class Meta:
        indexes = ((("dataset", "version"), True),)  # Unique together


class Block(BaseModel):
    """Block - a parquet file containing part of a version's data."""

    version = ForeignKeyField(Version, backref="blocks", on_delete="CASCADE")
    block_number = IntegerField()  # Order within version (0, 1, 2, ...)
    block_id = CharField()  # e.g., "block-abc123"
    size_bytes = BigIntegerField()
    row_count = IntegerField()
    created_at = DateTimeField(default=datetime.now)

    class Meta:
        indexes = ((("version", "block_number"), True),)  # Unique together


# ============================================================================
# Data Transfer Objects
# ============================================================================


@dataclass
class BlockInfo:
    """Information about a block to be added."""

    block_id: str
    size_bytes: int
    row_count: int


@dataclass
class VersionInfo:
    """Complete information about a version."""

    namespace: str
    dataset_name: str
    version: str
    description: Optional[str]
    row_count: int
    file_count: int
    size_bytes: int
    schema_json: Optional[str]
    block_ids: List[str]
    created_at: datetime


# ============================================================================
# MetaStore
# ============================================================================


class MetaStore:
    """
    Metadata store using Peewee ORM.

    Provides a clean interface for managing dataset/version/block metadata
    with automatic handling of SQLite vs PostgreSQL differences.
    """

    def __init__(self, database_url: str):
        """
        Initialize metastore.

        Args:
            database_url: Database connection string
                - "sqlite:///path/to/db.db" for SQLite
                - "postgresql://user:pass@host/db" for PostgreSQL
        """
        # Parse database URL and create appropriate database
        if database_url.startswith("sqlite://"):
            db_path = database_url.replace("sqlite:///", "")
            self.db = SqliteDatabase(db_path)
        elif database_url.startswith("postgresql://"):
            # Parse postgresql://user:pass@host:port/dbname
            from urllib.parse import urlparse

            parsed = urlparse(database_url)
            self.db = PostgresqlDatabase(
                parsed.path[1:],  # Remove leading /
                user=parsed.username,
                password=parsed.password,
                host=parsed.hostname,
                port=parsed.port or 5432,
            )
        else:
            raise ValueError(f"Unsupported database URL: {database_url}")

        # Bind models to database
        self.db.bind([Dataset, Version, Block])

        # Note: Tables are created via migrations, not here

    def create_dataset_with_version(
        self,
        namespace: str,
        name: str,
        version: str,
        description: Optional[str],
        blocks: List[BlockInfo],
        schema_json: Optional[str] = None,
    ) -> VersionInfo:
        """
        Create a new dataset with its first version.

        Args:
            namespace: Dataset namespace
            name: Dataset name
            version: Version identifier (e.g., "v1")
            description: Description of the dataset/version
            blocks: List of blocks to add
            schema_json: JSON string of schema

        Returns:
            VersionInfo with complete version details

        Raises:
            ValueError: If dataset already exists
        """
        with self.db.atomic():
            # Check if dataset exists
            existing = Dataset.get_or_none(
                (Dataset.namespace == namespace) & (Dataset.name == name)
            )
            if existing:
                raise ValueError(
                    f"Dataset {namespace}/{name} already exists. Use extend() to add data."
                )

            # Create dataset
            dataset = Dataset.create(
                namespace=namespace, name=name, description=description
            )

            # Calculate stats
            total_rows = sum(b.row_count for b in blocks)
            total_size = sum(b.size_bytes for b in blocks)

            # Create version
            version_obj = Version.create(
                dataset=dataset,
                version=version,
                description=description,
                row_count=total_rows,
                file_count=len(blocks),
                size_bytes=total_size,
                schema_json=schema_json,
            )

            # Create blocks
            for idx, block_info in enumerate(blocks):
                Block.create(
                    version=version_obj,
                    block_number=idx,
                    block_id=block_info.block_id,
                    size_bytes=block_info.size_bytes,
                    row_count=block_info.row_count,
                )

            return self._version_to_info(version_obj)

    def add_version(
        self,
        namespace: str,
        name: str,
        base_version: str,
        new_version: str,
        new_blocks: List[BlockInfo],
        description: Optional[str] = None,
    ) -> VersionInfo:
        """
        Create a new version by copying blocks from base version and adding new ones.

        Copy-on-write semantics: blocks are shared between versions.

        Args:
            namespace: Dataset namespace
            name: Dataset name
            base_version: Version to copy from (e.g., "v1")
            new_version: New version identifier (e.g., "v2")
            new_blocks: New blocks to add
            description: Description of the new version

        Returns:
            VersionInfo with complete version details

        Raises:
            ValueError: If base version doesn't exist
        """
        with self.db.atomic():
            # Get base version
            base_ver = self._get_version_obj(namespace, name, base_version)
            if not base_ver:
                raise ValueError(
                    f"Base version {namespace}/{name}/{base_version} not found"
                )

            # Get existing blocks
            existing_blocks = list(base_ver.blocks.order_by(Block.block_number))

            # Calculate new stats
            new_rows = sum(b.row_count for b in new_blocks)
            new_size = sum(b.size_bytes for b in new_blocks)
            total_rows = base_ver.row_count + new_rows
            total_size = base_ver.size_bytes + new_size
            total_files = len(existing_blocks) + len(new_blocks)

            # Create new version
            version_obj = Version.create(
                dataset=base_ver.dataset,
                version=new_version,
                description=description or f"Extended from {base_version}",
                row_count=total_rows,
                file_count=total_files,
                size_bytes=total_size,
                schema_json=base_ver.schema_json,
            )

            # Copy existing blocks (shared between versions!)
            for block in existing_blocks:
                Block.create(
                    version=version_obj,
                    block_number=block.block_number,
                    block_id=block.block_id,
                    size_bytes=block.size_bytes,
                    row_count=block.row_count,
                )

            # Add new blocks
            next_block_num = len(existing_blocks)
            for idx, block_info in enumerate(new_blocks):
                Block.create(
                    version=version_obj,
                    block_number=next_block_num + idx,
                    block_id=block_info.block_id,
                    size_bytes=block_info.size_bytes,
                    row_count=block_info.row_count,
                )

            return self._version_to_info(version_obj)

    def extend_version(
        self,
        namespace: str,
        name: str,
        version: str,
        new_blocks: List[BlockInfo],
    ) -> VersionInfo:
        """
        Extend an existing version by adding new blocks (mutable operation).

        Args:
            namespace: Dataset namespace
            name: Dataset name
            version: Version to extend
            new_blocks: Blocks to add

        Returns:
            Updated VersionInfo

        Raises:
            ValueError: If version doesn't exist
        """
        with self.db.atomic():
            # Get version
            version_obj = self._get_version_obj(namespace, name, version)
            if not version_obj:
                raise ValueError(f"Version {namespace}/{name}/{version} not found")

            # Get current block count
            current_blocks = list(version_obj.blocks)
            next_block_num = len(current_blocks)

            # Add new blocks
            for idx, block_info in enumerate(new_blocks):
                Block.create(
                    version=version_obj,
                    block_number=next_block_num + idx,
                    block_id=block_info.block_id,
                    size_bytes=block_info.size_bytes,
                    row_count=block_info.row_count,
                )

            # Update version stats
            new_rows = sum(b.row_count for b in new_blocks)
            new_size = sum(b.size_bytes for b in new_blocks)

            version_obj.row_count += new_rows
            version_obj.size_bytes += new_size
            version_obj.file_count += len(new_blocks)
            version_obj.save()

            return self._version_to_info(version_obj)

    def get_version(
        self, namespace: str, name: str, version: str
    ) -> Optional[VersionInfo]:
        """
        Get version information including block list.

        Args:
            namespace: Dataset namespace
            name: Dataset name
            version: Version identifier

        Returns:
            VersionInfo or None if not found
        """
        version_obj = self._get_version_obj(namespace, name, version)
        if not version_obj:
            return None
        return self._version_to_info(version_obj)

    def list_versions(self, namespace: str, name: str) -> List[str]:
        """
        List all version identifiers for a dataset.

        Args:
            namespace: Dataset namespace
            name: Dataset name

        Returns:
            List of version strings (e.g., ["v1", "v2", "v3"])
        """
        dataset = Dataset.get_or_none(
            (Dataset.namespace == namespace) & (Dataset.name == name)
        )
        if not dataset:
            return []

        versions = (
            Version.select(Version.version)
            .where(Version.dataset == dataset)
            .order_by(Version.created_at.desc())
        )

        return [v.version for v in versions]

    def delete_dataset(self, namespace: str, name: str) -> bool:
        """
        Delete entire dataset (all versions and blocks).

        Args:
            namespace: Dataset namespace
            name: Dataset name

        Returns:
            True if deleted, False if not found
        """
        dataset = Dataset.get_or_none(
            (Dataset.namespace == namespace) & (Dataset.name == name)
        )
        if not dataset:
            return False

        with self.db.atomic():
            # Cascade delete will handle versions and blocks
            dataset.delete_instance(recursive=True)
            return True

    def delete_version(self, namespace: str, name: str, version: str) -> bool:
        """
        Delete a specific version (blocks are cascade deleted).

        Note: Blocks may be shared with other versions. The datastore
        should check which blocks are still referenced before physical deletion.

        Args:
            namespace: Dataset namespace
            name: Dataset name
            version: Version identifier

        Returns:
            True if deleted, False if not found
        """
        version_obj = self._get_version_obj(namespace, name, version)
        if not version_obj:
            return False

        with self.db.atomic():
            version_obj.delete_instance(recursive=True)
            return True

    def get_all_block_ids_for_dataset(self, namespace: str, name: str) -> List[str]:
        """
        Get all unique block IDs across all versions of a dataset.

        Useful for determining which blocks can be safely deleted.

        Args:
            namespace: Dataset namespace
            name: Dataset name

        Returns:
            List of unique block IDs
        """
        dataset = Dataset.get_or_none(
            (Dataset.namespace == namespace) & (Dataset.name == name)
        )
        if not dataset:
            return []

        # Get distinct block IDs across all versions
        blocks = (
            Block.select(Block.block_id)
            .join(Version)
            .where(Version.dataset == dataset)
            .distinct()
        )

        return [b.block_id for b in blocks]

    # ========================================================================
    # Private helpers
    # ========================================================================

    def _get_version_obj(
        self, namespace: str, name: str, version: str
    ) -> Optional[Version]:
        """Get Version model object."""
        dataset = Dataset.get_or_none(
            (Dataset.namespace == namespace) & (Dataset.name == name)
        )
        if not dataset:
            return None

        return Version.get_or_none(
            (Version.dataset == dataset) & (Version.version == version)
        )

    def _version_to_info(self, version_obj: Version) -> VersionInfo:
        """Convert Version model to VersionInfo DTO."""
        blocks = list(version_obj.blocks.order_by(Block.block_number))
        block_ids = [b.block_id for b in blocks]

        return VersionInfo(
            namespace=version_obj.dataset.namespace,
            dataset_name=version_obj.dataset.name,
            version=version_obj.version,
            description=version_obj.description,
            row_count=version_obj.row_count,
            file_count=version_obj.file_count,
            size_bytes=version_obj.size_bytes,
            schema_json=version_obj.schema_json,
            block_ids=block_ids,
            created_at=version_obj.created_at,
        )

    def close(self):
        """Close database connection."""
        if not self.db.is_closed():
            self.db.close()


# Convenience factory function for backward compatibility
def create_metadata_store(database_url: Optional[str] = None) -> MetaStore:
    """
    Create a MetaStore instance.

    Args:
        database_url: Database connection string. If not provided,
                     uses METASTORE_URL environment variable or defaults
                     to "sqlite:////data/bliq/metastore.db"

    Returns:
        MetaStore instance

    Example:
        >>> store = create_metadata_store()  # Uses env var or default
        >>> store = create_metadata_store("sqlite:///./my_db.db")
    """
    import os

    if database_url is None:
        database_url = os.getenv("METASTORE_URL", "sqlite:////data/bliq/metastore.db")

    return MetaStore(database_url)
