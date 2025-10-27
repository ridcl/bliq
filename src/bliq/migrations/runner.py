"""
Migration runner for Bliq using Peewee ORM.

This runner:
1. Creates a migrations tracking table
2. Automatically creates all tables from Peewee models
3. Tracks which migrations have been applied
4. Works with both SQLite and PostgreSQL
"""

import logging
from datetime import datetime
from typing import Optional

from peewee import CharField, DateTimeField, Model, PostgresqlDatabase, SqliteDatabase

logger = logging.getLogger(__name__)


class MigrationHistory(Model):
    """Track applied migrations."""

    migration_name = CharField(unique=True, max_length=255)
    applied_at = DateTimeField(default=datetime.now)

    class Meta:
        table_name = "migration_history"


class MigrationRunner:
    """
    Migration runner that uses Peewee ORM.

    This ensures schema consistency across SQLite and PostgreSQL
    without manual SQL maintenance.
    """

    def __init__(self, database_url: str):
        """
        Initialize migration runner.

        Args:
            database_url: Database connection string
                - "sqlite:///path/to/db.db" for SQLite
                - "postgresql://user:pass@host/db" for PostgreSQL
        """
        self.database_url = database_url
        self.db = self._create_database(database_url)

        # Bind migration tracking model to this database
        self.db.bind([MigrationHistory])

    def _create_database(self, database_url: str):
        """Create database instance from URL."""
        if database_url.startswith("sqlite://"):
            db_path = database_url.replace("sqlite:///", "")
            return SqliteDatabase(db_path)
        elif database_url.startswith("postgresql://"):
            from urllib.parse import urlparse

            parsed = urlparse(database_url)
            return PostgresqlDatabase(
                parsed.path[1:],  # Remove leading /
                user=parsed.username,
                password=parsed.password,
                host=parsed.hostname,
                port=parsed.port or 5432,
            )
        else:
            raise ValueError(f"Unsupported database URL: {database_url}")

    def _ensure_migration_table(self):
        """Create migration tracking table if it doesn't exist."""
        if not self.db.table_exists("migration_history"):
            self.db.create_tables([MigrationHistory])
            logger.info("Created migration_history table")

    def _is_migration_applied(self, migration_name: str) -> bool:
        """Check if a migration has been applied."""
        return (
            MigrationHistory.select()
            .where(MigrationHistory.migration_name == migration_name)
            .exists()
        )

    def _record_migration(self, migration_name: str):
        """Record that a migration has been applied."""
        MigrationHistory.create(migration_name=migration_name)
        logger.info(f"Recorded migration: {migration_name}")

    def migrate(self) -> None:
        """
        Run all pending migrations.

        This creates all tables defined in the Peewee models.
        """
        logger.info(f"Starting migrations on: {self.database_url}")

        # Ensure migration tracking table exists
        self._ensure_migration_table()

        # Migration: Create initial schema
        migration_name = "001_create_initial_schema"

        if self._is_migration_applied(migration_name):
            logger.info(f"Migration {migration_name} already applied, skipping")
        else:
            logger.info(f"Applying migration: {migration_name}")

            # Import models here to avoid circular imports
            from bliq.metastore import Block, Dataset, Version

            # Bind models to this database
            self.db.bind([Dataset, Version, Block])

            # Create all tables
            self.db.create_tables([Dataset, Version, Block], safe=True)

            # Record migration
            self._record_migration(migration_name)

            logger.info(f"✓ Applied migration: {migration_name}")

        logger.info("All migrations completed successfully")

    def status(self) -> dict:
        """
        Get migration status.

        Returns:
            dict with migration information
        """
        self._ensure_migration_table()

        applied_migrations = list(
            MigrationHistory.select().order_by(MigrationHistory.applied_at)
        )

        all_migrations = [
            "001_create_initial_schema",
            # Add future migrations here
        ]

        pending_migrations = [
            m
            for m in all_migrations
            if not any(am.migration_name == m for am in applied_migrations)
        ]

        return {
            "database_url": self.database_url,
            "applied": [
                {
                    "name": m.migration_name,
                    "applied_at": m.applied_at.isoformat(),
                }
                for m in applied_migrations
            ],
            "pending": pending_migrations,
            "total_applied": len(applied_migrations),
            "total_pending": len(pending_migrations),
        }

    def has_pending_migrations(self) -> bool:
        """Check if there are pending migrations."""
        status = self.status()
        return status["total_pending"] > 0

    def close(self):
        """Close database connection."""
        if not self.db.is_closed():
            self.db.close()

    @classmethod
    def from_url(cls, database_url: str) -> "MigrationRunner":
        """
        Create migration runner from database URL.

        Args:
            database_url: Database connection string

        Returns:
            MigrationRunner instance
        """
        return cls(database_url)
