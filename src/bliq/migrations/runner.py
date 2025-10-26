"""
Simple SQL-file based migration runner.

Tracks applied migrations in a schema_migrations table and applies
pending migrations in order.
"""

import os
from pathlib import Path
from typing import Protocol, List, Tuple
import logging

logger = logging.getLogger(__name__)


class DBConnection(Protocol):
    """Protocol for database connections."""

    def execute(self, query: str, params=None):
        """Execute a query."""
        ...

    def commit(self):
        """Commit transaction."""
        ...

    def close(self):
        """Close connection."""
        ...


class MigrationRunner:
    """Simple SQL-file based migration runner."""

    def __init__(self, connection, dialect: str = "sqlite"):
        """
        Initialize migration runner.

        Args:
            connection: Database connection (sqlite3 or psycopg2)
            dialect: "sqlite" or "postgresql"
        """
        self.connection = connection
        self.dialect = dialect
        self.migrations_dir = Path(__file__).parent / "versions"

    def _init_migrations_table(self):
        """Create migrations tracking table if it doesn't exist."""
        if self.dialect == "sqlite":
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )
        else:  # postgresql
            cursor = self.connection.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )
        self.connection.commit()

    def _get_current_version(self) -> int:
        """Get the current schema version."""
        try:
            if self.dialect == "sqlite":
                cursor = self.connection.execute(
                    "SELECT MAX(version) FROM schema_migrations"
                )
            else:
                cursor = self.connection.cursor()
                cursor.execute("SELECT MAX(version) FROM schema_migrations")

            result = cursor.fetchone()
            return result[0] if result and result[0] is not None else 0
        except Exception:
            # Table doesn't exist yet
            return 0

    def _get_pending_migrations(self) -> List[Tuple[int, str, Path]]:
        """
        Get list of pending migrations.

        Returns:
            List of (version, filename, filepath) tuples
        """
        current_version = self._get_current_version()

        migrations = []
        for filepath in sorted(self.migrations_dir.glob("*.sql")):
            # Parse version from filename: 001_initial_schema.sql
            filename = filepath.name
            version_str = filename.split("_")[0]

            try:
                version = int(version_str)
                if version > current_version:
                    migrations.append((version, filename, filepath))
            except ValueError:
                logger.warning(f"Skipping invalid migration file: {filename}")

        return sorted(migrations, key=lambda x: x[0])

    def _apply_migration(self, version: int, name: str, filepath: Path):
        """Apply a single migration."""
        logger.info(f"Applying migration {version}: {name}")

        # Read SQL file
        sql = filepath.read_text()

        # Split by semicolons and execute each statement
        statements = [s.strip() for s in sql.split(";") if s.strip()]

        for statement in statements:
            # Skip dialect-specific statements
            if self._should_skip_statement(statement):
                continue

            # Adapt statement for dialect
            statement = self._adapt_statement(statement)

            if self.dialect == "sqlite":
                self.connection.execute(statement)
            else:
                cursor = self.connection.cursor()
                cursor.execute(statement)

        # Record migration
        if self.dialect == "sqlite":
            self.connection.execute(
                "INSERT INTO schema_migrations (version, name) VALUES (?, ?)",
                (version, name),
            )
        else:
            cursor = self.connection.cursor()
            cursor.execute(
                "INSERT INTO schema_migrations (version, name) VALUES (%s, %s)",
                (version, name),
            )

        self.connection.commit()
        logger.info(f"Successfully applied migration {version}")

    def _should_skip_statement(self, statement: str) -> bool:
        """Check if statement should be skipped based on dialect."""
        upper = statement.upper()

        # Skip empty or comment-only statements
        if not statement or statement.startswith("--"):
            return True

        return False

    def _adapt_statement(self, statement: str) -> str:
        """Adapt SQL statement for the current dialect."""
        if self.dialect == "postgresql":
            # Replace SQLite AUTOINCREMENT with PostgreSQL SERIAL
            statement = statement.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
            statement = statement.replace("AUTOINCREMENT", "")
        elif self.dialect == "sqlite":
            # Replace PostgreSQL SERIAL with SQLite AUTOINCREMENT
            statement = statement.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
            statement = statement.replace("BIGSERIAL", "INTEGER")

        return statement

    def migrate(self, target_version: int = None):
        """
        Run pending migrations up to target version.

        Args:
            target_version: Target version to migrate to (None = latest)
        """
        self._init_migrations_table()

        pending = self._get_pending_migrations()

        if not pending:
            logger.info("No pending migrations")
            return

        for version, name, filepath in pending:
            if target_version and version > target_version:
                break

            try:
                self._apply_migration(version, name, filepath)
            except Exception as e:
                logger.error(f"Migration {version} failed: {e}")
                raise

        logger.info("All migrations applied successfully")

    def current_version(self) -> int:
        """Get current schema version."""
        self._init_migrations_table()
        return self._get_current_version()

    def status(self):
        """Print migration status."""
        self._init_migrations_table()
        current = self._get_current_version()
        pending = self._get_pending_migrations()

        print(f"Current version: {current}")
        print(f"Pending migrations: {len(pending)}")

        if pending:
            print("\nPending:")
            for version, name, _ in pending:
                print(f"  {version}: {name}")
        else:
            print("\nAll migrations applied ✓")
