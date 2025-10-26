"""
CLI tool for Bliq dataset catalog management.

Provides commands for database migrations and other administrative tasks.
"""

import os
import sys
import click


@click.group()
def cli():
    """Bliq dataset catalog CLI."""
    pass


@cli.command()
@click.option(
    '--connection-string',
    envvar='METADATA_DB_URL',
    default='sqlite:///./data/metadata.db',
    help='Database connection string (or set METADATA_DB_URL env var)'
)
def migrate(connection_string):
    """Run pending database migrations."""
    from bliq.migrations.runner import MigrationRunner

    click.echo(f"Running migrations on: {connection_string}")

    # Parse connection string to get dialect and connection
    if connection_string.startswith("sqlite"):
        import sqlite3
        db_path = connection_string.replace("sqlite:///", "")

        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

        conn = sqlite3.connect(db_path)
        dialect = "sqlite"
    elif connection_string.startswith("postgresql"):
        try:
            import psycopg2
        except ImportError:
            click.echo("Error: psycopg2 not installed. Install with: pip install psycopg2-binary", err=True)
            sys.exit(1)

        conn = psycopg2.connect(connection_string)
        dialect = "postgresql"
    else:
        click.echo(f"Error: Unsupported connection string: {connection_string}", err=True)
        sys.exit(1)

    try:
        runner = MigrationRunner(conn, dialect)
        runner.migrate()
        click.echo("✓ All migrations applied successfully")
    except Exception as e:
        click.echo(f"✗ Migration failed: {e}", err=True)
        sys.exit(1)
    finally:
        conn.close()


@cli.command()
@click.option(
    '--connection-string',
    envvar='METADATA_DB_URL',
    default='sqlite:///./data/metadata.db',
    help='Database connection string (or set METADATA_DB_URL env var)'
)
def migration_status(connection_string):
    """Show migration status."""
    from bliq.migrations.runner import MigrationRunner

    # Parse connection string to get dialect and connection
    if connection_string.startswith("sqlite"):
        import sqlite3
        db_path = connection_string.replace("sqlite:///", "")

        if not os.path.exists(db_path):
            click.echo(f"Database not found: {db_path}")
            click.echo("Run 'bliq migrate' to initialize.")
            return

        conn = sqlite3.connect(db_path)
        dialect = "sqlite"
    elif connection_string.startswith("postgresql"):
        try:
            import psycopg2
        except ImportError:
            click.echo("Error: psycopg2 not installed. Install with: pip install psycopg2-binary", err=True)
            sys.exit(1)

        conn = psycopg2.connect(connection_string)
        dialect = "postgresql"
    else:
        click.echo(f"Error: Unsupported connection string: {connection_string}", err=True)
        sys.exit(1)

    try:
        runner = MigrationRunner(conn, dialect)
        click.echo(f"Database: {connection_string}")
        click.echo(f"Dialect: {dialect}")
        click.echo()
        runner.status()
    finally:
        conn.close()


@cli.command()
@click.option(
    '--connection-string',
    envvar='METADATA_DB_URL',
    default='sqlite:///./data/metadata.db',
    help='Database connection string (or set METADATA_DB_URL env var)'
)
@click.option('--namespace', default=None, help='Filter by namespace')
def list_datasets(connection_string, namespace):
    """List datasets in the catalog."""
    from bliq.metastore import create_metadata_store

    try:
        store = create_metadata_store(connection_string)
        datasets = store.list_datasets(namespace=namespace)

        if not datasets:
            click.echo("No datasets found.")
            return

        click.echo(f"Found {len(datasets)} dataset(s):\n")
        for ds in datasets:
            click.echo(f"  {ds.namespace}/{ds.name}")
            if ds.description:
                click.echo(f"    Description: {ds.description}")
            click.echo(f"    Created: {ds.created_at}")
            click.echo()

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument('namespace')
@click.argument('dataset_name')
@click.option(
    '--connection-string',
    envvar='METADATA_DB_URL',
    default='sqlite:///./data/metadata.db',
    help='Database connection string (or set METADATA_DB_URL env var)'
)
def show_dataset(connection_string, namespace, dataset_name):
    """Show detailed information about a dataset."""
    from bliq.metastore import create_metadata_store

    try:
        store = create_metadata_store(connection_string)
        dataset = store.get_dataset(namespace, dataset_name)

        if not dataset:
            click.echo(f"Dataset not found: {namespace}/{dataset_name}", err=True)
            sys.exit(1)

        click.echo(f"Dataset: {dataset.namespace}/{dataset.name}")
        click.echo(f"ID: {dataset.id}")
        if dataset.description:
            click.echo(f"Description: {dataset.description}")
        click.echo(f"Created: {dataset.created_at}")
        click.echo(f"Updated: {dataset.updated_at}")
        click.echo()

        # List versions
        versions = store.list_versions(dataset.id)
        click.echo(f"Versions ({len(versions)}):")
        for v in versions:
            click.echo(f"  {v.version}")
            if v.description:
                click.echo(f"    Description: {v.description}")
            click.echo(f"    Rows: {v.row_count}, Files: {v.file_count}")
            click.echo(f"    Created: {v.created_at}")
            if v.created_by:
                click.echo(f"    Created by: {v.created_by}")
            click.echo()

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    cli()
