"""
CLI tool for Bliq dataset catalog management.

Provides commands for database migrations and other administrative tasks.
Server commands require installation with: pip install bliq[server]
"""

import os
import sys

import click

# Check if server dependencies are available
try:
    import fastapi
    import uvicorn

    SERVER_AVAILABLE = True
except ImportError:
    SERVER_AVAILABLE = False


@click.group()
def cli():
    """Bliq dataset catalog CLI."""
    pass


def require_server(func):
    """Decorator to check if server dependencies are installed."""

    def wrapper(*args, **kwargs):
        if not SERVER_AVAILABLE:
            click.echo("Error: Server dependencies not installed.", err=True)
            click.echo("Install with: pip install bliq[server]")
            sys.exit(1)
        return func(*args, **kwargs)

    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__
    return wrapper


@cli.command()
@click.option(
    "--connection-string",
    envvar="METASTORE_URL",
    default="sqlite:////data/bliq/metastore.db",
    help="Database connection string (or set METASTORE_URL env var)",
)
@require_server
def migrate(connection_string):
    """Run pending database migrations. (Requires: pip install bliq[server])"""
    from bliq.migrations.runner import MigrationRunner

    click.echo(f"Running migrations on: {connection_string}")

    # Ensure directory exists for SQLite
    if connection_string.startswith("sqlite"):
        db_path = connection_string.replace("sqlite:///", "")
        os.makedirs(os.path.dirname(os.path.abspath(db_path)) or ".", exist_ok=True)

    try:
        runner = MigrationRunner(connection_string)
        runner.migrate()
        click.echo("✓ All migrations applied successfully")
    except Exception as e:
        click.echo(f"✗ Migration failed: {e}", err=True)
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        runner.close()


@cli.command()
@click.option(
    "--connection-string",
    envvar="METASTORE_URL",
    default="sqlite:////data/bliq/metastore.db",
    help="Database connection string (or set METASTORE_URL env var)",
)
@require_server
def migration_status(connection_string):
    """Show migration status. (Requires: pip install bliq[server])"""
    from bliq.migrations.runner import MigrationRunner

    # Check if SQLite database exists
    if connection_string.startswith("sqlite"):
        db_path = connection_string.replace("sqlite:///", "")
        if not os.path.exists(db_path):
            click.echo(f"Database not found: {db_path}")
            click.echo("Run 'bliq migrate' to initialize.")
            return

    try:
        runner = MigrationRunner(connection_string)
        status = runner.status()

        click.echo(f"Database: {status['database_url']}")
        click.echo()
        click.echo(f"Applied migrations: {status['total_applied']}")
        for migration in status["applied"]:
            click.echo(f"  ✓ {migration['name']} (applied: {migration['applied_at']})")

        if status["pending"]:
            click.echo()
            click.echo(f"Pending migrations: {status['total_pending']}")
            for migration in status["pending"]:
                click.echo(f"  ○ {migration}")
        else:
            click.echo()
            click.echo("✓ No pending migrations")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        runner.close()


@cli.command()
@click.option(
    "--connection-string",
    envvar="METASTORE_URL",
    default="sqlite:////data/bliq/metastore.db",
    help="Database connection string (or set METASTORE_URL env var)",
)
@click.option("--namespace", default=None, help="Filter by namespace")
@require_server
def list_datasets(connection_string, namespace):
    """List datasets in the catalog. (Requires: pip install bliq[server])"""
    import os

    from bliq.datastore import LocalDataStore
    from bliq.manager import DatasetManager
    from bliq.metastore import MetaStore

    try:
        metastore = MetaStore(connection_string)
        datastore_url = os.getenv("DATASTORE_URL", "/data/bliq/datastore")
        datastore = LocalDataStore(datastore_url)
        manager = DatasetManager(metastore, datastore)

        datasets = manager.list(namespace=namespace)

        if not datasets:
            click.echo("No datasets found.")
            return

        click.echo(f"Found {len(datasets)} dataset(s):\n")
        for ds in datasets:
            click.echo(f"  {ds['name']}")
            click.echo(f"    Namespace: {ds['namespace']}")
            click.echo(f"    Dataset: {ds['dataset']}")
            click.echo(f"    Version: {ds['version']}")
            click.echo(f"    Rows: {ds['row_count']:,}")
            click.echo(f"    Created: {ds['created_at']}")
            click.echo()

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("name")  # Full qualified name with version (e.g., test/employees/v1)
@click.option(
    "--connection-string",
    envvar="METASTORE_URL",
    default="sqlite:////data/bliq/metastore.db",
    help="Database connection string (or set METASTORE_URL env var)",
)
@require_server
def show_dataset(connection_string, name):
    """
    Show detailed information about a dataset. (Requires: pip install bliq[server])

    NAME should be the full qualified name with version (e.g., test/employees/v1)
    """
    import os

    from bliq.datastore import LocalDataStore
    from bliq.manager import DatasetManager
    from bliq.metastore import MetaStore

    try:
        metastore = MetaStore(connection_string)
        datastore_url = os.getenv("DATASTORE_URL", "/data/bliq/datastore")
        datastore = LocalDataStore(datastore_url)
        manager = DatasetManager(metastore, datastore)

        # Use describe method
        description = manager.describe(name)
        click.echo(description)

    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        import traceback

        traceback.print_exc()
        sys.exit(1)


@cli.command()
@click.option("--host", default="0.0.0.0", help="Host to bind to")
@click.option("--port", default=8000, help="Port to bind to")
@click.option("--reload", is_flag=True, help="Enable auto-reload on code changes")
@require_server
def serve(host, port, reload):
    """
    Start the Bliq API server. (Requires: pip install bliq[server])

    The server serves the REST API at /api/v1/*.
    Frontend is only available in Docker deployments.

    Examples:
      bliq serve                    # Start on 0.0.0.0:8000
      bliq serve --port 9000        # Start on port 9000
      bliq serve --reload           # Development mode with auto-reload
    """
    import uvicorn

    # Run migrations first
    click.echo("Running migrations...")
    connection_string = os.getenv("METASTORE_URL", "sqlite:////data/bliq/metastore.db")

    # Ensure directory exists for SQLite
    if connection_string.startswith("sqlite"):
        db_path = connection_string.replace("sqlite:///", "")
        os.makedirs(os.path.dirname(os.path.abspath(db_path)) or ".", exist_ok=True)

    from bliq.migrations.runner import MigrationRunner

    try:
        runner = MigrationRunner(connection_string)
        runner.migrate()
        runner.close()
        click.echo("✓ Migrations complete")
    except Exception as e:
        click.echo(f"✗ Migration failed: {e}", err=True)
        sys.exit(1)

    # Start server
    click.echo(f"\nStarting Bliq server on http://{host}:{port}")
    click.echo(f"API docs available at: http://{host}:{port}/docs")
    click.echo("\nPress CTRL+C to stop\n")

    uvicorn.run("bliq.main:app", host=host, port=port, reload=reload, log_level="info")


if __name__ == "__main__":
    cli()
