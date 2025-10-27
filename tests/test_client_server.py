"""
Test client-server communication with Arrow data format.

This script tests the full end-to-end workflow:
1. Start a temporary FastAPI server with temp database and storage
2. Use BliqClient to create, extend, load, describe, and erase datasets
3. Verify data integrity
4. Clean up temporary resources
"""

import pandas as pd
import tempfile
import time
import subprocess
import sys
import os
import shutil

from bliq.client import BliqClient


def start_test_server(db_path: str, storage_path: str, port: int = 8765):
    """
    Start a test FastAPI server with temporary database and storage.

    Returns:
        subprocess.Popen: The server process
    """
    env = os.environ.copy()
    env['METASTORE_URL'] = f'sqlite:///{db_path}'
    env['DATASTORE_URL'] = storage_path

    # Start server in background
    process = subprocess.Popen(
        [sys.executable, '-m', 'uvicorn', 'bliq.main:app', '--host', '127.0.0.1', '--port', str(port)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # Combine stderr with stdout
        text=True,
    )

    # Wait for server to start
    print(f"Starting test server on port {port}...")
    time.sleep(3)

    # Check if server started successfully
    if process.poll() is not None:
        output, _ = process.communicate()
        print(f"Server failed to start:\n{output}")
        raise RuntimeError("Server failed to start")

    return process


def test_client_server():
    """Test client-server communication with Arrow format."""

    print("=" * 70)
    print("Testing Client-Server Communication with BliqClient")
    print("=" * 70)

    # Create temporary directories
    temp_dir = tempfile.mkdtemp(prefix='bliq_test_')
    db_path = os.path.join(temp_dir, 'test_metadata.db')
    storage_path = os.path.join(temp_dir, 'test_storage')
    os.makedirs(storage_path, exist_ok=True)

    print(f"\nTemp directory: {temp_dir}")
    print(f"Database: {db_path}")
    print(f"Storage: {storage_path}")

    server_process = None

    try:
        # Start test server
        server_process = start_test_server(db_path, storage_path, port=8765)

        # Create client
        client = BliqClient("http://localhost:8765")

        # Create test DataFrame
        df = pd.DataFrame({
            'user_id': list(range(1, 101)),
            'name': [f'User{i}' for i in range(1, 101)],
            'age': [(20 + i % 50) for i in range(1, 101)],
            'score': [(i * 3.14) % 100 for i in range(1, 101)],
            'active': [i % 2 == 0 for i in range(1, 101)],
        })

        print(f"\nTest DataFrame: {len(df)} rows, {len(df.columns)} columns")
        print(df.head(3))
        print("...")

        # Test 1: Create dataset
        print("\n--- Test 1: Create Dataset ---")
        try:
            result = client.create('test/users', 'Test user data', df)
            print(f"✓ Created dataset: {result}")
            assert result == 'test/users/v1', f"Expected 'test/users/v1', got '{result}'"
        except Exception as e:
            print(f"✗ Failed to create: {e}")
            return False

        # Test 2: Describe dataset
        print("\n--- Test 2: Describe Dataset ---")
        try:
            description = client.describe('test/users/v1')
            print(f"✓ Description received:")
            print(description[:200] + "...")
            assert 'test/users' in description
            assert 'v1' in description
            assert '100' in description  # row count
        except Exception as e:
            print(f"✗ Failed to describe: {e}")
            return False

        # Test 3: List datasets
        print("\n--- Test 3: List Datasets ---")
        try:
            datasets = client.list()
            print(f"✓ Found {len(datasets)} dataset(s)")
            assert len(datasets) == 1
            assert datasets[0]['name'] == 'test/users/v1'
            assert datasets[0]['row_count'] == 100
        except Exception as e:
            print(f"✗ Failed to list: {e}")
            return False

        # Test 4: Load full dataset
        print("\n--- Test 4: Load Full Dataset ---")
        try:
            loaded = client.load('test/users/v1')
            print(f"✓ Loaded {len(loaded)} rows")

            # Compare data (allowing for minor type differences)
            assert len(loaded) == len(df), f"Row count mismatch: {len(loaded)} vs {len(df)}"
            assert list(loaded.columns) == list(df.columns), "Column mismatch"
            assert (loaded['user_id'] == df['user_id']).all(), "user_id mismatch"
            assert (loaded['name'] == df['name']).all(), "name mismatch"
            print("✓ Data integrity verified!")
        except Exception as e:
            print(f"✗ Failed to load: {e}")
            return False

        # Test 5: Load with filter
        print("\n--- Test 5: Load with Filter ---")
        try:
            filtered = client.load(
                'test/users/v1',
                filter='age > 30 AND active = true'
            )
            print(f"✓ Loaded {len(filtered)} rows with filter")
            print(f"  Age range: {filtered['age'].min()} - {filtered['age'].max()}")
            print(f"  All active: {filtered['active'].all()}")
            assert len(filtered) > 0
            assert filtered['age'].min() > 30
            assert filtered['active'].all()
        except Exception as e:
            print(f"✗ Failed to filter: {e}")
            return False

        # Test 6: Load with column selection and limit
        print("\n--- Test 6: Load with Column Selection ---")
        try:
            subset = client.load(
                'test/users/v1',
                columns=['name', 'score'],
                limit=10
            )
            print(f"✓ Loaded {len(subset)} rows with {len(subset.columns)} columns")
            print(f"  Columns: {list(subset.columns)}")
            assert len(subset) == 10
            assert list(subset.columns) == ['name', 'score']
        except Exception as e:
            print(f"✗ Failed column selection: {e}")
            return False

        # Test 7: Extend dataset with new version
        print("\n--- Test 7: Extend Dataset (New Version) ---")
        try:
            df_extra = pd.DataFrame({
                'user_id': [101, 102, 103],
                'name': ['User101', 'User102', 'User103'],
                'age': [25, 30, 35],
                'score': [50.0, 60.0, 70.0],
                'active': [True, False, True],
            })

            result = client.extend('test/users/v1', df_extra, create_new_version=True)
            print(f"✓ Extended dataset: {result}")
            assert result == 'test/users/v2', f"Expected 'test/users/v2', got '{result}'"

            # Verify v2 has more rows
            loaded_v2 = client.load('test/users/v2')
            print(f"  v2 has {len(loaded_v2)} rows (v1 had 100)")
            assert len(loaded_v2) == 103
        except Exception as e:
            print(f"✗ Failed to extend: {e}")
            return False

        # Test 8: Extend existing version (mutable)
        print("\n--- Test 8: Extend Existing Version (Mutable) ---")
        try:
            df_more = pd.DataFrame({
                'user_id': [104, 105],
                'name': ['User104', 'User105'],
                'age': [40, 45],
                'score': [80.0, 90.0],
                'active': [True, True],
            })

            result = client.extend('test/users/v2', df_more, create_new_version=False)
            print(f"✓ Extended existing version: {result}")
            assert result == 'test/users/v2'

            # Verify v2 now has even more rows
            loaded_v2 = client.load('test/users/v2')
            print(f"  v2 now has {len(loaded_v2)} rows")
            assert len(loaded_v2) == 105
        except Exception as e:
            print(f"✗ Failed to extend existing: {e}")
            return False

        # Test 9: List with namespace filter
        print("\n--- Test 9: List with Namespace Filter ---")
        try:
            test_datasets = client.list(namespace='test')
            print(f"✓ Found {len(test_datasets)} dataset(s) in 'test' namespace")
            assert len(test_datasets) == 2  # v1 and v2

            other_datasets = client.list(namespace='other')
            assert len(other_datasets) == 0
            print("✓ Namespace filtering works")
        except Exception as e:
            print(f"✗ Failed namespace filter: {e}")
            return False

        # Test 10: Erase single version
        print("\n--- Test 10: Erase Single Version ---")
        try:
            client.erase('test/users/v1')
            print("✓ Erased test/users/v1")

            # Verify v1 is gone but v2 still exists
            datasets = client.list()
            assert len(datasets) == 1
            assert datasets[0]['name'] == 'test/users/v2'
            print("✓ v1 removed, v2 still exists")
        except Exception as e:
            print(f"✗ Failed to erase version: {e}")
            return False

        # Test 11: Erase entire dataset
        print("\n--- Test 11: Erase Entire Dataset ---")
        try:
            client.erase('test/users')
            print("✓ Erased test/users (all versions)")

            # Verify dataset is completely gone
            datasets = client.list()
            assert len(datasets) == 0
            print("✓ Dataset completely removed")
        except Exception as e:
            print(f"✗ Failed to erase dataset: {e}")
            return False

        print("\n" + "=" * 70)
        print("All tests passed! ✓")
        print("=" * 70)

        client.close()
        return True

    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        # Clean up server
        if server_process:
            print("\nStopping test server...")
            server_process.terminate()
            try:
                server_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server_process.kill()

        # Clean up temporary directory
        print(f"Cleaning up {temp_dir}...")
        try:
            shutil.rmtree(temp_dir)
        except Exception as e:
            print(f"Warning: Failed to clean up temp dir: {e}")


if __name__ == "__main__":
    success = test_client_server()
    sys.exit(0 if success else 1)
