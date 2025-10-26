"""
Test client-server communication with Arrow data format.

This script tests the full end-to-end workflow:
1. Start the FastAPI server
2. Use the client to save datasets
3. Use the client to load datasets
4. Verify data integrity
"""

import pandas as pd
import tempfile
import time
import subprocess
import sys
from pathlib import Path

from bliq.client import DatasetCatalogClient


def test_client_server():
    """Test client-server communication with Arrow format."""

    print("=" * 70)
    print("Testing Client-Server Communication with Arrow Format")
    print("=" * 70)

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

    # Create client
    client = DatasetCatalogClient("http://localhost:8000")

    # Test 1: Save dataset
    print("\n--- Test 1: Save Dataset ---")
    try:
        result = client.save(df, "test-users", "v1")
        print(f"✓ Saved dataset: {result}")
    except Exception as e:
        print(f"✗ Failed to save: {e}")
        return False

    # Test 2: Load full dataset
    print("\n--- Test 2: Load Full Dataset ---")
    try:
        loaded = client.load("test-users", "v1")
        print(f"✓ Loaded {len(loaded)} rows")

        if df.equals(loaded):
            print("✓ Data integrity verified!")
        else:
            print("✗ Data mismatch!")
            print(f"Original shape: {df.shape}, Loaded shape: {loaded.shape}")
            return False
    except Exception as e:
        print(f"✗ Failed to load: {e}")
        return False

    # Test 3: Load with filter
    print("\n--- Test 3: Load with Filter ---")
    try:
        filtered = client.load(
            "test-users", "v1",
            filter_expression="age > 30 AND active = true"
        )
        print(f"✓ Loaded {len(filtered)} rows with filter")
        print(f"  Age range: {filtered['age'].min()} - {filtered['age'].max()}")
        print(f"  All active: {filtered['active'].all()}")
    except Exception as e:
        print(f"✗ Failed to filter: {e}")
        return False

    # Test 4: Load with column selection
    print("\n--- Test 4: Load with Column Selection ---")
    try:
        subset = client.load(
            "test-users", "v1",
            columns=["name", "score"],
            limit=10
        )
        print(f"✓ Loaded {len(subset)} rows with {len(subset.columns)} columns")
        print(f"  Columns: {list(subset.columns)}")
    except Exception as e:
        print(f"✗ Failed column selection: {e}")
        return False

    # Test 5: Get schema
    print("\n--- Test 5: Get Schema ---")
    try:
        schema = client.get_schema("test-users", "v1")
        print(f"✓ Schema retrieved: {schema}")
    except Exception as e:
        print(f"✗ Failed to get schema: {e}")
        return False

    # Test 6: Get statistics
    print("\n--- Test 6: Get Statistics ---")
    try:
        stats = client.get_statistics("test-users", "v1")
        print(f"✓ Statistics:")
        print(f"  Row count: {stats['row_count']}")
        print(f"  File count: {stats['file_count']}")
        print(f"  Columns: {len(stats['columns'])}")
    except Exception as e:
        print(f"✗ Failed to get statistics: {e}")
        return False

    # Test 7: Try to save duplicate version (should fail)
    print("\n--- Test 7: Duplicate Version Rejection ---")
    try:
        client.save(df, "test-users", "v1")
        print("✗ Should have failed with 409 error")
        return False
    except Exception as e:
        if "409" in str(e):
            print(f"✓ Correctly rejected duplicate: {e}")
        else:
            print(f"✗ Unexpected error: {e}")
            return False

    # Test 8: Save new version
    print("\n--- Test 8: Save New Version ---")
    try:
        df_v2 = df.copy()
        df_v2['score'] = df_v2['score'] * 2
        result = client.save(df_v2, "test-users", "v2")
        print(f"✓ Saved v2: {result}")

        # Verify v1 and v2 are different
        loaded_v1 = client.load("test-users", "v1")
        loaded_v2 = client.load("test-users", "v2")

        if loaded_v1['score'].sum() * 2 == loaded_v2['score'].sum():
            print("✓ Versions are independent")
        else:
            print("✗ Version data mismatch")
            return False
    except Exception as e:
        print(f"✗ Failed to save v2: {e}")
        return False

    print("\n" + "=" * 70)
    print("All tests passed! ✓")
    print("=" * 70)

    client.close()
    return True


if __name__ == "__main__":
    print("\n⚠ Note: This test requires the FastAPI server to be running.")
    print("Start the server with: uv run uvicorn main:app --reload")
    print("\nWaiting 3 seconds before starting tests...")
    time.sleep(3)

    success = test_client_server()
    sys.exit(0 if success else 1)
