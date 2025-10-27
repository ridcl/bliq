"""
Bliq - Dataset catalog with versioned storage.

This package can be installed in two modes:
- pip install bliq          -> Client library only
- pip install bliq[server]  -> Client + Server components

Basic usage (client):
    from bliq import BliqClient

    client = BliqClient("http://localhost:8000")
    df = client.load("team/dataset/v1")

Server usage:
    bliq serve --port 8000
"""

__version__ = "0.1.0"

# Always export client (minimal dependencies)
from bliq.client import BliqClient

__all__ = ["BliqClient", "__version__"]

# Server components are available but not exported by default
# They can be imported explicitly: from bliq.manager import DatasetManager
