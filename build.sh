#!/bin/bash
set -e

echo "========================================"
echo "Building Bliq Package"
echo "========================================"

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf dist/ build/ *.egg-info src/*.egg-info

# Build the package
echo "Building package..."
uv build

echo ""
echo "✓ Build complete!"
echo ""
echo "Built packages:"
ls -lh dist/

echo ""
echo "Package contents:"
echo "  Client: bliq.client (always included)"
echo "  Server: bliq.main, bliq.manager, etc. (included but optional deps)"
echo ""
echo "Installation options:"
echo "  pip install dist/bliq-*.whl              # Client only"
echo "  pip install dist/bliq-*.whl[server]      # Client + Server"
echo "  pip install dist/bliq-*.whl[all]         # Everything"
echo ""
echo "To test locally:"
echo "  # Client only"
echo "  uv pip install dist/bliq-*.whl"
echo "  python -c 'from bliq import BliqClient; print(\"Client OK\")"
echo ""
echo "  # With server"
echo "  uv pip install dist/bliq-*.whl[server]"
echo "  bliq serve"
echo ""
echo "To upload to TestPyPI:"
echo "  export UV_PUBLISH_TOKEN='your-test-pypi-token'"
echo "  uv publish --index https://test.pypi.org/legacy/"
echo ""
echo "To upload to PyPI:"
echo "  export UV_PUBLISH_TOKEN='your-pypi-token'"
echo "  uv publish"
