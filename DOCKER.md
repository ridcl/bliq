# Docker Deployment Guide for Bliq

This guide covers how to build and run the Bliq server Docker image, which includes both the backend API and frontend web interface.

## Table of Contents

- [Building the Docker Image](#building-the-docker-image)
- [Running the Container](#running-the-container)
- [Configuration](#configuration)
- [Storage Options](#storage-options)
- [Examples](#examples)
- [Troubleshooting](#troubleshooting)

---

## Building the Docker Image

The Dockerfile (`Dockerfile.prod`) creates a multi-stage build that:
1. Builds the React frontend (Vite)
2. Packages the Python backend (FastAPI)
3. Creates a minimal production image with both components

### Build Command

```bash
docker build -f Dockerfile.prod -t bliq:latest .
```

**Build arguments:**
- None required - the build is fully self-contained

**Build time:**
- Expected: 3-5 minutes on modern hardware
- Downloads: ~500MB of dependencies
- Final image size: ~800MB

---

## Running the Container

### Basic Usage

```bash
docker run -d \
  --name bliq \
  -p 8000:8000 \
  -v bliq-data:/data \
  bliq:latest
```

This will:
- Run Bliq in detached mode
- Expose the web interface on http://localhost:8000
- Store all data in a Docker volume named `bliq-data`
- Use default SQLite storage

### Access the Application

- **Web Interface:** http://localhost:5173
- **API Documentation:** http://localhost:8000/docs
- **Health Check:** http://localhost:8000/health

---

## Configuration

Bliq accepts configuration via environment variables.

### Environment Variables

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `METASTORE_URL` | Database connection string for metadata | `sqlite:////data/metastore/bliq.db` | `postgresql://user:pass@host/db` |
| `DATASTORE_URL` | Storage location for dataset blocks | `file:///data/datastore` | `s3://bucket/path` |

### Using Custom Configuration

```bash
docker run -d \
  --name bliq \
  -p 8000:8000 \
  -e METASTORE_URL="postgresql://user:pass@postgres:5432/bliq" \
  -e DATASTORE_URL="s3://my-bucket/datasets" \
  -v bliq-data:/data \
  bliq:latest
```

### Using Environment File

Create an `.env` file:
```env
METASTORE_URL=postgresql://user:pass@postgres:5432/bliq
DATASTORE_URL=s3://my-bucket/datasets
```

Run with env file:
```bash
docker run -d \
  --name bliq \
  -p 8000:8000 \
  --env-file .env \
  -v bliq-data:/data \
  bliq:latest
```

---

## Storage Options

Bliq supports multiple storage backends for both metadata and data.

### MetaStore (Metadata Database)

**SQLite (Default - Development):**
```bash
METASTORE_URL="sqlite:////data/metastore/bliq.db"
```

**PostgreSQL (Recommended - Production):**
```bash
METASTORE_URL="postgresql://username:password@hostname:5432/database"
```

### DataStore (Dataset Blocks)

**Local Filesystem (Default):**
```bash
DATASTORE_URL="file:///data/datastore"
```

**S3-Compatible Storage:**
```bash
DATASTORE_URL="s3://bucket-name/path/to/datasets"
# Also set AWS credentials:
AWS_ACCESS_KEY_ID="your-access-key"
AWS_SECRET_ACCESS_KEY="your-secret-key"
AWS_REGION="us-east-1"
```

**Azure Blob Storage:**
```bash
DATASTORE_URL="azure://container-name/path"
# Also set Azure credentials:
AZURE_STORAGE_CONNECTION_STRING="your-connection-string"
```

---

## Examples

### Example 1: Development Setup (SQLite + Local Files)

```bash
docker run -d \
  --name bliq-dev \
  -p 8000:8000 \
  -v $(pwd)/data:/data \
  bliq:latest
```

### Example 2: Production Setup (PostgreSQL + S3)

```bash
docker run -d \
  --name bliq-prod \
  -p 8000:8000 \
  -e METASTORE_URL="postgresql://bliq:password@postgres:5432/bliq" \
  -e DATASTORE_URL="s3://my-company-datasets/bliq" \
  -e AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE" \
  -e AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -e AWS_REGION="us-west-2" \
  --restart unless-stopped \
  bliq:latest
```

### Example 3: With External PostgreSQL

```bash
# Start PostgreSQL
docker run -d \
  --name bliq-postgres \
  -e POSTGRES_USER=bliq \
  -e POSTGRES_PASSWORD=bliq \
  -e POSTGRES_DB=bliq \
  -v postgres-data:/var/lib/postgresql/data \
  postgres:15

# Start Bliq
docker run -d \
  --name bliq \
  -p 8000:8000 \
  --link bliq-postgres:postgres \
  -e METASTORE_URL="postgresql://bliq:bliq@postgres:5432/bliq" \
  -e DATASTORE_URL="file:///data/datastore" \
  -v bliq-data:/data \
  bliq:latest
```

### Example 4: Custom Port

```bash
docker run -d \
  --name bliq \
  -p 9000:8000 \
  -v bliq-data:/data \
  bliq:latest

# Access at http://localhost:9000
```

---

## Container Management

### View Logs

```bash
docker logs bliq
docker logs -f bliq  # Follow logs
```

### Stop Container

```bash
docker stop bliq
```

### Start Stopped Container

```bash
docker start bliq
```

### Restart Container

```bash
docker restart bliq
```

### Remove Container

```bash
docker stop bliq
docker rm bliq
```

### Execute Commands Inside Container

```bash
# Open shell
docker exec -it bliq bash

# Run CLI command
docker exec bliq bliq list

# Check migration status
docker exec bliq bliq migrate --status
```

---

## Data Persistence

### Using Docker Volumes (Recommended)

```bash
# Create volume
docker volume create bliq-data

# Run with volume
docker run -d \
  --name bliq \
  -p 8000:8000 \
  -v bliq-data:/data \
  bliq:latest

# Backup volume
docker run --rm \
  -v bliq-data:/data \
  -v $(pwd):/backup \
  ubuntu tar czf /backup/bliq-backup.tar.gz /data
```

### Using Bind Mounts

```bash
# Create local directory
mkdir -p ./bliq-data

# Run with bind mount
docker run -d \
  --name bliq \
  -p 8000:8000 \
  -v $(pwd)/bliq-data:/data \
  bliq:latest
```

---

## Health Checks

The container includes a health check that runs every 30 seconds:

```bash
# Check container health
docker ps

# View health check logs
docker inspect bliq | jq '.[0].State.Health'
```

---

## Troubleshooting

### Container Won't Start

```bash
# Check logs
docker logs bliq

# Common issues:
# 1. Port already in use
docker ps | grep 8000

# 2. Permission issues with volumes
docker exec bliq ls -la /data

# 3. Database connection failed
docker exec bliq env | grep METASTORE_URL
```

### Frontend Not Loading

```bash
# Verify frontend files exist
docker exec bliq ls -la /app/frontend/dist

# Check if API is responding
curl http://localhost:8000/health

# View server logs
docker logs -f bliq
```

### Database Migration Errors

```bash
# Check migration status
docker exec bliq bliq migrate --status

# Manually run migrations
docker exec bliq bliq migrate

# Reset database (WARNING: destroys data)
docker exec bliq rm /data/metastore/bliq.db
docker restart bliq
```

### Performance Issues

```bash
# Check resource usage
docker stats bliq

# Increase container resources
docker update --memory 4g --cpus 2 bliq
```

### Cannot Connect to External Database

```bash
# Test connection from inside container
docker exec bliq python -c "
from bliq.metastore import MetaStore
import os
url = os.getenv('METASTORE_URL')
print(f'Testing: {url}')
store = MetaStore(url)
print('✓ Connection successful')
"
```

---

## Building for Different Architectures

### Build for ARM64 (Apple Silicon, AWS Graviton)

```bash
docker buildx build \
  --platform linux/arm64 \
  -f Dockerfile.prod \
  -t bliq:latest-arm64 \
  .
```

### Multi-Architecture Build

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f Dockerfile.prod \
  -t myregistry/bliq:latest \
  --push \
  .
```

---

## Publishing to Registry

### Docker Hub

```bash
# Tag image
docker tag bliq:latest yourusername/bliq:latest
docker tag bliq:latest yourusername/bliq:0.1.0

# Push to Docker Hub
docker push yourusername/bliq:latest
docker push yourusername/bliq:0.1.0
```

### GitHub Container Registry

```bash
# Login
echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin

# Tag and push
docker tag bliq:latest ghcr.io/username/bliq:latest
docker push ghcr.io/username/bliq:latest
```

### AWS ECR

```bash
# Login
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin 123456789012.dkr.ecr.us-east-1.amazonaws.com

# Tag and push
docker tag bliq:latest 123456789012.dkr.ecr.us-east-1.amazonaws.com/bliq:latest
docker push 123456789012.dkr.ecr.us-east-1.amazonaws.com/bliq:latest
```

---

## Best Practices

1. **Use PostgreSQL in Production**
   - SQLite is great for development but not recommended for production
   - PostgreSQL provides better concurrency and reliability

2. **Use External Storage for Datasets**
   - S3/Azure Blob for cloud deployments
   - Provides better scalability and durability

3. **Always Use Volumes**
   - Never store data inside containers
   - Use Docker volumes or bind mounts

4. **Set Resource Limits**
   ```bash
   docker run -d \
     --name bliq \
     --memory 2g \
     --cpus 1 \
     -p 8000:8000 \
     bliq:latest
   ```

5. **Enable Automatic Restarts**
   ```bash
   docker run -d \
     --name bliq \
     --restart unless-stopped \
     -p 8000:8000 \
     bliq:latest
   ```

6. **Monitor Logs**
   - Set up log rotation
   - Use centralized logging (ELK, CloudWatch, etc.)

---

## Security Considerations

1. **Don't Run as Root**
   - Container already runs as non-root user `bliq` (UID 1000)

2. **Secure Database Credentials**
   - Use secrets management (Docker secrets, Kubernetes secrets)
   - Never hardcode passwords in images

3. **Network Security**
   - Use HTTPS in production (reverse proxy like nginx)
   - Restrict network access with `--network`

4. **Keep Image Updated**
   - Rebuild regularly to get security updates
   - Scan images for vulnerabilities

---

## Next Steps

After deploying the Docker image:

1. Test the deployment:
   ```bash
   curl http://localhost:8000/health
   ```

2. Create your first dataset:
   ```bash
   pip install bliq-client
   python -c "
   from bliq.client import BliqClient
   import pandas as pd

   client = BliqClient('http://localhost:8000')
   df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
   result = client.create('test/users', 'Test dataset', df)
   print(f'Created: {result}')
   "
   ```

3. Access the web interface:
   - Open http://localhost:8000 in your browser
   - Explore datasets, view schemas, query data

---

## Resources

- [Docker Documentation](https://docs.docker.com/)
- [FastAPI Deployment](https://fastapi.tiangolo.com/deployment/docker/)
- [PostgreSQL Docker](https://hub.docker.com/_/postgres)
- [S3 Storage Configuration](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html)
