-- Initial schema for dataset catalog metadata
-- This migration creates the core tables for tracking datasets, versions, and blocks

-- Datasets table: Core dataset information
CREATE TABLE datasets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    namespace VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(namespace, name)
);

-- Index for faster namespace queries
CREATE INDEX idx_datasets_namespace ON datasets(namespace);

-- Dataset versions table: Track versions for each dataset
CREATE TABLE dataset_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id INTEGER NOT NULL,
    version VARCHAR(100) NOT NULL,
    description TEXT,
    row_count BIGINT,
    file_count INTEGER,
    size_bytes BIGINT,
    schema_json TEXT,
    created_by VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
    UNIQUE(dataset_id, version)
);

-- Index for faster version lookups
CREATE INDEX idx_dataset_versions_dataset_id ON dataset_versions(dataset_id);

-- Dataset blocks table: Track individual parquet file blocks
CREATE TABLE dataset_blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    relative_url VARCHAR(500) NOT NULL,
    size_bytes BIGINT,
    row_count BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (version_id) REFERENCES dataset_versions(id) ON DELETE CASCADE,
    UNIQUE(version_id, block_number)
);

-- Index for faster block lookups
CREATE INDEX idx_dataset_blocks_version_id ON dataset_blocks(version_id);
