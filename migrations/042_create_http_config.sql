-- Migration: Create http_config table for HTTP GET parameter read
-- Created: 2024

CREATE TABLE IF NOT EXISTS http_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_parameter INTEGER NOT NULL,
    url TEXT NOT NULL,
    method VARCHAR(10) DEFAULT 'GET',
    username VARCHAR(255),
    password VARCHAR(255),
    headers TEXT,
    timeout_ms INTEGER DEFAULT 5000,
    polling_ms INTEGER NOT NULL,
    field_mappings TEXT NOT NULL,
    description VARCHAR(255),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_parameter) REFERENCES parameters(id) ON DELETE CASCADE,
    UNIQUE(id_parameter)
);

CREATE INDEX IF NOT EXISTS idx_http_config_id_parameter ON http_config(id_parameter);
