-- Migration 029: Create set_point table
-- Set-point adalah state yang bisa diubah user, tidak terhubung langsung ke device.

CREATE TABLE IF NOT EXISTS set_point (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL,
    unit VARCHAR(20),
    value REAL NOT NULL,
    data_type VARCHAR(10) NOT NULL CHECK (data_type IN ('int', 'float', 'bool')),
    min_value REAL,
    max_value REAL,
    random_enabled INTEGER DEFAULT 0,
    random_step REAL,
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_set_point_name ON set_point(name);

