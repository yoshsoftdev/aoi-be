-- Migration: Create parameter_preset table
-- This table stores preset values for write operations

CREATE TABLE IF NOT EXISTS parameter_preset (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parameter_id INTEGER NOT NULL,
    name VARCHAR(50) NOT NULL, -- START / STOP / ON / OFF / etc
    value_type VARCHAR(20) NOT NULL, -- scalar | bitmask | raw_register
    value TEXT NOT NULL, -- JSON format
    description VARCHAR(255),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (parameter_id) REFERENCES parameters(id) ON DELETE CASCADE,
    UNIQUE(parameter_id, name)
);

CREATE INDEX IF NOT EXISTS idx_parameter_preset_parameter_id ON parameter_preset(parameter_id);
CREATE INDEX IF NOT EXISTS idx_parameter_preset_name ON parameter_preset(name);

