-- Migration 030: Create bypass table

CREATE TABLE IF NOT EXISTS bypass (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT 1,
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_bypass_enabled ON bypass(enabled);

