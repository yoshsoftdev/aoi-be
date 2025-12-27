-- Migration: Create Threshold table
-- Threshold/Alarm configuration for parameters

-- Table: threshold
-- One threshold configuration per parameter
CREATE TABLE IF NOT EXISTS threshold (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_parameter INTEGER NOT NULL UNIQUE, -- One threshold per parameter
    unit VARCHAR(20), -- Unit for display (V, A, etc.)
    threshold_type VARCHAR(10) DEFAULT 'max', -- "max" or "min"
    min_value REAL NOT NULL, -- Minimum normal value
    max_value REAL NOT NULL, -- Maximum normal value
    critical_low REAL NOT NULL, -- Critical low threshold
    critical_high REAL NOT NULL, -- Critical high threshold
    warning_low REAL NOT NULL, -- Warning low threshold
    warning_high REAL NOT NULL, -- Warning high threshold
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_parameter) REFERENCES parameters(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_threshold_id_parameter ON threshold(id_parameter);

