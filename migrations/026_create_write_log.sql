-- Migration: Create Write Log table
-- Database: aoi-write-log.db (separate database for write action logs)
-- This migration should be run on the log database, not the main database

-- Table: write_log
-- Log semua write action ke parameter (dari manual, workflow, atau API)
CREATE TABLE IF NOT EXISTS write_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parameter_id INTEGER NOT NULL,
    parameter_nama VARCHAR(100),
    device_id INTEGER NOT NULL,
    device_nama VARCHAR(100),
    write_mode VARCHAR(20) NOT NULL, -- TEST, MANUAL, PRESET, TRIGGER
    value REAL, -- Scalar value yang di-write
    register_values TEXT, -- JSON array of register values
    success BOOLEAN NOT NULL,
    message TEXT,
    error TEXT,
    read_back_value REAL,
    workflow_id INTEGER, -- Jika write dari workflow
    workflow_nama VARCHAR(100),
    triggered_by VARCHAR(50) NOT NULL, -- "MANUAL", "WORKFLOW", "API"
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_write_log_parameter_id ON write_log(parameter_id);
CREATE INDEX IF NOT EXISTS idx_write_log_device_id ON write_log(device_id);
CREATE INDEX IF NOT EXISTS idx_write_log_workflow_id ON write_log(workflow_id);
CREATE INDEX IF NOT EXISTS idx_write_log_created_at ON write_log(created_at);
CREATE INDEX IF NOT EXISTS idx_write_log_success ON write_log(success);
CREATE INDEX IF NOT EXISTS idx_write_log_triggered_by ON write_log(triggered_by);

